package sync

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"

	glob "github.com/bmatcuk/doublestar/v4"
	"github.com/containers/image/v5/docker"
	"github.com/containers/image/v5/docker/reference"
	"github.com/containers/image/v5/oci/layout"
	"github.com/containers/image/v5/types"
	guuid "github.com/gofrs/uuid"
	"github.com/notaryproject/notation-go-lib"
	notreg "github.com/notaryproject/notation/pkg/registry"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	"gopkg.in/resty.v1"
	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/common"
	"zotregistry.io/zot/pkg/extensions/monitoring"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"
	"zotregistry.io/zot/pkg/test"
)

type ReferenceList struct {
	References []notation.Descriptor `json:"references"`
}

// getTagFromRef returns a tagged reference from an image reference.
func getTagFromRef(ref types.ImageReference, log log.Logger) reference.Tagged {
	tagged, isTagged := ref.DockerReference().(reference.Tagged)
	if !isTagged {
		log.Warn().Msgf("internal server error, reference %s does not have a tag, skipping", ref.DockerReference())
	}

	return tagged
}

// getRepoFromRef returns repo name from a registry ImageReference.
func getRepoFromRef(ref types.ImageReference, registryDomain string) string {
	imageName := strings.Replace(ref.DockerReference().Name(), registryDomain, "", 1)
	imageName = strings.TrimPrefix(imageName, "/")

	return imageName
}

// parseRepositoryReference parses input into a reference.Named, and verifies that it names a repository, not an image.
func parseRepositoryReference(input string) (reference.Named, error) {
	ref, err := reference.ParseNormalizedNamed(input)
	if err != nil {
		return nil, err
	}

	if !reference.IsNameOnly(ref) {
		return nil, zerr.ErrInvalidRepositoryName
	}

	return ref, nil
}

// filterRepos filters repos based on prefix given in the config.
func filterRepos(repos []string, contentList []Content, log log.Logger) map[int][]string {
	filtered := make(map[int][]string)

	for _, repo := range repos {
		for contentID, content := range contentList {
			var prefix string
			// handle prefixes starting with '/'
			if strings.HasPrefix(content.Prefix, "/") {
				prefix = content.Prefix[1:]
			} else {
				prefix = content.Prefix
			}

			matched, err := glob.Match(prefix, repo)
			if err != nil {
				log.Error().Err(err).Str("pattern",
					prefix).Msg("error while parsing glob pattern, skipping it...")

				continue
			}

			if matched {
				filtered[contentID] = append(filtered[contentID], repo)

				break
			}
		}
	}

	return filtered
}

// Get sync.FileCredentials from file.
func getFileCredentials(filepath string) (CredentialsFile, error) {
	credsFile, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	var creds CredentialsFile

	err = json.Unmarshal(credsFile, &creds)
	if err != nil {
		return nil, err
	}

	return creds, nil
}

func getHTTPClient(regCfg *RegistryConfig, upstreamURL string, credentials Credentials,
	log log.Logger) (*resty.Client, *url.URL, error) {
	client := resty.New()

	if !common.Contains(regCfg.URLs, upstreamURL) {
		return nil, nil, zerr.ErrSyncInvalidUpstreamURL
	}

	registryURL, err := url.Parse(upstreamURL)
	if err != nil {
		log.Error().Err(err).Str("url", upstreamURL).Msg("couldn't parse url")

		return nil, nil, err
	}

	if regCfg.CertDir != "" {
		log.Debug().Msgf("sync: using certs directory: %s", regCfg.CertDir)
		clientCert := path.Join(regCfg.CertDir, "client.cert")
		clientKey := path.Join(regCfg.CertDir, "client.key")
		caCertPath := path.Join(regCfg.CertDir, "ca.crt")

		caCert, err := ioutil.ReadFile(caCertPath)
		if err != nil {
			log.Error().Err(err).Msg("couldn't read CA certificate")

			return nil, nil, err
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		client.SetTLSClientConfig(&tls.Config{RootCAs: caCertPool, MinVersion: tls.VersionTLS12})

		cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			log.Error().Err(err).Msg("couldn't read certificates key pairs")

			return nil, nil, err
		}

		client.SetCertificates(cert)
	}

	// nolint: gosec
	if regCfg.TLSVerify != nil && !*regCfg.TLSVerify && registryURL.Scheme == "https" {
		client.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	}

	if credentials.Username != "" && credentials.Password != "" {
		log.Debug().Msgf("sync: using basic auth")
		client.SetBasicAuth(credentials.Username, credentials.Password)
	}

	return client, registryURL, nil
}

func pushSyncedLocalImage(repo, tag, localCachePath string,
	storeController storage.StoreController, log log.Logger) error {
	log.Info().Msgf("pushing synced local image %s/%s:%s to local registry", localCachePath, repo, tag)

	imageStore := storeController.GetImageStore(repo)

	metrics := monitoring.NewMetricsServer(false, log)
	cacheImageStore := storage.NewImageStore(localCachePath, false, storage.DefaultGCDelay, false, false, log, metrics)

	manifestContent, _, _, err := cacheImageStore.GetImageManifest(repo, tag)
	if err != nil {
		log.Error().Err(err).Str("dir", path.Join(cacheImageStore.RootDir(), repo)).Msg("couldn't find index.json")

		return err
	}

	var manifest ispec.Manifest

	if err := json.Unmarshal(manifestContent, &manifest); err != nil {
		log.Error().Err(err).Str("dir", path.Join(cacheImageStore.RootDir(), repo)).Msg("invalid JSON")

		return err
	}

	for _, blob := range manifest.Layers {
		blobReader, _, err := cacheImageStore.GetBlob(repo, blob.Digest.String(), blob.MediaType)
		if err != nil {
			log.Error().Err(err).Str("dir", path.Join(cacheImageStore.RootDir(),
				repo)).Str("blob digest", blob.Digest.String()).Msg("couldn't read blob")

			return err
		}

		_, _, err = imageStore.FullBlobUpload(repo, blobReader, blob.Digest.String())
		if err != nil {
			log.Error().Err(err).Str("blob digest", blob.Digest.String()).Msg("couldn't upload blob")

			return err
		}
	}

	blobReader, _, err := cacheImageStore.GetBlob(repo, manifest.Config.Digest.String(), manifest.Config.MediaType)
	if err != nil {
		log.Error().Err(err).Str("dir", path.Join(cacheImageStore.RootDir(),
			repo)).Str("blob digest", manifest.Config.Digest.String()).Msg("couldn't read config blob")

		return err
	}

	_, _, err = imageStore.FullBlobUpload(repo, blobReader, manifest.Config.Digest.String())
	if err != nil {
		log.Error().Err(err).Str("blob digest", manifest.Config.Digest.String()).Msg("couldn't upload config blob")

		return err
	}

	_, err = imageStore.PutImageManifest(repo, tag, ispec.MediaTypeImageManifest, manifestContent)
	if err != nil {
		log.Error().Err(err).Msg("couldn't upload manifest")

		return err
	}

	log.Info().Msgf("removing temporary cached synced repo %s", path.Join(cacheImageStore.RootDir(), repo))

	if err := os.RemoveAll(cacheImageStore.RootDir()); err != nil {
		log.Error().Err(err).Msg("couldn't remove locally cached sync repo")

		return err
	}

	return nil
}

// sync needs transport to be stripped to not be wrongly interpreted as an image reference
// at a non-fully qualified registry (hostname as image and port as tag).
func StripRegistryTransport(url string) string {
	return strings.Replace(strings.Replace(url, "http://", "", 1), "https://", "", 1)
}

// get an ImageReference given the registry, repo and tag.
func getImageRef(registryDomain, repo, tag string) (types.ImageReference, error) {
	repoRef, err := parseRepositoryReference(fmt.Sprintf("%s/%s", registryDomain, repo))
	if err != nil {
		return nil, err
	}

	taggedRepoRef, err := reference.WithTag(repoRef, tag)
	if err != nil {
		return nil, err
	}

	imageRef, err := docker.NewReference(taggedRepoRef)
	if err != nil {
		return nil, err
	}

	return imageRef, err
}

// get a local ImageReference used to temporary store one synced image.
func getLocalImageRef(imageStore storage.ImageStore, repo, tag string) (types.ImageReference, string, error) {
	uuid, err := guuid.NewV4()
	// hard to reach test case, injected error, see pkg/test/dev.go
	if err := test.Error(err); err != nil {
		return nil, "", err
	}

	localCachePath := path.Join(imageStore.RootDir(), repo, SyncBlobUploadDir, uuid.String())

	if err = os.MkdirAll(path.Join(localCachePath, repo), storage.DefaultDirPerms); err != nil {
		return nil, "", err
	}

	localRepo := path.Join(localCachePath, repo)
	localTaggedRepo := fmt.Sprintf("%s:%s", localRepo, tag)

	localImageRef, err := layout.ParseReference(localTaggedRepo)
	if err != nil {
		return nil, "", err
	}

	return localImageRef, localCachePath, nil
}

// canSkipImage returns whether or not we already have this image synced (including signatures)
func canSkipImage(repo, tag, digest, cosignManifestDigest string, refs ReferenceList,
	imageStore storage.ImageStore, log log.Logger) (bool, error) {
	_, localManifestDigest, _, err := imageStore.GetImageManifest(repo, tag)
	if err != nil {
		if errors.Is(err, zerr.ErrRepoNotFound) || errors.Is(err, zerr.ErrManifestNotFound) {
			return false, nil
		}

		log.Error().Err(err).Msgf("couldn't get local image %s:%s manifest", repo, tag)

		return false, err
	}

	if localManifestDigest != digest {
		return false, nil
	}

	localRefs, err := imageStore.GetReferrers(repo, digest, notreg.ArtifactTypeNotation)
	if err != nil {
		return false, err
	}

	if cosignManifestDigest != localManifestDigest {
		return false, nil
	}

	if len(refs.References) != len(localRefs) {
		return false, nil
	}

	for _, upstreamRef := range refs.References {
		found := false

		for _, localRef := range localRefs {
			if upstreamRef.Digest != localRef.Digest {
				continue
			} else {
				found = true
			}
		}
		// if we didn't find an upstream sig locally.
		if !found {
			return false, nil
		}
	}

	return true, nil
}
