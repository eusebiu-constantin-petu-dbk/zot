package sync

import (
	"encoding/json"
	"net/url"
	"path"
	"strings"

	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"

	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	artifactspec "github.com/oras-project/artifacts-spec/specs-go/v1"
	"gopkg.in/resty.v1"
)

func getCosignManifest(client *resty.Client, regURL url.URL, repo, digest string, log log.Logger) (ispec.Manifest, string, error) {
	var m ispec.Manifest

	getCosignManifestURL := regURL

	getCosignManifestURL.Path = path.Join(getCosignManifestURL.Path, "v2", repo, "manifests", digest)

	getCosignManifestURL.RawQuery = getCosignManifestURL.Query().Encode()

	resp, err := client.R().Get(getCosignManifestURL.String())
	if err != nil {
		log.Error().Err(err).Str("url", getCosignManifestURL.String()).
			Msgf("couldn't get cosign manifest: %s", digest)

		return m, "", err
	}

	if resp.IsError() {
		log.Info().Msgf("couldn't find any cosign signature from %s, status code: %d skipping",
			getCosignManifestURL.String(), resp.StatusCode())

		return m, "", nil
	}

	err = json.Unmarshal(resp.Body(), &m)
	if err != nil {
		log.Error().Err(err).Str("url", getCosignManifestURL.String()).
			Msgf("couldn't unmarshal cosign manifest %s", digest)

		return m, "", err
	}

	cosignManifestDigest := resp.Header().Get("Docker-Content-Digest")

	return m, cosignManifestDigest, nil
}

func getNotaryRefs(client *resty.Client, regURL url.URL, repo, digest string, log log.Logger) (ReferenceList, error) {
	var referrers ReferenceList

	getReferrersURL := regURL

	// based on manifest digest get referrers
	getReferrersURL.Path = path.Join(getReferrersURL.Path, "oras/artifacts/v1/", repo, "manifests", digest, "referrers")
	getReferrersURL.RawQuery = getReferrersURL.Query().Encode()

	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetQueryParam("artifactType", "application/vnd.cncf.notary.v2.signature").
		Get(getReferrersURL.String())
	if err != nil {
		log.Error().Err(err).Msgf("couldn't get referrers from %s", getReferrersURL.String())

		return referrers, err
	}

	if resp.IsError() {
		log.Info().Msgf("couldn't find any notary signature from %s, status code: %d, skipping",
			getReferrersURL.String(), resp.StatusCode())

		return referrers, nil
	}

	err = json.Unmarshal(resp.Body(), &referrers)
	if err != nil {
		log.Error().Err(err).Msgf("couldn't unmarshal notary signature from %s", getReferrersURL.String())

		return referrers, err
	}

	return referrers, nil
}

func syncCosignSignature(client *resty.Client, storeController storage.StoreController,
	regURL url.URL, repo, digest, cosignManifestDigest string, cosignManifest ispec.Manifest, log log.Logger) error {
	log.Info().Msg("syncing cosign signatures")
	if !isCosignTag(digest) {
		digest = strings.Replace(digest, ":", "-", 1) + ".sig"
	}

	// if no manifest found
	if cosignManifestDigest == "" {
		return nil
	}

	imageStore := storeController.GetImageStore(repo)

	for _, blob := range cosignManifest.Layers {
		// get blob
		getBlobURL := regURL
		getBlobURL.Path = path.Join(getBlobURL.Path, "v2", repo, "blobs", blob.Digest.String())
		getBlobURL.RawQuery = getBlobURL.Query().Encode()

		resp, err := client.R().SetDoNotParseResponse(true).Get(getBlobURL.String())
		if err != nil {
			log.Error().Err(err).Msgf("couldn't get cosign blob: %s", blob.Digest.String())

			return err
		}

		if resp.IsError() {
			log.Info().Msgf("couldn't find cosign blob from %s, status code: %d", getBlobURL.String(), resp.StatusCode())

			return zerr.ErrBadBlobDigest
		}

		defer resp.RawBody().Close()

		// push blob
		_, _, err = imageStore.FullBlobUpload(repo, resp.RawBody(), blob.Digest.String())
		if err != nil {
			log.Error().Err(err).Msg("couldn't upload cosign blob")

			return err
		}
	}

	// get config blob
	getBlobURL := regURL
	getBlobURL.Path = path.Join(getBlobURL.Path, "v2", repo, "blobs", cosignManifest.Config.Digest.String())
	getBlobURL.RawQuery = getBlobURL.Query().Encode()

	resp, err := client.R().SetDoNotParseResponse(true).Get(getBlobURL.String())
	if err != nil {
		log.Error().Err(err).Msgf("couldn't get cosign config blob: %s", getBlobURL.String())

		return err
	}

	if resp.IsError() {
		log.Info().Msgf("couldn't find cosign config blob from %s, status code: %d", getBlobURL.String(), resp.StatusCode())

		return zerr.ErrBadBlobDigest
	}

	defer resp.RawBody().Close()

	// push config blob
	_, _, err = imageStore.FullBlobUpload(repo, resp.RawBody(), cosignManifest.Config.Digest.String())
	if err != nil {
		log.Error().Err(err).Msg("couldn't upload cosign blob")

		return err
	}

	manifestBuf, err := json.Marshal(cosignManifest)
	if err != nil {
		log.Error().Err(err).Msg("couldn't marshal cosign manifest")
	}

	// push manifest
	_, err = imageStore.PutImageManifest(repo, digest, ispec.MediaTypeImageManifest, manifestBuf)
	if err != nil {
		log.Error().Err(err).Msg("couldn't upload cosing manifest")

		return err
	}

	return nil
}

func syncNotarySignature(client *resty.Client, storeController storage.StoreController,
	regURL url.URL, repo, digest string, referrers ReferenceList, log log.Logger) error {
	log.Info().Msg("syncing notary signatures")

	imageStore := storeController.GetImageStore(repo)

	for _, ref := range referrers.References {
		// get referrer manifest
		getRefManifestURL := regURL
		getRefManifestURL.Path = path.Join(getRefManifestURL.Path, "v2", repo, "manifests", ref.Digest.String())
		getRefManifestURL.RawQuery = getRefManifestURL.Query().Encode()

		resp, err := client.R().
			Get(getRefManifestURL.String())
		if err != nil {
			log.Error().Err(err).Msgf("couldn't get notary manifest: %s", getRefManifestURL.String())

			return err
		}

		// read manifest
		var m artifactspec.Manifest

		err = json.Unmarshal(resp.Body(), &m)
		if err != nil {
			log.Error().Err(err).Msgf("couldn't unmarshal notary manifest: %s", getRefManifestURL.String())

			return err
		}

		for _, blob := range m.Blobs {
			getBlobURL := regURL
			getBlobURL.Path = path.Join(getBlobURL.Path, "v2", repo, "blobs", blob.Digest.String())
			getBlobURL.RawQuery = getBlobURL.Query().Encode()

			resp, err := client.R().SetDoNotParseResponse(true).Get(getBlobURL.String())
			if err != nil {
				log.Error().Err(err).Msgf("couldn't get notary blob: %s", getBlobURL.String())

				return err
			}

			defer resp.RawBody().Close()

			if resp.IsError() {
				log.Info().Msgf("couldn't find notary blob from %s, status code: %d",
					getBlobURL.String(), resp.StatusCode())

				return zerr.ErrBadBlobDigest
			}

			_, _, err = imageStore.FullBlobUpload(repo, resp.RawBody(), blob.Digest.String())
			if err != nil {
				log.Error().Err(err).Msg("couldn't upload notary sig blob")

				return err
			}
		}

		_, err = imageStore.PutImageManifest(repo, ref.Digest.String(), artifactspec.MediaTypeArtifactManifest, resp.Body())
		if err != nil {
			log.Error().Err(err).Msg("couldn't upload notary sig manifest")

			return err
		}
	}

	return nil
}

// sync feature will try to pull cosign signature because for sync cosign signature is just an image
// this function will check if tag is a cosign tag.
func isCosignTag(tag string) bool {
	if strings.HasPrefix(tag, "sha256-") && strings.HasSuffix(tag, ".sig") {
		return true
	}

	return false
}
