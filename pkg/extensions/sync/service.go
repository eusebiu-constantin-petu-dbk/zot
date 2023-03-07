//go:build sync
// +build sync

package sync

import (
	"context"
	"errors"
	"fmt"

	"github.com/containers/common/pkg/retry"
	"github.com/containers/image/v5/copy"
	"github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"

	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/common"
	syncconf "zotregistry.io/zot/pkg/extensions/config/sync"
	client "zotregistry.io/zot/pkg/extensions/sync/httpclient"
	"zotregistry.io/zot/pkg/extensions/sync/references"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/meta/repodb"
	"zotregistry.io/zot/pkg/storage"
)

type BaseService struct {
	config          syncconf.RegistryConfig
	credentials     syncconf.CredentialsFile
	remote          Remote
	local           Local
	retryOptions    *retry.RetryOptions
	contentManager  ContentManager
	storeController storage.StoreController
	repoDB          repodb.RepoDB
	repositories    []string
	references      references.References
	ociArtifact     references.OciArtifact
	client          *client.Client
	lastURL         string
	log             log.Logger
}

func NewService(opts syncconf.RegistryConfig, credentialsFilepath string,
	storeController storage.StoreController, repodb repodb.RepoDB, log log.Logger,
) (Service, error) {
	service := &BaseService{}

	service.config = opts
	service.log = log
	service.repoDB = repodb

	var err error

	var credentialsFile syncconf.CredentialsFile
	if credentialsFilepath != "" {
		credentialsFile, err = getFileCredentials(credentialsFilepath)
		if err != nil {
			log.Error().Str("errortype", common.TypeOf(err)).
				Err(err).Msgf("couldn't get registry credentials from %s", credentialsFilepath)
		}
	}

	service.credentials = credentialsFile

	service.contentManager = NewContentManager(opts.Content, log)
	service.local = NewLocalRegistry(storeController, repodb, log)

	retryOptions := &retry.RetryOptions{}

	if opts.MaxRetries != nil {
		retryOptions.MaxRetry = *opts.MaxRetries
		if opts.RetryDelay != nil {
			retryOptions.Delay = *opts.RetryDelay
		}
	}

	service.retryOptions = retryOptions
	service.storeController = storeController

	err = service.SetNextClient()
	if err != nil {
		return nil, err
	}

	service.references = references.NewReferences(
		service.client,
		service.storeController,
		service.repoDB,
		service.log,
	)

	service.ociArtifact = references.NewOciArtifact(
		service.client,
		service.storeController,
		service.repoDB,
		service.log,
	)

	service.remote = NewRemoteRegistry(
		service.client,
		service.log,
	)

	return service, nil
}

func (service *BaseService) getNextURL() string {
	found := false

	for _, url := range service.config.URLs {
		if url == service.lastURL {
			found = true

			continue
		}

		if found {
			return url
		}
	}

	return service.config.URLs[0]
}

func (service *BaseService) SetNextClient() error {
	if len(service.config.URLs) == 1 && service.client != nil {
		// noop
		return nil
	}

	url := service.getNextURL()
	service.lastURL = url

	remoteAddress := StripRegistryTransport(url)
	credentials := service.credentials[remoteAddress]

	tlsVerify := true
	if service.config.TLSVerify != nil {
		tlsVerify = *service.config.TLSVerify
	}

	options := client.Config{
		URL:       url,
		Username:  credentials.Username,
		Password:  credentials.Password,
		TLSVerify: tlsVerify,
		CertDir:   service.config.CertDir,
	}

	var err error

	if service.client != nil {
		err = service.client.SetConfig(options)
	} else {
		service.client, err = client.New(options, service.log)
	}

	if err != nil {
		return err
	}

	return nil
}

func (service *BaseService) GetRetryOptions() *retry.Options {
	return service.retryOptions
}

func (service *BaseService) getNextRepoFromCatalog(lastRepo string) string {
	var found bool

	var nextRepo string

	for _, repo := range service.repositories {
		if lastRepo == "" {
			nextRepo = repo

			break
		}

		if repo == lastRepo {
			found = true

			continue
		}

		if found {
			nextRepo = repo

			break
		}
	}

	return nextRepo
}

func (service *BaseService) GetNextRepo(lastRepo string) (string, error) {
	var err error

	if len(service.repositories) == 0 {
		if err = retry.RetryIfNecessary(context.Background(), func() error {
			service.repositories, err = service.remote.GetRepositories(context.Background())

			return err
		}, service.retryOptions); err != nil {
			service.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msgf("error while getting repositories from remote registry %s", service.client.GetConfig().URL)

			return "", err
		}
	}

	var matches bool

	for !matches {
		lastRepo = service.getNextRepoFromCatalog(lastRepo)
		if lastRepo == "" {
			break
		}

		matches = service.contentManager.MatchesContent(lastRepo)
	}

	return lastRepo, nil
}

// SyncReference on demand.
func (service *BaseService) SyncReference(repo string, subjectDigestStr string, referenceType string) error {
	remoteRepo := repo
	if len(service.config.Content) > 0 {
		remoteRepo = service.contentManager.GetRepoSource(repo)
		if remoteRepo == "" {
			service.log.Info().Msgf("will not sync %s/%s:%s %s reference, filtered out by content",
				service.client.GetConfig().URL, repo, subjectDigestStr, referenceType)

			return nil
		}
	}

	service.log.Info().Msgf("sync: syncing %s reference for %s/%s:%s",
		referenceType, service.client.GetConfig().URL, repo, subjectDigestStr)

	return service.references.SyncReference(repo, remoteRepo, subjectDigestStr, referenceType)
}

// SyncImage on demand.
func (service *BaseService) SyncImage(repo, reference string) error {
	remoteRepo := repo
	if len(service.config.Content) > 0 {
		remoteRepo = service.contentManager.GetRepoSource(repo)
		if remoteRepo == "" {
			service.log.Info().Msgf("will not sync %s/%s:%s image, filtered out by content",
				service.client.GetConfig().URL, repo, reference)

			return nil
		}
	}

	service.log.Info().Msgf("sync: syncing image %s/%s:%s", service.client.GetConfig().URL, repo, reference)

	manifestDigest, err := service.syncTag(repo, remoteRepo, reference)
	if err != nil {
		return err
	}

	err = service.references.SyncAll(remoteRepo, remoteRepo, manifestDigest.String())
	if err != nil && !errors.Is(err, zerr.ErrSyncReferrer) {
		return err
	}

	return nil
}

// sync repo periodically.
func (service *BaseService) SyncRepo(repo string) error {
	service.log.Info().Msgf("sync: syncing repo %s/%s", service.client.GetConfig().URL, repo)

	var err error

	var tags []string

	if err = retry.RetryIfNecessary(context.Background(), func() error {
		tags, err = service.remote.GetRepoTags(repo)

		return err
	}, service.retryOptions); err != nil {
		service.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msgf("error while getting tags for repo %s", repo)

		return err
	}

	// filter tags
	tags, err = service.contentManager.FilterTags(repo, tags)
	if err != nil {
		return err
	}

	service.log.Info().Str("repo", repo).Msgf("sync: syncing tags %v", tags)

	// apply content.destination rule
	localRepo := service.contentManager.GetRepoDestination(repo)

	for _, tag := range tags {
		if isCosignTag(tag) {
			continue
		}

		var manifestDigest digest.Digest

		if err = retry.RetryIfNecessary(context.Background(), func() error {
			manifestDigest, err = service.syncTag(localRepo, repo, tag)

			return err
		}, service.retryOptions); err != nil {
			service.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msgf("error while syncing tags for repo %s", repo)

			return err
		}

		if manifestDigest != "" {
			if err = retry.RetryIfNecessary(context.Background(), func() error {
				err = service.references.SyncAll(localRepo, repo, manifestDigest.String())
				if err != nil && !errors.Is(err, zerr.ErrSyncReferrer) {
					return err
				}

				return nil
			}, service.retryOptions); err != nil {
				service.log.Error().Str("errorType", common.TypeOf(err)).
					Err(err).Msgf("error while syncing tags for repo %s", repo)

				return err
			}
		}
	}

	return nil
}

func (service *BaseService) syncTag(localRepo, remoteRepo, tag string) (digest.Digest, error) {
	copyOptions := getCopyOptions(service.remote.GetContext(), service.local.GetContext())

	policyContext, err := getPolicyContext(service.log)
	if err != nil {
		return "", err
	}

	defer func() {
		_ = policyContext.Destroy()
	}()

	remoteImageRef, err := service.remote.GetImageReference(remoteRepo, tag)
	if err != nil {
		service.log.Error().Err(err).Str("errortype", common.TypeOf(err)).
			Str("repo", remoteRepo).Str("reference", tag).Msg("couldn't get a remote image reference")

		return "", err
	}

	manifestBuf, mediaType, manifestDigest, err := service.remote.GetManifestContent(remoteImageRef)
	if err != nil {
		return "", err
	}

	if !isSupportedMediaType(mediaType) {
		if mediaType == ispec.MediaTypeArtifactManifest {
			err = service.ociArtifact.Sync(localRepo, remoteRepo, tag, manifestBuf) //nolint
			if err != nil {
				service.log.Error().Err(err).Str("errortype", common.TypeOf(err)).
					Str("repo", remoteRepo).Str("reference", tag).Msg("couldn't sync OCI artifact image")

				return "", err
			}
		}

		return "", nil
	}

	if service.config.OnlySigned != nil && *service.config.OnlySigned {
		if ok, err := service.references.IsSigned(remoteRepo, manifestDigest.String()); err != nil {
			return "", err
		} else if !ok {
			// skip unsigned images
			service.log.Info().Msgf("skipping image without mandatory signature %s", remoteImageRef.DockerReference().String())

			return "", nil
		}
	}

	skipImage, err := service.local.CanSkipImage(localRepo, tag, manifestDigest)
	if err != nil {
		service.log.Error().Err(err).Str("errortype", common.TypeOf(err)).
			Str("repo", localRepo).Str("reference", tag).
			Msgf("couldn't check if the local image %s:%s can be skipped", localRepo, tag)
	}

	if !skipImage {
		localImageRef, err := service.local.GetImageReference(localRepo, tag)
		if err != nil {
			service.log.Error().Err(err).Str("errortype", common.TypeOf(err)).
				Str("repo", localRepo).Str("reference", tag).Msg("couldn't get a local image reference")

			return "", err
		}

		service.log.Info().Str("remote image", remoteImageRef.DockerReference().String()).
			Str("local image", fmt.Sprintf("%s:%s", localRepo, tag)).Msgf("syncing image")

		_, err = copy.Image(context.Background(), policyContext, localImageRef, remoteImageRef, &copyOptions)
		if err != nil {
			service.log.Error().Err(err).Str("errortype", common.TypeOf(err)).
				Str("remote image", remoteImageRef.DockerReference().String()).
				Str("local image", fmt.Sprintf("%s:%s", localRepo, tag)).Msgf("coulnd't sync image")

			return "", err
		}

		err = service.local.CommitImage(localImageRef, localRepo, tag)
		if err != nil {
			service.log.Error().Err(err).Str("errortype", common.TypeOf(err)).
				Str("repo", localRepo).Str("reference", tag).Msg("couldn't commit image to local image store")

			return "", err
		}
	} else {
		service.log.Info().Str("image", remoteImageRef.DockerReference().String()).
			Msg("skipping image because it's already synced")
	}

	return manifestDigest, nil
}

func (service *BaseService) ResetCatalog() {
	service.log.Info().Msg("resetting catalog")

	service.repositories = []string{}
}

func (service *BaseService) SetNextBackupURL() error {
	service.log.Info().Msg("rotating remote registry URLs")

	return service.SetNextClient()
}
