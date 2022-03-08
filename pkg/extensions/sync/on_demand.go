package sync

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/containers/common/pkg/retry"
	"github.com/containers/image/v5/copy"
	"github.com/containers/image/v5/docker"
	"github.com/containers/image/v5/signature"
	"github.com/containers/image/v5/types"
	"gopkg.in/resty.v1"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"
)

type syncContextUtils struct {
	imageStore   storage.ImageStore
	policyCtx    *signature.PolicyContext
	localCtx     *types.SystemContext
	upstreamCtx  *types.SystemContext
	client       *resty.Client
	url          *url.URL
	upstreamAddr string
	retryOptions *retry.RetryOptions
	copyOptions  copy.Options
}

// nolint: gochecknoglobals
var demandedImgs demandedImages

type demandedImages struct {
	syncedMap sync.Map
}

func (di *demandedImages) loadOrStoreChan(key string, value chan error) (chan error, bool) {
	val, found := di.syncedMap.LoadOrStore(key, value)
	errChannel, _ := val.(chan error)

	return errChannel, found
}

func (di *demandedImages) loadOrStoreStr(key string, value string) (string, bool) {
	val, found := di.syncedMap.LoadOrStore(key, value)
	str, _ := val.(string)

	return str, found
}

func (di *demandedImages) delete(key string) {
	di.syncedMap.Delete(key)
}

func OneImage(cfg Config, storeController storage.StoreController,
	repo, tag string, isArtifact bool, log log.Logger) error {
	// guard against multiple parallel requests
	demandedImage := fmt.Sprintf("%s:%s", repo, tag)
	// loadOrStore image-based channel
	imageChannel, found := demandedImgs.loadOrStoreChan(demandedImage, make(chan error))
	// if value found wait on channel receive or close
	if found {
		log.Info().Msgf("image %s already demanded by another client, waiting on imageChannel", demandedImage)

		err, ok := <-imageChannel
		// if channel closed exit
		if !ok {
			return nil
		}

		return err
	}

	defer demandedImgs.delete(demandedImage)
	defer close(imageChannel)

	go syncOneImage(imageChannel, cfg, storeController, repo, tag, isArtifact, log)

	err, ok := <-imageChannel
	if !ok {
		return nil
	}

	return err
}

func syncOneImage(imageChannel chan error, cfg Config, storeController storage.StoreController,
	repo, tag string, isArtifact bool, log log.Logger) {
	var credentialsFile CredentialsFile

	if cfg.CredentialsFile != "" {
		var err error

		credentialsFile, err = getFileCredentials(cfg.CredentialsFile)
		if err != nil {
			log.Error().Err(err).Msgf("couldn't get registry credentials from %s", cfg.CredentialsFile)

			imageChannel <- err

			return
		}
	}

	localCtx, policyCtx, err := getLocalContexts(log)
	if err != nil {
		imageChannel <- err

		return
	}

	imageStore := storeController.GetImageStore(repo)

	for _, registryCfg := range cfg.Registries {
		regCfg := registryCfg
		if !regCfg.OnDemand {
			log.Info().Msgf("skipping syncing on demand from %v, onDemand flag is false", regCfg.URLs)

			continue
		}

		// if content config is not specified, then don't filter, just sync demanded image
		if len(regCfg.Content) != 0 {
			repos := filterRepos([]string{repo}, regCfg.Content, log)
			if len(repos) == 0 {
				log.Info().Msgf("skipping syncing on demand %s from %v registry because it's filtered out by content config",
					repo, regCfg.URLs)

				continue
			}
		}

		retryOptions := &retry.RetryOptions{}

		if regCfg.MaxRetries != nil {
			retryOptions.MaxRetry = *regCfg.MaxRetries
			if regCfg.RetryDelay != nil {
				retryOptions.Delay = *regCfg.RetryDelay
			}
		}

		log.Info().Msgf("syncing on demand with %v", regCfg.URLs)

		for _, regCfgURL := range regCfg.URLs {
			upstreamURL := regCfgURL

			upstreamAddr := StripRegistryTransport(upstreamURL)

			httpClient, registryURL, err := getHTTPClient(&regCfg, upstreamURL, credentialsFile[upstreamAddr], log)
			if err != nil {
				imageChannel <- err

				return
			}

			// it's an image
			upstreamCtx := getUpstreamContext(&regCfg, credentialsFile[upstreamAddr])
			options := getCopyOptions(upstreamCtx, localCtx)

			// demanded 'image' is a signature
			if isCosignTag(tag) {
				// at tis point we should already have images synced, but not their signatures.
				// is notary signature
				upstreamDigest := strings.Replace(strings.Replace(tag, "-", ":", 1), ".sig", "", 1)
				cosignManifest, cosignManifestDigest, err := getCosignManifest(httpClient, *registryURL, repo, upstreamDigest, log)
				if err != nil {
					log.Error().Err(err).Msgf("couldn't get upstream image %s:%s:%s cosign manifest", upstreamURL, repo, upstreamDigest)

					continue
				}

				err = syncCosignSignature(httpClient, imageStore, *registryURL, repo, tag, cosignManifestDigest, cosignManifest, log)
				if err != nil {
					log.Error().Err(err).Msgf("couldn't copy upstream image cosign signature %s/%s:%s", upstreamURL, repo, tag)

					continue
				}

				imageChannel <- nil

				return

			} else if isArtifact {
				// is cosign signature
				refs, err := getNotaryRefs(httpClient, *registryURL, repo, tag, log)
				if err != nil {
					log.Error().Err(err).Msgf("couldn't get upstream image %s/%s:%s notary references", upstreamURL, repo, tag)

					continue
				}

				err = syncNotarySignature(httpClient, imageStore, *registryURL, repo, tag, refs, log)
				if err != nil {
					log.Error().Err(err).Msgf("couldn't copy image signature %s/%s:%s", upstreamURL, repo, tag)

					continue
				}

				imageChannel <- nil

				return
			}

			syncContextUtils := syncContextUtils{
				imageStore:   imageStore,
				policyCtx:    policyCtx,
				localCtx:     localCtx,
				upstreamCtx:  upstreamCtx,
				client:       httpClient,
				url:          registryURL,
				upstreamAddr: upstreamAddr,
				retryOptions: retryOptions,
				copyOptions:  options,
			}

			skipped, copyErr := syncRun(regCfg, repo, tag, syncContextUtils, log)
			if skipped {
				continue
			}

			demandedImageRef := fmt.Sprintf("%s/%s:%s", upstreamAddr, repo, tag)
			if copyErr != nil {
				// don't retry in background if maxretry is 0
				if retryOptions.MaxRetry == 0 {
					continue
				}
				_, found := demandedImgs.loadOrStoreStr(demandedImageRef, "")
				if found {
					log.Info().Msgf("image %s already demanded in background", demandedImageRef)
					/* we already have a go routine spawned for this image
					or retryOptions is not configured */
					continue
				}

				// spawn goroutine to later pull the image
				go func() {
					// remove image after syncing
					defer func() {
						demandedImgs.delete(demandedImageRef)
						log.Info().Msgf("sync routine: %s exited", demandedImageRef)
					}()

					log.Info().Msgf("sync routine: starting routine to copy image %s, cause err: %v",
						demandedImageRef, copyErr)
					time.Sleep(retryOptions.Delay)

					if err = retry.RetryIfNecessary(context.Background(), func() error {
						_, err := syncRun(regCfg, repo, tag, syncContextUtils, log)
						return err
					}, retryOptions); err != nil {
						log.Error().Err(err).Msgf("sync routine: error while copying image %s", demandedImageRef)
					}
				}()
			}
		}
	}

	imageChannel <- err
}

func syncRun(regCfg RegistryConfig, repo, tag string, utils syncContextUtils, log log.Logger) (bool, error) {
	upstreamImageRef, err := getImageRef(utils.upstreamAddr, repo, tag)
	if err != nil {
		log.Error().Err(err).Msgf("error creating docker reference for repository %s/%s:%s",
			utils.upstreamAddr, repo, tag)

		return false, err
	}

	upstreamImageDigest, err := docker.GetDigest(context.Background(), utils.upstreamCtx, upstreamImageRef)
	if err != nil {
		log.Error().Err(err).Msgf("couldn't get upstream image %s manifest", upstreamImageRef.DockerReference())

		return false, err
	}

	// get upstream signatures
	cosignManifest, cosignManifestDigest, err := getCosignManifest(utils.client, *utils.url, repo, upstreamImageDigest.String(), log)
	if err != nil {
		log.Error().Err(err).Msgf("couldn't get upstream image %s cosign manifest", upstreamImageRef.DockerReference())

		return false, err

	}

	refs, err := getNotaryRefs(utils.client, *utils.url, repo, upstreamImageDigest.String(), log)
	if err != nil {
		log.Error().Err(err).Msgf("couldn't get upstream image %s notary references", upstreamImageRef.DockerReference())

		return false, err

	}

	// check if upstream image is signed
	if cosignManifestDigest == "" && len(refs.References) == 0 {
		// upstream image not signed
		if regCfg.OnlySigned != nil && *regCfg.OnlySigned {
			// skip unsigned images
			log.Info().Msgf("skipping image without signature %s", upstreamImageRef.DockerReference())

			return true, nil
		}
	}

	localImageRef, localCachePath, err := getLocalImageRef(utils.imageStore, repo, tag)
	if err != nil {
		log.Error().Err(err).Msgf("couldn't obtain a valid image reference for reference %s/%s:%s",
			localCachePath, repo, tag)

		return false, err
	}

	defer os.RemoveAll(localCachePath)

	log.Info().Msgf("copying image %s to %s", upstreamImageRef.DockerReference(), localCachePath)

	_, err = copy.Image(context.Background(), utils.policyCtx, localImageRef, upstreamImageRef, &utils.copyOptions)
	if err != nil {
		log.Error().Err(err).Msgf("error encountered while syncing on demand %s to %s",
			upstreamImageRef.DockerReference(), localCachePath)

		return false, err
	}

	err = pushSyncedLocalImage(repo, tag, localCachePath, utils.imageStore, log)
	if err != nil {
		log.Error().Err(err).Msgf("error while pushing synced cached image %s",
			fmt.Sprintf("%s/%s:%s", localCachePath, repo, tag))

		return false, err
	}

	err = syncCosignSignature(utils.client, utils.imageStore, *utils.url, repo, upstreamImageDigest.String(),
		cosignManifestDigest, cosignManifest, log)
	if err != nil {
		return false, err
	}

	err = syncNotarySignature(utils.client, utils.imageStore, *utils.url, repo, upstreamImageDigest.String(), refs, log)
	if err != nil {
		return false, err

	}

	log.Info().Msgf("successfully synced %s/%s:%s", utils.upstreamAddr, repo, tag)

	return false, nil
}
