package sync

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/containers/common/pkg/retry"
	"github.com/containers/image/v5/copy"
	"github.com/containers/image/v5/docker"
	"gopkg.in/resty.v1"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"
)

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

	var copyErr error

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

			// demanded 'image' is a signature
			if isCosignTag(tag) {
				// at tis point we should already have images synced, but not their signatures.
				// is notary signature
				if isArtifact {
					err = syncCosignSignature(httpClient, storeController, *registryURL, repo, tag, log)
					if err != nil {
						log.Error().Err(err).Msgf("couldn't copy image signature %s/%s:%s", upstreamURL, repo, tag)

						continue
					}

					imageChannel <- nil

					return
				}
			} else if isArtifact {
				// is cosign signature
				err = syncNotarySignature(httpClient, storeController, *registryURL, repo, tag, log)
				if err != nil {
					log.Error().Err(err).Msgf("couldn't copy image signature %s/%s:%s", upstreamURL, repo, tag)

					continue
				}

				imageChannel <- nil

				return
			}

			// it's an image
			upstreamCtx := getUpstreamContext(&regCfg, credentialsFile[upstreamAddr])
			options := getCopyOptions(upstreamCtx, localCtx)

			upstreamImageRef, err := getImageRef(upstreamAddr, repo, tag)
			if err != nil {
				log.Error().Err(err).Msgf("error creating docker reference for repository %s/%s:%s",
					upstreamAddr, repo, tag)

				imageChannel <- err

				return
			}

			upstreamImageDigest, err := docker.GetDigest(context.Background(), upstreamCtx, upstreamImageRef)
			if err != nil {
				log.Error().Err(err).Msgf("couldn't get upstream image %s manifest", upstreamImageRef.DockerReference())

				continue
			}

			isSigned, err := isImageSigned(repo, tag, string(upstreamImageDigest), httpClient, registryURL, log)
			if err != nil {
				log.Error().Err(err).Msgf("couldn't check if upstream image %s is signed", upstreamImageRef.DockerReference())
			} else {
				if !isSigned && regCfg.OnlySigned != nil && *regCfg.OnlySigned {
					log.Info().Msgf("skipping image without signature %s", upstreamImageRef.DockerReference())

					imageChannel <- nil
				}
			}

			localImageRef, localCachePath, err := getLocalImageRef(imageStore, repo, tag)
			if err != nil {
				log.Error().Err(err).Msgf("couldn't obtain a valid image reference for reference %s/%s:%s",
					localCachePath, repo, tag)

				imageChannel <- err

				return
			}

			log.Info().Msgf("copying image %s to %s", upstreamImageRef.DockerReference(), localCachePath)

			demandedImageRef := fmt.Sprintf("%s/%s:%s", upstreamAddr, repo, tag)

			_, copyErr = copy.Image(context.Background(), policyCtx, localImageRef, upstreamImageRef, &options)
			if copyErr != nil {
				log.Error().Err(err).Msgf("error encountered while syncing on demand %s to %s",
					upstreamImageRef.DockerReference(), localCachePath)

				_, found := demandedImgs.loadOrStoreStr(demandedImageRef, "")
				if found || retryOptions.MaxRetry == 0 {
					defer os.RemoveAll(localCachePath)
					log.Info().Msgf("image %s already demanded in background or sync.registries[].MaxRetries == 0", demandedImageRef)
					/* we already have a go routine spawned for this image
					or retryOptions is not configured */
					continue
				}

				// spawn goroutine to later pull the image
				go func() {
					// remove image after syncing
					defer func() {
						_ = os.RemoveAll(localCachePath)

						demandedImgs.delete(demandedImageRef)
						log.Info().Msgf("sync routine: %s exited", demandedImageRef)
					}()

					log.Info().Msgf("sync routine: starting routine to copy image %s, cause err: %v",
						demandedImageRef, copyErr)
					time.Sleep(retryOptions.Delay)

					if err = retry.RetryIfNecessary(context.Background(), func() error {
						_, err := copy.Image(context.Background(), policyCtx, localImageRef, upstreamImageRef, &options)

						return err
					}, retryOptions); err != nil {
						log.Error().Err(err).Msgf("sync routine: error while copying image %s to %s",
							demandedImageRef, localCachePath)
					} else {
						_ = finishSyncing(repo, tag, localCachePath, string(upstreamImageDigest),
							registryURL, storeController, retryOptions, httpClient, log)
					}
				}()
			} else {
				err := finishSyncing(repo, tag, localCachePath, string(upstreamImageDigest),
					registryURL, storeController, retryOptions, httpClient, log)

				imageChannel <- err

				return
			}
		}
	}

	imageChannel <- err
}

// push the local image into the storage, sync signatures.
func finishSyncing(repo, tag, localCachePath, upstreamImageDigest string, registryURL *url.URL,
	storeController storage.StoreController, retryOptions *retry.RetryOptions,
	httpClient *resty.Client, log log.Logger) error {
	err := pushSyncedLocalImage(repo, tag, localCachePath, storeController, log)
	if err != nil {
		log.Error().Err(err).Msgf("error while pushing synced cached image %s",
			fmt.Sprintf("%s/%s:%s", localCachePath, repo, tag))

		return err
	}

	if err = retry.RetryIfNecessary(context.Background(), func() error {
		err = syncCosignSignature(httpClient, storeController, *registryURL, repo, upstreamImageDigest, log)

		return err
	}, retryOptions); err != nil {
		log.Error().Err(err).Msgf("couldn't copy cosign signature for %s/%s:%s", registryURL.Host, repo, tag)
	}

	if err = retry.RetryIfNecessary(context.Background(), func() error {
		err = syncNotarySignature(httpClient, storeController, *registryURL, repo, string(upstreamImageDigest), log)

		return err
	}, retryOptions); err != nil {
		log.Error().Err(err).Msgf("couldn't copy notary signature for %s/%s:%s", registryURL.Host, repo, tag)
	}

	log.Info().Msgf("successfully synced %s/%s:%s", registryURL.Host, repo, tag)

	return nil
}
