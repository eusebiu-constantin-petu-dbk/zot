//go:build sync
// +build sync

package sync

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/containers/image/v5/types"
	"github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"

	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/common"
	"zotregistry.io/zot/pkg/extensions/monitoring"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/meta/repodb"
	"zotregistry.io/zot/pkg/storage"
	"zotregistry.io/zot/pkg/storage/local"
)

type LocalRegistry struct {
	storeController storage.StoreController
	tempStorage     OciLayoutStorage
	repoDB          repodb.RepoDB
	log             log.Logger
}

func NewLocalRegistry(storeController storage.StoreController, repoDB repodb.RepoDB, log log.Logger) Local {
	return &LocalRegistry{
		storeController: storeController,
		repoDB:          repoDB,
		// first we sync from remote (using containers/image copy from docker:// to oci:) to a temp imageStore
		// then we copy the image from tempStorage to zot's storage using ImageStore APIs
		tempStorage: NewOciLayoutStorage(storeController),
		log:         log,
	}
}

func (registry *LocalRegistry) CanSkipImage(repo, tag string, imageDigest digest.Digest) (bool, error) {
	// check image already synced
	imageStore := registry.storeController.GetImageStore(repo)

	_, localImageManifestDigest, _, err := imageStore.GetImageManifest(repo, tag)
	if err != nil {
		if errors.Is(err, zerr.ErrRepoNotFound) || errors.Is(err, zerr.ErrManifestNotFound) {
			return false, nil
		}

		registry.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msgf("couldn't get local image %s:%s manifest", repo, tag)

		return false, err
	}

	if localImageManifestDigest != imageDigest {
		registry.log.Info().Msgf("upstream image %s:%s digest changed, syncing again", repo, tag)

		return false, nil
	}

	return true, nil
}

func (registry *LocalRegistry) GetContext() *types.SystemContext {
	return registry.tempStorage.GetContext()
}

func (registry *LocalRegistry) GetImageReference(repo, reference string) (types.ImageReference, error) {
	return registry.tempStorage.GetImageReference(repo, reference)
}

// finalize a syncing image.
func (registry *LocalRegistry) CommitImage(imageReference types.ImageReference, repo, reference string) error {
	imageStore := registry.storeController.GetImageStore(repo)

	// open tempImageStore
	var tempRootDir string

	if strings.HasSuffix(imageReference.StringWithinTransport(), reference) {
		tempRootDir = strings.ReplaceAll(imageReference.StringWithinTransport(), fmt.Sprintf("%s:%s", repo, reference), "")
	} else {
		tempRootDir = strings.ReplaceAll(imageReference.StringWithinTransport(), fmt.Sprintf("%s:", repo), "")
	}

	metrics := monitoring.NewMetricsServer(false, registry.log)

	tempImageStore := local.NewImageStore(tempRootDir, false,
		storage.DefaultGCDelay, false, false, log.Logger{}, metrics, nil, nil)

	defer os.RemoveAll(tempImageStore.RootDir())

	registry.log.Info().Msgf("pushing synced local image %s/%s:%s to local registry",
		tempImageStore.RootDir(), repo, reference)

	var lockLatency time.Time

	manifestBlob, manifestDigest, mediaType, err := tempImageStore.GetImageManifest(repo, reference)
	if err != nil {
		registry.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Str("dir", path.Join(tempImageStore.RootDir(), repo)).
			Msgf("couldn't find %s:%s manifest", repo, reference)

		return err
	}

	// is image manifest
	switch mediaType {
	case ispec.MediaTypeImageManifest:
		if err := registry.copyManifest(repo, manifestBlob, reference, tempImageStore); err != nil {
			if errors.Is(err, zerr.ErrImageLintAnnotations) {
				registry.log.Error().Str("errorType", common.TypeOf(err)).
					Err(err).Msg("couldn't upload manifest because of missing annotations")

				return nil
			}

			return err
		}
	case ispec.MediaTypeImageIndex:
		// is image index
		var indexManifest ispec.Index

		if err := json.Unmarshal(manifestBlob, &indexManifest); err != nil {
			registry.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Str("dir", path.Join(tempImageStore.RootDir(), repo)).
				Msg("invalid JSON")

			return err
		}

		for _, manifest := range indexManifest.Manifests {
			tempImageStore.RLock(&lockLatency)
			manifestBuf, err := tempImageStore.GetBlobContent(repo, manifest.Digest)
			tempImageStore.RUnlock(&lockLatency)

			if err != nil {
				registry.log.Error().Str("errorType", common.TypeOf(err)).
					Err(err).Str("dir", path.Join(tempImageStore.RootDir(), repo)).Str("digest", manifest.Digest.String()).
					Msg("couldn't find manifest which is part of an image index")

				return err
			}

			if err := registry.copyManifest(repo, manifestBuf, manifest.Digest.String(),
				tempImageStore); err != nil {
				if errors.Is(err, zerr.ErrImageLintAnnotations) {
					registry.log.Error().Str("errorType", common.TypeOf(err)).
						Err(err).Msg("couldn't upload manifest because of missing annotations")

					return nil
				}

				return err
			}
		}

		_, err = imageStore.PutImageManifest(repo, reference, mediaType, manifestBlob)
		if err != nil {
			registry.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msg("couldn't upload manifest")

			return err
		}

		if registry.repoDB != nil {
			err = repodb.SetMetadataFromInput(repo, reference, mediaType,
				manifestDigest, manifestBlob, imageStore, registry.repoDB, registry.log)
			if err != nil {
				return fmt.Errorf("repoDB: failed to set metadata for image '%s %s': %w", repo, reference, err)
			}

			registry.log.Debug().Msgf("repoDB: successfully set metadata for %s:%s", repo, reference)
		}
	}

	registry.log.Info().Str("image", fmt.Sprintf("%s:%s", repo, reference)).Msg("successfully synced image")

	return nil
}

func (registry *LocalRegistry) copyManifest(repo string, manifestContent []byte, reference string,
	tempImageStore storage.ImageStore,
) error {
	imageStore := registry.storeController.GetImageStore(repo)

	var manifest ispec.Manifest

	var err error

	if err := json.Unmarshal(manifestContent, &manifest); err != nil {
		registry.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Str("dir", path.Join(tempImageStore.RootDir(), repo)).
			Msg("invalid JSON")

		return err
	}

	for _, blob := range manifest.Layers {
		err = registry.copyBlob(repo, blob.Digest, blob.MediaType, tempImageStore)
		if err != nil {
			return err
		}
	}

	err = registry.copyBlob(repo, manifest.Config.Digest, manifest.Config.MediaType, tempImageStore)
	if err != nil {
		return err
	}

	digest, err := imageStore.PutImageManifest(repo, reference,
		ispec.MediaTypeImageManifest, manifestContent)
	if err != nil {
		registry.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msg("couldn't upload manifest")

		return err
	}

	if registry.repoDB != nil {
		err = repodb.SetMetadataFromInput(repo, reference, ispec.MediaTypeImageManifest,
			digest, manifestContent, imageStore, registry.repoDB, registry.log)
		if err != nil {
			registry.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msg("couldn't set metadata from input")

			return err
		}

		registry.log.Debug().Msgf("successfully set metadata for %s:%s", repo, reference)
	}

	return nil
}

// Copy a blob from one image store to another image store.
func (registry *LocalRegistry) copyBlob(repo string, blobDigest digest.Digest, blobMediaType string,
	tempImageStore storage.ImageStore,
) error {
	imageStore := registry.storeController.GetImageStore(repo)
	if found, _, _ := imageStore.CheckBlob(repo, blobDigest); found {
		// Blob is already at destination, nothing to do
		return nil
	}

	blobReadCloser, _, err := tempImageStore.GetBlob(repo, blobDigest, blobMediaType)
	if err != nil {
		registry.log.Error().Str("errorType", common.TypeOf(err)).Err(err).
			Str("dir", path.Join(tempImageStore.RootDir(), repo)).
			Str("blob digest", blobDigest.String()).Str("media type", blobMediaType).
			Msg("couldn't read blob")

		return err
	}
	defer blobReadCloser.Close()

	_, _, err = imageStore.FullBlobUpload(repo, blobReadCloser, blobDigest)
	if err != nil {
		registry.log.Error().Str("errorType", common.TypeOf(err)).Err(err).
			Str("blob digest", blobDigest.String()).Str("media type", blobMediaType).
			Msg("couldn't upload blob")
	}

	return err
}
