//go:build sync
// +build sync

package references

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	oras "github.com/oras-project/artifacts-spec/specs-go/v1"

	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/common"
	"zotregistry.io/zot/pkg/extensions/sync/constants"
	client "zotregistry.io/zot/pkg/extensions/sync/httpclient"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/meta/repodb"
	"zotregistry.io/zot/pkg/storage"
)

type OciReferences struct {
	client          *client.Client
	storeController storage.StoreController
	repoDB          repodb.RepoDB
	log             log.Logger
}

func NewOciReferences(httpClient *client.Client, storeController storage.StoreController,
	repoDB repodb.RepoDB, log log.Logger,
) OciReferences {
	return OciReferences{
		client:          httpClient,
		storeController: storeController,
		repoDB:          repoDB,
		log:             log,
	}
}

func (ref OciReferences) Name() string {
	return constants.OCI
}

func (ref OciReferences) IsSigned(remoteRepo, subjectDigestStr string) (bool, error) {
	// use artifactTypeFilter
	index, err := ref.getIndex(remoteRepo, subjectDigestStr)
	if err != nil {
		return false, err
	}

	if len(getNotationManifestsFromOCIRefs(index)) > 0 {
		return true, nil
	}

	return false, nil
}

func (ref OciReferences) canSkipReferences(localRepo, subjectDigestStr string, index ispec.Index) (bool, error) {
	imageStore := ref.storeController.GetImageStore(localRepo)
	digest := digest.Digest(subjectDigestStr)

	// check oci references already synced
	if len(index.Manifests) > 0 {
		localRefs, err := imageStore.GetReferrers(localRepo, digest, nil)
		if err != nil {
			if errors.Is(err, zerr.ErrManifestNotFound) {
				return false, nil
			}

			ref.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msgf("couldn't get local oci references for %s:%s manifest", localRepo, subjectDigestStr)

			return false, err
		}

		if !descriptorsEqual(localRefs.Manifests, index.Manifests) {
			ref.log.Info().Msgf("remote oci references for %s:%s changed, syncing again", localRepo, subjectDigestStr)

			return false, nil
		}
	}

	ref.log.Info().Msgf("skipping oci references %s:%s", localRepo, subjectDigestStr)

	return true, nil
}

func (ref OciReferences) SyncReferences(localRepo, remoteRepo, subjectDigestStr string) error {
	index, err := ref.getIndex(remoteRepo, subjectDigestStr)
	if err != nil {
		return err
	}

	skipOCIRefs, err := ref.canSkipReferences(localRepo, subjectDigestStr, index)
	if err != nil {
		ref.log.Error().Err(err).Msgf("couldn't check if the upstream image %s:%s oci references can be skipped",
			remoteRepo, subjectDigestStr)
	}

	if skipOCIRefs {
		return nil
	}

	imageStore := ref.storeController.GetImageStore(localRepo)

	ref.log.Info().Msgf("syncing oci references for %s:%s", localRepo, subjectDigestStr)

	for _, referrer := range index.Manifests {
		// why oras.Manifest?
		var artifactManifest oras.Manifest

		OCIRefBuf, _, statusCode, err := ref.client.MakeGetRequest(&artifactManifest, ispec.MediaTypeArtifactManifest,
			"v2", remoteRepo, "manifests", referrer.Digest.String())
		if err != nil {
			if statusCode == http.StatusNotFound {
				return zerr.ErrSyncReferrerNotFound
			}

			ref.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msgf("couldn't get oci reference manifest for subject %s", subjectDigestStr)

			return err
		}

		if referrer.MediaType == ispec.MediaTypeImageManifest {
			// read manifest
			var manifest ispec.Manifest

			err = json.Unmarshal(OCIRefBuf, &manifest)
			if err != nil {
				ref.log.Error().Str("errorType", common.TypeOf(err)).
					Err(err).Msgf("couldn't unmarshal oci reference manifest for subject %s", subjectDigestStr)

				return err
			}

			for _, layer := range manifest.Layers {
				if err := syncBlob(ref.client, imageStore, localRepo, remoteRepo, layer.Digest, ref.log); err != nil {
					return err
				}
			}

			// sync config blob
			if err := syncBlob(ref.client, imageStore, localRepo, remoteRepo, manifest.Config.Digest, ref.log); err != nil {
				return err
			}
		} else if referrer.MediaType == ispec.MediaTypeArtifactManifest {
			// read manifest
			var manifest ispec.Artifact

			err = json.Unmarshal(OCIRefBuf, &manifest)
			if err != nil {
				ref.log.Error().Str("errorType", common.TypeOf(err)).
					Err(err).Msgf("couldn't unmarshal oci reference manifest for subject %s", subjectDigestStr)

				return err
			}

			for _, layer := range manifest.Blobs {
				if err := syncBlob(ref.client, imageStore, localRepo, remoteRepo, layer.Digest, ref.log); err != nil {
					return err
				}
			}
		}

		digest, err := imageStore.PutImageManifest(localRepo, referrer.Digest.String(),
			referrer.MediaType, OCIRefBuf)
		if err != nil {
			ref.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msg("couldn't upload oci references manifest")

			return err
		}

		if ref.repoDB != nil {
			ref.log.Debug().Msgf("repoDB: trying to add oci references for repo %s digest %s", localRepo, subjectDigestStr)

			err = repodb.SetMetadataFromInput(localRepo, subjectDigestStr, referrer.MediaType,
				digest, OCIRefBuf, imageStore, ref.repoDB, ref.log)
			if err != nil {
				return fmt.Errorf("repoDB: failed to set metadata for oci references in '%s@%s': %w", localRepo,
					subjectDigestStr, err)
			}

			ref.log.Info().Msgf("repoDB: successfully added oci references to RepoDB for repo %s digest %s", localRepo,
				subjectDigestStr)
		}
	}

	ref.log.Info().Msgf("successfully synced oci references for repo %s subject digest %s", localRepo, subjectDigestStr)

	return nil
}

func (ref OciReferences) getIndex(repo, subjectDigestStr string) (ispec.Index, error) {
	var index ispec.Index

	_, _, statusCode, err := ref.client.MakeGetRequest(&index, ispec.MediaTypeImageIndex,
		"v2", repo, "referrers", subjectDigestStr)
	if err != nil {
		if statusCode == http.StatusNotFound {
			ref.log.Debug().Msgf("couldn't find any oci reference for subject %s, status code: %d, skipping",
				subjectDigestStr, statusCode)

			return index, zerr.ErrSyncReferrerNotFound
		}

		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msgf("couldn't get oci reference for subject %s, status code: %d skipping",
			subjectDigestStr, statusCode)

		return index, zerr.ErrSyncReferrer
	}

	return index, nil
}
