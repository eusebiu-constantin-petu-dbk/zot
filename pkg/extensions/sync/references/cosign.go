//go:build sync
// +build sync

package references

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sigstore/cosign/pkg/oci/remote"

	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/common"
	"zotregistry.io/zot/pkg/extensions/sync/constants"
	client "zotregistry.io/zot/pkg/extensions/sync/httpclient"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/meta/repodb"
	"zotregistry.io/zot/pkg/storage"
)

type CosignReference struct {
	client          *client.Client
	storeController storage.StoreController
	repoDB          repodb.RepoDB
	log             log.Logger
}

func NewCosignReference(httpClient *client.Client, storeController storage.StoreController,
	repoDB repodb.RepoDB, log log.Logger,
) CosignReference {
	return CosignReference{
		client:          httpClient,
		storeController: storeController,
		repoDB:          repoDB,
		log:             log,
	}
}

func (ref CosignReference) Name() string {
	return constants.Cosign
}

func (ref CosignReference) IsSigned(upstreamRepo, subjectDigestStr string) (bool, error) {
	_, err := ref.getManifest(upstreamRepo, subjectDigestStr)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (ref CosignReference) canSkipReferences(repo, subjectDigestStr string, manifest *ispec.Manifest) (bool, error) {
	if manifest == nil {
		return true, nil
	}

	imageStore := ref.storeController.GetImageStore(repo)
	// check cosign signature already synced

	var localManifest ispec.Manifest

	/* we need to use tag (cosign format: sha256-$IMAGE_TAG.sig) instead of digest to get local cosign manifest
	because of an issue where cosign digests differs between upstream and downstream */
	cosignManifestTag := getCosignTagFromSubjectDigest(subjectDigestStr)

	localManifestBuf, _, _, err := imageStore.GetImageManifest(repo, cosignManifestTag)
	if err != nil {
		if errors.Is(err, zerr.ErrManifestNotFound) {
			return false, nil
		}

		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msgf("couldn't get local cosign %s:%s manifest", repo, subjectDigestStr)

		return false, err
	}

	err = json.Unmarshal(localManifestBuf, &localManifest)
	if err != nil {
		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msgf("couldn't unmarshal local cosign signature %s:%s manifest", repo, subjectDigestStr)

		return false, err
	}

	if !manifestsEqual(localManifest, *manifest) {
		ref.log.Info().Msgf("upstream cosign signatures %s:%s changed, syncing again", repo, subjectDigestStr)

		return false, nil
	}

	ref.log.Info().Msgf("skipping cosign signature for subject %s:%s, already synced", repo, subjectDigestStr)

	return true, nil
}

func (ref CosignReference) SyncReferences(localRepo, remoteRepo, subjectDigestStr string) error {
	cosignTag := getCosignTagFromSubjectDigest(subjectDigestStr)

	manifest, err := ref.getManifest(remoteRepo, subjectDigestStr)
	if err != nil {
		return err
	}

	skip, err := ref.canSkipReferences(localRepo, subjectDigestStr, manifest)
	if err != nil {
		ref.log.Error().Err(err).Msgf("couldn't check if the remote image %s:%s cosign signature can be skipped",
			remoteRepo, subjectDigestStr)
	}

	if skip {
		return nil
	}

	imageStore := ref.storeController.GetImageStore(localRepo)

	ref.log.Info().Msgf("syncing cosign signatures for %s:%s", localRepo, subjectDigestStr)

	for _, blob := range manifest.Layers {
		if err := syncBlob(ref.client, imageStore, localRepo, remoteRepo, blob.Digest, ref.log); err != nil {
			return err
		}
	}

	// sync config blob
	if err := syncBlob(ref.client, imageStore, localRepo, remoteRepo, manifest.Config.Digest, ref.log); err != nil {
		return err
	}

	manifestBuf, err := json.Marshal(manifest)
	if err != nil {
		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msg("couldn't marshal cosign manifest")

		return err
	}

	// push manifest
	digest, err := imageStore.PutImageManifest(localRepo, cosignTag,
		ispec.MediaTypeImageManifest, manifestBuf)
	if err != nil {
		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msg("couldn't upload cosign manifest")

		return err
	}

	ref.log.Info().Msgf("successfully synced cosign signature for repo %s subject digest %s", localRepo,
		subjectDigestStr)

	if ref.repoDB != nil {
		ref.log.Debug().Msgf("repoDB: trying to sync cosign signature for repo %s digest %s", localRepo, subjectDigestStr)

		err = repodb.SetMetadataFromInput(localRepo, cosignTag, ispec.MediaTypeImageManifest,
			digest, manifestBuf, imageStore, ref.repoDB, ref.log)
		if err != nil {
			return fmt.Errorf("repoDB: failed to set metadata for cosign signature '%s@%s': %w", localRepo,
				subjectDigestStr, err)
		}

		ref.log.Info().Msgf("repoDB: successfully added cosign signature to RepoDB for repo %s digest %s", localRepo,
			subjectDigestStr)
	}

	return nil
}

func (ref CosignReference) getManifest(repo, subjectDigestStr string) (*ispec.Manifest, error) {
	var cosignManifest ispec.Manifest

	cosignTag := getCosignTagFromSubjectDigest(subjectDigestStr)

	_, _, statusCode, err := ref.client.MakeGetRequest(&cosignManifest, ispec.MediaTypeImageManifest,
		"v2", repo, "manifests", cosignTag)
	if err != nil {
		if statusCode == http.StatusNotFound {
			ref.log.Debug().Str("errorType", common.TypeOf(err)).
				Err(err).Msgf("couldn't find any cosign manifest for subject %s", subjectDigestStr)

			return nil, zerr.ErrSyncReferrerNotFound
		}

		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msgf("couldn't get cosign manifest for subject %s", subjectDigestStr)

		return nil, zerr.ErrSyncReferrer
	}

	return &cosignManifest, nil
}

func getCosignTagFromSubjectDigest(digestStr string) string {
	if !IsCosignTag(digestStr) {
		return strings.Replace(digestStr, ":", "-", 1) + "." + remote.SignatureTagSuffix
	}

	return digestStr
}

// sync feature will try to pull cosign signature because for sync cosign signature is just an image
// this function will check if tag is a cosign tag.
func IsCosignTag(tag string) bool {
	if strings.HasPrefix(tag, "sha256-") && strings.HasSuffix(tag, remote.SignatureTagSuffix) {
		return true
	}

	return false
}
