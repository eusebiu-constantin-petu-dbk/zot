//go:build sync
// +build sync

package references

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/opencontainers/go-digest"
	oras "github.com/oras-project/artifacts-spec/specs-go/v1"

	zerr "zotregistry.io/zot/errors"
	apiConstants "zotregistry.io/zot/pkg/api/constants"
	"zotregistry.io/zot/pkg/common"
	"zotregistry.io/zot/pkg/extensions/sync/constants"
	client "zotregistry.io/zot/pkg/extensions/sync/httpclient"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/meta/repodb"
	"zotregistry.io/zot/pkg/storage"
)

type ReferenceList struct {
	References []oras.Descriptor `json:"references"`
}

type ORASReferences struct {
	client          *client.Client
	storeController storage.StoreController
	repoDB          repodb.RepoDB
	log             log.Logger
}

func NewORASReferences(httpClient *client.Client, storeController storage.StoreController,
	repoDB repodb.RepoDB, log log.Logger,
) ORASReferences {
	return ORASReferences{
		client:          httpClient,
		storeController: storeController,
		repoDB:          repoDB,
		log:             log,
	}
}

func (ref ORASReferences) Name() string {
	return constants.Oras
}

func (ref ORASReferences) IsSigned(upstreamRepo, subjectDigestStr string) (bool, error) {
	return false, nil
}

func (ref ORASReferences) canSkipReferences(localRepo, subjectDigestStr string, referrers ReferenceList) (bool, error) {
	imageStore := ref.storeController.GetImageStore(localRepo)
	digest := digest.Digest(subjectDigestStr)

	// check oras artifacts already synced
	if len(referrers.References) > 0 {
		localRefs, err := imageStore.GetOrasReferrers(localRepo, digest, "")
		if err != nil {
			if errors.Is(err, zerr.ErrManifestNotFound) {
				return false, nil
			}

			ref.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msgf("couldn't get local ORAS artifact %s:%s manifest", localRepo, subjectDigestStr)

			return false, err
		}

		if !artifactDescriptorsEqual(localRefs, referrers.References) {
			ref.log.Info().Msgf("upstream ORAS artifacts %s:%s changed, syncing again", localRepo, subjectDigestStr)

			return false, nil
		}
	}

	ref.log.Info().Msgf("skipping ORAS artifact %s:%s, already synced", localRepo, subjectDigestStr)

	return true, nil
}

func (ref ORASReferences) SyncReferences(localRepo, upstreamRepo, subjectDigestStr string) error {
	referrers, err := ref.getReferenceList(upstreamRepo, subjectDigestStr)
	if err != nil {
		return err
	}

	skipORASRefs, err := ref.canSkipReferences(localRepo, subjectDigestStr, referrers)
	if err != nil {
		ref.log.Error().Err(err).Msgf("couldn't check if the upstream image %s:%s ORAS artifact can be skipped",
			localRepo, subjectDigestStr)
	}

	if skipORASRefs {
		return nil
	}

	imageStore := ref.storeController.GetImageStore(localRepo)

	ref.log.Info().Msgf("syncing ORAS artifacts for %s:%s", localRepo, subjectDigestStr)

	for _, referrer := range referrers.References {
		var artifactManifest oras.Manifest

		orasBuf, _, statusCode, err := ref.client.MakeGetRequest(&artifactManifest, oras.MediaTypeDescriptor,
			"v2", upstreamRepo, "manifests", referrer.Digest.String())
		if err != nil {
			if statusCode == http.StatusNotFound {
				return zerr.ErrSyncReferrerNotFound
			}

			ref.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msgf("couldn't get ORAS manifest for subject %s", subjectDigestStr)

			return err
		}

		for _, blob := range artifactManifest.Blobs {
			if err := syncBlob(ref.client, imageStore, localRepo, upstreamRepo, blob.Digest, ref.log); err != nil {
				return err
			}
		}

		_, err = imageStore.PutImageManifest(localRepo, referrer.Digest.String(),
			oras.MediaTypeArtifactManifest, orasBuf)
		if err != nil {
			ref.log.Error().Str("errorType", common.TypeOf(err)).
				Err(err).Msg("couldn't upload ORAS manifest")

			return err
		}

		if ref.repoDB != nil {
			ref.log.Debug().Msgf("repoDB: trying to sync oras artifact for repo %s digest %s", localRepo, subjectDigestStr)

			err = repodb.SetMetadataFromInput(localRepo, referrer.Digest.String(), referrer.MediaType,
				referrer.Digest, orasBuf, imageStore, ref.repoDB, ref.log)
			if err != nil {
				return fmt.Errorf("repoDB: failed to set metadata for oras artifact '%s@%s': %w", localRepo, subjectDigestStr, err)
			}

			ref.log.Info().Msgf("repoDB: successfully added oras artifacts to RepoDB for repo %s digest %s", localRepo,
				subjectDigestStr)
		}
	}

	ref.log.Info().Msgf("successfully synced ORAS artifacts for repo %s subject digest %s", localRepo, subjectDigestStr)

	return nil
}

func (ref ORASReferences) getReferenceList(repo, subjectDigestStr string) (ReferenceList, error) {
	var referrers ReferenceList

	_, _, statusCode, err := ref.client.MakeGetRequest(&referrers, "application/json",
		apiConstants.ArtifactSpecRoutePrefix, repo, "manifests", subjectDigestStr, "referrers")
	if err != nil {
		if statusCode == http.StatusNotFound {
			ref.log.Debug().Err(err).Msgf("couldn't find any ORAS artifact for subject %s", subjectDigestStr)

			return referrers, zerr.ErrSyncReferrerNotFound
		}

		ref.log.Error().Err(err).Msgf("couldn't get ORAS artifacts for subject %s", subjectDigestStr)

		return referrers, zerr.ErrSyncReferrer
	}

	return referrers, nil
}
