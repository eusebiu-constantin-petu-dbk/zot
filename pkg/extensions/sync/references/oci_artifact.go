//go:build sync
// +build sync

package references

import (
	"encoding/json"
	"errors"
	"fmt"

	ispec "github.com/opencontainers/image-spec/specs-go/v1"

	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/common"
	client "zotregistry.io/zot/pkg/extensions/sync/httpclient"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/meta/repodb"
	"zotregistry.io/zot/pkg/storage"
)

type OciArtifact struct {
	client          *client.Client
	storeController storage.StoreController
	repoDB          repodb.RepoDB
	log             log.Logger
}

func NewOciArtifact(httpClient *client.Client, storeController storage.StoreController,
	repoDB repodb.RepoDB, log log.Logger,
) OciArtifact {
	return OciArtifact{
		client:          httpClient,
		storeController: storeController,
		repoDB:          repoDB,
		log:             log,
	}
}

func (ref OciArtifact) canSkipReferences(localRepo, reference string, artifact ispec.Artifact) (bool, error) {
	imageStore := ref.storeController.GetImageStore(localRepo)

	var localArtifactManifest ispec.Artifact

	localArtifactBuf, _, _, err := imageStore.GetImageManifest(localRepo, reference)
	if err != nil {
		if errors.Is(err, zerr.ErrManifestNotFound) {
			return false, nil
		}

		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msgf("couldn't get local oci artifact %s:%s manifest", localRepo, reference)

		return false, err
	}

	err = json.Unmarshal(localArtifactBuf, &localArtifactManifest)
	if err != nil {
		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msgf("couldn't unmarshal local oci artifact %s:%s manifest", localRepo, reference)

		return false, err
	}

	if !artifactsEqual(localArtifactManifest, artifact) {
		ref.log.Info().Msgf("remote oci artifact %s:%s changed, syncing again", localRepo, reference)

		return false, nil
	}

	ref.log.Info().Msgf("skipping oci artifact %s:%s, already synced", localRepo, reference)

	return true, nil
}

func (ref OciArtifact) Sync(localRepo, remoteRepo, reference string, ociArtifactBuf []byte) error {
	var ociArtifact ispec.Artifact

	err := json.Unmarshal(ociArtifactBuf, &ociArtifact)
	if err != nil {
		ref.log.Error().Err(err).Msgf("couldn't unmarshal oci artifact from %s:%s", remoteRepo, reference)

		return err
	}

	canSkipOCIArtifact, err := ref.canSkipReferences(localRepo, reference, ociArtifact)
	if err != nil {
		ref.log.Error().Err(err).Msgf("couldn't check if oci artifact %s:%s can be skipped",
			remoteRepo, reference)
	}

	if canSkipOCIArtifact {
		return nil
	}

	imageStore := ref.storeController.GetImageStore(localRepo)

	ref.log.Info().Msg("syncing OCI artifacts")

	for _, blob := range ociArtifact.Blobs {
		if err := syncBlob(ref.client, imageStore, localRepo, remoteRepo, blob.Digest, ref.log); err != nil {
			return err
		}
	}

	artifactManifestBuf, err := json.Marshal(ociArtifact)
	if err != nil {
		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msg("couldn't marshal oci artifact")

		return err
	}

	// push manifest
	digest, err := imageStore.PutImageManifest(localRepo, reference,
		ispec.MediaTypeArtifactManifest, artifactManifestBuf)
	if err != nil {
		ref.log.Error().Str("errorType", common.TypeOf(err)).
			Err(err).Msg("couldn't upload oci artifact manifest")

		return err
	}

	ref.log.Info().Msgf("successfully synced oci artifact for repo %s tag %s", localRepo, reference)

	if ref.repoDB != nil {
		ref.log.Debug().Msgf("repoDB: trying to sync oci artifact for repo %s digest %s", localRepo, digest.String())

		err = repodb.SetMetadataFromInput(localRepo, reference, ispec.MediaTypeArtifactManifest,
			digest, artifactManifestBuf, imageStore, ref.repoDB, ref.log)
		if err != nil {
			return fmt.Errorf("repoDB: failed to set metadata for oci artifact '%s@%s': %w", localRepo, digest.String(), err)
		}

		ref.log.Info().Msgf("repoDB: successfully added oci artifact to RepoDB for repo %s digest %s", localRepo,
			digest.String())
	}

	return nil
}
