//go:build sync
// +build sync

package references

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"

	notreg "github.com/notaryproject/notation-go/registry"
	"github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	artifactspec "github.com/oras-project/artifacts-spec/specs-go/v1"
	"github.com/sigstore/cosign/pkg/oci/static"

	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/common"
	client "zotregistry.io/zot/pkg/extensions/sync/httpclient"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/meta/repodb"
	"zotregistry.io/zot/pkg/storage"
)

type Reference interface {
	Name() string
	IsSigned(upstreamRepo, subjectDigestStr string) (bool, error)
	SyncReferences(localRepo, upstreamRepo, subjectDigestStr string) error
}

type References struct {
	refernceList []Reference
	log          log.Logger
}

func NewReferences(httpClient *client.Client, storeController storage.StoreController,
	repoDB repodb.RepoDB, log log.Logger,
) References {
	refs := References{log: log}

	refs.refernceList = append(refs.refernceList, NewCosignReference(httpClient, storeController, repoDB, log))
	refs.refernceList = append(refs.refernceList, NewOciReferences(httpClient, storeController, repoDB, log))
	refs.refernceList = append(refs.refernceList, NewORASReferences(httpClient, storeController, repoDB, log))

	return refs
}

func (refs References) IsSigned(upstreamRepo, subjectDigestStr string) (bool, error) {
	for _, ref := range refs.refernceList {
		ok, err := ref.IsSigned(upstreamRepo, subjectDigestStr)
		if err != nil &&
			!errors.Is(err, zerr.ErrSyncReferrerNotFound) &&
			!errors.Is(err, zerr.ErrSyncReferrer) {
			refs.log.Error().Err(err).
				Str("reference type", ref.Name()).
				Str("image", fmt.Sprintf("%s:%s", upstreamRepo, subjectDigestStr)).
				Msg("couldn't check if image is signed")

			return false, err
		}

		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (refs References) SyncAll(localRepo, upstreamRepo, subjectDigestStr string) error {
	for _, ref := range refs.refernceList {
		if err := ref.SyncReferences(localRepo, upstreamRepo, subjectDigestStr); err != nil &&
			!errors.Is(err, zerr.ErrSyncReferrerNotFound) {
			refs.log.Error().Err(err).
				Str("reference type", ref.Name()).
				Str("image", fmt.Sprintf("%s:%s", upstreamRepo, subjectDigestStr)).
				Msg("couldn't sync image referrer")

			return err
		}
	}

	return nil
}

func (refs References) SyncReference(localRepo, upstreamRepo, subjectDigestStr, referenceType string) error {
	for _, ref := range refs.refernceList {
		if ref.Name() == referenceType {
			if err := ref.SyncReferences(localRepo, upstreamRepo, subjectDigestStr); err != nil {
				refs.log.Error().Err(err).
					Str("reference type", ref.Name()).
					Str("image", fmt.Sprintf("%s:%s", upstreamRepo, subjectDigestStr)).
					Msg("couldn't sync image referrer")

				return err
			}
		}
	}

	return nil
}

func syncBlob(client *client.Client, imageStore storage.ImageStore, remoteRepo, localRepo string,
	digest digest.Digest, log log.Logger,
) error {
	var resultPtr interface{}

	body, _, statusCode, err := client.MakeGetRequest(resultPtr, "", "v2", remoteRepo, "blobs", digest.String())
	if err != nil {
		if statusCode != http.StatusOK {
			log.Info().Str("digest", digest.String()).Msgf("couldn't get upstream blob, status code: %d",
				statusCode)

			return zerr.ErrSyncReferrer
		}
	}

	_, _, err = imageStore.FullBlobUpload(localRepo, bytes.NewBuffer(body), digest)
	if err != nil {
		log.Error().Str("errorType", common.TypeOf(err)).Str("digest", digest.String()).
			Err(err).Msg("couldn't upload blob")

		return err
	}

	return nil
}

func manifestsEqual(manifest1, manifest2 ispec.Manifest) bool {
	if manifest1.Config.Digest == manifest2.Config.Digest &&
		manifest1.Config.MediaType == manifest2.Config.MediaType &&
		manifest1.Config.Size == manifest2.Config.Size {
		if descriptorsEqual(manifest1.Layers, manifest2.Layers) {
			return true
		}
	}

	return false
}

func artifactsEqual(manifest1, manifest2 ispec.Artifact) bool {
	if manifest1.ArtifactType == manifest2.ArtifactType &&
		manifest1.MediaType == manifest2.MediaType {
		if descriptorsEqual(manifest1.Blobs, manifest2.Blobs) {
			return true
		}
	}

	return false
}

func artifactDescriptorsEqual(desc1, desc2 []artifactspec.Descriptor) bool {
	if len(desc1) != len(desc2) {
		return false
	}

	for id, desc := range desc1 {
		if desc.Digest != desc2[id].Digest ||
			desc.Size != desc2[id].Size ||
			desc.MediaType != desc2[id].MediaType ||
			desc.ArtifactType != desc2[id].ArtifactType {
			return false
		}
	}

	return true
}

func descriptorsEqual(desc1, desc2 []ispec.Descriptor) bool {
	if len(desc1) != len(desc2) {
		return false
	}

	for id, desc := range desc1 {
		if !descriptorEqual(desc, desc2[id]) {
			return false
		}
	}

	return true
}

func descriptorEqual(desc1, desc2 ispec.Descriptor) bool {
	if desc1.Size == desc2.Size &&
		desc1.Digest == desc2.Digest &&
		desc1.MediaType == desc2.MediaType &&
		desc1.Annotations[static.SignatureAnnotationKey] == desc2.Annotations[static.SignatureAnnotationKey] {
		return true
	}

	return false
}

func getNotationManifestsFromOCIRefs(ociRefs ispec.Index) []ispec.Descriptor {
	notaryManifests := []ispec.Descriptor{}

	for _, ref := range ociRefs.Manifests {
		if ref.ArtifactType == notreg.ArtifactTypeNotation {
			notaryManifests = append(notaryManifests, ref)
		}
	}

	return notaryManifests
}
