//go:build sync
// +build sync

package references

import (
	"encoding/json"
	"errors"
	"testing"

	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	artifactspec "github.com/oras-project/artifacts-spec/specs-go/v1"
	. "github.com/smartystreets/goconvey/convey"

	zerr "zotregistry.io/zot/errors"
	client "zotregistry.io/zot/pkg/extensions/sync/httpclient"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"
	"zotregistry.io/zot/pkg/test/mocks"
)

var errRef = errors.New("err")

func TestCosign(t *testing.T) {
	Convey("trigger errors", t, func() {
		cfg := client.Config{
			URL:       "url",
			TLSVerify: false,
		}

		client, err := client.New(cfg, log.NewLogger("debug", ""))
		So(err, ShouldBeNil)

		cosign := NewCosignReference(client, storage.StoreController{DefaultStore: mocks.MockedImageStore{
			GetImageManifestFn: func(repo, reference string) ([]byte, godigest.Digest, string, error) {
				return []byte{}, "", "", errRef
			},
		}}, nil, log.NewLogger("debug", ""))

		ok, err := cosign.canSkipReferences("repo", "tag", nil)
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)

		// trigger GetImageManifest err
		ok, err = cosign.canSkipReferences("repo", "tag", &ispec.Manifest{MediaType: ispec.MediaTypeImageManifest})
		So(err, ShouldNotBeNil)
		So(ok, ShouldBeFalse)

		cosign = NewCosignReference(client, storage.StoreController{DefaultStore: mocks.MockedImageStore{
			GetImageManifestFn: func(repo, reference string) ([]byte, godigest.Digest, string, error) {
				return []byte{}, "", "", nil
			},
		}}, nil, log.NewLogger("debug", ""))

		// trigger unmarshal err
		ok, err = cosign.canSkipReferences("repo", "tag", &ispec.Manifest{MediaType: ispec.MediaTypeImageManifest})
		So(err, ShouldNotBeNil)
		So(ok, ShouldBeFalse)
	})
}

func TestOci(t *testing.T) {
	Convey("trigger errors", t, func() {
		cfg := client.Config{
			URL:       "url",
			TLSVerify: false,
		}

		client, err := client.New(cfg, log.NewLogger("debug", ""))
		So(err, ShouldBeNil)

		oci := NewOciReferences(client, storage.StoreController{DefaultStore: mocks.MockedImageStore{
			GetReferrersFn: func(repo string, digest godigest.Digest, artifactTypes []string) (ispec.Index, error) {
				return ispec.Index{}, zerr.ErrManifestNotFound
			},
		}}, nil, log.NewLogger("debug", ""))

		ok, err := oci.IsSigned("repo", "")
		So(err, ShouldNotBeNil)
		So(ok, ShouldBeFalse)

		// trigger GetReferrers err
		ok, err = oci.canSkipReferences("repo", "tag", ispec.Index{Manifests: []ispec.Descriptor{{Digest: "digest1"}}})
		So(err, ShouldBeNil)
		So(ok, ShouldBeFalse)
	})
}

func TestOciArtifact(t *testing.T) {
	Convey("trigger errors", t, func() {
		cfg := client.Config{
			URL:       "url",
			TLSVerify: false,
		}

		client, err := client.New(cfg, log.NewLogger("debug", ""))
		So(err, ShouldBeNil)

		ociArtifact := NewOciArtifact(client, storage.StoreController{DefaultStore: mocks.MockedImageStore{
			GetImageManifestFn: func(repo, reference string) ([]byte, godigest.Digest, string, error) {
				return []byte{}, "", "", nil
			},
		}}, nil, log.NewLogger("debug", ""))

		// trigger Unmarshal err
		ok, err := ociArtifact.canSkipReferences("repo", "tag", ispec.Artifact{})
		So(err, ShouldNotBeNil)
		So(ok, ShouldBeFalse)

		artifact := ispec.Artifact{
			MediaType:    "artifact",
			ArtifactType: "artifact",
			Blobs: []ispec.Descriptor{
				{
					MediaType: "blob",
					Digest:    "digest1",
				},
			},
		}
		artifactBuf, err := json.Marshal(artifact)
		So(err, ShouldBeNil)

		ociArtifact.storeController = storage.StoreController{
			DefaultStore: mocks.MockedImageStore{
				GetImageManifestFn: func(repo, reference string) ([]byte, godigest.Digest, string, error) {
					return artifactBuf, godigest.FromBytes(artifactBuf), "artifact", nil
				},
			},
		}

		ok, err = ociArtifact.canSkipReferences("repo", "tag", artifact)
		So(ok, ShouldBeTrue)
		So(err, ShouldBeNil)

		// trigger artifactsEqual err
		ok, err = ociArtifact.canSkipReferences("repo", "tag", ispec.Artifact{})
		So(ok, ShouldBeFalse)
		So(err, ShouldBeNil)

		// trigger Sync Unmarshal err
		err = ociArtifact.Sync("repo", "remoteRepo", "1.0", []byte{})
		So(err, ShouldNotBeNil)

		err = ociArtifact.Sync("repo", "remoteRepo", "1.0", artifactBuf)
		So(err, ShouldBeNil)

		artifact2 := ispec.Artifact{
			MediaType:    "artifact",
			ArtifactType: "artifact",
			Blobs: []ispec.Descriptor{
				{
					MediaType: "blob",
					Digest:    "digest2",
				},
			},
		}
		artifact2Buf, err := json.Marshal(artifact2)
		So(err, ShouldBeNil)

		err = ociArtifact.Sync("repo", "remoteRepo", "1.0", artifact2Buf)
		So(err, ShouldNotBeNil)
	})
}

func TestORAS(t *testing.T) {
	Convey("trigger errors", t, func() {
		cfg := client.Config{
			URL:       "url",
			TLSVerify: false,
		}

		client, err := client.New(cfg, log.NewLogger("debug", ""))
		So(err, ShouldBeNil)

		orasRefs := []artifactspec.Descriptor{
			{
				MediaType:    "oras",
				ArtifactType: "oras",
				Digest:       "digest1",
			},
		}

		oras := NewORASReferences(client, storage.StoreController{DefaultStore: mocks.MockedImageStore{
			GetOrasReferrersFn: func(repo string, digest godigest.Digest, artifactType string) (
				[]artifactspec.Descriptor, error,
			) {
				return orasRefs, nil
			},
		}}, nil, log.NewLogger("debug", ""))

		// trigger artifactDescriptors not equal
		ok, err := oras.canSkipReferences("repo", "tag", ReferenceList{[]artifactspec.Descriptor{
			{
				MediaType:    "oras",
				ArtifactType: "oras",
				Digest:       "digest2",
			},
		}})
		So(err, ShouldBeNil)
		So(ok, ShouldBeFalse)
	})
}

func TestCompareManifest(t *testing.T) {
	testCases := []struct {
		manifest1 ispec.Manifest
		manifest2 ispec.Manifest
		expected  bool
	}{
		{
			manifest1: ispec.Manifest{
				Config: ispec.Descriptor{
					Digest: "digest1",
				},
			},
			manifest2: ispec.Manifest{
				Config: ispec.Descriptor{
					Digest: "digest2",
				},
			},
			expected: false,
		},
		{
			manifest1: ispec.Manifest{
				Config: ispec.Descriptor{
					Digest: "digest",
				},
			},
			manifest2: ispec.Manifest{
				Config: ispec.Descriptor{
					Digest: "digest",
				},
			},
			expected: true,
		},
		{
			manifest1: ispec.Manifest{
				Layers: []ispec.Descriptor{{
					Digest: "digest",
					Size:   1,
				}},
			},
			manifest2: ispec.Manifest{
				Layers: []ispec.Descriptor{{
					Digest: "digest",
					Size:   1,
				}},
			},
			expected: true,
		},
		{
			manifest1: ispec.Manifest{
				Layers: []ispec.Descriptor{{
					Digest: "digest1",
					Size:   1,
				}},
			},
			manifest2: ispec.Manifest{
				Layers: []ispec.Descriptor{{
					Digest: "digest2",
					Size:   2,
				}},
			},
			expected: false,
		},
		{
			manifest1: ispec.Manifest{
				Layers: []ispec.Descriptor{
					{
						Digest: "digest",
						Size:   1,
					},
					{
						Digest: "digest1",
						Size:   1,
					},
				},
			},
			manifest2: ispec.Manifest{
				Layers: []ispec.Descriptor{{
					Digest: "digest",
					Size:   1,
				}},
			},
			expected: false,
		},
		{
			manifest1: ispec.Manifest{
				Layers: []ispec.Descriptor{
					{
						Digest: "digest1",
						Size:   1,
					},
					{
						Digest: "digest2",
						Size:   2,
					},
				},
			},
			manifest2: ispec.Manifest{
				Layers: []ispec.Descriptor{
					{
						Digest: "digest1",
						Size:   1,
					},
					{
						Digest: "digest2",
						Size:   2,
					},
				},
			},
			expected: true,
		},
		{
			manifest1: ispec.Manifest{
				Layers: []ispec.Descriptor{
					{
						Digest: "digest",
						Size:   1,
					},
					{
						Digest: "digest1",
						Size:   1,
					},
				},
			},
			manifest2: ispec.Manifest{
				Layers: []ispec.Descriptor{
					{
						Digest: "digest",
						Size:   1,
					},
					{
						Digest: "digest2",
						Size:   2,
					},
				},
			},
			expected: false,
		},
	}

	Convey("Test manifestsEqual()", t, func() {
		for _, test := range testCases {
			actualResult := manifestsEqual(test.manifest1, test.manifest2)
			So(actualResult, ShouldEqual, test.expected)
		}
	})
}

func TestCompareArtifactRefs(t *testing.T) {
	testCases := []struct {
		refs1    []artifactspec.Descriptor
		refs2    []artifactspec.Descriptor
		expected bool
	}{
		{
			refs1: []artifactspec.Descriptor{
				{
					Digest: "digest1",
				},
			},
			refs2: []artifactspec.Descriptor{
				{
					Digest: "digest2",
				},
			},
			expected: false,
		},
		{
			refs1: []artifactspec.Descriptor{
				{
					Digest: "digest",
				},
			},
			refs2: []artifactspec.Descriptor{
				{
					Digest: "digest",
				},
			},
			expected: true,
		},
		{
			refs1: []artifactspec.Descriptor{
				{
					Digest: "digest",
				},
				{
					Digest: "digest2",
				},
			},
			refs2: []artifactspec.Descriptor{
				{
					Digest: "digest",
				},
			},
			expected: false,
		},
		{
			refs1: []artifactspec.Descriptor{
				{
					Digest: "digest1",
				},
				{
					Digest: "digest2",
				},
			},
			refs2: []artifactspec.Descriptor{
				{
					Digest: "digest1",
				},
				{
					Digest: "digest2",
				},
			},
			expected: true,
		},
		{
			refs1: []artifactspec.Descriptor{
				{
					Digest: "digest",
				},
				{
					Digest: "digest1",
				},
			},
			refs2: []artifactspec.Descriptor{
				{
					Digest: "digest1",
				},
				{
					Digest: "digest2",
				},
			},
			expected: false,
		},
	}

	Convey("Test manifestsEqual()", t, func() {
		for _, test := range testCases {
			actualResult := artifactDescriptorsEqual(test.refs1, test.refs2)
			So(actualResult, ShouldEqual, test.expected)
		}
	})
}

func TestCompareArtifactManifests(t *testing.T) {
	testCases := []struct {
		refs1    ispec.Artifact
		refs2    ispec.Artifact
		expected bool
	}{
		{
			refs1: ispec.Artifact{
				MediaType:    "mediatype",
				ArtifactType: "signature",
				Blobs: []ispec.Descriptor{
					{
						Digest: "digest1",
					},
				},
			},
			refs2: ispec.Artifact{
				MediaType:    "mediatype",
				ArtifactType: "signature",
				Blobs: []ispec.Descriptor{
					{
						Digest: "digest1",
					},
				},
			},
			expected: true,
		},
		{
			refs1: ispec.Artifact{
				MediaType:    "mediatype",
				ArtifactType: "signature",
				Blobs: []ispec.Descriptor{
					{
						Digest: "digest1",
					},
				},
			},
			refs2: ispec.Artifact{
				MediaType:    "mediatype",
				ArtifactType: "signature",
				Blobs: []ispec.Descriptor{
					{
						Digest: "digest2",
					},
				},
			},
			expected: false,
		},
	}

	Convey("Test artifactsEqual()", t, func() {
		for _, test := range testCases {
			actualResult := artifactsEqual(test.refs1, test.refs2)
			So(actualResult, ShouldEqual, test.expected)
		}
	})
}
