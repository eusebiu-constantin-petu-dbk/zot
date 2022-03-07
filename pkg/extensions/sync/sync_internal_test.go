package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	goSync "sync"
	"testing"
	"time"

	"github.com/containers/image/v5/docker"
	"github.com/containers/image/v5/docker/reference"
	"github.com/containers/image/v5/types"
	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/rs/zerolog"
	. "github.com/smartystreets/goconvey/convey"
	"zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/extensions/monitoring"
	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"
	"zotregistry.io/zot/pkg/test"
)

const (
	testImage    = "zot-test"
	testImageTag = "0.0.1"

	host = "127.0.0.1:45117"
)

func TestInjectSyncUtils(t *testing.T) {
	Convey("Inject errors in utils functions", t, func() {
		repositoryReference := fmt.Sprintf("%s/%s", host, testImage)
		ref, err := parseRepositoryReference(repositoryReference)
		So(err, ShouldBeNil)
		So(ref.Name(), ShouldEqual, repositoryReference)

		taggedRef, err := reference.WithTag(ref, "tag")
		So(err, ShouldBeNil)

		injected := test.InjectFailure(0)
		if injected {
			_, err = getImageTags(context.Background(), &types.SystemContext{}, taggedRef)
			So(err, ShouldNotBeNil)
		}

		injected = test.InjectFailure(0)
		_, _, err = getLocalContexts(log.NewLogger("debug", ""))
		if injected {
			So(err, ShouldNotBeNil)
		} else {
			So(err, ShouldBeNil)
		}

		storageDir, err := ioutil.TempDir("", "oci-dest-repo-test")
		if err != nil {
			panic(err)
		}

		defer os.RemoveAll(storageDir)

		log := log.Logger{Logger: zerolog.New(os.Stdout)}
		metrics := monitoring.NewMetricsServer(false, log)

		imageStore := storage.NewImageStore(storageDir, false, storage.DefaultGCDelay, false, false, log, metrics)

		injected = test.InjectFailure(0)
		_, _, err = getLocalImageRef(imageStore, testImage, testImageTag)
		if injected {
			So(err, ShouldNotBeNil)
		} else {
			So(err, ShouldBeNil)
		}
	})
}

func TestSyncInternal(t *testing.T) {
	Convey("Verify parseRepositoryReference func", t, func() {
		repositoryReference := fmt.Sprintf("%s/%s", host, testImage)
		ref, err := parseRepositoryReference(repositoryReference)
		So(err, ShouldBeNil)
		So(ref.Name(), ShouldEqual, repositoryReference)

		repositoryReference = fmt.Sprintf("%s/%s:tagged", host, testImage)
		_, err = parseRepositoryReference(repositoryReference)
		So(err, ShouldEqual, errors.ErrInvalidRepositoryName)

		repositoryReference = fmt.Sprintf("http://%s/%s", host, testImage)
		_, err = parseRepositoryReference(repositoryReference)
		So(err, ShouldNotBeNil)

		repositoryReference = fmt.Sprintf("docker://%s/%s", host, testImage)
		_, err = parseRepositoryReference(repositoryReference)
		So(err, ShouldNotBeNil)

		_, err = getFileCredentials("/path/to/inexistent/file")
		So(err, ShouldNotBeNil)

		tempFile, err := ioutil.TempFile("", "sync-credentials-")
		if err != nil {
			panic(err)
		}

		content := []byte(`{`)
		if err := ioutil.WriteFile(tempFile.Name(), content, 0o600); err != nil {
			panic(err)
		}

		_, err = getFileCredentials(tempFile.Name())
		So(err, ShouldNotBeNil)

		srcCtx := &types.SystemContext{}
		_, err = getImageTags(context.Background(), srcCtx, ref)
		So(err, ShouldNotBeNil)

		taggedRef, err := reference.WithTag(ref, "tag")
		So(err, ShouldBeNil)

		_, err = getImageTags(context.Background(), &types.SystemContext{}, taggedRef)
		So(err, ShouldNotBeNil)

		dockerRef, err := docker.NewReference(taggedRef)
		So(err, ShouldBeNil)

		So(getTagFromRef(dockerRef, log.NewLogger("debug", "")), ShouldNotBeNil)

		var tlsVerify bool
		updateDuration := time.Microsecond
		port := test.GetFreePort()
		baseURL := test.GetBaseURL(port)
		syncRegistryConfig := RegistryConfig{
			Content: []Content{
				{
					Prefix: testImage,
				},
			},
			URLs:         []string{baseURL},
			PollInterval: updateDuration,
			TLSVerify:    &tlsVerify,
			CertDir:      "",
		}

		cfg := Config{Registries: []RegistryConfig{syncRegistryConfig}, CredentialsFile: "/invalid/path/to/file"}

		So(Run(cfg, storage.StoreController{}, new(goSync.WaitGroup), log.NewLogger("debug", "")), ShouldNotBeNil)

		_, err = getFileCredentials("/invalid/path/to/file")
		So(err, ShouldNotBeNil)
	})

	Convey("Verify getLocalImageRef()", t, func() {
		storageDir, err := ioutil.TempDir("", "oci-dest-repo-test")
		if err != nil {
			panic(err)
		}

		defer os.RemoveAll(storageDir)

		log := log.Logger{Logger: zerolog.New(os.Stdout)}
		metrics := monitoring.NewMetricsServer(false, log)

		imageStore := storage.NewImageStore(storageDir, false, storage.DefaultGCDelay, false, false, log, metrics)

		err = os.Chmod(imageStore.RootDir(), 0o000)
		So(err, ShouldBeNil)

		_, _, err = getLocalImageRef(imageStore, testImage, testImageTag)
		So(err, ShouldNotBeNil)

		err = os.Chmod(imageStore.RootDir(), 0o755)
		So(err, ShouldBeNil)

		_, _, err = getLocalImageRef(imageStore, "zot][]321", "tag_tag][]")
		So(err, ShouldNotBeNil)
	})

	Convey("Test filterRepos()", t, func() {
		repos := []string{"repo", "repo1", "repo2", "repo/repo2", "repo/repo2/repo3/repo4"}
		contents := []Content{
			{
				Prefix: "repo",
			},
			{
				Prefix: "/repo/**",
			},
			{
				Prefix: "repo*",
			},
		}
		filteredRepos := filterRepos(repos, contents, log.NewLogger("", ""))
		So(filteredRepos[0], ShouldResemble, []string{"repo"})
		So(filteredRepos[1], ShouldResemble, []string{"repo/repo2", "repo/repo2/repo3/repo4"})
		So(filteredRepos[2], ShouldResemble, []string{"repo1", "repo2"})

		contents = []Content{
			{
				Prefix: "[repo%#@",
			},
		}

		filteredRepos = filterRepos(repos, contents, log.NewLogger("", ""))
		So(len(filteredRepos), ShouldEqual, 0)
	})

	Convey("Verify pushSyncedLocalImage func", t, func() {
		storageDir, err := ioutil.TempDir("", "oci-dest-repo-test")
		if err != nil {
			panic(err)
		}

		defer os.RemoveAll(storageDir)

		log := log.Logger{Logger: zerolog.New(os.Stdout)}
		metrics := monitoring.NewMetricsServer(false, log)

		imageStore := storage.NewImageStore(storageDir, false, storage.DefaultGCDelay, false, false, log, metrics)

		storeController := storage.StoreController{}
		storeController.DefaultStore = imageStore

		testRootDir := path.Join(imageStore.RootDir(), testImage, SyncBlobUploadDir)
		// testImagePath := path.Join(testRootDir, testImage)

		err = pushSyncedLocalImage(testImage, testImageTag, testRootDir, storeController, log)
		So(err, ShouldNotBeNil)

		err = os.MkdirAll(testRootDir, 0o755)
		if err != nil {
			panic(err)
		}

		err = test.CopyFiles("../../../test/data", testRootDir)
		if err != nil {
			panic(err)
		}

		testImageStore := storage.NewImageStore(testRootDir, false, storage.DefaultGCDelay, false, false, log, metrics)
		manifestContent, _, _, err := testImageStore.GetImageManifest(testImage, testImageTag)
		So(err, ShouldBeNil)

		var manifest ispec.Manifest

		if err := json.Unmarshal(manifestContent, &manifest); err != nil {
			panic(err)
		}

		if err := os.Chmod(storageDir, 0o000); err != nil {
			panic(err)
		}

		if os.Geteuid() != 0 {
			So(func() {
				_ = pushSyncedLocalImage(testImage, testImageTag, testRootDir, storeController, log)
			}, ShouldPanic)
		}

		if err := os.Chmod(storageDir, 0o755); err != nil {
			panic(err)
		}

		if err := os.Chmod(path.Join(testRootDir, testImage, "blobs", "sha256",
			manifest.Layers[0].Digest.Hex()), 0o000); err != nil {
			panic(err)
		}

		err = pushSyncedLocalImage(testImage, testImageTag, testRootDir, storeController, log)
		So(err, ShouldNotBeNil)

		if err := os.Chmod(path.Join(testRootDir, testImage, "blobs", "sha256",
			manifest.Layers[0].Digest.Hex()), 0o755); err != nil {
			panic(err)
		}

		cachedManifestConfigPath := path.Join(imageStore.RootDir(), testImage, SyncBlobUploadDir,
			testImage, "blobs", "sha256", manifest.Config.Digest.Hex())
		if err := os.Chmod(cachedManifestConfigPath, 0o000); err != nil {
			panic(err)
		}

		err = pushSyncedLocalImage(testImage, testImageTag, testRootDir, storeController, log)
		So(err, ShouldNotBeNil)

		if err := os.Chmod(cachedManifestConfigPath, 0o755); err != nil {
			panic(err)
		}

		manifestConfigPath := path.Join(imageStore.RootDir(), testImage, "blobs", "sha256", manifest.Config.Digest.Hex())
		if err := os.MkdirAll(manifestConfigPath, 0o000); err != nil {
			panic(err)
		}

		err = pushSyncedLocalImage(testImage, testImageTag, testRootDir, storeController, log)
		So(err, ShouldNotBeNil)

		if err := os.Remove(manifestConfigPath); err != nil {
			panic(err)
		}

		mDigest := godigest.FromBytes(manifestContent)

		manifestPath := path.Join(imageStore.RootDir(), testImage, "blobs", mDigest.Algorithm().String(), mDigest.Encoded())
		if err := os.MkdirAll(manifestPath, 0o000); err != nil {
			panic(err)
		}

		err = pushSyncedLocalImage(testImage, testImageTag, testRootDir, storeController, log)
		So(err, ShouldNotBeNil)
	})
}
