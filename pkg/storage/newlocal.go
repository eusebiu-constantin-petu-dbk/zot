package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	// Add filesystem driver support.
	"github.com/docker/distribution/registry/storage/driver"
	// Load filesystem driver.
	_ "github.com/docker/distribution/registry/storage/driver/filesystem"
	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/umoci"
	"github.com/opencontainers/umoci/oci/casext"
	artifactspec "github.com/oras-project/artifacts-spec/specs-go/v1"
	"github.com/rs/zerolog"
	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/extensions/monitoring"
	zlog "zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/test"
)

const (
	RLOCK       = "RLock"
	RWLOCK      = "RWLock"
	CacheDBName = "cache"
)

// LocalStorage provides the image storage operations.
type LocalStorage struct {
	rootDir string
	store   driver.StorageDriver
	lock    *sync.RWMutex
	log     zerolog.Logger
	// We must keep track of multi part uploads to s3, because the lib
	// which we are using doesn't cancel multiparts uploads
	// see: https://github.com/distribution/distribution/blob/main/registry/storage/driver/s3-aws/s3.go#L545
	multiPartUploads sync.Map
	metrics          monitoring.MetricServer
	cache            *Cache
	linter           Lint
	gc               bool
	dedupe           bool
	commit           bool
	gcDelay          time.Duration
}

func (is *LocalStorage) RootDir() string {
	return is.rootDir
}

func (is *LocalStorage) DirExists(d string) bool {
	return DirExists(d)
}

// NewObjectStorage returns a new image store backed by cloud storages.
// see https://github.com/docker/docker.github.io/tree/master/registry/storage-drivers
func NewImageStore(rootDir string, gc bool, gcDelay time.Duration, dedupe, commit bool,
	log zlog.Logger, metrics monitoring.MetricServer, linter Lint,
	store driver.StorageDriver,
) ImageStore {
	imgStore := &LocalStorage{
		rootDir:          rootDir,
		store:            store,
		lock:             &sync.RWMutex{},
		log:              log.With().Caller().Logger(),
		multiPartUploads: sync.Map{},
		metrics:          metrics,
		dedupe:           dedupe,
		linter:           linter,
		gcDelay:          gcDelay,
		gc:               gc,
		commit:           commit,
	}

	if dedupe {
		imgStore.cache = NewCache(rootDir, CacheDBName, true, log)
	}

	return imgStore
}

// RLock read-lock.
func (is *LocalStorage) RLock(lockStart *time.Time) {
	RLock(lockStart, is.lock)
}

// RUnlock read-unlock.
func (is *LocalStorage) RUnlock(lockStart *time.Time) {
	RUnlock(is, lockStart, is.lock, is.metrics)
}

// Lock write-lock.
func (is *LocalStorage) Lock(lockStart *time.Time) {
	Lock(lockStart, is.lock)
}

// Unlock write-unlock.
func (is *LocalStorage) Unlock(lockStart *time.Time) {
	Unlock(is, lockStart, is.lock, is.metrics)
}

func (is *LocalStorage) initRepo(name string) error {
	return InitRepo(is, is.store, name, is.log)
}

// func (is *LocalStorage) writeFile(filename string, data []byte) error {
// 	if !is.commit {
// 		return os.WriteFile(filename, data, DefaultFilePerms)
// 	}

// 	fhandle, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, DefaultFilePerms)
// 	if err != nil {
// 		return err
// 	}

// 	_, err = fhandle.Write(data)

// 	if err1 := test.Error(fhandle.Sync()); err1 != nil && err == nil {
// 		err = err1
// 		is.log.Error().Err(err).Str("filename", filename).Msg("unable to sync file")
// 	}

// 	if err1 := test.Error(fhandle.Close()); err1 != nil && err == nil {
// 		err = err1
// 	}

// 	return err
// }

// InitRepo creates an image repository under this store.
func (is *LocalStorage) InitRepo(name string) error {
	var lockLatency time.Time

	is.Lock(&lockLatency)
	defer is.Unlock(&lockLatency)

	return InitRepo(is, is.store, name, is.log)
}

// ValidateRepo validates that the repository layout is complaint with the OCI repo layout.
func (is *LocalStorage) ValidateRepo(name string) (bool, error) {
	return ValidateRepo(is.rootDir, is.store, name, is.log)
}

// GetRepositories returns a list of all the repositories under this store.
func (is *LocalStorage) GetRepositories() ([]string, error) {
	return GetRepositories(is, is.store)
}

// GetImageTags returns a list of image tags available in the specified repository.
func (is *LocalStorage) GetImageTags(repo string) ([]string, error) {
	return GetImageTags(is, is.store, repo, is.log)
}

// GetImageManifest returns the image manifest of an image in the specific repository.
func (is *LocalStorage) GetImageManifest(repo, reference string) ([]byte, string, string, error) {
	return GetImageManifest(is, is.store, repo, reference, is.metrics, is.log)
}

// PutImageManifest adds an image manifest to the repository.
func (is *LocalStorage) PutImageManifest(repo, reference, mediaType string, //nolint: gocyclo
	body []byte) (string, error,
) {
	return PutImageManifest(is, is.store, repo, reference, mediaType, body, is.linter, is.metrics, is.log)
}

// DeleteImageManifest deletes the image manifest from the repository.
func (is *LocalStorage) DeleteImageManifest(repo, reference string) error {
	return DeleteImageManifest(is, is.store, repo, reference, is.metrics, is.log)
}

// BlobUploadPath returns the upload path for a blob in this store.
func (is *LocalStorage) BlobUploadPath(repo, uuid string) string {
	return BlobUploadPath(is.rootDir, repo, uuid)
}

// NewBlobUpload returns the unique ID for an upload in progress.
func (is *LocalStorage) NewBlobUpload(repo string) (string, error) {
	return NewBlobUpload(is, is.store, repo, is.log)
}

// GetBlobUpload returns the current size of a blob upload.
func (is *LocalStorage) GetBlobUpload(repo, uuid string) (int64, error) {
	return GetBlobUpload(is, is.store, is.multiPartUploads, repo, uuid)
}

// PutBlobChunkStreamed appends another chunk of data to the specified blob. It returns
// the number of actual bytes to the blob.
func (is *LocalStorage) PutBlobChunkStreamed(repo, uuid string, body io.Reader) (int64, error) {
	return PutBlobChunkStreamed(is, is.store, is.multiPartUploads, repo, uuid, body, is.log)
}

// PutBlobChunk writes another chunk of data to the specified blob. It returns
// the number of actual bytes to the blob.
func (is *LocalStorage) PutBlobChunk(repo, uuid string, from, to int64,
	body io.Reader,
) (int64, error) {
	return PutBlobChunk(is, is.store, is.multiPartUploads, repo, uuid, from, to, body, is.log)
}

// BlobUploadInfo returns the current blob size in bytes.
func (is *LocalStorage) BlobUploadInfo(repo, uuid string) (int64, error) {
	return BlobUploadInfo(is, is.store, is.multiPartUploads, repo, uuid, is.log)
}

// FinishBlobUpload finalizes the blob upload and moves blob the repository.
func (is *LocalStorage) FinishBlobUpload(repo, uuid string, body io.Reader, digest string) error {
	cache := is.cache != nil
	return FinishBlobUpload(is, is.store, is.multiPartUploads, repo, uuid, body, digest, is.dedupe, cache, is.log)
}

// FullBlobUpload handles a full blob upload, and no partial session is created.
func (is *LocalStorage) FullBlobUpload(repo string, body io.Reader, digest string) (string, int64, error) {
	cache := is.cache != nil
	return FullBlobUpload(is, is.store, repo, body, digest, is.dedupe, cache, is.log)
}

func (is *LocalStorage) DedupeBlob(src string, dstDigest godigest.Digest, dst string) error {
retry:
	is.log.Debug().Str("src", src).Str("dstDigest", dstDigest.String()).Str("dst", dst).Msg("dedupe: enter")

	dstRecord, err := is.cache.GetBlob(dstDigest.String())

	if err != nil && !errors.Is(err, zerr.ErrCacheMiss) {
		is.log.Error().Err(err).Str("blobPath", dst).Msg("dedupe: unable to lookup blob record")

		return err
	}

	if dstRecord == "" {
		// cache record doesn't exist, so first disk and cache entry for this diges
		if err := is.cache.PutBlob(dstDigest.String(), dst); err != nil {
			is.log.Error().Err(err).Str("blobPath", dst).Msg("dedupe: unable to insert blob record")

			return err
		}

		// move the blob from uploads to final dest
		if err := is.store.Move(context.Background(), src, dst); err != nil {
			is.log.Error().Err(err).Str("src", src).Str("dst", dst).Msg("dedupe: unable to rename blob")

			return err
		}

		is.log.Debug().Str("src", src).Str("dst", dst).Msg("dedupe: rename")
	} else {
		// cache record exists, but due to GC and upgrades from older versions,
		// disk content and cache records may go out of sync
		dstRecord = path.Join(is.rootDir, dstRecord)

		dstRecordFi, err := os.Stat(dstRecord)
		if err != nil {
			is.log.Error().Err(err).Str("blobPath", dstRecord).Msg("dedupe: unable to stat")
			// the actual blob on disk may have been removed by GC, so sync the cache
			if err := is.cache.DeleteBlob(dstDigest.String(), dstRecord); err != nil {
				//nolint:lll // gofumpt conflicts with lll
				is.log.Error().Err(err).Str("dstDigest", dstDigest.String()).Str("dst", dst).Msg("dedupe: unable to delete blob record")

				return err
			}

			goto retry
		}

		dstFi, err := os.Stat(dst)
		if err != nil && !os.IsNotExist(err) {
			is.log.Error().Err(err).Str("blobPath", dstRecord).Msg("dedupe: unable to stat")

			return err
		}

		if !os.SameFile(dstFi, dstRecordFi) {
			// blob lookup cache out of sync with actual disk contents
			if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
				is.log.Error().Err(err).Str("dst", dst).Msg("dedupe: unable to remove blob")

				return err
			}

			is.log.Debug().Str("blobPath", dst).Str("dstRecord", dstRecord).Msg("dedupe: creating hard link")

			if err := os.Link(dstRecord, dst); err != nil {
				is.log.Error().Err(err).Str("blobPath", dst).Str("link", dstRecord).Msg("dedupe: unable to hard link")

				return err
			}
		}

		if err := os.Remove(src); err != nil {
			is.log.Error().Err(err).Str("src", src).Msg("dedupe: uname to remove blob")

			return err
		}

		is.log.Debug().Str("src", src).Msg("dedupe: remove")
	}

	return nil
}

func (is *LocalStorage) RunGCRepo(repo string) {
	is.log.Info().Msg(fmt.Sprintf("executing GC of orphaned blobs for %s", path.Join(is.RootDir(), repo)))

	if err := is.gcRepo(repo); err != nil {
		errMessage := fmt.Sprintf("error while running GC for %s", path.Join(is.RootDir(), repo))
		is.log.Error().Err(err).Msg(errMessage)
	}

	is.log.Info().Msg(fmt.Sprintf("GC completed for %s", path.Join(is.RootDir(), repo)))
}

// DeleteBlobUpload deletes an existing blob upload that is currently in progress.
func (is *LocalStorage) DeleteBlobUpload(repo, uuid string) error {
	return nil
}

// BlobPath returns the repository path of a blob.
func (is *LocalStorage) BlobPath(repo string, digest godigest.Digest) string {
	return path.Join(is.rootDir, repo, "blobs", digest.Algorithm().String(), digest.Encoded())
}

// CheckBlob verifies a blob and returns true if the blob is correct.
func (is *LocalStorage) CheckBlob(repo, digest string) (bool, int64, error) {
	var lockLatency time.Time

	parsedDigest, err := godigest.Parse(digest)
	if err != nil {
		is.log.Error().Err(err).Str("digest", digest).Msg("failed to parse digest")

		return false, -1, zerr.ErrBadBlobDigest
	}

	blobPath := is.BlobPath(repo, parsedDigest)

	if is.dedupe && is.cache != nil {
		is.Lock(&lockLatency)
		defer is.Unlock(&lockLatency)
	} else {
		is.RLock(&lockLatency)
		defer is.RUnlock(&lockLatency)
	}

	binfo, err := os.Stat(blobPath)
	if err == nil {
		is.log.Debug().Str("blob path", blobPath).Msg("blob path found")

		return true, binfo.Size(), nil
	}

	is.log.Error().Err(err).Str("blob", blobPath).Msg("failed to stat blob")

	// Check blobs in cache
	dstRecord, err := is.checkCacheBlob(digest)
	if err != nil {
		is.log.Error().Err(err).Str("digest", digest).Msg("cache: not found")

		return false, -1, zerr.ErrBlobNotFound
	}

	// If found copy to location
	blobSize, err := is.copyBlob(repo, blobPath, dstRecord)
	if err != nil {
		return false, -1, zerr.ErrBlobNotFound
	}

	if err := is.cache.PutBlob(digest, blobPath); err != nil {
		is.log.Error().Err(err).Str("blobPath", blobPath).Msg("dedupe: unable to insert blob record")

		return false, -1, err
	}

	return true, blobSize, nil
}

func (is *LocalStorage) checkCacheBlob(digest string) (string, error) {
	if !is.dedupe || is.cache == nil {
		return "", zerr.ErrBlobNotFound
	}

	dstRecord, err := is.cache.GetBlob(digest)
	if err != nil {
		return "", err
	}

	dstRecord = path.Join(is.rootDir, dstRecord)

	is.log.Debug().Str("digest", digest).Str("dstRecord", dstRecord).Msg("cache: found dedupe record")

	return dstRecord, nil
}

func (is *LocalStorage) copyBlob(repo string, blobPath string, dstRecord string) (int64, error) {
	if err := is.InitRepo(repo); err != nil {
		is.log.Error().Err(err).Str("repo", repo).Msg("unable to initialize an empty repo")

		return -1, err
	}

	_ = ensureDir(filepath.Dir(blobPath), is.log)

	if err := os.Link(dstRecord, blobPath); err != nil {
		is.log.Error().Err(err).Str("blobPath", blobPath).Str("link", dstRecord).Msg("dedupe: unable to hard link")

		return -1, zerr.ErrBlobNotFound
	}

	binfo, err := os.Stat(blobPath)
	if err == nil {
		return binfo.Size(), nil
	}

	return -1, zerr.ErrBlobNotFound
}

// GetBlob returns a stream to read the blob.
// blob selector instead of directly downloading the blob.
func (is *LocalStorage) GetBlob(repo, digest, mediaType string) (io.ReadCloser, int64, error) {
	var lockLatency time.Time

	parsedDigest, err := godigest.Parse(digest)
	if err != nil {
		is.log.Error().Err(err).Str("digest", digest).Msg("failed to parse digest")

		return nil, -1, zerr.ErrBadBlobDigest
	}

	blobPath := is.BlobPath(repo, parsedDigest)

	is.RLock(&lockLatency)
	defer is.RUnlock(&lockLatency)

	binfo, err := os.Stat(blobPath)
	if err != nil {
		is.log.Error().Err(err).Str("blob", blobPath).Msg("failed to stat blob")

		return nil, -1, zerr.ErrBlobNotFound
	}

	blobReadCloser, err := os.Open(blobPath)
	if err != nil {
		is.log.Error().Err(err).Str("blob", blobPath).Msg("failed to open blob")

		return nil, -1, err
	}

	// The caller function is responsible for calling Close()
	return blobReadCloser, binfo.Size(), nil
}

func (is *LocalStorage) GetBlobContent(repo, digest string) ([]byte, error) {
	return []byte{}, nil
}

func (is *LocalStorage) GetReferrers(repo, digest, artifactType string) ([]artifactspec.Descriptor, error) {
	var lockLatency time.Time

	dir := path.Join(is.rootDir, repo)
	if !is.DirExists(dir) {
		return nil, zerr.ErrRepoNotFound
	}

	gdigest, err := godigest.Parse(digest)
	if err != nil {
		is.log.Error().Err(err).Str("digest", digest).Msg("failed to parse digest")

		return nil, zerr.ErrBadBlobDigest
	}

	is.RLock(&lockLatency)
	defer is.RUnlock(&lockLatency)

	buf, err := os.ReadFile(path.Join(dir, "index.json"))
	if err != nil {
		is.log.Error().Err(err).Str("dir", dir).Msg("failed to read index.json")

		if os.IsNotExist(err) {
			return nil, zerr.ErrRepoNotFound
		}

		return nil, err
	}

	var index ispec.Index
	if err := json.Unmarshal(buf, &index); err != nil {
		is.log.Error().Err(err).Str("dir", dir).Msg("invalid JSON")

		return nil, err
	}

	found := false

	result := []artifactspec.Descriptor{}

	for _, manifest := range index.Manifests {
		if manifest.MediaType != artifactspec.MediaTypeArtifactManifest {
			continue
		}

		p := path.Join(dir, "blobs", manifest.Digest.Algorithm().String(), manifest.Digest.Encoded())

		buf, err = os.ReadFile(p)

		if err != nil {
			is.log.Error().Err(err).Str("blob", p).Msg("failed to read manifest")

			if os.IsNotExist(err) {
				return nil, zerr.ErrManifestNotFound
			}

			return nil, err
		}

		var artManifest artifactspec.Manifest
		if err := json.Unmarshal(buf, &artManifest); err != nil {
			is.log.Error().Err(err).Str("dir", dir).Msg("invalid JSON")

			return nil, err
		}

		if artifactType != artManifest.ArtifactType || gdigest != artManifest.Subject.Digest {
			continue
		}

		result = append(result, artifactspec.Descriptor{
			MediaType:    manifest.MediaType,
			ArtifactType: artManifest.ArtifactType,
			Digest:       manifest.Digest,
			Size:         manifest.Size,
			Annotations:  manifest.Annotations,
		})

		found = true
	}

	if !found {
		return nil, zerr.ErrManifestNotFound
	}

	return result, nil
}

func (is *LocalStorage) GetIndexContent(repo string) ([]byte, error) {
	return GetIndexContent(is, is.store, repo, is.log)
}

// DeleteBlob removes the blob from the repository.
func (is *LocalStorage) DeleteBlob(repo, digest string) error {
	var lockLatency time.Time

	dgst, err := godigest.Parse(digest)
	if err != nil {
		is.log.Error().Err(err).Str("digest", digest).Msg("failed to parse digest")

		return zerr.ErrBlobNotFound
	}

	blobPath := is.BlobPath(repo, dgst)

	is.Lock(&lockLatency)
	defer is.Unlock(&lockLatency)

	_, err = os.Stat(blobPath)
	if err != nil {
		is.log.Error().Err(err).Str("blob", blobPath).Msg("failed to stat blob")

		return zerr.ErrBlobNotFound
	}

	if is.cache != nil {
		if err := is.cache.DeleteBlob(digest, blobPath); err != nil {
			is.log.Error().Err(err).Str("digest", digest).Str("blobPath", blobPath).Msg("unable to remove blob path from cache")

			return err
		}
	}

	if err := os.Remove(blobPath); err != nil {
		is.log.Error().Err(err).Str("blobPath", blobPath).Msg("unable to remove blob path")

		return err
	}

	return nil
}

func (is *LocalStorage) gcRepo(repo string) error {
	dir := path.Join(is.RootDir(), repo)

	var lockLatency time.Time

	is.Lock(&lockLatency)

	err := is.GarbageCollect(dir, repo)

	is.Unlock(&lockLatency)

	if err != nil {
		return err
	}

	return nil
}

func (is *LocalStorage) GarbageCollect(dir string, repo string) error {
	if !is.gc {
		return nil
	}

	oci, err := umoci.OpenLayout(dir)
	if err := test.Error(err); err != nil {
		return err
	}
	defer oci.Close()

	err = oci.GC(context.Background(), ifOlderThan(is, repo, is.gcDelay))
	if err := test.Error(err); err != nil {
		return err
	}

	return nil
}

func ensureDir(dir string, log zerolog.Logger) error {
	if err := os.MkdirAll(dir, DefaultDirPerms); err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("unable to create dir")

		return err
	}

	return nil
}

func ifOlderThan(imgStore *LocalStorage, repo string, delay time.Duration) casext.GCPolicy {
	return func(ctx context.Context, digest godigest.Digest) (bool, error) {
		blobPath := imgStore.BlobPath(repo, digest)

		fi, err := os.Stat(blobPath)
		if err != nil {
			return false, err
		}

		if fi.ModTime().Add(delay).After(time.Now()) {
			return false, nil
		}

		imgStore.log.Info().Str("digest", digest.String()).Str("blobPath", blobPath).Msg("perform GC on blob")

		return true, nil
	}
}

func ValidateHardLink(rootDir string) error {
	if err := os.MkdirAll(rootDir, DefaultDirPerms); err != nil {
		return err
	}

	err := os.WriteFile(path.Join(rootDir, "hardlinkcheck.txt"),
		[]byte("check whether hardlinks work on filesystem"), DefaultFilePerms)
	if err != nil {
		return err
	}

	err = os.Link(path.Join(rootDir, "hardlinkcheck.txt"), path.Join(rootDir, "duphardlinkcheck.txt"))
	if err != nil {
		// Remove hardlinkcheck.txt if hardlink fails
		zerr := os.RemoveAll(path.Join(rootDir, "hardlinkcheck.txt"))
		if zerr != nil {
			return zerr
		}

		return err
	}

	err = os.RemoveAll(path.Join(rootDir, "hardlinkcheck.txt"))
	if err != nil {
		return err
	}

	return os.RemoveAll(path.Join(rootDir, "duphardlinkcheck.txt"))
}

func DirExists(d string) bool {
	if !utf8.ValidString(d) {
		return false
	}

	fileInfo, err := os.Stat(d)
	if err != nil {
		if e, ok := err.(*fs.PathError); ok && errors.Is(e.Err, syscall.ENAMETOOLONG) || //nolint: errorlint
			errors.Is(e.Err, syscall.EINVAL) {
			return false
		}
	}

	if err != nil && os.IsNotExist(err) {
		return false
	}

	if !fileInfo.IsDir() {
		return false
	}

	return true
}
