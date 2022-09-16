package s3

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"time"

	// Add s3 support.
	"github.com/docker/distribution/registry/storage/driver"
	// Load s3 driver.
	_ "github.com/docker/distribution/registry/storage/driver/s3-aws"
	godigest "github.com/opencontainers/go-digest"
	artifactspec "github.com/oras-project/artifacts-spec/specs-go/v1"
	"github.com/rs/zerolog"
	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/extensions/monitoring"
	zlog "zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"
	"zotregistry.io/zot/pkg/test"
)

const (
	RLOCK       = "RLock"
	RWLOCK      = "RWLock"
	CacheDBName = "s3_cache"
)

// ObjectStorage provides the image storage operations.
type ObjectStorage struct {
	rootDir string
	store   driver.StorageDriver
	lock    *sync.RWMutex
	log     zerolog.Logger
	// We must keep track of multi part uploads to s3, because the lib
	// which we are using doesn't cancel multiparts uploads
	// see: https://github.com/distribution/distribution/blob/main/registry/storage/driver/s3-aws/s3.go#L545
	multiPartUploads sync.Map
	gcDelay          time.Duration
	metrics          monitoring.MetricServer
	cache            *storage.Cache
	dedupe           bool
	linter           storage.Lint
	commit           bool
	gc               bool
}

func (is *ObjectStorage) RootDir() string {
	return ""
}

func (is *ObjectStorage) DirExists(d string) bool {
	return false
}

type Storage struct {
	storage.Base
}

// NewObjectStorage returns a new image store backed by cloud storages.
// see https://github.com/docker/docker.github.io/tree/master/registry/storage-drivers
func NewImageStore(rootDir string, cacheDir string, gc bool, gcDelay time.Duration, dedupe, commit bool,
	log zlog.Logger, metrics monitoring.MetricServer, linter storage.Lint,
	store driver.StorageDriver,
) storage.ImageStore {
	imgStore := &ObjectStorage{
		rootDir:          rootDir,
		store:            store,
		lock:             &sync.RWMutex{},
		log:              log.With().Caller().Logger(),
		multiPartUploads: sync.Map{},
		metrics:          metrics,
		dedupe:           dedupe,
		linter:           linter,
		gcDelay:          gcDelay, // not implemented for s3
		gc:               false,   // not implemented for s3
		commit:           commit,
	}

	cachePath := path.Join(cacheDir, CacheDBName+storage.DBExtensionName)

	if dedupe {
		imgStore.cache = storage.NewCache(cacheDir, CacheDBName, false, log)
	} else {
		// if dedupe was used in previous runs use it to serve blobs correctly
		if _, err := os.Stat(cachePath); err == nil {
			log.Info().Str("cache path", cachePath).Msg("found cache database")
			imgStore.cache = storage.NewCache(cacheDir, CacheDBName, false, log)
		}
	}

	return &Storage{
		Base: storage.Base{
			ImageStore:       imgStore,
			RootDirectory:    rootDir,
			Store:            store,
			Locker:           imgStore.lock,
			MultiPartUploads: imgStore.multiPartUploads,
			Metrics:          metrics,
			Dedupe:           dedupe,
			Linter:           linter,
			Cache:            imgStore.cache,
			Commit:           commit,
			GcDelay:          gcDelay,
			Gc:               false,
		},
	}
}

// RLock read-lock.
func (is *ObjectStorage) RLock(lockStart *time.Time) {
}

// RUnlock read-unlock.
func (is *ObjectStorage) RUnlock(lockStart *time.Time) {
}

// Lock write-lock.
func (is *ObjectStorage) Lock(lockStart *time.Time) {
}

// Unlock write-unlock.
func (is *ObjectStorage) Unlock(lockStart *time.Time) {
}

func (is *ObjectStorage) initRepo(name string) error {
	return nil
}

// InitRepo creates an image repository under this store.
func (is *ObjectStorage) InitRepo(name string) error {
	return nil
}

// ValidateRepo validates that the repository layout is complaint with the OCI repo layout.
func (is *ObjectStorage) ValidateRepo(name string) (bool, error) {
	return false, nil
}

// GetRepositories returns a list of all the repositories under this store.
func (is *ObjectStorage) GetRepositories() ([]string, error) {
	return []string{}, nil
}

// GetImageTags returns a list of image tags available in the specified repository.
func (is *ObjectStorage) GetImageTags(repo string) ([]string, error) {
	return []string{}, nil
}

// GetImageManifest returns the image manifest of an image in the specific repository.
func (is *ObjectStorage) GetImageManifest(repo, reference string) ([]byte, string, string, error) {
	return []byte{}, "", "", nil
}

// PutImageManifest adds an image manifest to the repository.
func (is *ObjectStorage) PutImageManifest(repo, reference, mediaType string, //nolint: gocyclo
	body []byte) (string, error,
) {
	return "", nil
}

// DeleteImageManifest deletes the image manifest from the repository.
func (is *ObjectStorage) DeleteImageManifest(repo, reference string) error {
	return nil
}

// BlobUploadPath returns the upload path for a blob in this store.
func (is *ObjectStorage) BlobUploadPath(repo, uuid string) string {
	return ""
}

// NewBlobUpload returns the unique ID for an upload in progress.
func (is *ObjectStorage) NewBlobUpload(repo string) (string, error) {
	return "", nil
}

// GetBlobUpload returns the current size of a blob upload.
func (is *ObjectStorage) GetBlobUpload(repo, uuid string) (int64, error) {
	return 0, nil
}

// PutBlobChunkStreamed appends another chunk of data to the specified blob. It returns
// the number of actual bytes to the blob.
func (is *ObjectStorage) PutBlobChunkStreamed(repo, uuid string, body io.Reader) (int64, error) {
	return 0, nil
}

// PutBlobChunk writes another chunk of data to the specified blob. It returns
// the number of actual bytes to the blob.
func (is *ObjectStorage) PutBlobChunk(repo, uuid string, from, to int64,
	body io.Reader,
) (int64, error) {
	return 0, nil
}

// BlobUploadInfo returns the current blob size in bytes.
func (is *ObjectStorage) BlobUploadInfo(repo, uuid string) (int64, error) {
	return 0, nil
}

// FinishBlobUpload finalizes the blob upload and moves blob the repository.
func (is *ObjectStorage) FinishBlobUpload(repo, uuid string, body io.Reader, digest string) error {
	return nil
}

// FullBlobUpload handles a full blob upload, and no partial session is created.
func (is *ObjectStorage) FullBlobUpload(repo string, body io.Reader, digest string) (string, int64, error) {
	return "", 0, nil
}

func (is *ObjectStorage) DedupeBlob(src string, dstDigest godigest.Digest, dst string) error {
retry:
	is.log.Debug().Str("src", src).Str("dstDigest", dstDigest.String()).Str("dst", dst).Msg("dedupe: enter")

	dstRecord, err := is.cache.GetBlob(dstDigest.String())
	if err := test.Error(err); err != nil && !errors.Is(err, zerr.ErrCacheMiss) {
		is.log.Error().Err(err).Str("blobPath", dst).Msg("dedupe: unable to lookup blob record")

		return err
	}

	if dstRecord == "" {
		// cache record doesn't exist, so first disk and cache entry for this digest
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
		_, err := is.store.Stat(context.Background(), dstRecord)
		if err != nil {
			is.log.Error().Err(err).Str("blobPath", dstRecord).Msg("dedupe: unable to stat")
			// the actual blob on disk may have been removed by GC, so sync the cache
			err := is.cache.DeleteBlob(dstDigest.String(), dstRecord)
			if err = test.Error(err); err != nil {
				// nolint:lll
				is.log.Error().Err(err).Str("dstDigest", dstDigest.String()).Str("dst", dst).Msg("dedupe: unable to delete blob record")

				return err
			}

			goto retry
		}

		fileInfo, err := is.store.Stat(context.Background(), dst)
		if err != nil && !errors.As(err, &driver.PathNotFoundError{}) {
			is.log.Error().Err(err).Str("blobPath", dstRecord).Msg("dedupe: unable to stat")

			return err
		}

		// prevent overwrite original blob
		if fileInfo == nil && dstRecord != dst {
			// put empty file so that we are compliant with oci layout, this will act as a deduped blob
			err = is.store.PutContent(context.Background(), dst, []byte{})
			if err != nil {
				is.log.Error().Err(err).Str("blobPath", dstRecord).Msg("dedupe: unable to write empty file")

				return err
			}

			if err := is.cache.PutBlob(dstDigest.String(), dst); err != nil {
				is.log.Error().Err(err).Str("blobPath", dst).Msg("dedupe: unable to insert blob record")

				return err
			}
		}

		// remove temp blobupload
		if err := is.store.Delete(context.Background(), src); err != nil {
			is.log.Error().Err(err).Str("src", src).Msg("dedupe: unable to remove blob")

			return err
		}

		is.log.Debug().Str("src", src).Msg("dedupe: remove")
	}

	return nil
}

func (is *ObjectStorage) RunGCRepo(repo string) {
}

// DeleteBlobUpload deletes an existing blob upload that is currently in progress.
func (is *ObjectStorage) DeleteBlobUpload(repo, uuid string) error {
	return nil
}

// BlobPath returns the repository path of a blob.
func (is *ObjectStorage) BlobPath(repo string, digest godigest.Digest) string {
	return path.Join(is.rootDir, repo, "blobs", digest.Algorithm().String(), digest.Encoded())
}

// CheckBlob verifies a blob and returns true if the blob is correct.
func (is *ObjectStorage) CheckBlob(repo, digest string) (bool, int64, error) {
	var lockLatency time.Time

	dgst, err := godigest.Parse(digest)
	if err != nil {
		is.log.Error().Err(err).Str("digest", digest).Msg("failed to parse digest")

		return false, -1, zerr.ErrBadBlobDigest
	}

	blobPath := is.BlobPath(repo, dgst)

	if is.dedupe && is.cache != nil {
		is.Lock(&lockLatency)
		defer is.Unlock(&lockLatency)
	} else {
		is.RLock(&lockLatency)
		defer is.RUnlock(&lockLatency)
	}

	binfo, err := is.store.Stat(context.Background(), blobPath)
	if err == nil && binfo.Size() > 0 {
		is.log.Debug().Str("blob path", blobPath).Msg("blob path found")

		return true, binfo.Size(), nil
	}
	// otherwise is a 'deduped' blob (empty file)

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

	// put deduped blob in cache
	if err := is.cache.PutBlob(digest, blobPath); err != nil {
		is.log.Error().Err(err).Str("blobPath", blobPath).Msg("dedupe: unable to insert blob record")

		return false, -1, err
	}

	return true, blobSize, nil
}

func (is *ObjectStorage) checkCacheBlob(digest string) (string, error) {
	if is.cache == nil {
		return "", zerr.ErrBlobNotFound
	}

	dstRecord, err := is.cache.GetBlob(digest)
	if err != nil {
		return "", err
	}

	is.log.Debug().Str("digest", digest).Str("dstRecord", dstRecord).Msg("cache: found dedupe record")

	return dstRecord, nil
}

func (is *ObjectStorage) copyBlob(repo string, blobPath string, dstRecord string) (int64, error) {
	if err := is.initRepo(repo); err != nil {
		is.log.Error().Err(err).Str("repo", repo).Msg("unable to initialize an empty repo")

		return -1, err
	}

	if err := is.store.PutContent(context.Background(), blobPath, []byte{}); err != nil {
		is.log.Error().Err(err).Str("blobPath", blobPath).Str("link", dstRecord).Msg("dedupe: unable to link")

		return -1, zerr.ErrBlobNotFound
	}

	// return original blob with content instead of the deduped one (blobPath)
	binfo, err := is.store.Stat(context.Background(), dstRecord)
	if err == nil {
		return binfo.Size(), nil
	}

	return -1, zerr.ErrBlobNotFound
}

// GetBlob returns a stream to read the blob.
// blob selector instead of directly downloading the blob.
func (is *ObjectStorage) GetBlob(repo, digest, mediaType string) (io.ReadCloser, int64, error) {
	var lockLatency time.Time

	dgst, err := godigest.Parse(digest)
	if err != nil {
		is.log.Error().Err(err).Str("digest", digest).Msg("failed to parse digest")

		return nil, -1, zerr.ErrBadBlobDigest
	}

	blobPath := is.BlobPath(repo, dgst)

	is.RLock(&lockLatency)
	defer is.RUnlock(&lockLatency)

	binfo, err := is.store.Stat(context.Background(), blobPath)
	if err != nil {
		is.log.Error().Err(err).Str("blob", blobPath).Msg("failed to stat blob")

		return nil, -1, zerr.ErrBlobNotFound
	}

	blobReadCloser, err := is.store.Reader(context.Background(), blobPath, 0)
	if err != nil {
		is.log.Error().Err(err).Str("blob", blobPath).Msg("failed to open blob")

		return nil, -1, err
	}

	// is a 'deduped' blob
	if binfo.Size() == 0 {
		// Check blobs in cache
		dstRecord, err := is.checkCacheBlob(digest)
		if err != nil {
			is.log.Error().Err(err).Str("digest", digest).Msg("cache: not found")

			return nil, -1, zerr.ErrBlobNotFound
		}

		binfo, err := is.store.Stat(context.Background(), dstRecord)
		if err != nil {
			is.log.Error().Err(err).Str("blob", dstRecord).Msg("failed to stat blob")

			// the actual blob on disk may have been removed by GC, so sync the cache
			if err := is.cache.DeleteBlob(digest, dstRecord); err != nil {
				is.log.Error().Err(err).Str("dstDigest", digest).Str("dst", dstRecord).Msg("dedupe: unable to delete blob record")

				return nil, -1, err
			}

			return nil, -1, zerr.ErrBlobNotFound
		}

		blobReadCloser, err := is.store.Reader(context.Background(), dstRecord, 0)
		if err != nil {
			is.log.Error().Err(err).Str("blob", dstRecord).Msg("failed to open blob")

			return nil, -1, err
		}

		return blobReadCloser, binfo.Size(), nil
	}

	// The caller function is responsible for calling Close()
	return blobReadCloser, binfo.Size(), nil
}

func (is *ObjectStorage) GetBlobContent(repo, digest string) ([]byte, error) {
	return []byte{}, nil
}

func (is *ObjectStorage) GetReferrers(repo, digest, mediaType string) ([]artifactspec.Descriptor, error) {
	return nil, zerr.ErrMethodNotSupported
}

func (is *ObjectStorage) GetIndexContent(repo string) ([]byte, error) {
	return []byte{}, nil
}

// DeleteBlob removes the blob from the repository.
func (is *ObjectStorage) DeleteBlob(repo, digest string) error {
	var lockLatency time.Time

	dgst, err := godigest.Parse(digest)
	if err != nil {
		is.log.Error().Err(err).Str("digest", digest).Msg("failed to parse digest")

		return zerr.ErrBlobNotFound
	}

	blobPath := is.BlobPath(repo, dgst)

	is.Lock(&lockLatency)
	defer is.Unlock(&lockLatency)

	_, err = is.store.Stat(context.Background(), blobPath)
	if err != nil {
		is.log.Error().Err(err).Str("blob", blobPath).Msg("failed to stat blob")

		return zerr.ErrBlobNotFound
	}

	if is.cache != nil {
		dstRecord, err := is.cache.GetBlob(digest)
		if err != nil && !errors.Is(err, zerr.ErrCacheMiss) {
			is.log.Error().Err(err).Str("blobPath", dstRecord).Msg("dedupe: unable to lookup blob record")

			return err
		}

		// remove cache entry and move blob contents to the next candidate if there is any
		if err := is.cache.DeleteBlob(digest, blobPath); err != nil {
			is.log.Error().Err(err).Str("digest", digest).Str("blobPath", blobPath).Msg("unable to remove blob path from cache")

			return err
		}

		// if the deleted blob is one with content
		if dstRecord == blobPath {
			// get next candidate
			dstRecord, err := is.cache.GetBlob(digest)
			if err != nil && !errors.Is(err, zerr.ErrCacheMiss) {
				is.log.Error().Err(err).Str("blobPath", dstRecord).Msg("dedupe: unable to lookup blob record")

				return err
			}

			// if we have a new candidate move the blob content to it
			if dstRecord != "" {
				if err := is.store.Move(context.Background(), blobPath, dstRecord); err != nil {
					is.log.Error().Err(err).Str("blobPath", blobPath).Msg("unable to remove blob path")

					return err
				}

				return nil
			}
		}
	}

	if err := is.store.Delete(context.Background(), blobPath); err != nil {
		is.log.Error().Err(err).Str("blobPath", blobPath).Msg("unable to remove blob path")

		return err
	}

	return nil
}

func (is *ObjectStorage) GarbageCollect(dir string, repo string) error {
	return nil
}
