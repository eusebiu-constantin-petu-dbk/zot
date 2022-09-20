package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	// Add s3 support.
	"github.com/docker/distribution/registry/storage/driver"
	guuid "github.com/gofrs/uuid"
	"github.com/notaryproject/notation-go"
	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	artifactspec "github.com/oras-project/artifacts-spec/specs-go/v1"
	"github.com/rs/zerolog"
	"github.com/sigstore/cosign/pkg/oci/remote"
	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/extensions/monitoring"
)

const (
	// BlobUploadDir defines the upload directory for blob uploads.
	BlobUploadDir    = ".uploads"
	SchemaVersion    = 2
	DefaultFilePerms = 0o600
	DefaultDirPerms  = 0o700
)

type StoreController struct {
	DefaultStore ImageStore
	SubStore     map[string]ImageStore
}

func (sc StoreController) GetImageStore(name string) ImageStore {
	if sc.SubStore != nil {
		// SubStore is being provided, now we need to find equivalent image store and this will be found by splitting name
		prefixName := getRoutePrefix(name)

		imgStore, ok := sc.SubStore[prefixName]
		if !ok {
			imgStore = sc.DefaultStore
		}

		return imgStore
	}

	return sc.DefaultStore
}

func getRoutePrefix(name string) string {
	names := strings.SplitN(name, "/", 2) //nolint:gomnd

	if len(names) != 2 { //nolint:gomnd
		// it means route is of global storage e.g "centos:latest"
		if len(names) == 1 {
			return "/"
		}
	}

	return fmt.Sprintf("/%s", names[0])
}

func dirExists(d string, store driver.StorageDriver) bool {
	if fi, err := store.Stat(context.Background(), d); err == nil && fi.IsDir() {
		return true
	}

	return false
}

// RLock read-lock.
func RLock(lockStart *time.Time, lock *sync.RWMutex) {
	*lockStart = time.Now()

	lock.RLock()
}

// RUnlock read-unlock.
func RUnlock(is ImageStore, lockStart *time.Time, lock *sync.RWMutex, metrics monitoring.MetricServer) {
	lock.RUnlock()

	lockEnd := time.Now()
	// includes time spent in acquiring and holding a lock
	latency := lockEnd.Sub(*lockStart)
	monitoring.ObserveStorageLockLatency(metrics, latency, is.RootDir(), RLOCK) // histogram
}

// Lock write-lock.
func Lock(lockStart *time.Time, lock *sync.RWMutex) {
	*lockStart = time.Now()

	lock.Lock()
}

// Unlock write-unlock.
func Unlock(is ImageStore, lockStart *time.Time, lock *sync.RWMutex, metrics monitoring.MetricServer) {
	lock.Unlock()

	lockEnd := time.Now()
	// includes time spent in acquiring and holding a lock
	latency := lockEnd.Sub(*lockStart)
	monitoring.ObserveStorageLockLatency(metrics, latency, is.RootDir(), RWLOCK) // histogram
}

func InitRepo(is ImageStore, store driver.StorageDriver, name string, log zerolog.Logger) error {
	repoDir := path.Join(is.RootDir(), name)

	// "oci-layout" file - create if it doesn't exist
	ilPath := path.Join(repoDir, ispec.ImageLayoutFile)
	if _, err := store.Stat(context.Background(), ilPath); err != nil {
		il := ispec.ImageLayout{Version: ispec.ImageLayoutVersion}

		buf, err := json.Marshal(il)
		if err != nil {
			log.Error().Err(err).Msg("unable to marshal JSON")

			return err
		}

		if err := store.PutContent(context.Background(), ilPath, buf); err != nil {
			log.Error().Err(err).Str("file", ilPath).Msg("unable to write file")

			return err
		}
	}

	// "index.json" file - create if it doesn't exist
	indexPath := path.Join(repoDir, "index.json")
	if _, err := store.Stat(context.Background(), indexPath); err != nil {
		index := ispec.Index{}
		index.SchemaVersion = 2

		buf, err := json.Marshal(index)
		if err != nil {
			log.Error().Err(err).Msg("unable to marshal JSON")

			return err
		}

		if err := store.PutContent(context.Background(), indexPath, buf); err != nil {
			log.Error().Err(err).Str("file", ilPath).Msg("unable to write file")

			return err
		}
	}

	return nil
}

// ValidateRepo validates that the repository layout is complaint with the OCI repo layout.
func ValidateRepo(rootDir string, store driver.StorageDriver, name string, log zerolog.Logger) (bool, error) {
	// https://github.com/opencontainers/image-spec/blob/master/image-layout.md#content
	// at least, expect at least 3 entries - ["blobs", "oci-layout", "index.json"]
	// and an additional/optional BlobUploadDir in each image store
	// for objects storage we can not create empty dirs, so we check only against index.json and oci-layout
	dir := path.Join(rootDir, name)
	if fi, err := store.Stat(context.Background(), dir); err != nil || !fi.IsDir() {
		return false, zerr.ErrRepoNotFound
	}

	files, err := store.List(context.Background(), dir)
	if err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("unable to read directory")

		return false, zerr.ErrRepoNotFound
	}

	// nolint:gomnd
	if len(files) < 2 {
		return false, zerr.ErrRepoBadVersion
	}

	found := map[string]bool{
		ispec.ImageLayoutFile: false,
		"index.json":          false,
	}

	for _, file := range files {
		_, err := store.Stat(context.Background(), file)
		if err != nil {
			return false, err
		}

		filename, err := filepath.Rel(dir, file)
		if err != nil {
			return false, err
		}

		found[filename] = true
	}

	for k, v := range found {
		if !v && k != BlobUploadDir {
			return false, nil
		}
	}

	buf, err := store.GetContent(context.Background(), path.Join(dir, ispec.ImageLayoutFile))
	if err != nil {
		return false, err
	}

	var il ispec.ImageLayout
	if err := json.Unmarshal(buf, &il); err != nil {
		return false, err
	}

	if il.Version != ispec.ImageLayoutVersion {
		return false, zerr.ErrRepoBadVersion
	}

	return true, nil
}

// GetRepositories returns a list of all the repositories under this store.
func GetRepositories(is ImageStore, store driver.StorageDriver) ([]string, error) {
	var lockLatency time.Time

	dir := is.RootDir()

	is.RLock(&lockLatency)
	defer is.RUnlock(&lockLatency)

	stores := make([]string, 0)
	err := store.Walk(context.Background(), dir, func(fileInfo driver.FileInfo) error {
		if !fileInfo.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(is.RootDir(), fileInfo.Path())
		if err != nil {
			return nil //nolint:nilerr // ignore paths that are not under root dir
		}

		if ok, err := is.ValidateRepo(rel); !ok || err != nil {
			return nil //nolint:nilerr // ignore invalid repos
		}

		stores = append(stores, rel)

		return nil
	})

	// if the root directory is not yet created then return an empty slice of repositories
	var perr driver.PathNotFoundError
	if errors.As(err, &perr) {
		return stores, nil
	}

	return stores, err
}

// GetImageTags returns a list of image tags available in the specified repository.
func GetImageTags(is ImageStore, store driver.StorageDriver, repo string, log zerolog.Logger) ([]string, error) {
	dir := path.Join(is.RootDir(), repo)
	if fi, err := store.Stat(context.Background(), dir); err != nil || !fi.IsDir() {
		return nil, zerr.ErrRepoNotFound
	}

	buf, err := is.GetIndexContent(repo)
	if err != nil {
		return nil, err
	}

	var index ispec.Index
	if err := json.Unmarshal(buf, &index); err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("invalid JSON")

		return nil, zerr.ErrRepoNotFound
	}

	tags := make([]string, 0)

	for _, manifest := range index.Manifests {
		v, ok := manifest.Annotations[ispec.AnnotationRefName]
		if ok {
			tags = append(tags, v)
		}
	}

	return tags, nil
}

// GetImageManifest returns the image manifest of an image in the specific repository.
func GetImageManifest(is ImageStore, store driver.StorageDriver, repo, reference string, metrics monitoring.MetricServer, log zerolog.Logger) ([]byte, string, string, error) {
	var lockLatency time.Time

	dir := path.Join(is.RootDir(), repo)
	if fi, err := store.Stat(context.Background(), dir); err != nil || !fi.IsDir() {
		return nil, "", "", zerr.ErrRepoNotFound
	}

	buf, err := is.GetIndexContent(repo)
	if err != nil {
		return nil, "", "", err
	}

	var index ispec.Index
	if err := json.Unmarshal(buf, &index); err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("invalid JSON")

		return nil, "", "", err
	}

	found := false

	var digest godigest.Digest

	mediaType := ""

	for _, manifest := range index.Manifests {
		if reference == manifest.Digest.String() {
			digest = manifest.Digest
			mediaType = manifest.MediaType
			found = true

			break
		}

		v, ok := manifest.Annotations[ispec.AnnotationRefName]
		if ok && v == reference {
			digest = manifest.Digest
			mediaType = manifest.MediaType
			found = true

			break
		}
	}

	if !found {
		return nil, "", "", zerr.ErrManifestNotFound
	}

	manifestPath := path.Join(dir, "blobs", digest.Algorithm().String(), digest.Encoded())

	is.RLock(&lockLatency)
	defer is.RUnlock(&lockLatency)

	buf, err = store.GetContent(context.Background(), manifestPath)
	if err != nil {
		log.Error().Err(err).Str("blob", manifestPath).Msg("failed to read manifest")

		return nil, "", "", err
	}

	var manifest ispec.Manifest
	if err := json.Unmarshal(buf, &manifest); err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("invalid JSON")

		return nil, "", "", err
	}

	monitoring.IncDownloadCounter(metrics, repo)

	return buf, digest.String(), mediaType, nil
}

/**
before an image index manifest is pushed to a repo, its constituent manifests
are pushed first, so when updating/removing this image index manifest, we also
need to determine if there are other image index manifests which refer to the
same constitutent manifests so that they can be garbage-collected correctly

pruneImageManifestsFromIndex is a helper routine to achieve this.
*/
func PruneImageManifestsFromIndex(imageStore ImageStore, store driver.StorageDriver, dir string, digest godigest.Digest, // nolint: gocyclo
	outIndex ispec.Index, otherImgIndexes []ispec.Descriptor, log zerolog.Logger,
) ([]ispec.Descriptor, error) {
	indexPath := path.Join(dir, "blobs", digest.Algorithm().String(), digest.Encoded())

	buf, err := store.GetContent(context.Background(), indexPath)
	if err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("failed to read index.json")

		return nil, err
	}

	var imgIndex ispec.Index
	if err := json.Unmarshal(buf, &imgIndex); err != nil {
		log.Error().Err(err).Str("path", indexPath).Msg("invalid JSON")

		return nil, err
	}

	inUse := map[string]uint{}

	for _, manifest := range imgIndex.Manifests {
		inUse[manifest.Digest.Encoded()]++
	}

	for _, otherIndex := range otherImgIndexes {
		indexPath := path.Join(dir, "blobs", otherIndex.Digest.Algorithm().String(), otherIndex.Digest.Encoded())

		buf, err := store.GetContent(context.Background(), indexPath)
		if err != nil {
			log.Error().Err(err).Str("dir", dir).Msg("failed to read index.json")

			return nil, err
		}

		var oindex ispec.Index
		if err := json.Unmarshal(buf, &oindex); err != nil {
			log.Error().Err(err).Str("path", indexPath).Msg("invalid JSON")

			return nil, err
		}

		for _, omanifest := range oindex.Manifests {
			_, ok := inUse[omanifest.Digest.Encoded()]
			if ok {
				inUse[omanifest.Digest.Encoded()]++
			}
		}
	}

	prunedManifests := []ispec.Descriptor{}

	// for all manifests in the index, skip those that either have a tag or
	// are used in other imgIndexes
	for _, outManifest := range outIndex.Manifests {
		if outManifest.MediaType != ispec.MediaTypeImageManifest {
			prunedManifests = append(prunedManifests, outManifest)

			continue
		}

		_, ok := outManifest.Annotations[ispec.AnnotationRefName]
		if ok {
			prunedManifests = append(prunedManifests, outManifest)

			continue
		}

		count, ok := inUse[outManifest.Digest.Encoded()]
		if !ok {
			prunedManifests = append(prunedManifests, outManifest)

			continue
		}

		if count != 1 {
			// this manifest is in use in other image indexes
			prunedManifests = append(prunedManifests, outManifest)

			continue
		}
	}

	return prunedManifests, nil
}

// PutImageManifest adds an image manifest to the repository.
func PutImageManifest(is ImageStore, store driver.StorageDriver, repo, reference, mediaType string, //nolint: gocyclo
	body []byte, linter Lint, metrics monitoring.MetricServer, log zerolog.Logger) (string, error,
) {
	if err := is.InitRepo(repo); err != nil {
		log.Debug().Err(err).Msg("init repo")

		return "", err
	}

	// validate the manifest
	if !IsSupportedMediaType(mediaType) {
		log.Debug().Interface("actual", mediaType).
			Msg("bad manifest media type")

		return "", zerr.ErrBadManifest
	}

	if len(body) == 0 {
		log.Debug().Int("len", len(body)).Msg("invalid body length")

		return "", zerr.ErrBadManifest
	}

	dig, err := validateManifest(is, store, repo, reference, mediaType, body, log)
	if err != nil {
		return dig, err
	}

	mDigest := godigest.FromBytes(body)
	refIsDigest := false
	dgst, err := godigest.Parse(reference)

	if err == nil {
		if dgst.String() != mDigest.String() {
			log.Error().Str("actual", mDigest.String()).Str("expected", dgst.String()).
				Msg("manifest digest is not valid")

			return "", zerr.ErrBadManifest
		}

		refIsDigest = true
	}

	dir := path.Join(is.RootDir(), repo)

	buf, err := is.GetIndexContent(repo)
	if err != nil {
		return "", err
	}

	var lockLatency time.Time

	is.Lock(&lockLatency)
	defer is.Unlock(&lockLatency)

	var index ispec.Index
	if err := json.Unmarshal(buf, &index); err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("invalid JSON")

		return "", zerr.ErrRepoBadVersion
	}

	updateIndex := true
	// create a new descriptor
	desc := ispec.Descriptor{
		MediaType: mediaType, Size: int64(len(body)), Digest: mDigest,
		Platform: &ispec.Platform{Architecture: "amd64", OS: "linux"},
	}
	if !refIsDigest {
		desc.Annotations = map[string]string{ispec.AnnotationRefName: reference}
	}

	var oldDgst godigest.Digest

	for midx, manifest := range index.Manifests {
		if reference == manifest.Digest.String() {
			// nothing changed, so don't update
			desc = manifest
			updateIndex = false

			break
		}

		v, ok := manifest.Annotations[ispec.AnnotationRefName]
		if ok && v == reference {
			if manifest.Digest.String() == mDigest.String() {
				// nothing changed, so don't update
				desc = manifest
				updateIndex = false

				break
			}
			// manifest contents have changed for the same tag,
			// so update index.json descriptor

			log.Info().
				Int64("old size", desc.Size).
				Int64("new size", int64(len(body))).
				Str("old digest", desc.Digest.String()).
				Str("new digest", mDigest.String()).
				Str("old digest", desc.Digest.String()).
				Str("new digest", mDigest.String()).
				Msg("updating existing tag with new manifest contents")

			// changing media-type is disallowed!
			if manifest.MediaType != mediaType {
				err = zerr.ErrBadManifest
				log.Error().Err(err).
					Str("old mediaType", manifest.MediaType).
					Str("new mediaType", mediaType).Msg("cannot change media-type")

				return "", err
			}

			desc = manifest
			oldDgst = manifest.Digest
			desc.Size = int64(len(body))
			desc.Digest = mDigest

			index.Manifests = append(index.Manifests[:midx], index.Manifests[midx+1:]...)

			break
		}
	}

	if !updateIndex {
		return desc.Digest.String(), nil
	}

	// write manifest to "blobs"
	dir = path.Join(is.RootDir(), repo, "blobs", mDigest.Algorithm().String())
	manifestPath := path.Join(dir, mDigest.Encoded())

	if err = store.PutContent(context.Background(), manifestPath, body); err != nil {
		log.Error().Err(err).Str("file", manifestPath).Msg("unable to write")

		return "", err
	}

	/* additionally, unmarshal an image index and for all manifests in that
	index, ensure that they do not have a name or they are not in other
	manifest indexes else GC can never clean them */
	if (mediaType == ispec.MediaTypeImageIndex) && (oldDgst != "") {
		otherImgIndexes := []ispec.Descriptor{}

		for _, manifest := range index.Manifests {
			if manifest.MediaType == ispec.MediaTypeImageIndex {
				otherImgIndexes = append(otherImgIndexes, manifest)
			}
		}

		otherImgIndexes = append(otherImgIndexes, desc)

		dir := path.Join(is.RootDir(), repo)

		prunedManifests, err := PruneImageManifestsFromIndex(is, store, dir, oldDgst, index, otherImgIndexes, log)
		if err != nil {
			return "", err
		}

		index.Manifests = prunedManifests
	}

	// now update "index.json"
	index.Manifests = append(index.Manifests, desc)
	dir = path.Join(is.RootDir(), repo)
	indexPath := path.Join(dir, "index.json")
	buf, err = json.Marshal(index)

	if err != nil {
		log.Error().Err(err).Str("file", indexPath).Msg("unable to marshal JSON")

		return "", err
	}

	// apply linter only on images, not signatures
	if linter != nil {
		if mediaType == ispec.MediaTypeImageManifest &&
			// check that image manifest is not cosign signature
			!strings.HasPrefix(reference, "sha256-") &&
			!strings.HasSuffix(reference, remote.SignatureTagSuffix) {
			// lint new index with new manifest before writing to disk
			pass, err := linter.Lint(repo, mDigest, is)
			if err != nil {
				log.Error().Err(err).Msg("linter error")

				return "", err
			}

			if !pass {
				return "", zerr.ErrImageLintAnnotations
			}
		}
	}

	if err = store.PutContent(context.Background(), indexPath, buf); err != nil {
		log.Error().Err(err).Str("file", manifestPath).Msg("unable to write")

		return "", err
	}

	if err := is.GarbageCollect(dir, repo); err != nil {
		return "", err
	}

	monitoring.SetStorageUsage(metrics, is.RootDir(), repo)
	monitoring.IncUploadCounter(metrics, repo)

	return desc.Digest.String(), nil
}

// DeleteImageManifest deletes the image manifest from the repository.
func DeleteImageManifest(is ImageStore, store driver.StorageDriver, repo, reference string, metrics monitoring.MetricServer, log zerolog.Logger) error {
	var lockLatency time.Time

	dir := path.Join(is.RootDir(), repo)
	if fi, err := store.Stat(context.Background(), dir); err != nil || !fi.IsDir() {
		return zerr.ErrRepoNotFound
	}

	isTag := false

	// as per spec "reference" can only be a digest and not a tag
	dgst, err := godigest.Parse(reference)
	if err != nil {
		log.Debug().Str("invalid digest: ", reference).Msg("storage: assuming tag")

		isTag = true
	}

	buf, err := is.GetIndexContent(repo)
	if err != nil {
		return err
	}

	var index ispec.Index
	if err := json.Unmarshal(buf, &index); err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("invalid JSON")

		return err
	}

	found := false

	isImageIndex := false

	var manifest ispec.Descriptor

	// we are deleting, so keep only those manifests that don't match
	outIndex := index
	outIndex.Manifests = []ispec.Descriptor{}

	otherImgIndexes := []ispec.Descriptor{}

	for _, manifest = range index.Manifests {
		if isTag {
			tag, ok := manifest.Annotations[ispec.AnnotationRefName]
			if ok && tag == reference {
				log.Debug().Str("deleting tag", tag).Msg("")

				dgst = manifest.Digest

				found = true

				if manifest.MediaType == ispec.MediaTypeImageIndex {
					isImageIndex = true
				}

				continue
			}
		} else if reference == manifest.Digest.String() {
			log.Debug().Str("deleting reference", reference).Msg("")
			found = true

			if manifest.MediaType == ispec.MediaTypeImageIndex {
				isImageIndex = true
			}

			continue
		}

		outIndex.Manifests = append(outIndex.Manifests, manifest)

		if manifest.MediaType == ispec.MediaTypeImageIndex {
			otherImgIndexes = append(otherImgIndexes, manifest)
		}
	}

	if !found {
		return zerr.ErrManifestNotFound
	}

	is.Lock(&lockLatency)
	defer is.Unlock(&lockLatency)

	/* additionally, unmarshal an image index and for all manifests in that
	index, ensure that they do not have a name or they are not in other
	manifest indexes else GC can never clean them */
	if isImageIndex {
		prunedManifests, err := PruneImageManifestsFromIndex(is, store, dir, dgst, outIndex, otherImgIndexes, log)
		if err != nil {
			return err
		}

		outIndex.Manifests = prunedManifests
	}

	// now update "index.json"
	dir = path.Join(is.RootDir(), repo)
	file := path.Join(dir, "index.json")
	buf, err = json.Marshal(outIndex)

	if err != nil {
		return err
	}

	if _, err := WriteFile(store, file, buf); err != nil {
		log.Debug().Str("deleting reference", reference).Msg("")

		return err
	}

	if err := is.GarbageCollect(dir, repo); err != nil {
		return err
	}

	// Delete blob only when blob digest not present in manifest entry.
	// e.g. 1.0.1 & 1.0.2 have same blob digest so if we delete 1.0.1, blob should not be removed.
	toDelete := true

	for _, manifest = range outIndex.Manifests {
		if dgst.String() == manifest.Digest.String() {
			toDelete = false

			break
		}
	}

	if toDelete {
		p := path.Join(dir, "blobs", dgst.Algorithm().String(), dgst.Encoded())

		err = store.Delete(context.Background(), p)
		if err != nil {
			return err
		}
	}

	monitoring.SetStorageUsage(metrics, is.RootDir(), repo)

	return nil
}

// BlobUploadPath returns the upload path for a blob in this store.
func BlobUploadPath(rootDir string, repo, uuid string) string {
	dir := path.Join(rootDir, repo)
	blobUploadPath := path.Join(dir, BlobUploadDir, uuid)

	return blobUploadPath
}

// NewBlobUpload returns the unique ID for an upload in progress.
func NewBlobUpload(is ImageStore, store driver.StorageDriver, repo string, log zerolog.Logger) (string, error) {
	if err := is.InitRepo(repo); err != nil {
		log.Error().Err(err).Msg("error initializing repo")

		return "", err
	}

	uuid, err := guuid.NewV4()
	if err != nil {
		return "", err
	}

	uid := uuid.String()

	blobUploadPath := is.BlobUploadPath(repo, uid)

	// here we should create an empty multi part upload, but that's not possible
	// so we just create a regular empty file which will be overwritten by FinishBlobUpload
	err = store.PutContent(context.Background(), blobUploadPath, []byte{})
	if err != nil {
		return "", zerr.ErrRepoNotFound
	}

	return uid, nil
}

// GetBlobUpload returns the current size of a blob upload.
func GetBlobUpload(is ImageStore, store driver.StorageDriver, multiPartUploads sync.Map, repo, uuid string) (int64, error) {
	var fileSize int64

	blobUploadPath := is.BlobUploadPath(repo, uuid)

	// if it's not a multipart upload check for the regular empty file
	// created by NewBlobUpload, it should have 0 size every time
	_, hasStarted := multiPartUploads.Load(blobUploadPath)
	if !hasStarted {
		binfo, err := store.Stat(context.Background(), blobUploadPath)
		if err != nil {
			var perr driver.PathNotFoundError
			if errors.As(err, &perr) {
				return -1, zerr.ErrUploadNotFound
			}

			return -1, err
		}

		fileSize = binfo.Size()
	} else {
		// otherwise get the size of multi parts upload
		fi, err := GetMultipartFileWriter(store, multiPartUploads, blobUploadPath)
		if err != nil {
			return -1, err
		}

		defer fi.Close()

		fileSize = fi.Size()
	}

	return fileSize, nil
}

// PutBlobChunkStreamed appends another chunk of data to the specified blob. It returns
// the number of actual bytes to the blob.
func PutBlobChunkStreamed(is ImageStore, store driver.StorageDriver, multiPartUploads sync.Map, repo, uuid string, body io.Reader, log zerolog.Logger) (int64, error) {
	if err := is.InitRepo(repo); err != nil {
		return -1, err
	}

	blobUploadPath := is.BlobUploadPath(repo, uuid)

	_, err := store.Stat(context.Background(), blobUploadPath)
	if err != nil {
		return -1, zerr.ErrUploadNotFound
	}

	file, err := GetMultipartFileWriter(store, multiPartUploads, blobUploadPath)
	if err != nil {
		log.Error().Err(err).Msg("failed to create multipart upload")

		return -1, err
	}

	defer file.Close()

	buf := new(bytes.Buffer)

	_, err = buf.ReadFrom(body)
	if err != nil {
		log.Error().Err(err).Msg("failed to read blob")

		return -1, err
	}

	nbytes, err := file.Write(buf.Bytes())
	if err != nil {
		log.Error().Err(err).Msg("failed to append to file")

		return -1, err
	}

	return int64(nbytes), err
}

// PutBlobChunk writes another chunk of data to the specified blob. It returns
// the number of actual bytes to the blob.
func PutBlobChunk(is ImageStore, store driver.StorageDriver, multiPartUploads sync.Map, repo, uuid string, from, to int64,
	body io.Reader, log zerolog.Logger,
) (int64, error) {
	if err := is.InitRepo(repo); err != nil {
		return -1, err
	}

	blobUploadPath := is.BlobUploadPath(repo, uuid)

	_, err := store.Stat(context.Background(), blobUploadPath)
	if err != nil {
		return -1, zerr.ErrUploadNotFound
	}

	file, err := GetMultipartFileWriter(store, multiPartUploads, blobUploadPath)
	if err != nil {
		log.Error().Err(err).Msg("failed to create multipart upload")

		return -1, err
	}

	defer file.Close()

	if from != file.Size() {
		// cancel multipart upload
		multiPartUploads.Delete(blobUploadPath)

		err := file.Cancel()
		if err != nil {
			log.Error().Err(err).Msg("failed to cancel multipart upload")

			return -1, err
		}

		log.Error().Int64("expected", from).Int64("actual", file.Size()).
			Msg("invalid range start for blob upload")

		return -1, zerr.ErrBadUploadRange
	}

	buf := new(bytes.Buffer)

	_, err = buf.ReadFrom(body)
	if err != nil {
		log.Error().Err(err).Msg("failed to read blob")

		return -1, err
	}

	nbytes, err := file.Write(buf.Bytes())
	if err != nil {
		log.Error().Err(err).Msg("failed to append to file")

		return -1, err
	}

	return int64(nbytes), err
}

// BlobUploadInfo returns the current blob size in bytes.
func BlobUploadInfo(is ImageStore, store driver.StorageDriver, multiPartUploads sync.Map, repo, uuid string, log zerolog.Logger) (int64, error) {
	var fileSize int64

	blobUploadPath := is.BlobUploadPath(repo, uuid)

	// if it's not a multipart upload check for the regular empty file
	// created by NewBlobUpload, it should have 0 size every time
	_, hasStarted := multiPartUploads.Load(blobUploadPath)
	if !hasStarted {
		uploadInfo, err := store.Stat(context.Background(), blobUploadPath)
		if err != nil {
			log.Error().Err(err).Str("blob", blobUploadPath).Msg("failed to stat blob")

			return -1, err
		}

		fileSize = uploadInfo.Size()
	} else {
		// otherwise get the size of multi parts upload
		binfo, err := GetMultipartFileWriter(store, multiPartUploads, blobUploadPath)
		if err != nil {
			log.Error().Err(err).Str("blob", blobUploadPath).Msg("failed to stat blob")

			return -1, err
		}

		fileSize = binfo.Size()
	}

	return fileSize, nil
}

// FinishBlobUpload finalizes the blob upload and moves blob the repository.
func FinishBlobUpload(is ImageStore, store driver.StorageDriver, multiPartUploads sync.Map, repo,
	uuid string, body io.Reader, digest string, dedupe bool, cache bool, log zerolog.Logger) error {
	dstDigest, err := godigest.Parse(digest)
	if err != nil {
		log.Error().Err(err).Str("digest", digest).Msg("failed to parse digest")

		return zerr.ErrBadBlobDigest
	}

	src := is.BlobUploadPath(repo, uuid)

	// complete multiUploadPart
	fileWriter, err := store.Writer(context.Background(), src, true)
	if err != nil {
		log.Error().Err(err).Str("blob", src).Msg("failed to open blob")

		return zerr.ErrBadBlobDigest
	}

	if err := fileWriter.Commit(); err != nil {
		log.Error().Err(err).Msg("failed to commit file")

		return err
	}

	if err := fileWriter.Close(); err != nil {
		log.Error().Err(err).Msg("failed to close file")
	}

	fileReader, err := store.Reader(context.Background(), src, 0)
	if err != nil {
		log.Error().Err(err).Str("blob", src).Msg("failed to open file")

		return zerr.ErrUploadNotFound
	}

	srcDigest, err := godigest.FromReader(fileReader)
	if err != nil {
		log.Error().Err(err).Str("blob", src).Msg("failed to open blob")

		return zerr.ErrBadBlobDigest
	}

	if srcDigest != dstDigest {
		log.Error().Str("srcDigest", srcDigest.String()).
			Str("dstDigest", dstDigest.String()).Msg("actual digest not equal to expected digest")

		return zerr.ErrBadBlobDigest
	}

	defer fileReader.Close()

	dst := is.BlobPath(repo, dstDigest)

	var lockLatency time.Time

	is.Lock(&lockLatency)
	defer is.Unlock(&lockLatency)

	if dedupe && cache {
		if err := is.DedupeBlob(src, dstDigest, dst); err != nil {
			log.Error().Err(err).Str("src", src).Str("dstDigest", dstDigest.String()).
				Str("dst", dst).Msg("unable to dedupe blob")

			return err
		}
	} else {
		if err := store.Move(context.Background(), src, dst); err != nil {
			log.Error().Err(err).Str("src", src).Str("dstDigest", dstDigest.String()).
				Str("dst", dst).Msg("unable to finish blob")

			return err
		}
	}

	multiPartUploads.Delete(src)

	return nil
}

// FullBlobUpload handles a full blob upload, and no partial session is created.
func FullBlobUpload(is ImageStore, store driver.StorageDriver, repo string, body io.Reader,
	digest string, dedupe, cache bool, log zerolog.Logger) (string, int64, error) {
	if err := is.InitRepo(repo); err != nil {
		return "", -1, err
	}

	dstDigest, err := godigest.Parse(digest)
	if err != nil {
		log.Error().Err(err).Str("digest", digest).Msg("failed to parse digest")

		return "", -1, zerr.ErrBadBlobDigest
	}

	u, err := guuid.NewV4()
	if err != nil {
		return "", -1, err
	}

	uuid := u.String()
	src := is.BlobUploadPath(repo, uuid)
	digester := sha256.New()
	buf := new(bytes.Buffer)

	_, err = buf.ReadFrom(body)
	if err != nil {
		log.Error().Err(err).Msg("failed to read blob")

		return "", -1, err
	}

	nbytes, err := WriteFile(store, src, buf.Bytes())
	if err != nil {
		log.Error().Err(err).Msg("failed to write blob")

		return "", -1, err
	}

	_, err = digester.Write(buf.Bytes())
	if err != nil {
		log.Error().Err(err).Msg("digester failed to write")

		return "", -1, err
	}

	srcDigest := godigest.NewDigestFromEncoded(godigest.SHA256, fmt.Sprintf("%x", digester.Sum(nil)))
	if srcDigest != dstDigest {
		log.Error().Str("srcDigest", srcDigest.String()).
			Str("dstDigest", dstDigest.String()).Msg("actual digest not equal to expected digest")

		return "", -1, zerr.ErrBadBlobDigest
	}

	var lockLatency time.Time

	is.Lock(&lockLatency)
	defer is.Unlock(&lockLatency)

	dst := is.BlobPath(repo, dstDigest)

	if dedupe && cache {
		if err := is.DedupeBlob(src, dstDigest, dst); err != nil {
			log.Error().Err(err).Str("src", src).Str("dstDigest", dstDigest.String()).
				Str("dst", dst).Msg("unable to dedupe blob")

			return "", -1, err
		}
	} else {
		if err := store.Move(context.Background(), src, dst); err != nil {
			log.Error().Err(err).Str("src", src).Str("dstDigest", dstDigest.String()).
				Str("dst", dst).Msg("unable to finish blob")

			return "", -1, err
		}
	}

	return uuid, int64(nbytes), nil
}

// DeleteBlobUpload deletes an existing blob upload that is currently in progress.
func DeleteBlobUpload(is ImageStore, store driver.StorageDriver, repo, uuid string, log zerolog.Logger) error {
	blobUploadPath := is.BlobUploadPath(repo, uuid)
	if err := store.Delete(context.Background(), blobUploadPath); err != nil {
		log.Error().Err(err).Str("blobUploadPath", blobUploadPath).Msg("error deleting blob upload")

		return err
	}

	return nil
}

func GetBlobContent(is ImageStore, repo, digest string, log zerolog.Logger) ([]byte, error) {
	blob, _, err := is.GetBlob(repo, digest, ispec.MediaTypeImageManifest)
	if err != nil {
		return []byte{}, err
	}
	defer blob.Close()

	buf := new(bytes.Buffer)

	_, err = buf.ReadFrom(blob)
	if err != nil {
		log.Error().Err(err).Msg("failed to read blob")

		return []byte{}, err
	}

	return buf.Bytes(), nil
}

func GetIndexContent(is ImageStore, store driver.StorageDriver, repo string, log zerolog.Logger) ([]byte, error) {
	var lockLatency time.Time

	dir := path.Join(is.RootDir(), repo)

	is.RLock(&lockLatency)
	defer is.RUnlock(&lockLatency)

	buf, err := store.GetContent(context.Background(), path.Join(dir, "index.json"))
	if err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("failed to read index.json")

		return []byte{}, zerr.ErrRepoNotFound
	}

	return buf, nil
}

// Do not use for multipart upload, buf must not be empty.
// If you want to create an empty file use is.Store.PutContent().
func WriteFile(store driver.StorageDriver, filepath string, buf []byte) (int, error) {
	var n int

	if stwr, err := store.Writer(context.Background(), filepath, false); err == nil {
		defer stwr.Close()

		if n, err = stwr.Write(buf); err != nil {
			return -1, err
		}

		if err := stwr.Commit(); err != nil {
			return -1, err
		}
	} else {
		return -1, err
	}

	return n, nil
}

// get a multipart upload FileWriter based on wheather or not one has already been started.
func GetMultipartFileWriter(store driver.StorageDriver, multiPartUploads sync.Map, filepath string) (driver.FileWriter, error) {
	var file driver.FileWriter

	var err error

	_, hasStarted := multiPartUploads.Load(filepath)
	if !hasStarted {
		// start multipart upload
		file, err = store.Writer(context.Background(), filepath, false)
		if err != nil {
			return file, err
		}

		multiPartUploads.Store(filepath, true)
	} else {
		// continue multipart upload
		file, err = store.Writer(context.Background(), filepath, true)
		if err != nil {
			return file, err
		}
	}

	return file, nil
}

func ValidateOCIManifest(is ImageStore, store driver.StorageDriver, repo, reference string, manifest *ispec.Manifest, log zerolog.Logger) (string, error) {
	if manifest.SchemaVersion != SchemaVersion {
		log.Error().Int("SchemaVersion", manifest.SchemaVersion).Msg("invalid manifest")

		return "", zerr.ErrBadManifest
	}

	// validate image config
	config := manifest.Config
	if config.MediaType != ispec.MediaTypeImageConfig {
		return "", zerr.ErrBadManifest
	}

	digest := config.Digest

	blobPath := is.BlobPath(repo, digest)
	if _, err := store.Stat(context.Background(), blobPath); err != nil {
		log.Error().Err(err).Str("blobPath", blobPath).Msg("unable to find blob")

		return digest.String(), zerr.ErrBlobNotFound
	}

	blobFile, err := store.Reader(context.Background(), blobPath, 0)
	if err != nil {
		log.Error().Err(err).Str("blobPath", blobPath).Msg("unable to find blob")

		return digest.String(), zerr.ErrBlobNotFound
	}

	defer blobFile.Close()

	dec := json.NewDecoder(blobFile)

	var cspec ispec.Image
	if err := dec.Decode(&cspec); err != nil {
		return "", zerr.ErrBadManifest
	}

	// validate the layers
	for _, l := range manifest.Layers {
		digest = l.Digest
		blobPath = is.BlobPath(repo, digest)
		log.Info().Str("blobPath", blobPath).Str("reference", reference).Msg("manifest layers")

		if _, err := store.Stat(context.Background(), blobPath); err != nil {
			log.Error().Err(err).Str("blobPath", blobPath).Msg("unable to find blob")

			return digest.String(), zerr.ErrBlobNotFound
		}
	}

	return "", nil
}

func IsSupportedMediaType(mediaType string) bool {
	return mediaType == ispec.MediaTypeImageIndex ||
		mediaType == ispec.MediaTypeImageManifest ||
		mediaType == artifactspec.MediaTypeArtifactManifest
}

func validateManifest(imgStore ImageStore, store driver.StorageDriver, repo, reference,
	mediaType string, body []byte, log zerolog.Logger,
) (string, error) {
	if mediaType == ispec.MediaTypeImageManifest {
		var manifest ispec.Manifest
		if err := json.Unmarshal(body, &manifest); err != nil {
			log.Error().Err(err).Msg("unable to unmarshal JSON")

			return "", zerr.ErrBadManifest
		}

		if manifest.Config.MediaType == ispec.MediaTypeImageConfig {
			digest, err := ValidateOCIManifest(imgStore, store, repo, reference, &manifest, log)
			if err != nil {
				log.Error().Err(err).Msg("invalid oci image manifest")

				return digest, err
			}
		}
	} else if mediaType == artifactspec.MediaTypeArtifactManifest {
		var m notation.Descriptor
		if err := json.Unmarshal(body, &m); err != nil {
			log.Error().Err(err).Msg("unable to unmarshal JSON")

			return "", zerr.ErrBadManifest
		}
	}

	return "", nil
}
