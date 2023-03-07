//go:build sync
// +build sync

package sync

import (
	"context"

	"github.com/containers/common/pkg/retry"
	"github.com/containers/image/v5/types"
	"github.com/opencontainers/go-digest"

	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/scheduler"
)

type Service interface {
	GetNextRepo(lastRepo string) (string, error)
	SyncRepo(repo string) error
	SyncImage(repo, reference string) error
	SyncReference(repo string, subjectDigestStr string, referenceType string) error
	ResetCatalog()
	SetNextBackupURL() error
	GetRetryOptions() *retry.Options
}

type OciLayoutStorage interface {
	// get temp ImageReference
	GetImageReference(repo string, tag string) (types.ImageReference, error)
	GetContext() *types.SystemContext
}

type Registry interface {
	GetContext() *types.SystemContext
	// ImageReference - describes a registry/repo:tag
	GetImageReference(repo, tag string) (types.ImageReference, error)
}
type Remote interface {
	GetRepositories(ctx context.Context) ([]string, error)
	GetRepoTags(repo string) ([]string, error)
	GetManifestContent(imageReference types.ImageReference) ([]byte, string, digest.Digest, error)
	Registry
}

type Local interface {
	CanSkipImage(repo, tag string, imageDigest digest.Digest) (bool, error)
	CommitImage(imageReference types.ImageReference, repo, tag string) error
	Registry
}

type TaskGenerator struct {
	Service  Service
	lastRepo string
	done     bool
	log      log.Logger
}

func NewGenerator(service Service, log log.Logger) *TaskGenerator {
	return &TaskGenerator{
		Service:  service,
		done:     false,
		lastRepo: "",
		log:      log,
	}
}

func (gen *TaskGenerator) GenerateTask() (scheduler.Task, error) {
	repo, err := gen.Service.GetNextRepo(gen.lastRepo)
	if err != nil {
		if err := gen.Service.SetNextBackupURL(); err != nil {
			return nil, err
		}

		return nil, nil
	}

	if repo == "" {
		gen.log.Info().Msg("sync: finished syncing")
		gen.done = true

		return nil, nil
	}

	gen.lastRepo = repo

	return newGCTask(gen.lastRepo, gen.Service), nil
}

func (gen *TaskGenerator) IsDone() bool {
	return gen.done
}

func (gen *TaskGenerator) Reset() {
	gen.lastRepo = ""
	gen.Service.ResetCatalog()
	gen.done = false
}

type gcTask struct {
	repo    string
	service Service
}

func newGCTask(repo string, service Service) *gcTask {
	return &gcTask{repo, service}
}

func (gcT *gcTask) DoWork() error {
	return gcT.service.SyncRepo(gcT.repo)
}
