//go:build sync
// +build sync

package sync

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/containers/common/pkg/retry"

	zerr "zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/common"
	"zotregistry.io/zot/pkg/log"
)

type request struct {
	repo      string
	reference string
	// used for background retries, at most one background retry per service
	serviceID    int
	isBackground bool
}

/*
	a request can be an image/signature/sbom

keep track of all parallel requests, if two requests of same image/signature/sbom comes at the same time,
process just the first one, also keep track of all background retrying routines.
*/
type BaseOnDemand struct {
	services []Service
	// map[request]chan err
	requestStore *sync.Map
	log          log.Logger
}

func NewOnDemandService(log log.Logger) *BaseOnDemand {
	return &BaseOnDemand{log: log, requestStore: &sync.Map{}}
}

func (onDemand *BaseOnDemand) AddService(service Service) {
	onDemand.services = append(onDemand.services, service)
}

func (onDemand *BaseOnDemand) SyncImage(repo, reference string) error {
	req := request{
		repo:      repo,
		reference: reference,
	}

	val, found := onDemand.requestStore.Load(req)
	if found {
		onDemand.log.Info().Msgf("image %s:%s already demanded, waiting on channel", repo, reference)

		syncResult, _ := val.(chan error)

		err, ok := <-syncResult
		// if channel closed exit
		if !ok {
			return nil
		}

		return err
	}

	syncResult := make(chan error)
	onDemand.requestStore.Store(req, syncResult)

	defer onDemand.requestStore.Delete(req)
	defer close(syncResult)

	go onDemand.syncImage(repo, reference, syncResult)

	err, ok := <-syncResult
	if !ok {
		return nil
	}

	return err
}

func (onDemand *BaseOnDemand) SyncReference(repo string, subjectDigestStr string, referenceType string) error {
	for _, service := range onDemand.services {
		err := service.SyncReference(repo, subjectDigestStr, referenceType)
		if err != nil {
			if errors.Is(err, zerr.ErrSyncReferrerNotFound) || errors.Is(err, zerr.ErrSyncReferrer) {
				continue
			}

			err := service.SetNextBackupURL()
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}

	return nil
}

func (onDemand *BaseOnDemand) syncImage(repo, reference string, syncResult chan error) {
	var err error
	for serviceID, service := range onDemand.services {
		err = service.SyncImage(repo, reference)
		if err != nil {
			if errors.Is(err, zerr.ErrManifestNotFound) {
				continue
			}

			req := request{
				repo:         repo,
				reference:    reference,
				serviceID:    serviceID,
				isBackground: true,
			}

			// if there is already a background routine, skip
			if _, requested := onDemand.requestStore.LoadOrStore(req, struct{}{}); requested {
				continue
			}

			retryOptions := service.GetRetryOptions()

			if retryOptions.MaxRetry > 0 {
				// retry in background
				go func() {
					// remove image after syncing
					defer func() {
						onDemand.requestStore.Delete(req)
						onDemand.log.Info().Msgf("sync routine for image %s:%s exited", repo, reference)
					}()

					onDemand.log.Info().Msgf("sync routine: starting routine to copy image %s:%s, cause err: %v",
						repo, reference, err)

					time.Sleep(retryOptions.Delay)

					if err = retry.RetryIfNecessary(context.Background(), func() error {
						err := service.SyncImage(repo, reference)

						return err
					}, retryOptions); err != nil {
						onDemand.log.Error().Str("errorType", common.TypeOf(err)).
							Err(err).Msgf("sync routine: error while copying image %s:%s", repo, reference)
					}
				}()
			}

			// change remote registry to next back up url
			err := service.SetNextBackupURL()
			if err != nil {
				syncResult <- err

				return
			}
		} else {
			break
		}
	}

	syncResult <- err
}
