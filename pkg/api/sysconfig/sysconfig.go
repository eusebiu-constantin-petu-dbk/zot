package sysconfig

import (
	"context"
	"sync"

	"zotregistry.io/zot/pkg/log"
	"zotregistry.io/zot/pkg/storage"

	ext "zotregistry.io/zot/pkg/extensions"

	"zotregistry.io/zot/pkg/api/config"
)

type ctlrReloadConfigFunc func(reloadCtx context.Context, config *config.Config) *config.Config

type SysConfigManager struct {
	config          *config.Config
	storeController storage.StoreController
	wgShutDown      *sync.WaitGroup
	reloadCtx       context.Context
	cancelRoutines  context.CancelFunc
	log             log.Logger
}

func NewSysConfigManager(config *config.Config, storeController storage.StoreController, wgShutDown *sync.WaitGroup, logger log.Logger) *SysConfigManager {
	scm := &SysConfigManager{
		config:          config,
		storeController: storeController,
		wgShutDown:      wgShutDown,
		log:             logger,
	}

	scm.reloadCtx, scm.cancelRoutines = context.WithCancel(context.Background())

	return scm
}

func (scm *SysConfigManager) LoadNewConfig(config *config.Config) {
	scm.cancelRoutines()
	scm.reloadCtx, scm.cancelRoutines = context.WithCancel(context.Background())

	// scm.config = scm.reloadConfigFunc(scm.reloadCtx, config)
	// reload access control config
	scm.config.AccessControl = config.AccessControl
	//c.Config.HTTP.RawAccessControl = config.HTTP.RawAccessControl

	// Enable extensions if extension config is provided
	if config.Extensions != nil && config.Extensions.Sync != nil {
		// reload sync config
		scm.config.Extensions.Sync = config.Extensions.Sync
		ext.EnableSyncExtension(scm.reloadCtx, scm.config, scm.wgShutDown, scm.storeController, scm.log)
	} else if scm.config.Extensions != nil {
		scm.config.Extensions.Sync = nil
	}

	scm.log.Info().Interface("reloaded params", scm.config.Sanitize()).Msg("new configuration settings")
}

func (scm *SysConfigManager) GetContext() context.Context {
	return scm.reloadCtx
}
