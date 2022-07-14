package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"zotregistry.io/zot/pkg/api/config"
	"zotregistry.io/zot/pkg/log"

	ext "zotregistry.io/zot/pkg/extensions"
	"zotregistry.io/zot/pkg/storage"
)

type ConfigManager struct {
	config          *config.Config
	storeController storage.StoreController
	wgShutDown      *sync.WaitGroup
	reloadCtx       context.Context
	cancelRoutines  context.CancelFunc
	log             log.Logger
}

func NewSysConfigManager(config *config.Config, storeController storage.StoreController, wgShutDown *sync.WaitGroup, logger log.Logger) *ConfigManager {
	scm := &ConfigManager{
		config:          config,
		storeController: storeController,
		wgShutDown:      wgShutDown,
		log:             logger,
	}

	scm.reloadCtx, scm.cancelRoutines = context.WithCancel(context.Background())

	return scm
}

func (scm *ConfigManager) LoadNewConfig(config *config.Config) {
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

func (scm *ConfigManager) GetContext() context.Context {
	return scm.reloadCtx
}

// extension enabled
func (scm *ConfigManager) Handler(response http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		scm.log.Info().Msg("scm get")
		config, err := json.Marshal(scm.config)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)

			return
		}

		response.WriteHeader(http.StatusOK)
		response.Write(config)

		return
	case http.MethodPost:
		scm.log.Info().Msg("scm post")
		cfg := config.New()
		scm.LoadNewConfig(cfg)
	case http.MethodPatch:
		scm.log.Info().Msg("scm patch")
	default:
		http.Error(response, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	response.WriteHeader(http.StatusAccepted)
}
