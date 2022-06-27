package sysconfig

import (
	"context"
	"net/http"

	"gopkg.in/square/go-jose.v2/json"
	"zotregistry.io/zot/pkg/log"

	"zotregistry.io/zot/pkg/api/config"
)

type ctlrReloadConfigFunc func(reloadCtx context.Context, config *config.Config) *config.Config

type SysConfigManager struct {
	config           *config.Config
	reloadConfigFunc ctlrReloadConfigFunc
	reloadCtx        context.Context
	cancelRoutines   context.CancelFunc
	log              log.Logger
}

func NewSysConfigManager(config *config.Config, reloadConfigFunc ctlrReloadConfigFunc, logger log.Logger) *SysConfigManager {
	scm := &SysConfigManager{
		reloadConfigFunc: reloadConfigFunc,
		config:           config,
		log:              logger,
	}

	scm.reloadCtx, scm.cancelRoutines = context.WithCancel(context.Background())

	return scm
}

func (scm *SysConfigManager) LoadNewConfig(config *config.Config) {
	scm.cancelRoutines()
	scm.reloadCtx, scm.cancelRoutines = context.WithCancel(context.Background())

	scm.config = scm.reloadConfigFunc(scm.reloadCtx, config)
}

func (scm *SysConfigManager) GetContext() context.Context {
	return scm.reloadCtx
}

// extension enabled
func (scm *SysConfigManager) SystemConfigurationHandler(response http.ResponseWriter, request *http.Request) {
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
