package sysconfig

import (
	"encoding/json"
	"net/http"

	"zotregistry.io/zot/pkg/api/config"
)

// extension enabled
func SystemConfigurationHandler(response http.ResponseWriter, request *http.Request) {
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
