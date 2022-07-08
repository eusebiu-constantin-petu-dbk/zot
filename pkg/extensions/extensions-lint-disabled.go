//go:build !lint
// +build !lint

package extensions

import (
	"zotregistry.io/zot/pkg/api/config"
	"zotregistry.io/zot/pkg/extensions/lint"
	"zotregistry.io/zot/pkg/log"
)

// EnableScrubExtension enables scrub extension.
func GetLinter(config *config.Config, log log.Logger) *lint.Linter {
	log.Warn().Msg("disabled")

	return nil
}
