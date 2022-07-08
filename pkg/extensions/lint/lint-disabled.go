//go:build !lint
// +build !lint

package lint

import (
	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	"zotregistry.io/zot/pkg/log"
)

type MandatoryAnnotationsConfig struct {
	AnnotationsList []string
}

type Config struct {
	Enabled              *bool
	MandatoryAnnotations *MandatoryAnnotationsConfig
}

type Linter struct {
	config *Config
	log    log.Logger
}

func NewLinter(config *Config, log log.Logger) *Linter {
	log.Warn().Msg("disabled")
	return nil
}

func (linter *Linter) CheckMandatoryAnnotations(rootDir string, repo string,
	index ispec.Index, manifestDigest godigest.Digest,
) bool {
	return true
}
