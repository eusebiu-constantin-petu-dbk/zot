//go:build lint
// +build lint

package lint

import (
	"encoding/json"
	"io/ioutil"
	"path"

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
	return &Linter{
		config: config,
		log:    log,
	}
}

func (linter *Linter) CheckMandatoryAnnotations(rootDir string, repo string,
	index ispec.Index, manifestDigest godigest.Digest,
) bool {
	if linter.config == nil {
		return true
	}

	if (linter.config != nil && !*linter.config.Enabled) || len(linter.config.MandatoryAnnotations.AnnotationsList) == 0 {
		return true
	}

	annotationList := linter.config.MandatoryAnnotations.AnnotationsList

	for _, manifest := range index.Manifests {
		if manifest.Digest == manifestDigest {
			manifestPath := path.Join(rootDir, repo, "blobs",
				manifestDigest.Algorithm().String(), manifestDigest.Encoded())

			buf, err := ioutil.ReadFile(manifestPath)
			if err != nil {
				linter.log.Error().Err(err).Str("dir", manifestPath).Msg("failed to read manifest blob")
			}

			var manif ispec.Manifest

			if err := json.Unmarshal(buf, &manif); err != nil {
				linter.log.Error().Err(err).Str("dir", manifestPath).Msg("invalid JSON")
			}

			annotations := manif.Annotations

			if len(annotations) == 0 {
				return false
			}

			for i := 0; i < len(annotationList); i++ {
				_, found := annotations[annotationList[i]]

				if !found {
					return false
				}
			}
		}
	}

	return true
}
