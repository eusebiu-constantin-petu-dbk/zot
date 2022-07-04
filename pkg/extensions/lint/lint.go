package lint

import (
	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
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
}

func NewLinter(config *Config) Linter {
	return Linter{
		config: config,
	}
}

func (linter Linter) CheckMandatoryAnnotations(index ispec.Index, manifestDigest godigest.Digest) bool {
	if linter.config != nil && !*linter.config.Enabled || len(linter.config.MandatoryAnnotations.AnnotationsList) == 0 {
		return true
	}

	annotationList := linter.config.MandatoryAnnotations.AnnotationsList

	for _, manifest := range index.Manifests {
		if manifest.Digest == manifestDigest {
			annotations := manifest.Annotations

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

	return false
}
