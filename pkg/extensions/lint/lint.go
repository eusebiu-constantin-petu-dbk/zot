package lint

import (
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type MandatoryAnnotationsConfig struct {
	AnnotationsList []string
}

type Config struct {
	Enabled              *bool
	MandatoryAnnotations *MandatoryAnnotationsConfig
}

func CheckMandatoryAnnotations(manifest ispec.Manifest, annotationsList []string, lintEnabled bool) bool {
	if !lintEnabled || len(annotationsList) == 0 {
		return true
	}

	annotations := manifest.Annotations

	if len(annotations) == 0 {
		return false
	}

	for i := 0; i < len(annotationsList); i++ {
		_, found := annotations[annotationsList[i]]

		if !found {
			return false
		}
	}

	return true
}
