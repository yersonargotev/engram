package store

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	MaxLocalEvidenceReferences     = 32
	MaxLocalEvidenceReferenceBytes = 64
)

var localEvidenceReferencePattern = regexp.MustCompile(`^(?:prompt|summary):[1-9][0-9]*$`)

// normalizeLocalEvidenceReferences validates local provenance identifiers shared
// by reviewable proposal stores without coupling their lifecycles or metrics.
func normalizeLocalEvidenceReferences(values []string, subject string) ([]string, error) {
	if len(values) > MaxLocalEvidenceReferences {
		return nil, fmt.Errorf("%s has %d evidence references: maximum is %d", subject, len(values), MaxLocalEvidenceReferences)
	}
	result := make([]string, 0, len(values))
	for index, raw := range values {
		value := strings.TrimSpace(raw)
		if len([]byte(value)) > MaxLocalEvidenceReferenceBytes ||
			(value != "session-summary" && !localEvidenceReferencePattern.MatchString(value)) {
			return nil, fmt.Errorf("invalid evidence reference at index %d", index)
		}
		result = append(result, value)
	}
	return result, nil
}
