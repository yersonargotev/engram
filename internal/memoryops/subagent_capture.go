package memoryops

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/store"
)

const (
	SubagentDiagnosticKind                = "engram_diagnostic"
	MaxSubagentDiagnosticEnvelopeBytes    = 4 * 1024
	MaxSubagentDiagnosticTitleBytes       = 160
	MaxSubagentDiagnosticLearningBytes    = 2 * 1024
	MaxSubagentDiagnosticEvidenceRefBytes = 512
)

var ErrSubagentDiagnosticEnvelope = errors.New("invalid subagent Diagnostic envelope")

type SubagentDiagnosticInput struct {
	Project   string
	SessionID string
	Envelope  string
}

type SubagentDiagnosticEnvelope struct {
	Kind        string `json:"kind"`
	Title       string `json:"title"`
	Learning    string `json:"learning"`
	EvidenceRef string `json:"evidence_ref,omitempty"`
}

func (s *Service) CaptureSubagentDiagnostic(input SubagentDiagnosticInput) (*CaptureResult, error) {
	status, err := s.CaptureStatus(CaptureStatusInput{
		Project: input.Project, ContentType: store.CaptureContentTypeSubagentOutput,
		SessionID: input.SessionID,
	})
	if err != nil {
		return nil, err
	}
	if !status.Enabled {
		return &CaptureResult{Captured: false, ReasonCode: CaptureReasonConsentDisabled}, nil
	}
	if strings.TrimSpace(input.SessionID) == "" {
		return nil, fmt.Errorf("%w: session_id is required", ErrSubagentDiagnosticEnvelope)
	}

	envelope, err := decodeSubagentDiagnosticEnvelope(input.Envelope)
	if err != nil {
		return nil, err
	}
	content, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("%w: encode canonical content", ErrSubagentDiagnosticEnvelope)
	}
	return s.Capture(CaptureInput{
		Project: input.Project, ContentType: store.CaptureContentTypeSubagentOutput,
		SessionID: input.SessionID, Content: string(content),
	})
}

func decodeSubagentDiagnosticEnvelope(raw string) (SubagentDiagnosticEnvelope, error) {
	var envelope SubagentDiagnosticEnvelope
	if !utf8.ValidString(raw) || len(raw) > MaxSubagentDiagnosticEnvelopeBytes {
		return envelope, fmt.Errorf("%w: envelope must be valid UTF-8 and at most %d bytes", ErrSubagentDiagnosticEnvelope, MaxSubagentDiagnosticEnvelopeBytes)
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return envelope, fmt.Errorf("%w: expected one JSON object", ErrSubagentDiagnosticEnvelope)
	}
	seen := make(map[string]bool, 4)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return envelope, fmt.Errorf("%w: expected one JSON object", ErrSubagentDiagnosticEnvelope)
		}
		key, ok := keyToken.(string)
		if !ok || seen[key] {
			return envelope, fmt.Errorf("%w: fields must be unique", ErrSubagentDiagnosticEnvelope)
		}
		seen[key] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return envelope, fmt.Errorf("%w: fields must be JSON strings", ErrSubagentDiagnosticEnvelope)
		}
		decoded, err := decodeSubagentDiagnosticString(value)
		if err != nil {
			return envelope, err
		}
		switch key {
		case "kind":
			envelope.Kind = decoded
		case "title":
			envelope.Title = decoded
		case "learning":
			envelope.Learning = decoded
		case "evidence_ref":
			envelope.EvidenceRef = decoded
		default:
			return envelope, fmt.Errorf("%w: unknown field %q", ErrSubagentDiagnosticEnvelope, key)
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return envelope, fmt.Errorf("%w: expected one JSON object", ErrSubagentDiagnosticEnvelope)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return envelope, fmt.Errorf("%w: trailing JSON content", ErrSubagentDiagnosticEnvelope)
	}
	envelope.Kind = strings.TrimSpace(envelope.Kind)
	envelope.Title = strings.TrimSpace(envelope.Title)
	envelope.Learning = strings.TrimSpace(envelope.Learning)
	envelope.EvidenceRef = strings.TrimSpace(envelope.EvidenceRef)
	if envelope.Kind != SubagentDiagnosticKind {
		return envelope, fmt.Errorf("%w: kind must be %q", ErrSubagentDiagnosticEnvelope, SubagentDiagnosticKind)
	}
	if envelope.Title == "" || envelope.Learning == "" {
		return envelope, fmt.Errorf("%w: title and learning are required", ErrSubagentDiagnosticEnvelope)
	}
	if len(envelope.Title) > MaxSubagentDiagnosticTitleBytes {
		return envelope, fmt.Errorf("%w: title exceeds %d bytes", ErrSubagentDiagnosticEnvelope, MaxSubagentDiagnosticTitleBytes)
	}
	if len(envelope.Learning) > MaxSubagentDiagnosticLearningBytes {
		return envelope, fmt.Errorf("%w: learning exceeds %d bytes", ErrSubagentDiagnosticEnvelope, MaxSubagentDiagnosticLearningBytes)
	}
	if len(envelope.EvidenceRef) > MaxSubagentDiagnosticEvidenceRefBytes {
		return envelope, fmt.Errorf("%w: evidence_ref exceeds %d bytes", ErrSubagentDiagnosticEnvelope, MaxSubagentDiagnosticEvidenceRefBytes)
	}
	return envelope, nil
}

func decodeSubagentDiagnosticString(raw json.RawMessage) (string, error) {
	encoded := strings.TrimSpace(string(raw))
	if len(encoded) < 2 || encoded[0] != '"' {
		return "", fmt.Errorf("%w: fields must be JSON strings", ErrSubagentDiagnosticEnvelope)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%w: fields must be JSON strings", ErrSubagentDiagnosticEnvelope)
	}
	return value, nil
}
