package memoryops

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

const (
	CaptureConsentScopeNone    = "none"
	CaptureConsentScopeProject = "project"
	CaptureConsentScopeSession = "session"

	CaptureStateDefaultDisabled = "default_disabled"
	CaptureStateConsented       = "consented"
	CaptureStateExpired         = "expired"
	CaptureStateUnavailable     = "unavailable"

	CaptureReasonCaptured        = "captured"
	CaptureReasonConsentDisabled = "consent_disabled"
)

var (
	ErrCaptureInvalidContentType  = errors.New("invalid capture content type")
	ErrCaptureInvalidRetention    = errors.New("invalid capture retention")
	ErrCaptureInvalidSessionGrant = errors.New("invalid capture session grant")
)

type CaptureStatusInput struct {
	Project     string
	ContentType string
	SessionID   string
}

type CaptureStatusResult struct {
	Project         string     `json:"project"`
	ContentType     string     `json:"content_type"`
	SessionID       string     `json:"session_id,omitempty"`
	State           string     `json:"state"`
	Enabled         bool       `json:"enabled"`
	Scope           string     `json:"scope"`
	RetentionDays   int        `json:"retention_days"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	StoredCount     int64      `json:"stored_count"`
	LegacyFTSStatus string     `json:"legacy_prompt_fts_status"`
}

type CaptureEnableInput struct {
	Project       string
	ContentType   string
	SessionID     string
	ExpiresAt     *time.Time
	RetentionDays int
}

type CaptureDisableInput struct {
	Project     string
	ContentType string
	SessionID   string
}

type CaptureInput struct {
	Project     string
	ContentType string
	SessionID   string
	Content     string
	ObservedAt  time.Time
}

type CaptureResult struct {
	Captured   bool       `json:"captured"`
	ReasonCode string     `json:"reason_code"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
}

type CapturePurgeInput struct {
	Project     string
	ContentType string
}

type CapturePurgeResult struct {
	Project     string `json:"project"`
	ContentType string `json:"content_type"`
	Deleted     int64  `json:"deleted"`
}

func (s *Service) CaptureStatus(input CaptureStatusInput) (*CaptureStatusResult, error) {
	project, contentType, err := validateCaptureScope(input.Project, input.ContentType)
	if err != nil {
		return nil, err
	}
	status, err := s.store.CaptureConsentStatus(project, contentType, strings.TrimSpace(input.SessionID), s.now())
	if err != nil {
		return nil, fmt.Errorf("inspect capture consent: %w", err)
	}
	result := &CaptureStatusResult{
		Project: project, ContentType: contentType, SessionID: strings.TrimSpace(input.SessionID),
		State: CaptureStateDefaultDisabled,
		Scope: CaptureConsentScopeNone, RetentionDays: store.DefaultDiagnosticRetentionDays,
		StoredCount: status.StoredCount, LegacyFTSStatus: status.LegacyFTSStatus,
	}
	if status.Consent == nil {
		if status.Expired {
			result.State = CaptureStateExpired
		}
		return result, nil
	}
	result.State = CaptureStateConsented
	result.Enabled = true
	result.RetentionDays = status.Consent.RetentionDays
	result.ExpiresAt = status.Consent.ExpiresAt
	result.Scope = CaptureConsentScopeProject
	if status.Consent.SessionID != "" {
		result.Scope = CaptureConsentScopeSession
	}
	return result, nil
}

func (s *Service) EnableCapture(input CaptureEnableInput) (*CaptureStatusResult, error) {
	project, contentType, err := validateCaptureScope(input.Project, input.ContentType)
	if err != nil {
		return nil, err
	}
	retentionDays := input.RetentionDays
	if retentionDays == 0 {
		retentionDays = store.DefaultDiagnosticRetentionDays
	}
	if retentionDays < 1 || retentionDays > store.MaxDiagnosticRetentionDays {
		return nil, fmt.Errorf("%w: retention_days must be between 1 and %d", ErrCaptureInvalidRetention, store.MaxDiagnosticRetentionDays)
	}
	sessionID := strings.TrimSpace(input.SessionID)
	if (sessionID == "") != (input.ExpiresAt == nil) {
		return nil, fmt.Errorf("%w: session_id and expires_at must be provided together", ErrCaptureInvalidSessionGrant)
	}
	if input.ExpiresAt != nil && !input.ExpiresAt.After(s.now()) {
		return nil, fmt.Errorf("%w: expires_at must be in the future", ErrCaptureInvalidSessionGrant)
	}
	now := s.now().UTC()
	consent := store.CaptureConsent{
		Project: project, ContentType: contentType, SessionID: sessionID,
		RetentionDays: retentionDays, ExpiresAt: input.ExpiresAt, UpdatedAt: now,
	}
	if err := s.store.UpsertCaptureConsent(consent); err != nil {
		return nil, fmt.Errorf("enable capture consent: %w", err)
	}
	return s.CaptureStatus(CaptureStatusInput{Project: project, ContentType: contentType, SessionID: sessionID})
}

func (s *Service) DisableCapture(input CaptureDisableInput) (*CaptureStatusResult, error) {
	project, contentType, err := validateCaptureScope(input.Project, input.ContentType)
	if err != nil {
		return nil, err
	}
	sessionID := strings.TrimSpace(input.SessionID)
	if _, err := s.store.DeleteCaptureConsent(project, contentType, sessionID); err != nil {
		return nil, fmt.Errorf("disable capture consent: %w", err)
	}
	return s.CaptureStatus(CaptureStatusInput{Project: project, ContentType: contentType, SessionID: sessionID})
}

func (s *Service) Capture(input CaptureInput) (*CaptureResult, error) {
	project, contentType, err := validateCaptureScope(input.Project, input.ContentType)
	if err != nil {
		return nil, err
	}
	if contentType == store.CaptureContentTypePrompt && strings.TrimSpace(input.SessionID) == "" {
		return nil, errors.New("session id is required for prompt capture")
	}
	observedAt := input.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = s.now().UTC()
	}
	result, err := s.store.CaptureDiagnostic(store.CaptureDiagnosticParams{
		Project: project, ContentType: contentType, SessionID: strings.TrimSpace(input.SessionID),
		Content: input.Content, Now: observedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("capture Diagnostic content: %w", err)
	}
	reason := CaptureReasonConsentDisabled
	if result.Captured {
		reason = CaptureReasonCaptured
	}
	return &CaptureResult{Captured: result.Captured, ReasonCode: reason, ExpiresAt: result.ExpiresAt}, nil
}

func (s *Service) PurgeCapture(input CapturePurgeInput) (*CapturePurgeResult, error) {
	project, contentType, err := validateCaptureScope(input.Project, input.ContentType)
	if err != nil {
		return nil, err
	}
	deleted, err := s.store.PurgeDiagnosticCapture(project, contentType)
	if err != nil {
		return nil, fmt.Errorf("purge Diagnostic capture: %w", err)
	}
	return &CapturePurgeResult{Project: project, ContentType: contentType, Deleted: deleted}, nil
}

func validateCaptureScope(project, contentType string) (string, string, error) {
	project, _ = store.NormalizeProject(project)
	if project == "" {
		return "", "", ErrProjectRequired
	}
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	switch contentType {
	case store.CaptureContentTypePrompt, store.CaptureContentTypeSubagentOutput:
		return project, contentType, nil
	default:
		return "", "", fmt.Errorf("%w: %q", ErrCaptureInvalidContentType, contentType)
	}
}
