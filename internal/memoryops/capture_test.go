package memoryops

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

func TestCaptureConsentDefaultsOffAndProjectGrantControlsDiagnosticPromptCapture(t *testing.T) {
	service := newTestService(t)
	now := time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	status, err := service.CaptureStatus(CaptureStatusInput{
		Project:     "Engram",
		ContentType: store.CaptureContentTypePrompt,
		SessionID:   "session-102",
	})
	if err != nil {
		t.Fatalf("default capture status: %v", err)
	}
	if status.Enabled || status.Scope != CaptureConsentScopeNone || status.RetentionDays != store.DefaultDiagnosticRetentionDays {
		t.Fatalf("default capture status = %+v, want disabled seven-day default", status)
	}

	grant, err := service.EnableCapture(CaptureEnableInput{
		Project:     "Engram",
		ContentType: store.CaptureContentTypePrompt,
	})
	if err != nil {
		t.Fatalf("enable project capture: %v", err)
	}
	if !grant.Enabled || grant.Scope != CaptureConsentScopeProject || grant.RetentionDays != store.DefaultDiagnosticRetentionDays {
		t.Fatalf("project grant = %+v", grant)
	}

	const sentinel = "DIAGNOSTIC-PROMPT-CONTENT-102"
	captured, err := service.Capture(CaptureInput{
		Project:     "Engram",
		ContentType: store.CaptureContentTypePrompt,
		SessionID:   "session-102",
		Content:     sentinel,
	})
	if err != nil {
		t.Fatalf("capture prompt: %v", err)
	}
	if !captured.Captured || captured.ReasonCode != CaptureReasonCaptured || captured.ExpiresAt == nil {
		t.Fatalf("capture result = %+v", captured)
	}
	if want := now.Add(7 * 24 * time.Hour); !captured.ExpiresAt.Equal(want) {
		t.Fatalf("expires_at = %s, want %s", captured.ExpiresAt, want)
	}

	var diagnosticCount, legacyCount, mutationCount, observationCount, proposalCount, checkpointCount int
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM diagnostic_captures WHERE content = 'DIAGNOSTIC-PROMPT-CONTENT-102'`: &diagnosticCount,
		`SELECT COUNT(*) FROM user_prompts`:                           &legacyCount,
		`SELECT COUNT(*) FROM sync_mutations WHERE entity = 'prompt'`: &mutationCount,
		`SELECT COUNT(*) FROM observations`:                           &observationCount,
		`SELECT COUNT(*) FROM memory_proposals`:                       &proposalCount,
		`SELECT COUNT(*) FROM memory_checkpoints`:                     &checkpointCount,
	} {
		if err := service.store.DB().QueryRow(query).Scan(target); err != nil {
			t.Fatalf("query capture boundary %q: %v", query, err)
		}
	}
	if diagnosticCount != 1 || legacyCount != 0 || mutationCount != 0 || observationCount != 0 || proposalCount != 0 || checkpointCount != 0 {
		t.Fatalf("capture boundary counts diagnostic=%d legacy=%d mutations=%d observations=%d proposals=%d checkpoints=%d", diagnosticCount, legacyCount, mutationCount, observationCount, proposalCount, checkpointCount)
	}

	encodedStatus, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	if string(encodedStatus) == "" || containsBytes(encodedStatus, []byte(sentinel)) {
		t.Fatalf("content-free status leaked capture content: %s", encodedStatus)
	}
}

func TestCaptureUsesObservedTimeSoLaterConsentCannotCaptureEarlierContent(t *testing.T) {
	service := newTestService(t)
	observedAt := time.Date(2026, time.September, 1, 12, 0, 0, 100_000_000, time.UTC)
	grantedAt := observedAt.Add(100 * time.Microsecond)
	if err := service.store.UpsertCaptureConsent(store.CaptureConsent{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
		RetentionDays: store.DefaultDiagnosticRetentionDays, UpdatedAt: grantedAt,
	}); err != nil {
		t.Fatalf("seed later consent: %v", err)
	}

	earlier, err := service.Capture(CaptureInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
		SessionID: "session-before-consent", Content: "must-not-be-retroactively-captured", ObservedAt: observedAt,
	})
	if err != nil || earlier.Captured || earlier.ReasonCode != CaptureReasonConsentDisabled {
		t.Fatalf("earlier capture = %+v err=%v, want disabled", earlier, err)
	}
	later, err := service.Capture(CaptureInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
		SessionID: "session-after-consent", Content: "eligible-after-consent", ObservedAt: grantedAt,
	})
	if err != nil || !later.Captured || later.ReasonCode != CaptureReasonCaptured {
		t.Fatalf("later capture = %+v err=%v, want captured", later, err)
	}
}

func TestCaptureSessionGrantExpiresAndProjectGrantRemainsIndependent(t *testing.T) {
	service := newTestService(t)
	now := time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	expiresAt := now.Add(90 * time.Minute)

	grant, err := service.EnableCapture(CaptureEnableInput{
		Project:       "engram",
		ContentType:   store.CaptureContentTypePrompt,
		SessionID:     "session-expiring",
		ExpiresAt:     &expiresAt,
		RetentionDays: 3,
	})
	if err != nil {
		t.Fatalf("enable session grant: %v", err)
	}
	if grant.Scope != CaptureConsentScopeSession || grant.ExpiresAt == nil || !grant.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("session grant = %+v", grant)
	}

	active, err := service.CaptureStatus(CaptureStatusInput{Project: "engram", ContentType: store.CaptureContentTypePrompt, SessionID: "session-expiring"})
	if err != nil || !active.Enabled || active.Scope != CaptureConsentScopeSession || active.RetentionDays != 3 {
		t.Fatalf("active session status = %+v, err=%v", active, err)
	}

	now = expiresAt.Add(time.Second)
	expired, err := service.CaptureStatus(CaptureStatusInput{Project: "engram", ContentType: store.CaptureContentTypePrompt, SessionID: "session-expiring"})
	if err != nil {
		t.Fatalf("expired session status: %v", err)
	}
	if expired.Enabled || expired.Scope != CaptureConsentScopeNone {
		t.Fatalf("expired session status = %+v, want disabled", expired)
	}
}

func TestCaptureDisableDoesNotPurgeAndPurgeDoesNotChangeConsent(t *testing.T) {
	service := newTestService(t)
	now := time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	if _, err := service.EnableCapture(CaptureEnableInput{Project: "engram", ContentType: store.CaptureContentTypePrompt}); err != nil {
		t.Fatalf("enable capture: %v", err)
	}
	if _, err := service.Capture(CaptureInput{Project: "engram", ContentType: store.CaptureContentTypePrompt, SessionID: "session-102", Content: "purge-me"}); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if _, err := service.DisableCapture(CaptureDisableInput{Project: "engram", ContentType: store.CaptureContentTypePrompt}); err != nil {
		t.Fatalf("disable capture: %v", err)
	}

	var before int
	if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM diagnostic_captures`).Scan(&before); err != nil {
		t.Fatalf("count before purge: %v", err)
	}
	if before != 1 {
		t.Fatalf("disable removed %d captures, want 1 retained", 1-before)
	}

	purged, err := service.PurgeCapture(CapturePurgeInput{Project: "engram", ContentType: store.CaptureContentTypePrompt})
	if err != nil {
		t.Fatalf("purge capture: %v", err)
	}
	if purged.Deleted != 1 {
		t.Fatalf("purged = %+v", purged)
	}
	status, err := service.CaptureStatus(CaptureStatusInput{Project: "engram", ContentType: store.CaptureContentTypePrompt})
	if err != nil || status.Enabled {
		t.Fatalf("status after purge = %+v, err=%v", status, err)
	}
}

func TestCaptureRejectsInvalidTypeRetentionAndSessionGrantShape(t *testing.T) {
	service := newTestService(t)
	now := time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	tests := []CaptureEnableInput{
		{Project: "engram", ContentType: "transcript"},
		{Project: "engram", ContentType: store.CaptureContentTypePrompt, RetentionDays: 31},
		{Project: "engram", ContentType: store.CaptureContentTypePrompt, SessionID: "session-without-expiry"},
		{Project: "engram", ContentType: store.CaptureContentTypePrompt, ExpiresAt: ptrTime(now.Add(time.Hour))},
		{Project: "engram", ContentType: store.CaptureContentTypePrompt, SessionID: "expired", ExpiresAt: ptrTime(now)},
	}
	for _, input := range tests {
		if _, err := service.EnableCapture(input); err == nil {
			t.Fatalf("EnableCapture(%+v) succeeded, want validation error", input)
		}
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

func containsBytes(haystack, needle []byte) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
