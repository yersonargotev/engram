package memoryops

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

func TestSubagentDiagnosticCaptureRequiresIndependentConsentAndExactEnvelope(t *testing.T) {
	service := newTestService(t)
	now := time.Date(2026, time.August, 31, 20, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	if _, err := service.EnableCapture(CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
	}); err != nil {
		t.Fatalf("enable prompt capture: %v", err)
	}

	const envelope = `{"kind":"engram_diagnostic","title":"Validated parser boundary","learning":"Subagent diagnostics must remain outside durable Memory.","evidence_ref":"internal/memoryops/subagent_capture_test.go"}`
	withoutConsent, err := service.CaptureSubagentDiagnostic(SubagentDiagnosticInput{
		Project: "engram", SessionID: "session-103", Envelope: envelope,
	})
	if err != nil {
		t.Fatalf("capture without subagent consent: %v", err)
	}
	if withoutConsent.Captured || withoutConsent.ReasonCode != CaptureReasonConsentDisabled {
		t.Fatalf("capture without subagent consent = %+v", withoutConsent)
	}

	if _, err := service.EnableCapture(CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
	}); err != nil {
		t.Fatalf("enable subagent capture: %v", err)
	}
	if _, err := service.CaptureSubagentDiagnostic(SubagentDiagnosticInput{
		Project: "engram", SessionID: "session-103", Envelope: "ordinary subagent answer",
	}); !errors.Is(err, ErrSubagentDiagnosticEnvelope) {
		t.Fatalf("raw subagent answer error = %v, want %v", err, ErrSubagentDiagnosticEnvelope)
	}

	captured, err := service.CaptureSubagentDiagnostic(SubagentDiagnosticInput{
		Project: "engram", SessionID: "session-103", Envelope: envelope,
	})
	if err != nil {
		t.Fatalf("capture exact Diagnostic envelope: %v", err)
	}
	if !captured.Captured || captured.ReasonCode != CaptureReasonCaptured || captured.ExpiresAt == nil {
		t.Fatalf("captured envelope = %+v", captured)
	}

	status, err := service.CaptureStatus(CaptureStatusInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
	})
	if err != nil {
		t.Fatalf("inspect subagent capture status: %v", err)
	}
	if status.StoredCount != 1 {
		t.Fatalf("stored subagent Diagnostic captures = %d, want 1", status.StoredCount)
	}

	for table, want := range map[string]int{
		"diagnostic_captures": 1,
		"observations":        0,
		"memory_proposals":    0,
		"memory_checkpoints":  0,
		"sessions":            0,
		"sync_mutations":      0,
	} {
		var count int
		if err := service.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != want {
			t.Fatalf("%s count = %d, want %d", table, count, want)
		}
	}
	searchResults, err := service.store.Search("Subagent diagnostics", store.SearchOptions{Project: "engram"})
	if err != nil || len(searchResults) != 0 {
		t.Fatalf("durable search results = %+v, err=%v", searchResults, err)
	}
	context, err := service.store.FormatContext("engram", "project")
	if err != nil || strings.Contains(context, "Subagent diagnostics") {
		t.Fatalf("durable context = %q, err=%v", context, err)
	}
	exported, err := service.store.ExportProject("engram")
	if err != nil {
		t.Fatalf("export project: %v", err)
	}
	exportedJSON, err := json.Marshal(exported)
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}
	if strings.Contains(string(exportedJSON), "Subagent diagnostics") || strings.Contains(string(exportedJSON), "engram_diagnostic") {
		t.Fatalf("ordinary export exposed subagent Diagnostic capture: %s", exportedJSON)
	}
	var retiredAdmissionObjects int
	if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name LIKE 'admission_%'`).Scan(&retiredAdmissionObjects); err != nil {
		t.Fatalf("count retired Admission objects: %v", err)
	}
	if retiredAdmissionObjects != 0 {
		t.Fatalf("subagent capture restored %d retired Admission schema objects", retiredAdmissionObjects)
	}
}

func TestSubagentDiagnosticCaptureRejectsMalformedAndOversizedEnvelopes(t *testing.T) {
	service := newTestService(t)
	if _, err := service.EnableCapture(CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
	}); err != nil {
		t.Fatalf("enable subagent capture: %v", err)
	}

	tests := map[string]string{
		"legacy learning section": "## Key Learnings:\n- this must not enter passive Memory",
		"wrong kind":              `{"kind":"memory","title":"Wrong boundary","learning":"Must be rejected."}`,
		"unknown field":           `{"kind":"engram_diagnostic","title":"Unknown field","learning":"Must be rejected.","content":"raw fallback"}`,
		"duplicate field":         `{"kind":"engram_diagnostic","title":"First","title":"Second","learning":"Must be rejected."}`,
		"null evidence":           `{"kind":"engram_diagnostic","title":"Wrong type","learning":"Must be rejected.","evidence_ref":null}`,
		"missing title":           `{"kind":"engram_diagnostic","learning":"Must be rejected."}`,
		"trailing JSON":           `{"kind":"engram_diagnostic","title":"First","learning":"First object."}{"kind":"engram_diagnostic","title":"Second","learning":"Second object."}`,
		"oversized envelope":      strings.Repeat("x", MaxSubagentDiagnosticEnvelopeBytes+1),
		"oversized title":         `{"kind":"engram_diagnostic","title":"` + strings.Repeat("t", MaxSubagentDiagnosticTitleBytes+1) + `","learning":"Bounded."}`,
		"oversized learning":      `{"kind":"engram_diagnostic","title":"Bounded","learning":"` + strings.Repeat("l", MaxSubagentDiagnosticLearningBytes+1) + `"}`,
		"oversized evidence":      `{"kind":"engram_diagnostic","title":"Bounded","learning":"Bounded.","evidence_ref":"` + strings.Repeat("e", MaxSubagentDiagnosticEvidenceRefBytes+1) + `"}`,
	}
	for name, envelope := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := service.CaptureSubagentDiagnostic(SubagentDiagnosticInput{
				Project: "engram", SessionID: "session-103", Envelope: envelope,
			}); !errors.Is(err, ErrSubagentDiagnosticEnvelope) {
				t.Fatalf("capture error = %v, want %v", err, ErrSubagentDiagnosticEnvelope)
			}
		})
	}

	status, err := service.CaptureStatus(CaptureStatusInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
	})
	if err != nil {
		t.Fatalf("inspect rejected captures: %v", err)
	}
	if status.StoredCount != 0 {
		t.Fatalf("malformed envelopes persisted %d Diagnostic captures", status.StoredCount)
	}
}

func TestSubagentCaptureConsentIsSessionScopedAndReportsExpiry(t *testing.T) {
	service := newTestService(t)
	now := time.Date(2026, time.August, 31, 21, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	defaultStatus, err := service.CaptureStatus(CaptureStatusInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
		SessionID: "session-103",
	})
	if err != nil || defaultStatus.State != CaptureStateDefaultDisabled {
		t.Fatalf("default subagent status = %+v, err=%v", defaultStatus, err)
	}

	if _, err := service.EnableCapture(CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
	}); err != nil {
		t.Fatalf("enable independent prompt capture: %v", err)
	}
	expiresAt := now.Add(90 * time.Minute)
	grant, err := service.EnableCapture(CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
		SessionID: "session-103", ExpiresAt: &expiresAt, RetentionDays: 3,
	})
	if err != nil {
		t.Fatalf("enable session subagent capture: %v", err)
	}
	if grant.State != CaptureStateConsented || grant.Scope != CaptureConsentScopeSession {
		t.Fatalf("subagent session grant = %+v", grant)
	}

	other, err := service.CaptureStatus(CaptureStatusInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
		SessionID: "other-session",
	})
	if err != nil || other.State != CaptureStateDefaultDisabled || other.Enabled {
		t.Fatalf("other session status = %+v, err=%v", other, err)
	}

	now = expiresAt.Add(time.Second)
	expired, err := service.CaptureStatus(CaptureStatusInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
		SessionID: "session-103",
	})
	if err != nil {
		t.Fatalf("inspect expired subagent capture: %v", err)
	}
	if expired.State != CaptureStateExpired || expired.Enabled || expired.Scope != CaptureConsentScopeNone {
		t.Fatalf("expired subagent status = %+v", expired)
	}
}
