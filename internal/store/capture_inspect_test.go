package store

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestInspectCaptureConsentReadOnlyMissingStoreDefaultsOffWithoutCreatingState(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "missing")

	status, err := InspectCaptureConsentReadOnly(dataDir, "engram", CaptureContentTypePrompt, "", time.Now().UTC())
	if err != nil {
		t.Fatalf("inspect missing capture consent: %v", err)
	}
	if status.SchemaPresent || status.Consent != nil {
		t.Fatalf("missing store status = %#v, want absent schema and disabled consent", status)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("read-only inspection created data directory: %v", err)
	}
}

func TestInspectCaptureConsentReadOnlyReportsGrantWithoutReadingOrMutatingCaptureState(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	if err := s.UpsertCaptureConsent(CaptureConsent{
		Project:       "engram",
		ContentType:   CaptureContentTypePrompt,
		RetentionDays: DefaultDiagnosticRetentionDays,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatalf("seed capture consent: %v", err)
	}
	if _, err := s.DB().Exec(`INSERT INTO diagnostic_captures (
		project, content_type, session_id, content, created_at, expires_at
	) VALUES ('engram', 'prompt', 'session-secret', 'synthetic private prompt', ?, ?)`,
		now.Format(time.RFC3339Nano), now.Add(24*time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed Diagnostic capture: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	before := snapshotCaptureInspectionDir(t, cfg.DataDir)
	// Inspection happens after the stored Diagnostic content has expired. The
	// setup/read-only seam must still avoid opening the mutating Store lifecycle.
	status, err := InspectCaptureConsentReadOnly(cfg.DataDir, "engram", CaptureContentTypePrompt, "", now.Add(48*time.Hour))
	if err != nil {
		t.Fatalf("inspect capture consent: %v", err)
	}
	after := snapshotCaptureInspectionDir(t, cfg.DataDir)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("read-only inspection mutated store files:\nbefore=%#v\nafter=%#v", before, after)
	}
	if !status.SchemaPresent || status.Consent == nil {
		t.Fatalf("capture consent status = %#v", status)
	}
	if status.Consent.Project != "engram" || status.Consent.ContentType != CaptureContentTypePrompt || status.Consent.RetentionDays != DefaultDiagnosticRetentionDays {
		t.Fatalf("capture consent = %#v", status.Consent)
	}
}

func TestInspectCaptureConsentReadOnlyReadsCommittedGrantFromActiveWAL(t *testing.T) {
	inspectionTempRoot := t.TempDir()
	t.Setenv("TMPDIR", inspectionTempRoot)
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	if err := s.UpsertCaptureConsent(CaptureConsent{
		Project:       "engram",
		ContentType:   CaptureContentTypePrompt,
		RetentionDays: 14,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatalf("seed capture consent: %v", err)
	}

	before := snapshotCaptureInspectionDir(t, cfg.DataDir)
	status, err := InspectCaptureConsentReadOnly(cfg.DataDir, "engram", CaptureContentTypePrompt, "", now)
	if err != nil {
		t.Fatalf("inspect active capture consent: %v", err)
	}
	after := snapshotCaptureInspectionDir(t, cfg.DataDir)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("active read-only inspection mutated store files:\nbefore=%#v\nafter=%#v", before, after)
	}
	if status.Consent == nil || status.Consent.RetentionDays != 14 {
		t.Fatalf("active WAL consent = %#v, want committed grant", status.Consent)
	}
	tempEntries, err := os.ReadDir(inspectionTempRoot)
	if err != nil {
		t.Fatalf("read inspection temp root: %v", err)
	}
	if len(tempEntries) != 0 {
		t.Fatalf("read-only inspection left temporary state: %#v", tempEntries)
	}
}

func TestInspectCaptureConsentAggregateReadOnlyDistinguishesLifecycleStates(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	now := time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC)
	if err := s.UpsertCaptureConsent(CaptureConsent{
		Project: "engram", ContentType: CaptureContentTypeSubagentOutput,
		SessionID: "expired-secret", RetentionDays: DefaultDiagnosticRetentionDays,
		ExpiresAt: pointerCaptureInspectionTime(now.Add(-time.Minute)), UpdatedAt: now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("seed expired consent: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	expired, err := InspectCaptureConsentAggregateReadOnly(cfg.DataDir, "engram", CaptureContentTypeSubagentOutput, now)
	if err != nil {
		t.Fatalf("inspect expired consent: %v", err)
	}
	if !expired.SchemaPresent || !expired.Expired || expired.Consent != nil || expired.SessionScoped {
		t.Fatalf("expired aggregate status = %#v", expired)
	}

	s, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	if err := s.UpsertCaptureConsent(CaptureConsent{
		Project: "engram", ContentType: CaptureContentTypeSubagentOutput,
		SessionID: "active-secret", RetentionDays: 3,
		ExpiresAt: pointerCaptureInspectionTime(now.Add(time.Hour)), UpdatedAt: now,
	}); err != nil {
		t.Fatalf("seed active session consent: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	before := snapshotCaptureInspectionDir(t, cfg.DataDir)
	active, err := InspectCaptureConsentAggregateReadOnly(cfg.DataDir, "engram", CaptureContentTypeSubagentOutput, now)
	if err != nil {
		t.Fatalf("inspect active session consent: %v", err)
	}
	if active.Consent == nil || !active.SessionScoped || active.Expired || active.Consent.RetentionDays != 3 {
		t.Fatalf("active aggregate status = %#v", active)
	}
	if active.Consent.SessionID != "" {
		t.Fatalf("aggregate inspection exposed opaque session identity %q", active.Consent.SessionID)
	}
	after := snapshotCaptureInspectionDir(t, cfg.DataDir)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("aggregate read-only inspection mutated store files")
	}
}

func pointerCaptureInspectionTime(value time.Time) *time.Time { return &value }

func snapshotCaptureInspectionDir(t *testing.T, dir string) map[string][sha256.Size]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read capture data directory: %v", err)
	}
	result := make(map[string][sha256.Size]byte, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("read capture state %s: %v", entry.Name(), err)
		}
		result[entry.Name()] = sha256.Sum256(data)
	}
	return result
}
