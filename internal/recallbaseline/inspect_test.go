package recallbaseline

import (
	"crypto/sha256"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestInspectOperationReadOnlySupportsV1WithoutMigrating(t *testing.T) {
	dataDir := t.TempDir()
	path := DatabasePath(dataDir)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	_, err = db.Exec(`CREATE TABLE baseline_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		schema_version TEXT NOT NULL,
		occurred_at TEXT NOT NULL,
		expires_at TEXT NOT NULL,
		kind TEXT NOT NULL,
		surface TEXT NOT NULL,
		operation TEXT NOT NULL,
		outcome TEXT NOT NULL,
		link_key TEXT NOT NULL,
		latency_micros INTEGER,
		delivered_utf8_bytes INTEGER
	) STRICT;
	PRAGMA user_version = 1;`)
	if err != nil {
		t.Fatalf("create v1 schema: %v", err)
	}
	now := time.Now().UTC()
	_, err = db.Exec(`INSERT INTO baseline_events(
		schema_version, occurred_at, expires_at, kind, surface, operation, outcome, link_key, latency_micros, delivered_utf8_bytes
	) VALUES (?, ?, ?, ?, ?, ?, ?, '', ?, ?)`, "recall-baseline-events-v1", now.Format(time.RFC3339Nano),
		now.Add(time.Hour).Format(time.RFC3339Nano), EventOperation, SurfaceLifecycle, "session_start", OutcomeSuccess, 9000, 55)
	if err != nil {
		t.Fatalf("insert v1 event: %v", err)
	}
	_ = db.Close()
	before := snapshotBaselineInspectionDir(t, dataDir)

	report, observed, err := InspectOperationReadOnly(Config{DataDir: dataDir, Now: func() time.Time { return now }}, SurfaceLifecycle, "session_start")
	if err != nil || !observed || report.Events != 1 || report.P50LatencyMillis != 9 || report.TotalUTF8Bytes != 55 {
		t.Fatalf("v1 lifecycle metrics = %+v observed=%t err=%v", report, observed, err)
	}
	hostReport, hostObserved, hostErr := InspectHostOperationReadOnly(Config{DataDir: dataDir, Now: func() time.Time { return now }}, SurfaceLifecycle, "session_start", HostCursor)
	if hostErr != nil || hostObserved || hostReport.Events != 0 {
		t.Fatalf("v1 host metrics = %+v observed=%t err=%v", hostReport, hostObserved, hostErr)
	}
	after := snapshotBaselineInspectionDir(t, dataDir)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("read-only v1 inspection migrated baseline files:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestInspectOperationReadOnlyReportsLifecycleLatencyAndInjectedBytesWithoutMutation(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	ledger, err := Open(Config{DataDir: dataDir, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("open baseline ledger: %v", err)
	}
	for _, event := range []Event{
		{Kind: EventOperation, Surface: SurfaceLifecycle, Operation: "session_start", Outcome: OutcomeSuccess, Latency: KnownLatency(12 * time.Millisecond), DeliveredUTF8Bytes: KnownBytes(180)},
		{Kind: EventOperation, Surface: SurfaceLifecycle, Operation: "session_start", Outcome: OutcomeSuccess, Latency: KnownLatency(20 * time.Millisecond), DeliveredUTF8Bytes: KnownBytes(220)},
	} {
		if err := ledger.Record(event); err != nil {
			t.Fatalf("record lifecycle metric: %v", err)
		}
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("close baseline ledger: %v", err)
	}
	before := snapshotBaselineInspectionDir(t, dataDir)

	report, observed, err := InspectOperationReadOnly(Config{DataDir: dataDir, Now: func() time.Time { return now }}, SurfaceLifecycle, "session_start")
	if err != nil {
		t.Fatalf("inspect lifecycle metrics: %v", err)
	}
	if !observed || report.Events != 2 || report.LatencySamples != 2 || report.P50LatencyMillis != 12 || report.P95LatencyMillis != 20 || report.ByteSamples != 2 || report.TotalUTF8Bytes != 400 {
		t.Fatalf("lifecycle metrics = %+v observed=%t", report, observed)
	}
	after := snapshotBaselineInspectionDir(t, dataDir)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("read-only lifecycle inspection mutated baseline files:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestInspectOperationReadOnlyMissingLedgerDoesNotCreateState(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "missing")
	report, observed, err := InspectOperationReadOnly(Config{DataDir: dataDir}, SurfaceLifecycle, "session_start")
	if err != nil || observed || report.Events != 0 {
		t.Fatalf("missing lifecycle metrics = %+v observed=%t err=%v", report, observed, err)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("read-only lifecycle inspection created state: %v", err)
	}
}

func TestInspectHostOperationReadOnlyFiltersOtherMCPHosts(t *testing.T) {
	dataDir := t.TempDir()
	ledger, err := Open(Config{DataDir: dataDir})
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []Host{HostCodex, HostCursor} {
		if err := ledger.Record(Event{Kind: EventOperation, Surface: SurfaceMCP, Operation: "mem_current_project", Outcome: OutcomeSuccess, Host: host}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	before := snapshotBaselineInspectionDir(t, dataDir)
	report, observed, err := InspectHostOperationReadOnly(Config{DataDir: dataDir}, SurfaceMCP, "mem_current_project", HostCursor)
	if err != nil || !observed || report.Events != 1 || report.Succeeded != 1 || report.Host != HostCursor {
		t.Fatalf("cursor observation = %+v, %t, %v", report, observed, err)
	}
	if after := snapshotBaselineInspectionDir(t, dataDir); !reflect.DeepEqual(before, after) {
		t.Fatal("host inspection mutated the baseline")
	}
	if _, _, err := InspectHostOperationReadOnly(Config{DataDir: dataDir}, SurfaceMCP, "mem_current_project", HostUnknown); err == nil {
		t.Fatal("unknown host should not yield an attributable report")
	}
}

func snapshotBaselineInspectionDir(t *testing.T, dir string) map[string][sha256.Size]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read baseline inspection directory: %v", err)
	}
	result := make(map[string][sha256.Size]byte, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("read baseline inspection file: %v", err)
		}
		result[entry.Name()] = sha256.Sum256(raw)
	}
	return result
}
