package recallbaseline

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

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
