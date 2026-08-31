package recallbaseline

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/store"
)

func TestOpenRejectsUnknownOperationalSchemaVersion(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	ledger, err := Open(Config{DataDir: dataDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	db, err := sql.Open("sqlite", DatabasePath(dataDir))
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 2`); err != nil {
		t.Fatalf("set future schema: %v", err)
	}
	_ = db.Close()

	if _, err := Open(Config{DataDir: dataDir}); err == nil || !strings.Contains(err.Error(), "unsupported recall baseline schema version 2") {
		t.Fatalf("Open(future schema) error = %v", err)
	}
}

func TestLoadOrCreateSaltRetriesConcurrentPartialKey(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	path := KeyPath(dataDir)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create partial key: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close partial key: %v", err)
	}
	want := bytes.Repeat([]byte{7}, sha256.Size)
	written := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		written <- os.WriteFile(path, want, 0o600)
	}()

	got, err := loadOrCreateSalt(dataDir, bytes.NewReader(bytes.Repeat([]byte{9}, sha256.Size)))
	if err != nil {
		t.Fatalf("loadOrCreateSalt() error = %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("complete concurrent key: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("loadOrCreateSalt() = %x, want %x", got, want)
	}
}

func TestOperationalSchemaV1HasStableContentFreeColumns(t *testing.T) {
	t.Parallel()

	ledger, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()

	rows, err := ledger.db.Query(`SELECT name FROM pragma_table_info('baseline_events') ORDER BY cid`)
	if err != nil {
		t.Fatalf("read baseline event schema: %v", err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan baseline event schema: %v", err)
		}
		columns = append(columns, column)
	}
	want := []string{"id", "schema_version", "occurred_at", "expires_at", "kind", "surface", "operation", "outcome", "link_key", "latency_micros", "delivered_utf8_bytes"}
	if strings.Join(columns, ",") != strings.Join(want, ",") {
		t.Fatalf("baseline event columns = %v, want %v", columns, want)
	}
	for _, forbidden := range []string{"prompt", "query", "content", "assistant", "path", "diff", "credential", "host", "session", "turn", "memory_id"} {
		for _, column := range columns {
			if strings.Contains(column, forbidden) {
				t.Fatalf("baseline event schema contains forbidden column %q", column)
			}
		}
	}
}

func TestLedgerPersistsOnlyContentFreeSaltedEvents(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	ledger, err := Open(Config{DataDir: dataDir, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()

	sentinels := []string{
		"host-secret", "session-secret", "turn-secret", "memory-secret",
		"prompt sentinel", "query sentinel", "assistant sentinel",
		"/private/transcript.jsonl", "repository diff sentinel", "credential sentinel",
	}
	err = ledger.Record(Event{
		Kind:      EventCheckpoint,
		Surface:   SurfaceLifecycle,
		Operation: "terminal_checkpoint",
		Outcome:   OutcomeCompleted,
		Link: Linkage{
			Host:       sentinels[0],
			SessionID:  sentinels[1],
			RootTurnID: sentinels[2],
		},
	})
	if err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if err := ledger.Record(Event{
		Kind: EventOperation, Surface: SurfaceCLI,
		Operation: sentinels[5], Outcome: OutcomeSuccess,
	}); err == nil {
		t.Fatal("Record() accepted unbounded query text as an operation name")
	}
	if err := ledger.Record(Event{
		Kind: EventOperation, Surface: SurfaceCLI, Operation: "search", Outcome: OutcomeSuccess,
		Link: Linkage{Host: sentinels[0], SessionID: sentinels[1], RootTurnID: sentinels[2]},
	}); err == nil {
		t.Fatal("Record() accepted unnecessary identity linkage for an operation")
	}

	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("json.Marshal(report) error = %v", err)
	}

	if err := ledger.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	databaseBytes, err := os.ReadFile(DatabasePath(dataDir))
	if err != nil {
		t.Fatalf("ReadFile(database) error = %v", err)
	}
	keyBytes, err := os.ReadFile(KeyPath(dataDir))
	if err != nil {
		t.Fatalf("ReadFile(key) error = %v", err)
	}
	if len(keyBytes) != 32 {
		t.Fatalf("install key length = %d, want 32", len(keyBytes))
	}
	if bytes.Contains(databaseBytes, keyBytes) {
		t.Fatal("baseline database contains its own HMAC key")
	}
	if info, err := os.Stat(KeyPath(dataDir)); err != nil {
		t.Fatalf("Stat(key) error = %v", err)
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("install key mode = %o, want 600", info.Mode().Perm())
	}
	combined := string(reportJSON) + string(databaseBytes)
	for _, sentinel := range sentinels {
		if strings.Contains(combined, sentinel) {
			t.Fatalf("baseline artifacts leaked forbidden value %q", sentinel)
		}
	}
	if report.SchemaVersion != ReportSchemaVersion {
		t.Fatalf("report schema = %q, want %q", report.SchemaVersion, ReportSchemaVersion)
	}
	if report.EventSchemaVersion != EventSchemaVersion {
		t.Fatalf("event schema = %q, want %q", report.EventSchemaVersion, EventSchemaVersion)
	}
}

func TestPerInstallSaltAndSeparateDatabasePreventOrdinaryMemoryJoins(t *testing.T) {
	t.Parallel()

	identity := Linkage{Host: "codex", SessionID: "shared-session", RootTurnID: "shared-turn"}
	keys := make([]string, 0, 2)
	for index, saltByte := range []byte{1, 2} {
		dataDir := t.TempDir()
		ledger, err := Open(Config{DataDir: dataDir, Random: bytes.NewReader(bytes.Repeat([]byte{saltByte}, 32))})
		if err != nil {
			t.Fatalf("Open(install %d) error = %v", index, err)
		}
		if err := ledger.Record(Event{Kind: EventCheckpoint, Surface: SurfaceLifecycle, Operation: "terminal_checkpoint", Outcome: OutcomeCompleted, Link: identity}); err != nil {
			t.Fatalf("Record(install %d) error = %v", index, err)
		}
		var key string
		if err := ledger.db.QueryRow(`SELECT link_key FROM baseline_events`).Scan(&key); err != nil {
			t.Fatalf("read salted key for install %d: %v", index, err)
		}
		keys = append(keys, key)
		if err := ledger.Close(); err != nil {
			t.Fatalf("Close(install %d) error = %v", index, err)
		}

		memoryStore, err := store.New(store.Config{DataDir: dataDir})
		if err != nil {
			t.Fatalf("store.New(install %d) error = %v", index, err)
		}
		exported, err := memoryStore.Export()
		if err != nil {
			t.Fatalf("Export(install %d) error = %v", index, err)
		}
		exportedJSON, err := json.Marshal(exported)
		if err != nil {
			t.Fatalf("json.Marshal(export %d) error = %v", index, err)
		}
		context, err := memoryStore.FormatContext("", "")
		if err != nil {
			t.Fatalf("FormatContext(install %d) error = %v", index, err)
		}
		_ = memoryStore.Close()
		ordinarySurface := string(exportedJSON) + context
		keyBytes, err := os.ReadFile(KeyPath(dataDir))
		if err != nil {
			t.Fatalf("ReadFile(key %d) error = %v", index, err)
		}
		for _, forbidden := range []string{identity.Host, identity.SessionID, identity.RootTurnID, "shared-memory", key, EventSchemaVersion} {
			if strings.Contains(ordinarySurface, forbidden) {
				t.Fatalf("ordinary Memory surface joined baseline value %q", forbidden)
			}
		}
		if bytes.Contains(exportedJSON, keyBytes) || strings.Contains(context, string(keyBytes)) {
			t.Fatal("ordinary Memory surface exposed the baseline HMAC key")
		}
	}
	if keys[0] == keys[1] {
		t.Fatalf("two installations derived the same linkage key %q", keys[0])
	}
}

func TestReportSeparatesLifecycleAndOperationDenominators(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	ledger, err := Open(Config{DataDir: t.TempDir(), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()

	link := func(turn string) Linkage {
		return Linkage{Host: "codex", SessionID: "session", RootTurnID: turn}
	}
	record := func(event Event) {
		t.Helper()
		if err := ledger.Record(event); err != nil {
			t.Fatalf("Record(%+v) error = %v", event, err)
		}
	}

	// The same turn can be observed missing before the single Stop recovery and
	// completed afterward; checkpoint coverage retains one final denominator.
	record(Event{Kind: EventCheckpoint, Surface: SurfaceLifecycle, Operation: "terminal_checkpoint", Outcome: OutcomeMissing, Link: link("turn-1")})
	record(Event{Kind: EventCheckpoint, Surface: SurfaceLifecycle, Operation: "terminal_checkpoint", Outcome: OutcomeCompleted, Link: link("turn-1")})
	record(Event{Kind: EventCheckpoint, Surface: SurfaceLifecycle, Operation: "terminal_checkpoint", Outcome: OutcomeConflict, Link: link("turn-2")})
	record(Event{Kind: EventCheckpoint, Surface: SurfaceLifecycle, Operation: "terminal_checkpoint", Outcome: OutcomeUnknown, Link: link("turn-3")})
	record(Event{Kind: EventStop, Surface: SurfaceLifecycle, Operation: "stop", Outcome: OutcomeContinuationRequired})
	record(Event{Kind: EventStop, Surface: SurfaceLifecycle, Operation: "stop", Outcome: OutcomeCompleted})
	record(Event{Kind: EventCapture, Surface: SurfaceLifecycle, Operation: "prompt", Outcome: OutcomeEnabled})
	record(Event{Kind: EventCapture, Surface: SurfaceLifecycle, Operation: "subagent", Outcome: OutcomeUnknown})
	record(Event{Kind: EventSubagentStop, Surface: SurfaceLifecycle, Operation: "subagent_stop", Outcome: OutcomeObserved})

	record(Event{Kind: EventOperation, Surface: SurfaceCLI, Operation: "search", Outcome: OutcomeSuccess, Latency: KnownLatency(10 * time.Millisecond), DeliveredUTF8Bytes: KnownBytes(6)})
	record(Event{Kind: EventOperation, Surface: SurfaceCLI, Operation: "search", Outcome: OutcomeError})
	record(Event{Kind: EventOperation, Surface: SurfaceMCP, Operation: "mem_search", Outcome: OutcomeSuccess, Latency: KnownLatency(30 * time.Millisecond), DeliveredUTF8Bytes: KnownBytes(9)})

	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	coverage := report.Lifecycle.Checkpoints
	if coverage.EligibleTurns != 3 || coverage.Completed != 1 || coverage.Missing != 0 || coverage.Conflicting != 1 || coverage.Unknown != 1 {
		t.Fatalf("checkpoint coverage = %+v", coverage)
	}
	if report.Lifecycle.Stop.Events != 2 || report.Lifecycle.Stop.Continuations != 1 {
		t.Fatalf("Stop report = %+v", report.Lifecycle.Stop)
	}
	if report.Lifecycle.Capture.Events != 2 || report.Lifecycle.Capture.Enabled != 1 || report.Lifecycle.Capture.Unknown != 1 {
		t.Fatalf("Capture report = %+v", report.Lifecycle.Capture)
	}
	if report.Lifecycle.SubagentStop.Events != 1 || report.Lifecycle.SubagentStop.Observed != 1 {
		t.Fatalf("SubagentStop report = %+v", report.Lifecycle.SubagentStop)
	}

	if len(report.Operations) != 2 {
		t.Fatalf("operation groups = %d, want 2", len(report.Operations))
	}
	cli := report.Operations[0]
	if cli.Surface != SurfaceCLI || cli.Operation != "search" || cli.Events != 2 || cli.UnknownLatency != 1 || cli.UnknownBytes != 1 || cli.P50LatencyMillis != 10 || cli.P95LatencyMillis != 10 || cli.TotalUTF8Bytes != 6 {
		t.Fatalf("CLI operation report = %+v", cli)
	}
	mcp := report.Operations[1]
	if mcp.Surface != SurfaceMCP || mcp.Operation != "mem_search" || mcp.Events != 1 || mcp.UnknownLatency != 0 || mcp.UnknownBytes != 0 || mcp.P50LatencyMillis != 30 || mcp.P95LatencyMillis != 30 || mcp.TotalUTF8Bytes != 9 {
		t.Fatalf("MCP operation report = %+v", mcp)
	}
}

func TestReportAppliesDeclaredRetentionBeforeAggregation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	clock := now
	ledger, err := Open(Config{
		DataDir: t.TempDir(),
		Now:     func() time.Time { return clock },
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()

	old := now.Add(-8 * 24 * time.Hour)
	if err := ledger.Record(Event{Kind: EventOperation, Surface: SurfaceCLI, Operation: "search", Outcome: OutcomeSuccess, OccurredAt: old}); err != nil {
		t.Fatalf("Record(old) error = %v", err)
	}
	clock = old
	if err := ledger.RecordCollectionLoss(2, 1); err != nil {
		t.Fatalf("RecordCollectionLoss(old) error = %v", err)
	}
	clock = now
	if err := ledger.Record(Event{Kind: EventOperation, Surface: SurfaceCLI, Operation: "search", Outcome: OutcomeSuccess}); err != nil {
		t.Fatalf("Record(current) error = %v", err)
	}

	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if len(report.Operations) != 1 || report.Operations[0].Events != 1 {
		t.Fatalf("operations after retention = %+v", report.Operations)
	}
	if report.Retention.WindowHours != 7*24 || report.Retention.ExpiredEventsPurged != 1 || report.Retention.ExpiredCollectionRecordsPurged != 1 {
		t.Fatalf("retention report = %+v", report.Retention)
	}
	if report.Collection.DroppedEvents != 0 || report.Collection.WriteFailures != 0 {
		t.Fatalf("expired collection loss remained in report = %+v", report.Collection)
	}
}

func TestLedgerRejectsFutureOccurrenceThatCouldEvadeRetention(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	ledger, err := Open(Config{DataDir: t.TempDir(), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()
	err = ledger.Record(Event{
		Kind: EventOperation, Surface: SurfaceCLI, Operation: "search", Outcome: OutcomeSuccess,
		OccurredAt: now.Add(time.Nanosecond),
	})
	if err == nil || !strings.Contains(err.Error(), "must not be in the future") {
		t.Fatalf("Record(future) error = %v", err)
	}
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if len(report.Operations) != 0 {
		t.Fatalf("future event entered retention window: %+v", report.Operations)
	}
}

func TestReportAggregationIsDeterministicAcrossInsertionOrder(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	events := []Event{
		{Kind: EventOperation, Surface: SurfaceMCP, Operation: "mem_search", Outcome: OutcomeSuccess, Latency: KnownLatency(30 * time.Millisecond), DeliveredUTF8Bytes: KnownBytes(9)},
		{Kind: EventOperation, Surface: SurfaceCLI, Operation: "search", Outcome: OutcomeSuccess, Latency: KnownLatency(20 * time.Millisecond), DeliveredUTF8Bytes: KnownBytes(6)},
		{Kind: EventOperation, Surface: SurfaceCLI, Operation: "search", Outcome: OutcomeSuccess, Latency: KnownLatency(10 * time.Millisecond), DeliveredUTF8Bytes: KnownBytes(4)},
	}
	reportFor := func(ordered []Event) Report {
		t.Helper()
		ledger, err := Open(Config{DataDir: t.TempDir(), Now: func() time.Time { return now }})
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer ledger.Close()
		for _, event := range ordered {
			if err := ledger.Record(event); err != nil {
				t.Fatalf("Record() error = %v", err)
			}
		}
		report, err := ledger.Report(protocolcontract.CompatibilityReport{})
		if err != nil {
			t.Fatalf("Report() error = %v", err)
		}
		return report
	}

	forward := reportFor(events)
	reverse := reportFor([]Event{events[2], events[1], events[0]})
	forwardJSON, _ := json.Marshal(forward)
	reverseJSON, _ := json.Marshal(reverse)
	if !bytes.Equal(forwardJSON, reverseJSON) {
		t.Fatalf("reports differ by insertion order:\n%s\n%s", forwardJSON, reverseJSON)
	}
	if forward.Operations[0].P50LatencyMillis != 10 || forward.Operations[0].P95LatencyMillis != 20 {
		t.Fatalf("nearest-rank percentiles = %+v", forward.Operations[0])
	}
}
