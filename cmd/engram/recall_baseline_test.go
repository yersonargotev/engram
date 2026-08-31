package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	engrammcp "github.com/yersonargotev/engram/internal/mcp"
	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/recallbaseline"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCheckpointStopRecordsLifecycleCoverageWithoutRawIdentity(t *testing.T) {
	t.Setenv("ENGRAM_RECALL_BASELINE", "1")
	cfg := store.Config{DataDir: t.TempDir()}
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	_, err = memoryops.New(s).RecordCheckpoint(memoryops.CheckpointRecordInput{
		Host: "codex", SessionID: "session-raw-sentinel", RootTurnID: "turn-raw-sentinel",
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	})
	if err != nil {
		t.Fatalf("RecordCheckpoint() error = %v", err)
	}
	_ = s.Close()

	stdout, stderr := captureOutput(t, func() {
		cmdCheckpointVerifyStop(cfg, "codex", strings.NewReader(`{"session_id":"session-raw-sentinel","turn_id":"turn-raw-sentinel","stop_hook_active":false}`))
	})
	if stderr != "" || strings.TrimSpace(stdout) != "{}" {
		t.Fatalf("verify-stop stdout=%q stderr=%q", stdout, stderr)
	}

	ledger, err := recallbaseline.Open(recallbaseline.Config{DataDir: cfg.DataDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if report.Lifecycle.Checkpoints.EligibleTurns != 1 || report.Lifecycle.Checkpoints.Completed != 1 || report.Lifecycle.Stop.Events != 1 {
		t.Fatalf("lifecycle report = %+v", report.Lifecycle)
	}
	if len(report.Operations) != 1 || report.Operations[0].Operation != "checkpoint_verify_stop" || report.Operations[0].UnknownLatency != 0 || report.Operations[0].UnknownBytes != 0 {
		t.Fatalf("Stop operation report = %+v", report.Operations)
	}

	if raw, err := os.ReadFile(recallbaseline.DatabasePath(cfg.DataDir)); err != nil {
		t.Fatalf("ReadFile(database) error = %v", err)
	} else if strings.Contains(string(raw), "session-raw-sentinel") || strings.Contains(string(raw), "turn-raw-sentinel") {
		t.Fatal("baseline database persisted raw Stop identity")
	}
}

func TestCheckpointCLIRecordsConflictingCoverage(t *testing.T) {
	stubExitWithPanic(t)
	t.Setenv("ENGRAM_RECALL_BASELINE", "1")
	cfg := store.Config{DataDir: t.TempDir()}
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	_, err = memoryops.New(s).RecordCheckpoint(memoryops.CheckpointRecordInput{
		Host: "codex", SessionID: "session-conflict-raw", RootTurnID: "turn-conflict-raw",
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	})
	if err != nil {
		t.Fatalf("RecordCheckpoint() error = %v", err)
	}
	_ = s.Close()

	withArgs(t,
		"engram", "checkpoint", "record",
		"--host", "codex", "--session-id", "session-conflict-raw", "--root-turn-id", "turn-conflict-raw",
		"--disposition", store.CheckpointDispositionNeedsReview, "--project", "engram",
		`--proposal-json={"title":"conflicting proposal","content":"must remain outside baseline"}`,
		"--json",
	)
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
	if recovered == nil || !strings.Contains(stderr, memoryops.CheckpointErrorCodeConflict) {
		t.Fatalf("checkpoint conflict recovered=%v stderr=%q", recovered, stderr)
	}

	ledger, err := recallbaseline.Open(recallbaseline.Config{DataDir: cfg.DataDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if report.Lifecycle.Checkpoints.EligibleTurns != 1 || report.Lifecycle.Checkpoints.Conflicting != 1 {
		t.Fatalf("checkpoint conflict coverage = %+v", report.Lifecycle.Checkpoints)
	}
	if raw, err := os.ReadFile(recallbaseline.DatabasePath(cfg.DataDir)); err != nil {
		t.Fatalf("ReadFile(database) error = %v", err)
	} else if strings.Contains(string(raw), "session-conflict-raw") || strings.Contains(string(raw), "turn-conflict-raw") || strings.Contains(string(raw), "conflicting proposal") {
		t.Fatal("baseline database persisted raw CLI checkpoint content or identity")
	}
}

func TestRecallBaselineMCPObserverPersistsBoundedOperation(t *testing.T) {
	t.Setenv("ENGRAM_RECALL_BASELINE", "1")
	cfg := store.Config{DataDir: t.TempDir()}
	observe, observeCheckpoint, closeObserver := newRecallBaselineMCPObservers(cfg)
	if observe == nil {
		t.Fatal("newRecallBaselineMCPObservers() returned nil operation observer")
	}
	if observeCheckpoint == nil {
		t.Fatal("newRecallBaselineMCPObservers() returned nil checkpoint observer")
	}
	bytes := int64(21)
	observe(engrammcp.OperationObservation{
		Operation: "mem_search", Outcome: engrammcp.OperationSuccess,
		Latency: 8 * time.Millisecond, DeliveredUTF8Bytes: &bytes,
	})
	observeCheckpoint(engrammcp.CheckpointObservation{
		Host: "codex", SessionID: "session-mcp-raw", RootTurnID: "turn-mcp-raw",
		Outcome: engrammcp.CheckpointConflict,
	})
	closeObserver()

	ledger, err := recallbaseline.Open(recallbaseline.Config{DataDir: cfg.DataDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if len(report.Operations) != 1 || report.Operations[0].Surface != recallbaseline.SurfaceMCP || report.Operations[0].TotalUTF8Bytes != 21 {
		t.Fatalf("MCP operations = %+v", report.Operations)
	}
	if report.Lifecycle.Checkpoints.EligibleTurns != 1 || report.Lifecycle.Checkpoints.Conflicting != 1 {
		t.Fatalf("MCP checkpoint coverage = %+v", report.Lifecycle.Checkpoints)
	}
	if raw, err := os.ReadFile(recallbaseline.DatabasePath(cfg.DataDir)); err != nil {
		t.Fatalf("ReadFile(database) error = %v", err)
	} else if strings.Contains(string(raw), "session-mcp-raw") || strings.Contains(string(raw), "turn-mcp-raw") {
		t.Fatal("baseline database persisted raw MCP checkpoint identity")
	}
}

func TestRecallBaselineAutomaticCollectionIsOptIn(t *testing.T) {
	t.Setenv("ENGRAM_RECALL_BASELINE", "")
	cfg := store.Config{DataDir: filepath.Join(t.TempDir(), "must-not-exist")}

	observe, observeCheckpoint, closeObserver := newRecallBaselineMCPObservers(cfg)
	if observe != nil {
		t.Fatal("newRecallBaselineMCPObservers() enabled operation collection without opt-in")
	}
	if observeCheckpoint != nil {
		t.Fatal("newRecallBaselineMCPObservers() enabled checkpoint collection without opt-in")
	}
	closeObserver()
	recordRecallBaselineEvents(cfg, recallbaseline.Event{
		Kind: recallbaseline.EventOperation, Surface: recallbaseline.SurfaceCLI,
		Operation: "search", Outcome: recallbaseline.OutcomeSuccess,
	})
	if _, err := os.Stat(cfg.DataDir); !os.IsNotExist(err) {
		t.Fatalf("disabled automatic collection created state: %v", err)
	}
}

func TestRecallBaselineCollectsCurrentCLISearchOperation(t *testing.T) {
	t.Setenv("ENGRAM_RECALL_BASELINE", "1")
	cfg := store.Config{DataDir: t.TempDir()}
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	if err := s.CreateSession("session", "engram", "/tmp"); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	observationID, err := s.AddObservation(store.AddObservationParams{SessionID: "session", Project: "engram", Scope: "project", Type: "decision", Title: "Baseline", Content: "searchable baseline"})
	if err != nil {
		t.Fatalf("AddObservation() error = %v", err)
	}
	_ = s.Close()

	withArgs(t, "engram", "search", "searchable", "--project", "engram", "--json")
	stdout, stderr := captureOutput(t, func() { cmdSearch(cfg) })
	if stderr != "" || !json.Valid([]byte(stdout)) {
		t.Fatalf("search stdout=%q stderr=%q", stdout, stderr)
	}
	withArgs(t, "engram", "context", "engram", "--json")
	contextOutput, contextErr := captureOutput(t, func() { cmdContext(cfg) })
	if contextErr != "" || !json.Valid([]byte(contextOutput)) {
		t.Fatalf("context stdout=%q stderr=%q", contextOutput, contextErr)
	}
	withArgs(t, "engram", "get", strconv.FormatInt(observationID, 10), "--json")
	getOutput, getErr := captureOutput(t, func() { cmdGet(cfg) })
	if getErr != "" || !json.Valid([]byte(getOutput)) {
		t.Fatalf("get stdout=%q stderr=%q", getOutput, getErr)
	}

	ledger, err := recallbaseline.Open(recallbaseline.Config{DataDir: cfg.DataDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if len(report.Operations) != 3 || report.Operations[0].Operation != "context" || report.Operations[0].TotalUTF8Bytes != int64(len(contextOutput)) || report.Operations[1].Operation != "get" || report.Operations[1].TotalUTF8Bytes != int64(len(getOutput)) || report.Operations[2].Surface != recallbaseline.SurfaceCLI || report.Operations[2].Operation != "search" || report.Operations[2].LatencySamples != 1 || report.Operations[2].TotalUTF8Bytes != int64(len(stdout)) {
		t.Fatalf("CLI search report = %+v", report.Operations)
	}
}

func TestRecallBaselineCountsNonRecallCLICommands(t *testing.T) {
	t.Setenv("ENGRAM_RECALL_BASELINE", "1")
	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)
	withArgs(t, "engram", "stats", "--json")
	stdout, stderr := captureOutput(t, main)
	if stderr != "" || !json.Valid([]byte(stdout)) {
		t.Fatalf("stats stdout=%q stderr=%q", stdout, stderr)
	}

	ledger, err := recallbaseline.Open(recallbaseline.Config{DataDir: dataDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if len(report.Operations) != 1 || report.Operations[0].Operation != "stats" ||
		report.Operations[0].Succeeded != 1 || report.Operations[0].LatencySamples != 1 ||
		report.Operations[0].UnknownBytes != 1 {
		t.Fatalf("stats operation report = %+v", report.Operations)
	}
}

func TestRecallBaselineNonzeroExitRecordsExactlyOneFailedOperation(t *testing.T) {
	t.Setenv("ENGRAM_RECALL_BASELINE", "1")
	cfg := store.Config{DataDir: t.TempDir()}
	originalProcessExit := processExit
	originalExitFunc := exitFunc
	processExit = func(code int) { panic(code) }
	exitFunc = exitWithRecallBaseline
	t.Cleanup(func() {
		processExit = originalProcessExit
		exitFunc = originalExitFunc
	})

	_, _, recovered := captureOutputAndRecover(t, func() {
		runRecallBaselineCLI(cfg, "stats", func() { exitFunc(1) })
	})
	if recovered != 1 {
		t.Fatalf("exit recovery = %v, want 1", recovered)
	}

	ledger, err := recallbaseline.Open(recallbaseline.Config{DataDir: cfg.DataDir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer ledger.Close()
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if len(report.Operations) != 1 || report.Operations[0].Events != 1 ||
		report.Operations[0].Succeeded != 0 || report.Operations[0].Failed != 1 {
		t.Fatalf("failed stats operation report = %+v", report.Operations)
	}
}

func TestRecallBaselinePowerAndHelpAreConfigFree(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "must-not-exist")
	t.Setenv("ENGRAM_DATA_DIR", dataDir)

	for _, args := range [][]string{
		{"engram", "recall-baseline", "--help"},
		{"engram", "recall-baseline", "power", "--baseline-rate", "0.5", "--minimum-detectable-difference", "0.1", "--alpha", "0.05", "--power", "0.8", "--comparisons", "3", "--treatments", "3", "--json"},
	} {
		withArgs(t, args...)
		stdout, stderr := captureOutput(t, func() {
			if !handleConfigFreeCommand(os.Args[1:]) {
				t.Fatalf("%v was not handled before configuration", args)
			}
		})
		if stdout == "" || stderr != "" {
			t.Fatalf("%v stdout=%q stderr=%q", args, stdout, stderr)
		}
		if shouldCheckForUpdates(os.Args[1:]) {
			t.Fatalf("%v should not perform update checks", args)
		}
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("config-free command created baseline state: %v", err)
	}
}

func TestRecallBaselineCLIRecordsReportsAndPlansWithoutHeldOutInput(t *testing.T) {
	stubExitWithPanic(t)
	cfg := store.Config{DataDir: t.TempDir()}
	originalInspector := inspectRecallBaselineCompatibility
	inspectRecallBaselineCompatibility = func() (protocolcontract.CompatibilityReport, error) {
		return protocolcontract.CompatibilityReport{
			SchemaVersion: "protocol-compatibility-v1",
			Status:        protocolcontract.CompatibilityReady,
			Axes: []protocolcontract.CompatibilityAxis{
				{Name: protocolcontract.AxisManagedPack, Version: "3.2.0", Provenance: "pack@sha"},
				{Name: protocolcontract.AxisEngramBinary, Version: "3.0.0", Provenance: "binary@sha"},
				{Name: protocolcontract.AxisCodexPlugin, Version: "0.1.5", Provenance: "plugin@sha"},
				{Name: protocolcontract.AxisProtocolContract, Version: "1", Provenance: "engram-core"},
			},
		}, nil
	}
	t.Cleanup(func() { inspectRecallBaselineCompatibility = originalInspector })

	run := func(args ...string) string {
		t.Helper()
		withArgs(t, append([]string{"engram", "recall-baseline"}, args...)...)
		stdout, stderr := captureOutput(t, func() { cmdRecallBaseline(cfg) })
		if stderr != "" {
			t.Fatalf("cmdRecallBaseline(%v) stderr = %q", args, stderr)
		}
		return stdout
	}

	identityFlags := []string{"--host", "codex", "--session-id", "session-secret", "--root-turn-id", "turn-secret"}
	run(append([]string{"record", "--kind", "checkpoint", "--surface", "lifecycle", "--operation", "terminal_checkpoint", "--outcome", "missing"}, identityFlags...)...)
	run(append([]string{"record", "--kind", "checkpoint", "--surface", "lifecycle", "--operation", "terminal_checkpoint", "--outcome", "completed"}, identityFlags...)...)
	run("record", "--kind", "operation", "--surface", "cli", "--operation", "search", "--outcome", "success", "--latency-ms", "12.5", "--delivered-bytes", "17")

	reportOutput := run("report", "--json")
	var report recallbaseline.Report
	if err := json.Unmarshal([]byte(reportOutput), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, reportOutput)
	}
	if report.Lifecycle.Checkpoints.EligibleTurns != 1 || report.Lifecycle.Checkpoints.Completed != 1 {
		t.Fatalf("checkpoint report = %+v", report.Lifecycle.Checkpoints)
	}
	if len(report.Operations) != 1 || report.Operations[0].P50LatencyMillis != 12.5 || report.Operations[0].TotalUTF8Bytes != 17 {
		t.Fatalf("operation report = %+v", report.Operations)
	}
	if len(report.Compatibility.Axes) != 4 || report.Compatibility.Axes[3].Name != protocolcontract.AxisProtocolContract {
		t.Fatalf("compatibility tuple = %+v", report.Compatibility)
	}

	powerOutput := run("power", "--baseline-rate", "0.5", "--minimum-detectable-difference", "0.1", "--alpha", "0.05", "--power", "0.8", "--comparisons", "3", "--treatments", "3", "--json")
	var power recallbaseline.PowerAnalysis
	if err := json.Unmarshal([]byte(powerOutput), &power); err != nil {
		t.Fatalf("decode power analysis: %v\n%s", err, powerOutput)
	}
	if power.RequiredPerTreatment != 517 || power.HeldOutAccessed {
		t.Fatalf("power analysis = %+v", power)
	}

	withArgs(t, "engram", "recall-baseline", "power", "--held-out", "/private/held-out.json", "--json")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdRecallBaseline(cfg) })
	if recovered == nil || !strings.Contains(stderr, "unknown recall-baseline flag --held-out") {
		t.Fatalf("held-out input rejection recovered=%v stderr=%q", recovered, stderr)
	}
}
