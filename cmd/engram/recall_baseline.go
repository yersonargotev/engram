package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	engrammcp "github.com/yersonargotev/engram/internal/mcp"
	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/recallbaseline"
	"github.com/yersonargotev/engram/internal/store"
)

var inspectRecallBaselineCompatibility = func() (protocolcontract.CompatibilityReport, error) {
	status, err := setupInspectCodexStatus(version, commit, currentCWD())
	if err != nil {
		return protocolcontract.CompatibilityReport{}, err
	}
	return status.Compatibility, nil
}

type recallBaselineRecordFlags struct {
	Kind, Surface, Operation, Outcome         string
	Host, SessionID, RootTurnID               string
	OccurredAt, LatencyMillis, DeliveredBytes string
	JSONMode                                  bool
}

func cmdRecallBaseline(cfg store.Config) {
	if len(os.Args) < 3 {
		failCLI(hasArg("--json"), "invalid_arguments", "recall-baseline requires record, report, power, or purge", nil)
		return
	}
	subcommand := strings.ToLower(strings.TrimSpace(os.Args[2]))
	switch subcommand {
	case "help", "--help", "-h":
		printRecallBaselineUsage()
	case "record":
		cmdRecallBaselineRecord(cfg, os.Args[3:])
	case "report":
		cmdRecallBaselineReport(cfg, os.Args[3:])
	case "power":
		cmdRecallBaselinePower(os.Args[3:])
	case "purge":
		cmdRecallBaselinePurge(cfg, os.Args[3:])
	default:
		failCLI(hasArg("--json"), "invalid_arguments", fmt.Sprintf("unknown recall-baseline command %q", os.Args[2]), nil)
	}
}

func cmdRecallBaselineRecord(cfg store.Config, args []string) {
	flags := recallBaselineRecordFlags{}
	set := newRecallBaselineFlagSet("record")
	set.StringVar(&flags.Kind, "kind", "", "bounded event kind")
	set.StringVar(&flags.Surface, "surface", "", "operation or lifecycle surface")
	set.StringVar(&flags.Operation, "operation", "", "bounded operation name")
	set.StringVar(&flags.Outcome, "outcome", "", "bounded event outcome")
	set.StringVar(&flags.Host, "host", "", "opaque host identifier")
	set.StringVar(&flags.SessionID, "session-id", "", "opaque session identifier")
	set.StringVar(&flags.RootTurnID, "root-turn-id", "", "opaque root-turn identifier")
	set.StringVar(&flags.OccurredAt, "occurred-at", "", "event time in RFC3339")
	set.StringVar(&flags.LatencyMillis, "latency-ms", "", "monotonic elapsed milliseconds")
	set.StringVar(&flags.DeliveredBytes, "delivered-bytes", "", "UTF-8 bytes delivered")
	set.BoolVar(&flags.JSONMode, "json", false, "emit JSON")
	if !parseRecallBaselineFlags(set, args, hasArg("--json")) {
		return
	}

	event := recallbaseline.Event{
		Kind: recallbaseline.EventKind(flags.Kind), Surface: recallbaseline.Surface(flags.Surface),
		Operation: flags.Operation, Outcome: recallbaseline.Outcome(flags.Outcome),
		Link: recallbaseline.Linkage{Host: flags.Host, SessionID: flags.SessionID, RootTurnID: flags.RootTurnID},
	}
	if flags.OccurredAt != "" {
		occurredAt, err := time.Parse(time.RFC3339Nano, flags.OccurredAt)
		if err != nil {
			failCLI(flags.JSONMode, "invalid_arguments", "--occurred-at must be RFC3339", nil)
			return
		}
		event.OccurredAt = occurredAt
	}
	if flags.LatencyMillis != "" {
		milliseconds, err := strconv.ParseFloat(flags.LatencyMillis, 64)
		if err != nil || milliseconds < 0 {
			failCLI(flags.JSONMode, "invalid_arguments", "--latency-ms must be a non-negative number", nil)
			return
		}
		duration := time.Duration(milliseconds * float64(time.Millisecond))
		event.Latency = recallbaseline.KnownLatency(duration)
	}
	if flags.DeliveredBytes != "" {
		bytes, err := strconv.ParseInt(flags.DeliveredBytes, 10, 64)
		if err != nil || bytes < 0 {
			failCLI(flags.JSONMode, "invalid_arguments", "--delivered-bytes must be a non-negative integer", nil)
			return
		}
		event.DeliveredUTF8Bytes = recallbaseline.KnownBytes(bytes)
	}

	ledger, err := openRecallBaselineLedger(cfg)
	if err != nil {
		failCLI(flags.JSONMode, "baseline_store_error", err.Error(), nil)
		return
	}
	defer ledger.Close()
	if err := ledger.Record(event); err != nil {
		failCLI(flags.JSONMode, "invalid_baseline_event", err.Error(), nil)
		return
	}
	if flags.JSONMode {
		_ = writeCLIJSON(map[string]any{"schema_version": recallbaseline.EventSchemaVersion, "recorded": true})
		return
	}
	fmt.Println("Recall baseline event recorded locally.")
}

func cmdRecallBaselineReport(cfg store.Config, args []string) {
	set := newRecallBaselineFlagSet("report")
	jsonMode := false
	set.BoolVar(&jsonMode, "json", false, "emit JSON")
	if !parseRecallBaselineFlags(set, args, hasArg("--json")) {
		return
	}
	compatibility, err := inspectRecallBaselineCompatibility()
	if err != nil {
		failCLI(jsonMode, "compatibility_unavailable", err.Error(), nil)
		return
	}
	ledger, err := openRecallBaselineLedger(cfg)
	if err != nil {
		failCLI(jsonMode, "baseline_store_error", err.Error(), nil)
		return
	}
	defer ledger.Close()
	report, err := ledger.Report(compatibility)
	if err != nil {
		failCLI(jsonMode, "baseline_report_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(report)
		return
	}
	printRecallBaselineReport(report)
}

func cmdRecallBaselinePower(args []string) {
	set := newRecallBaselineFlagSet("power")
	var baseline, difference, alpha, desiredPower string
	var comparisons, treatments string
	jsonMode := false
	set.StringVar(&baseline, "baseline-rate", "", "baseline outcome rate")
	set.StringVar(&difference, "minimum-detectable-difference", "", "absolute effect")
	set.StringVar(&alpha, "alpha", "", "familywise alpha")
	set.StringVar(&desiredPower, "power", "", "desired statistical power")
	set.StringVar(&comparisons, "comparisons", "", "planned comparisons")
	set.StringVar(&treatments, "treatments", "", "planned treatments")
	set.BoolVar(&jsonMode, "json", false, "emit JSON")
	if !parseRecallBaselineFlags(set, args, hasArg("--json")) {
		return
	}
	parseFloat := func(name, raw string) (float64, bool) {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || raw == "" {
			failCLI(jsonMode, "invalid_arguments", name+" requires a number", nil)
			return 0, false
		}
		return value, true
	}
	parseInt := func(name, raw string) (int, bool) {
		value, err := strconv.Atoi(raw)
		if err != nil || raw == "" {
			failCLI(jsonMode, "invalid_arguments", name+" requires an integer", nil)
			return 0, false
		}
		return value, true
	}
	baselineValue, ok := parseFloat("--baseline-rate", baseline)
	if !ok {
		return
	}
	differenceValue, ok := parseFloat("--minimum-detectable-difference", difference)
	if !ok {
		return
	}
	alphaValue, ok := parseFloat("--alpha", alpha)
	if !ok {
		return
	}
	powerValue, ok := parseFloat("--power", desiredPower)
	if !ok {
		return
	}
	comparisonCount, ok := parseInt("--comparisons", comparisons)
	if !ok {
		return
	}
	treatmentCount, ok := parseInt("--treatments", treatments)
	if !ok {
		return
	}

	analysis, err := recallbaseline.AnalyzePower(recallbaseline.PowerAssumptions{
		BaselineRate: baselineValue, MinimumDetectableDifference: differenceValue,
		Alpha: alphaValue, Power: powerValue, Comparisons: comparisonCount, Treatments: treatmentCount,
	})
	if err != nil {
		failCLI(jsonMode, "invalid_power_assumptions", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(analysis)
		return
	}
	fmt.Printf("Controlled study: %d per treatment; %d total (%s).\n", analysis.RequiredPerTreatment, analysis.RequiredTotal, analysis.Method)
}

func cmdRecallBaselinePurge(cfg store.Config, args []string) {
	set := newRecallBaselineFlagSet("purge")
	jsonMode := false
	set.BoolVar(&jsonMode, "json", false, "emit JSON")
	if !parseRecallBaselineFlags(set, args, hasArg("--json")) {
		return
	}
	ledger, err := openRecallBaselineLedger(cfg)
	if err != nil {
		failCLI(jsonMode, "baseline_store_error", err.Error(), nil)
		return
	}
	defer ledger.Close()
	purged, err := ledger.PurgeExpired()
	if err != nil {
		failCLI(jsonMode, "baseline_purge_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(map[string]any{
			"schema_version":                    recallbaseline.EventSchemaVersion,
			"expired_events_purged":             purged.ExpiredEventsPurged,
			"expired_collection_records_purged": purged.ExpiredCollectionRecordsPurged,
		})
		return
	}
	fmt.Printf("Purged %d expired recall baseline events and %d collection-loss records.\n",
		purged.ExpiredEventsPurged, purged.ExpiredCollectionRecordsPurged)
}

func openRecallBaselineLedger(cfg store.Config) (*recallbaseline.Ledger, error) {
	retention := time.Duration(0)
	if raw := strings.TrimSpace(os.Getenv("ENGRAM_RECALL_BASELINE_RETENTION_DAYS")); raw != "" {
		days, err := strconv.Atoi(raw)
		if err != nil || days < 1 || days > 30 {
			return nil, fmt.Errorf("ENGRAM_RECALL_BASELINE_RETENTION_DAYS must be between 1 and 30")
		}
		retention = time.Duration(days) * 24 * time.Hour
	}
	return recallbaseline.Open(recallbaseline.Config{DataDir: cfg.DataDir, Retention: retention})
}

func recordRecallBaselineEvents(cfg store.Config, events ...recallbaseline.Event) {
	if !recallBaselineCollectionEnabled() {
		return
	}
	ledger, err := openRecallBaselineLedger(cfg)
	if err != nil {
		return
	}
	defer ledger.Close()
	for _, event := range events {
		_ = ledger.Record(event)
	}
}

func observeRecallBaselineCLI(cfg store.Config, operation string, started time.Time, outcome recallbaseline.Outcome, deliveredBytes *int64) {
	recordRecallBaselineEvents(cfg, recallbaseline.Event{
		Kind: recallbaseline.EventOperation, Surface: recallbaseline.SurfaceCLI,
		Operation: operation, Outcome: outcome,
		Latency: recallbaseline.KnownLatency(time.Since(started)), DeliveredUTF8Bytes: deliveredBytes,
	})
}

type recallBaselineCLIExecution struct {
	cfg       store.Config
	operation string
	started   time.Time
	once      sync.Once
}

var (
	recallBaselineCLIExecutionMu sync.Mutex
	activeRecallBaselineCLI      *recallBaselineCLIExecution
)

func (execution *recallBaselineCLIExecution) finish(outcome recallbaseline.Outcome) {
	if execution == nil {
		return
	}
	execution.once.Do(func() {
		observeRecallBaselineCLI(execution.cfg, execution.operation, execution.started, outcome, nil)
	})
}

func finishActiveRecallBaselineCLI(outcome recallbaseline.Outcome) {
	recallBaselineCLIExecutionMu.Lock()
	execution := activeRecallBaselineCLI
	recallBaselineCLIExecutionMu.Unlock()
	execution.finish(outcome)
}

func exitWithRecallBaseline(code int) {
	outcome := recallbaseline.OutcomeError
	if code == 0 {
		outcome = recallbaseline.OutcomeSuccess
	}
	finishActiveRecallBaselineCLI(outcome)
	processExit(code)
}

func runRecallBaselineCLI(cfg store.Config, operation string, run func()) {
	if !recallBaselineCollectionEnabled() {
		run()
		return
	}
	execution := &recallBaselineCLIExecution{cfg: cfg, operation: operation, started: time.Now()}
	recallBaselineCLIExecutionMu.Lock()
	previous := activeRecallBaselineCLI
	activeRecallBaselineCLI = execution
	recallBaselineCLIExecutionMu.Unlock()
	defer func() {
		recallBaselineCLIExecutionMu.Lock()
		if activeRecallBaselineCLI == execution {
			activeRecallBaselineCLI = previous
		}
		recallBaselineCLIExecutionMu.Unlock()
		execution.finish(recallbaseline.OutcomeError)
	}()
	run()
	execution.finish(recallbaseline.OutcomeSuccess)
}

func cliJSONBytes(value any) *int64 {
	encoded, err := jsonMarshalIndent(value, "", "  ")
	if err != nil {
		return nil
	}
	bytes := int64(len(encoded) + 1)
	return &bytes
}

func recallBaselineCollectionEnabled() bool {
	return strings.TrimSpace(os.Getenv("ENGRAM_RECALL_BASELINE")) == "1"
}

func newRecallBaselineMCPObservers(cfg store.Config) (
	func(engrammcp.OperationObservation),
	func(engrammcp.CheckpointObservation),
	func(),
) {
	if !recallBaselineCollectionEnabled() {
		return nil, nil, func() {}
	}
	ledger, err := openRecallBaselineLedger(cfg)
	if err != nil {
		log.Printf("[engram] recall baseline telemetry unavailable: %v", err)
		return nil, nil, func() {}
	}
	recorder := recallbaseline.NewAsyncRecorder(ledger, 1024)
	observe := func(observation engrammcp.OperationObservation) {
		outcome := recallbaseline.OutcomeSuccess
		if observation.Outcome == engrammcp.OperationError {
			outcome = recallbaseline.OutcomeError
		}
		recorder.Record(recallbaseline.Event{
			Kind: recallbaseline.EventOperation, Surface: recallbaseline.SurfaceMCP,
			Operation: observation.Operation, Outcome: outcome,
			Latency: recallbaseline.KnownLatency(observation.Latency), DeliveredUTF8Bytes: observation.DeliveredUTF8Bytes,
		})
	}
	observeCheckpoint := func(observation engrammcp.CheckpointObservation) {
		outcome := recallbaseline.OutcomeUnknown
		switch observation.Outcome {
		case engrammcp.CheckpointCompleted:
			outcome = recallbaseline.OutcomeCompleted
		case engrammcp.CheckpointConflict:
			outcome = recallbaseline.OutcomeConflict
		}
		recorder.Record(recallbaseline.Event{
			Kind: recallbaseline.EventCheckpoint, Surface: recallbaseline.SurfaceLifecycle,
			Operation: "terminal_checkpoint", Outcome: outcome,
			Link: recallbaseline.Linkage{
				Host: observation.Host, SessionID: observation.SessionID, RootTurnID: observation.RootTurnID,
			},
		})
	}
	closeObserver := func() {
		if err := recorder.Close(); err != nil {
			log.Printf("[engram] recall baseline telemetry close failed: %v", err)
		}
	}
	return observe, observeCheckpoint, closeObserver
}

func newRecallBaselineFlagSet(subcommand string) *flag.FlagSet {
	set := flag.NewFlagSet("recall-baseline "+subcommand, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	return set
}

func parseRecallBaselineFlags(set *flag.FlagSet, args []string, jsonMode bool) bool {
	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printRecallBaselineUsage()
			return false
		}
		message := err.Error()
		if strings.Contains(message, "flag provided but not defined") {
			name := strings.TrimSpace(strings.TrimPrefix(message, "flag provided but not defined:"))
			name = strings.TrimLeft(name, "-")
			message = "unknown recall-baseline flag --" + name
		}
		failCLI(jsonMode, "invalid_arguments", message, nil)
		return false
	}
	if set.NArg() > 0 {
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unexpected recall-baseline argument %q", set.Arg(0)), nil)
		return false
	}
	return true
}

func printRecallBaselineReport(report recallbaseline.Report) {
	fmt.Printf("Recall baseline %s (%s)\n", report.SchemaVersion, report.EventSchemaVersion)
	fmt.Printf("Compatibility: %s (%s)\n", report.Compatibility.Status, report.Compatibility.ReasonCode)
	for _, axis := range report.Compatibility.Axes {
		fmt.Printf("  %s: version %s; provenance %s; %s (%s)\n",
			axis.Name, axis.Version, axis.Provenance, axis.Status, axis.ReasonCode)
	}
	fmt.Printf("Collection loss: %d dropped events; %d write failures\n",
		report.Collection.DroppedEvents, report.Collection.WriteFailures)
	fmt.Printf("Checkpoint denominator: %d; completed %d; missing %d; conflicting %d; unknown %d\n",
		report.Lifecycle.Checkpoints.EligibleTurns, report.Lifecycle.Checkpoints.Completed,
		report.Lifecycle.Checkpoints.Missing, report.Lifecycle.Checkpoints.Conflicting, report.Lifecycle.Checkpoints.Unknown)
	fmt.Printf("Stop events: %d; completed %d; continuations %d; recovery exhausted %d; integration failures %d; unknown %d\n",
		report.Lifecycle.Stop.Events, report.Lifecycle.Stop.Completed, report.Lifecycle.Stop.Continuations, report.Lifecycle.Stop.RecoveryExhausted,
		report.Lifecycle.Stop.IntegrationFailures, report.Lifecycle.Stop.Unknown)
	fmt.Printf("Capture events: %d; enabled %d; disabled %d; unknown %d\n",
		report.Lifecycle.Capture.Events, report.Lifecycle.Capture.Enabled,
		report.Lifecycle.Capture.Disabled, report.Lifecycle.Capture.Unknown)
	fmt.Printf("SubagentStop events: %d; observed %d; skipped %d; unknown %d\n",
		report.Lifecycle.SubagentStop.Events, report.Lifecycle.SubagentStop.Observed,
		report.Lifecycle.SubagentStop.Skipped, report.Lifecycle.SubagentStop.Unknown)
	for _, operation := range report.Operations {
		fmt.Printf("%s/%s: %d operations; latency p50 %.3f ms p95 %.3f ms (%d unknown); %d UTF-8 bytes (%d unknown)\n",
			operation.Surface, operation.Operation, operation.Events, operation.P50LatencyMillis,
			operation.P95LatencyMillis, operation.UnknownLatency, operation.TotalUTF8Bytes, operation.UnknownBytes)
	}
}

func printRecallBaselineUsage() {
	fmt.Println(`usage: engram recall-baseline record|report|power|purge [options]

  record  --kind KIND --surface SURFACE --operation NAME --outcome OUTCOME
          [--host ID --session-id ID --root-turn-id ID]
          [--occurred-at RFC3339] [--latency-ms N] [--delivered-bytes N] [--json]
  report  [--json]
  power   --baseline-rate N --minimum-detectable-difference N --alpha N --power N
          --comparisons N --treatments N [--json]
  purge   [--json]`)
}
