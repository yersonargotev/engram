// Package recallbaseline owns privacy-safe, local-only operational baseline data.
package recallbaseline

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/yersonargotev/engram/internal/protocolcontract"
	_ "modernc.org/sqlite"
)

const (
	EventSchemaVersion  = "recall-baseline-events-v1"
	ReportSchemaVersion = "recall-baseline-report-v1"
	databaseFilename    = "recall-baseline-v1.db"
	keyFilename         = "recall-baseline-v1.key"
	defaultRetention    = 7 * 24 * time.Hour
	maximumRetention    = 30 * 24 * time.Hour
)

var errInstallKeyMalformed = errors.New("recall baseline install key is malformed")

type EventKind string

const (
	EventCheckpoint   EventKind = "checkpoint"
	EventStop         EventKind = "stop"
	EventCapture      EventKind = "capture"
	EventSubagentStop EventKind = "subagent_stop"
	EventOperation    EventKind = "operation"
)

type Surface string

const (
	SurfaceLifecycle Surface = "lifecycle"
	SurfaceCLI       Surface = "cli"
	SurfaceMCP       Surface = "mcp"
)

type Outcome string

const (
	OutcomeCompleted            Outcome = "completed"
	OutcomeMissing              Outcome = "missing"
	OutcomeConflict             Outcome = "conflict"
	OutcomeUnknown              Outcome = "unknown"
	OutcomeContinuationRequired Outcome = "continuation_required"
	OutcomeRecoveryExhausted    Outcome = "recovery_exhausted"
	OutcomeIntegrationFailure   Outcome = "integration_failure"
	OutcomeEnabled              Outcome = "enabled"
	OutcomeDisabled             Outcome = "disabled"
	OutcomeObserved             Outcome = "observed"
	OutcomeSkipped              Outcome = "skipped"
	OutcomeSuccess              Outcome = "success"
	OutcomeError                Outcome = "error"
)

// Linkage contains opaque identifiers accepted only long enough to derive one
// per-install keyed digest. Its fields are never persisted or reported.
type Linkage struct {
	Host       string `json:"-"`
	SessionID  string `json:"-"`
	RootTurnID string `json:"-"`
}

// Event is one bounded operational observation. The public shape deliberately
// has no fields for prompts, queries, Memory content, assistant text, paths,
// repository diffs, or credentials.
type Event struct {
	Kind               EventKind
	Surface            Surface
	Operation          string
	Outcome            Outcome
	Link               Linkage
	OccurredAt         time.Time
	Latency            *time.Duration
	DeliveredUTF8Bytes *int64
}

func KnownLatency(value time.Duration) *time.Duration { return &value }
func KnownBytes(value int64) *int64                   { return &value }

type Config struct {
	DataDir   string
	Now       func() time.Time
	Random    io.Reader
	Retention time.Duration
}

type Ledger struct {
	db        *sql.DB
	now       func() time.Time
	salt      []byte
	retention time.Duration
}

type Report struct {
	SchemaVersion      string                               `json:"schema_version"`
	EventSchemaVersion string                               `json:"event_schema_version"`
	GeneratedAt        string                               `json:"generated_at"`
	Compatibility      protocolcontract.CompatibilityReport `json:"compatibility"`
	Retention          RetentionReport                      `json:"retention"`
	Collection         CollectionReport                     `json:"collection"`
	Lifecycle          LifecycleReport                      `json:"lifecycle"`
	Operations         []OperationReport                    `json:"operations"`
}

type RetentionReport struct {
	WindowHours                    int64 `json:"window_hours"`
	ExpiredEventsPurged            int64 `json:"expired_events_purged"`
	ExpiredCollectionRecordsPurged int64 `json:"expired_collection_records_purged"`
}

type CollectionReport struct {
	DroppedEvents int64 `json:"dropped_events"`
	WriteFailures int64 `json:"write_failures"`
}

type LifecycleReport struct {
	Checkpoints  CheckpointCoverage `json:"checkpoints"`
	Stop         StopReport         `json:"stop"`
	Capture      CaptureReport      `json:"capture"`
	SubagentStop SubagentStopReport `json:"subagent_stop"`
}

type CheckpointCoverage struct {
	EligibleTurns int64 `json:"eligible_turns"`
	Completed     int64 `json:"completed"`
	Missing       int64 `json:"missing"`
	Conflicting   int64 `json:"conflicting"`
	Unknown       int64 `json:"unknown"`
}

type StopReport struct {
	Events              int64 `json:"events"`
	Completed           int64 `json:"completed"`
	Continuations       int64 `json:"continuations"`
	RecoveryExhausted   int64 `json:"recovery_exhausted"`
	IntegrationFailures int64 `json:"integration_failures"`
	Unknown             int64 `json:"unknown"`
}

type CaptureReport struct {
	Events   int64 `json:"events"`
	Enabled  int64 `json:"enabled"`
	Disabled int64 `json:"disabled"`
	Unknown  int64 `json:"unknown"`
}

type SubagentStopReport struct {
	Events   int64 `json:"events"`
	Observed int64 `json:"observed"`
	Skipped  int64 `json:"skipped"`
	Unknown  int64 `json:"unknown"`
}

type OperationReport struct {
	Surface          Surface `json:"surface"`
	Operation        string  `json:"operation"`
	Events           int64   `json:"events"`
	Succeeded        int64   `json:"succeeded"`
	Failed           int64   `json:"failed"`
	UnknownOutcome   int64   `json:"unknown_outcome"`
	LatencySamples   int64   `json:"latency_samples"`
	UnknownLatency   int64   `json:"unknown_latency"`
	P50LatencyMillis float64 `json:"p50_latency_ms"`
	P95LatencyMillis float64 `json:"p95_latency_ms"`
	ByteSamples      int64   `json:"byte_samples"`
	UnknownBytes     int64   `json:"unknown_bytes"`
	TotalUTF8Bytes   int64   `json:"total_utf8_bytes"`
}

func DatabasePath(dataDir string) string {
	return filepath.Join(dataDir, databaseFilename)
}

func KeyPath(dataDir string) string {
	return filepath.Join(dataDir, keyFilename)
}

func Open(cfg Config) (*Ledger, error) {
	if !filepath.IsAbs(cfg.DataDir) {
		return nil, fmt.Errorf("recall baseline data directory must be absolute")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Random == nil {
		cfg.Random = rand.Reader
	}
	if cfg.Retention == 0 {
		cfg.Retention = defaultRetention
	}
	if cfg.Retention < time.Hour || cfg.Retention > maximumRetention {
		return nil, fmt.Errorf("recall baseline retention must be between one hour and thirty days")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create recall baseline data directory: %w", err)
	}
	path := DatabasePath(cfg.DataDir)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open recall baseline database: %w", err)
	}
	cleanup := func(openErr error) (*Ledger, error) {
		_ = db.Close()
		return nil, openErr
	}
	var schemaVersion int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&schemaVersion); err != nil {
		return cleanup(fmt.Errorf("read recall baseline schema version: %w", err))
	}
	if schemaVersion != 0 && schemaVersion != 1 {
		return cleanup(fmt.Errorf("unsupported recall baseline schema version %d", schemaVersion))
	}
	for _, statement := range []string{
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA journal_mode = DELETE`,
		`CREATE TABLE IF NOT EXISTS baseline_events (
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
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS baseline_collection (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			schema_version TEXT NOT NULL,
			occurred_at TEXT NOT NULL,
			expires_at TEXT NOT NULL,
			dropped_events INTEGER NOT NULL CHECK (dropped_events >= 0),
			write_failures INTEGER NOT NULL CHECK (write_failures >= 0)
		) STRICT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS baseline_checkpoint_link
			ON baseline_events(kind, link_key) WHERE kind = 'checkpoint'`,
	} {
		if _, err := db.Exec(statement); err != nil {
			return cleanup(fmt.Errorf("initialize recall baseline schema: %w", err))
		}
	}
	if schemaVersion == 0 {
		if _, err := db.Exec(`PRAGMA user_version = 1`); err != nil {
			return cleanup(fmt.Errorf("set recall baseline schema version: %w", err))
		}
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return cleanup(fmt.Errorf("restrict recall baseline database: %w", err))
	}

	salt, err := loadOrCreateSalt(cfg.DataDir, cfg.Random)
	if err != nil {
		return cleanup(err)
	}
	return &Ledger{db: db, now: cfg.Now, salt: salt, retention: cfg.Retention}, nil
}

func loadOrCreateSalt(dataDir string, source io.Reader) ([]byte, error) {
	path := KeyPath(dataDir)
	read := func() ([]byte, error) {
		salt, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(salt) != sha256.Size {
			return nil, errInstallKeyMalformed
		}
		if err := os.Chmod(path, 0o600); err != nil {
			return nil, fmt.Errorf("restrict recall baseline install key: %w", err)
		}
		return salt, nil
	}
	readWithRetry := func() ([]byte, error) {
		for attempts := 0; ; attempts++ {
			salt, err := read()
			if err == nil || errors.Is(err, os.ErrNotExist) {
				return salt, err
			}
			if !errors.Is(err, errInstallKeyMalformed) || attempts == 9 {
				return nil, err
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if salt, err := readWithRetry(); err == nil {
		return salt, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read recall baseline install key: %w", err)
	}

	salt := make([]byte, sha256.Size)
	if _, err := io.ReadFull(source, salt); err != nil {
		return nil, fmt.Errorf("generate recall baseline install key: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		if existing, readErr := readWithRetry(); readErr == nil {
			return existing, nil
		}
		return nil, fmt.Errorf("read concurrently created recall baseline install key")
	}
	if err != nil {
		return nil, fmt.Errorf("create recall baseline install key: %w", err)
	}
	if _, err := file.Write(salt); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("persist recall baseline install key: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("sync recall baseline install key: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("close recall baseline install key: %w", err)
	}
	return salt, nil
}

func (ledger *Ledger) Close() error {
	if ledger == nil || ledger.db == nil {
		return nil
	}
	return ledger.db.Close()
}

func (ledger *Ledger) Record(event Event) error {
	if ledger == nil || ledger.db == nil {
		return fmt.Errorf("recall baseline ledger is closed")
	}
	if err := validateEvent(event); err != nil {
		return err
	}
	linkKey, err := ledger.linkKey(event.Link, event.Kind == EventCheckpoint)
	if err != nil {
		return err
	}
	now := ledger.now().UTC()
	if !event.OccurredAt.IsZero() {
		if event.OccurredAt.After(now) {
			return fmt.Errorf("recall baseline occurrence time must not be in the future")
		}
		now = event.OccurredAt.UTC()
	}
	var latencyMicros any
	if event.Latency != nil {
		latencyMicros = event.Latency.Microseconds()
	}
	var deliveredBytes any
	if event.DeliveredUTF8Bytes != nil {
		deliveredBytes = *event.DeliveredUTF8Bytes
	}
	_, err = ledger.db.Exec(`INSERT INTO baseline_events(
		schema_version, occurred_at, expires_at, kind, surface, operation, outcome, link_key,
		latency_micros, delivered_utf8_bytes
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(kind, link_key) WHERE kind = 'checkpoint' DO UPDATE SET
		occurred_at = excluded.occurred_at,
		expires_at = excluded.expires_at,
		outcome = CASE
			WHEN baseline_events.outcome = 'conflict' OR excluded.outcome = 'conflict' THEN 'conflict'
			WHEN excluded.outcome = 'completed' THEN 'completed'
			WHEN baseline_events.outcome = 'completed' THEN 'completed'
			WHEN excluded.outcome = 'missing' THEN 'missing'
			ELSE baseline_events.outcome
		END`,
		EventSchemaVersion, now.Format(time.RFC3339Nano), now.Add(ledger.retention).Format(time.RFC3339Nano),
		event.Kind, event.Surface, event.Operation, event.Outcome, linkKey, latencyMicros, deliveredBytes,
	)
	if err != nil {
		return fmt.Errorf("record recall baseline event: %w", err)
	}
	return nil
}

// RecordCollectionLoss preserves content-free completeness counters for the
// non-blocking observer. These counters contain no event identity or payload.
func (ledger *Ledger) RecordCollectionLoss(dropped, writeFailures int64) error {
	if ledger == nil || ledger.db == nil {
		return fmt.Errorf("recall baseline ledger is closed")
	}
	if dropped < 0 || writeFailures < 0 {
		return fmt.Errorf("recall baseline collection loss must not be negative")
	}
	if dropped == 0 && writeFailures == 0 {
		return nil
	}
	now := ledger.now().UTC()
	if _, err := ledger.db.Exec(`INSERT INTO baseline_collection(
		schema_version, occurred_at, expires_at, dropped_events, write_failures
	) VALUES (?, ?, ?, ?, ?)`, EventSchemaVersion, now.Format(time.RFC3339Nano),
		now.Add(ledger.retention).Format(time.RFC3339Nano), dropped, writeFailures); err != nil {
		return fmt.Errorf("record recall baseline collection loss: %w", err)
	}
	return nil
}

func validateEvent(event Event) error {
	if strings.TrimSpace(event.Operation) == "" || len(event.Operation) > 64 {
		return fmt.Errorf("recall baseline operation must contain 1 to 64 characters")
	}
	if event.Latency != nil && *event.Latency < 0 {
		return fmt.Errorf("recall baseline latency must not be negative")
	}
	if event.DeliveredUTF8Bytes != nil && *event.DeliveredUTF8Bytes < 0 {
		return fmt.Errorf("recall baseline delivered bytes must not be negative")
	}
	if event.Kind != EventCheckpoint && (event.Link.Host != "" || event.Link.SessionID != "" || event.Link.RootTurnID != "") {
		return fmt.Errorf("recall baseline linkage is supported only for checkpoint coverage")
	}
	allowed := map[EventKind]struct {
		surface  Surface
		outcomes map[Outcome]bool
	}{
		EventCheckpoint:   {SurfaceLifecycle, map[Outcome]bool{OutcomeCompleted: true, OutcomeMissing: true, OutcomeConflict: true, OutcomeUnknown: true}},
		EventStop:         {SurfaceLifecycle, map[Outcome]bool{OutcomeCompleted: true, OutcomeContinuationRequired: true, OutcomeRecoveryExhausted: true, OutcomeIntegrationFailure: true, OutcomeUnknown: true}},
		EventCapture:      {SurfaceLifecycle, map[Outcome]bool{OutcomeEnabled: true, OutcomeDisabled: true, OutcomeUnknown: true}},
		EventSubagentStop: {SurfaceLifecycle, map[Outcome]bool{OutcomeObserved: true, OutcomeSkipped: true, OutcomeUnknown: true}},
		EventOperation:    {event.Surface, map[Outcome]bool{OutcomeSuccess: true, OutcomeError: true, OutcomeUnknown: true}},
	}
	contract, ok := allowed[event.Kind]
	if !ok || !contract.outcomes[event.Outcome] {
		return fmt.Errorf("unsupported recall baseline event")
	}
	if event.Kind == EventOperation {
		if event.Surface != SurfaceCLI && event.Surface != SurfaceMCP && event.Surface != SurfaceLifecycle {
			return fmt.Errorf("recall baseline operations require cli, mcp, or lifecycle surface")
		}
	} else if event.Surface != contract.surface {
		return fmt.Errorf("recall baseline lifecycle event requires lifecycle surface")
	}
	if !allowedOperation(event.Kind, event.Surface, event.Operation) {
		return fmt.Errorf("unsupported recall baseline operation %q", event.Operation)
	}
	return nil
}

func allowedOperation(kind EventKind, surface Surface, operation string) bool {
	switch kind {
	case EventCheckpoint:
		return operation == "terminal_checkpoint"
	case EventStop:
		return operation == "stop"
	case EventCapture:
		return operation == "prompt" || operation == "subagent"
	case EventSubagentStop:
		return operation == "subagent_stop"
	case EventOperation:
		switch surface {
		case SurfaceLifecycle:
			return operation == "session_start"
		case SurfaceCLI:
			return boundedCLIOperations[operation]
		case SurfaceMCP:
			return boundedMCPTools[operation]
		}
	}
	return false
}

var boundedCLIOperations = map[string]bool{
	"serve": true, "mcp": true, "tui": true, "test": true,
	"search": true, "save": true, "get": true, "update": true, "review": true,
	"pin": true, "unpin": true, "current_project": true, "suggest_topic_key": true,
	"delete": true, "timeline": true, "conflicts": true, "doctor": true, "context": true,
	"checkpoint_record": true, "checkpoint_status": true, "checkpoint_verify_stop": true,
	"stats": true, "export": true, "import": true, "sync": true, "cloud": true,
	"obsidian_export": true, "projects": true, "setup": true, "protocol_mode": true,
	"activation_study": true, "recall_baseline": true, "version": true,
}

var boundedMCPTools = map[string]bool{
	"mem_capture_passive": true, "mem_checkpoint": true, "mem_checkpoint_status": true,
	"mem_compare": true, "mem_context": true, "mem_current_project": true,
	"mem_delete": true, "mem_doctor": true, "mem_get_observation": true,
	"mem_judge": true, "mem_merge_projects": true, "mem_pin": true,
	"mem_review": true, "mem_save": true, "mem_save_prompt": true,
	"mem_search": true, "mem_session_end": true, "mem_session_start": true,
	"mem_session_summary": true, "mem_stats": true, "mem_suggest_topic_key": true,
	"mem_timeline": true, "mem_unpin": true, "mem_update": true,
}

func (ledger *Ledger) linkKey(link Linkage, required bool) (string, error) {
	if !required {
		return "", nil
	}
	values := []string{link.Host, link.SessionID, link.RootTurnID}
	if strings.TrimSpace(link.Host) == "" || strings.TrimSpace(link.SessionID) == "" || strings.TrimSpace(link.RootTurnID) == "" {
		return "", fmt.Errorf("recall baseline checkpoint linkage requires host, session, and root turn")
	}
	mac := hmac.New(sha256.New, ledger.salt)
	for _, value := range values {
		_, _ = fmt.Fprintf(mac, "%d:", len(value))
		_, _ = mac.Write([]byte(value))
	}
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func (ledger *Ledger) Report(compatibility protocolcontract.CompatibilityReport) (Report, error) {
	purged, err := ledger.PurgeExpired()
	if err != nil {
		return Report{}, err
	}
	lifecycle, err := ledger.lifecycleReport()
	if err != nil {
		return Report{}, err
	}
	operations, err := ledger.operationReports()
	if err != nil {
		return Report{}, err
	}
	collection, err := ledger.collectionReport()
	if err != nil {
		return Report{}, err
	}
	return Report{
		SchemaVersion: ReportSchemaVersion, EventSchemaVersion: EventSchemaVersion,
		GeneratedAt: ledger.now().UTC().Format(time.RFC3339Nano), Compatibility: compatibility,
		Retention: RetentionReport{
			WindowHours:                    int64(ledger.retention / time.Hour),
			ExpiredEventsPurged:            purged.ExpiredEventsPurged,
			ExpiredCollectionRecordsPurged: purged.ExpiredCollectionRecordsPurged,
		},
		Collection: collection, Lifecycle: lifecycle, Operations: operations,
	}, nil
}

func (ledger *Ledger) collectionReport() (CollectionReport, error) {
	row := ledger.db.QueryRow(`SELECT COALESCE(SUM(dropped_events), 0), COALESCE(SUM(write_failures), 0)
		FROM baseline_collection`)
	report := CollectionReport{}
	if err := row.Scan(&report.DroppedEvents, &report.WriteFailures); err != nil {
		return CollectionReport{}, fmt.Errorf("decode recall baseline collection aggregate: %w", err)
	}
	return report, nil
}

type PurgeResult struct {
	ExpiredEventsPurged            int64 `json:"expired_events_purged"`
	ExpiredCollectionRecordsPurged int64 `json:"expired_collection_records_purged"`
}

func (ledger *Ledger) PurgeExpired() (PurgeResult, error) {
	tx, err := ledger.db.Begin()
	if err != nil {
		return PurgeResult{}, fmt.Errorf("begin recall baseline purge: %w", err)
	}
	defer tx.Rollback()
	cutoff := ledger.now().UTC().Format(time.RFC3339Nano)
	result, err := tx.Exec(`DELETE FROM baseline_events WHERE expires_at <= ?`, cutoff)
	if err != nil {
		return PurgeResult{}, fmt.Errorf("purge expired recall baseline events: %w", err)
	}
	events, err := result.RowsAffected()
	if err != nil {
		return PurgeResult{}, fmt.Errorf("count expired recall baseline events: %w", err)
	}
	result, err = tx.Exec(`DELETE FROM baseline_collection WHERE expires_at <= ?`, cutoff)
	if err != nil {
		return PurgeResult{}, fmt.Errorf("purge expired recall baseline collection records: %w", err)
	}
	collection, err := result.RowsAffected()
	if err != nil {
		return PurgeResult{}, fmt.Errorf("count expired recall baseline collection records: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PurgeResult{}, fmt.Errorf("commit recall baseline purge: %w", err)
	}
	return PurgeResult{ExpiredEventsPurged: events, ExpiredCollectionRecordsPurged: collection}, nil
}

func (ledger *Ledger) lifecycleReport() (LifecycleReport, error) {
	rows, err := ledger.db.Query(`SELECT kind, outcome, COUNT(*) FROM baseline_events
		WHERE surface = ? GROUP BY kind, outcome ORDER BY kind, outcome`, SurfaceLifecycle)
	if err != nil {
		return LifecycleReport{}, fmt.Errorf("aggregate recall baseline lifecycle: %w", err)
	}
	defer rows.Close()
	report := LifecycleReport{}
	for rows.Next() {
		var kind EventKind
		var outcome Outcome
		var count int64
		if err := rows.Scan(&kind, &outcome, &count); err != nil {
			return LifecycleReport{}, fmt.Errorf("decode recall baseline lifecycle aggregate: %w", err)
		}
		switch kind {
		case EventCheckpoint:
			report.Checkpoints.EligibleTurns += count
			switch outcome {
			case OutcomeCompleted:
				report.Checkpoints.Completed += count
			case OutcomeMissing:
				report.Checkpoints.Missing += count
			case OutcomeConflict:
				report.Checkpoints.Conflicting += count
			default:
				report.Checkpoints.Unknown += count
			}
		case EventStop:
			report.Stop.Events += count
			switch outcome {
			case OutcomeCompleted:
				report.Stop.Completed += count
			case OutcomeContinuationRequired:
				report.Stop.Continuations += count
			case OutcomeRecoveryExhausted:
				report.Stop.RecoveryExhausted += count
			case OutcomeIntegrationFailure:
				report.Stop.IntegrationFailures += count
			case OutcomeUnknown:
				report.Stop.Unknown += count
			}
		case EventCapture:
			report.Capture.Events += count
			switch outcome {
			case OutcomeEnabled:
				report.Capture.Enabled += count
			case OutcomeDisabled:
				report.Capture.Disabled += count
			default:
				report.Capture.Unknown += count
			}
		case EventSubagentStop:
			report.SubagentStop.Events += count
			switch outcome {
			case OutcomeObserved:
				report.SubagentStop.Observed += count
			case OutcomeSkipped:
				report.SubagentStop.Skipped += count
			default:
				report.SubagentStop.Unknown += count
			}
		}
	}
	if err := rows.Err(); err != nil {
		return LifecycleReport{}, fmt.Errorf("aggregate recall baseline lifecycle rows: %w", err)
	}
	return report, nil
}

func (ledger *Ledger) operationReports() ([]OperationReport, error) {
	rows, err := ledger.db.Query(`SELECT surface, operation, outcome, latency_micros, delivered_utf8_bytes
		FROM baseline_events WHERE kind = ? ORDER BY surface, operation, id`, EventOperation)
	if err != nil {
		return nil, fmt.Errorf("aggregate recall baseline operations: %w", err)
	}
	defer rows.Close()
	type accumulator struct {
		report    OperationReport
		latencies []float64
	}
	groups := map[string]*accumulator{}
	keys := make([]string, 0)
	for rows.Next() {
		var surface Surface
		var operation string
		var outcome Outcome
		var latency, delivered sql.NullInt64
		if err := rows.Scan(&surface, &operation, &outcome, &latency, &delivered); err != nil {
			return nil, fmt.Errorf("decode recall baseline operation: %w", err)
		}
		key := string(surface) + "\x00" + operation
		group := groups[key]
		if group == nil {
			group = &accumulator{report: OperationReport{Surface: surface, Operation: operation}}
			groups[key] = group
			keys = append(keys, key)
		}
		group.report.Events++
		switch outcome {
		case OutcomeSuccess:
			group.report.Succeeded++
		case OutcomeError:
			group.report.Failed++
		default:
			group.report.UnknownOutcome++
		}
		if latency.Valid {
			group.report.LatencySamples++
			group.latencies = append(group.latencies, float64(latency.Int64)/1000)
		} else {
			group.report.UnknownLatency++
		}
		if delivered.Valid {
			group.report.ByteSamples++
			group.report.TotalUTF8Bytes += delivered.Int64
		} else {
			group.report.UnknownBytes++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("aggregate recall baseline operation rows: %w", err)
	}
	sort.Strings(keys)
	reports := make([]OperationReport, 0, len(keys))
	for _, key := range keys {
		group := groups[key]
		sort.Float64s(group.latencies)
		group.report.P50LatencyMillis = percentile(group.latencies, 0.50)
		group.report.P95LatencyMillis = percentile(group.latencies, 0.95)
		reports = append(reports, group.report)
	}
	return reports, nil
}

func percentile(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values))*quantile+0.999999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
