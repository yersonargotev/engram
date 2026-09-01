package recallbaseline

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// InspectOperationReadOnly aggregates one declared, unexpired operation from
// an existing baseline ledger without creating state, purging rows, or loading
// the install linkage key.
func InspectOperationReadOnly(cfg Config, surface Surface, operation string) (OperationReport, bool, error) {
	if !filepath.IsAbs(cfg.DataDir) {
		return OperationReport{}, false, fmt.Errorf("recall baseline data directory must be absolute")
	}
	if !allowedOperation(EventOperation, surface, operation) {
		return OperationReport{}, false, fmt.Errorf("unsupported recall baseline operation %q", operation)
	}
	path := DatabasePath(cfg.DataDir)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return OperationReport{}, false, nil
		}
		return OperationReport{}, false, fmt.Errorf("inspect recall baseline database: %w", err)
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path), RawQuery: "mode=ro"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return OperationReport{}, false, fmt.Errorf("open recall baseline database read-only: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var schemaVersion int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&schemaVersion); err != nil {
		return OperationReport{}, false, fmt.Errorf("read recall baseline schema version: %w", err)
	}
	if schemaVersion != 1 {
		return OperationReport{}, false, fmt.Errorf("unsupported recall baseline schema version %d", schemaVersion)
	}
	now := time.Now().UTC()
	if cfg.Now != nil {
		now = cfg.Now().UTC()
	}
	rows, err := db.Query(`SELECT outcome, latency_micros, delivered_utf8_bytes
		FROM baseline_events
		WHERE kind = ? AND surface = ? AND operation = ? AND expires_at > ?
		ORDER BY id`, EventOperation, surface, operation, now.Format(time.RFC3339Nano))
	if err != nil {
		return OperationReport{}, false, fmt.Errorf("inspect recall baseline operation: %w", err)
	}
	defer rows.Close()
	report := OperationReport{Surface: surface, Operation: operation}
	var latencies []float64
	for rows.Next() {
		var outcome Outcome
		var latency, delivered sql.NullInt64
		if err := rows.Scan(&outcome, &latency, &delivered); err != nil {
			return OperationReport{}, false, fmt.Errorf("decode recall baseline operation: %w", err)
		}
		report.Events++
		switch outcome {
		case OutcomeSuccess:
			report.Succeeded++
		case OutcomeError:
			report.Failed++
		default:
			report.UnknownOutcome++
		}
		if latency.Valid {
			report.LatencySamples++
			latencies = append(latencies, float64(latency.Int64)/1000)
		} else {
			report.UnknownLatency++
		}
		if delivered.Valid {
			report.ByteSamples++
			report.TotalUTF8Bytes += delivered.Int64
		} else {
			report.UnknownBytes++
		}
	}
	if err := rows.Err(); err != nil {
		return OperationReport{}, false, fmt.Errorf("inspect recall baseline operation rows: %w", err)
	}
	if report.Events == 0 {
		return OperationReport{}, false, nil
	}
	sort.Float64s(latencies)
	report.P50LatencyMillis = percentile(latencies, 0.50)
	report.P95LatencyMillis = percentile(latencies, 0.95)
	return report, true, nil
}
