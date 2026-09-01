package store

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
	"strings"
	"time"
)

const (
	RecallUtilityDecisive  = "decisive"
	RecallUtilityOrienting = "orienting"
	RecallUtilityDuplicate = "duplicate"
	RecallUtilityUnused    = "unused"

	RecallQualityCurrent       = "current"
	RecallQualityStale         = "stale"
	RecallQualityContradictory = "contradictory"
	RecallQualityUnknown       = "unknown"

	RecallFeedbackSourceAgentExplicit = "agent_explicit"
	RecallFeedbackSourceUserExplicit  = "user_explicit"
	RecallFeedbackSourceEvaluator     = "evaluator"

	recallFeedbackKeyFilename = "recall-feedback-v1.key"
)

var (
	ErrRecallFeedbackInvalid          = errors.New("invalid Recall feedback")
	ErrRecallFeedbackResultNotExposed = errors.New("Recall feedback result was not exposed")
	ErrRecallFeedbackTurnMismatch     = errors.New("Recall run was not exposed in the declared root turn")
	ErrRecallFeedbackConflict         = errors.New("Recall feedback already recorded with different labels")
	errRecallFeedbackSaltMalformed    = errors.New("Recall feedback salt is malformed")
)

type RecallFeedbackLabelInput struct {
	ResultID string
	Utility  string
	Quality  string
	Source   string
}

type RecordRecallFeedbackParams struct {
	Identity         CheckpointIdentity
	RecallID         string
	Results          []RecallFeedbackLabelInput
	FalseEmpty       *bool
	FalseEmptySource string
}

type RecallFeedbackRecordResult struct {
	LabelsRecorded              int
	LabelsAlreadyRecorded       int
	EmptyReviewsRecorded        int
	EmptyReviewsAlreadyRecorded int
}

type RecallFeedbackOperationalMetric struct {
	Operation          string
	ExposedResults     int
	DeliveredUTF8Bytes *int64
	ElapsedMonotonicMS *int64
}

type RecallFeedbackRunMetric struct {
	RunKey              string
	TurnKey             string
	ResultCount         int
	ElapsedMonotonicMS  *int64
	StartedAtUnixNano   *int64
	CompletedAtUnixNano *int64
}

type RecallFeedbackExposureMetric struct {
	RunKey    string
	TurnKey   string
	MemoryKey string
}

type RecallFeedbackLabelMetric struct {
	TurnKey   string
	RunKey    string
	MemoryKey string
	Utility   string
	Quality   string
	Source    string
}

type RecallFalseEmptyMetric struct {
	TurnKey string
	RunKey  string
	Source  string
	Value   bool
}

type RecallFeedbackReportSnapshot struct {
	Operations   []RecallFeedbackOperationalMetric
	Runs         []RecallFeedbackRunMetric
	Exposures    []RecallFeedbackExposureMetric
	Labels       []RecallFeedbackLabelMetric
	EmptyReviews []RecallFalseEmptyMetric
}

func (s *Store) migrateRecallFeedback() error {
	_, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS recall_feedback_runs (
			run_key      TEXT PRIMARY KEY CHECK (length(run_key) = 64),
			turn_key     TEXT NOT NULL CHECK (length(turn_key) = 64),
			result_count INTEGER NOT NULL CHECK (result_count >= 0),
			delivered_utf8_bytes INTEGER,
			elapsed_monotonic_ms INTEGER,
			protocol_version INTEGER,
			binary_version TEXT,
			binary_revision TEXT,
			created_at   TEXT NOT NULL DEFAULT (datetime('now'))
		);
		CREATE INDEX IF NOT EXISTS idx_recall_feedback_runs_turn
			ON recall_feedback_runs(turn_key, created_at);

		CREATE TABLE IF NOT EXISTS recall_feedback_exposures (
			run_key     TEXT NOT NULL REFERENCES recall_feedback_runs(run_key) ON DELETE CASCADE,
			memory_key  TEXT NOT NULL CHECK (length(memory_key) = 64),
			result_rank INTEGER NOT NULL CHECK (result_rank >= 0),
			PRIMARY KEY (run_key, memory_key),
			UNIQUE (run_key, result_rank)
		);

		CREATE TABLE IF NOT EXISTS recall_feedback_labels (
			turn_key     TEXT NOT NULL CHECK (length(turn_key) = 64),
			run_key      TEXT NOT NULL,
			memory_key   TEXT NOT NULL CHECK (length(memory_key) = 64),
			utility      TEXT NOT NULL DEFAULT '' CHECK (utility IN ('', 'decisive', 'orienting', 'duplicate', 'unused')),
			quality      TEXT NOT NULL DEFAULT '' CHECK (quality IN ('', 'current', 'stale', 'contradictory', 'unknown')),
			label_source TEXT NOT NULL CHECK (label_source IN ('agent_explicit', 'user_explicit', 'evaluator')),
			created_at   TEXT NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (turn_key, memory_key, label_source),
			FOREIGN KEY (run_key, memory_key)
				REFERENCES recall_feedback_exposures(run_key, memory_key) ON DELETE CASCADE,
			CHECK (utility <> '' OR quality <> '')
		);
		CREATE INDEX IF NOT EXISTS idx_recall_feedback_run
			ON recall_feedback_labels(run_key, created_at);

		CREATE TABLE IF NOT EXISTS recall_false_empty_reviews (
			turn_key     TEXT NOT NULL CHECK (length(turn_key) = 64),
			run_key      TEXT NOT NULL REFERENCES recall_feedback_runs(run_key) ON DELETE CASCADE,
			label_source TEXT NOT NULL CHECK (label_source IN ('agent_explicit', 'user_explicit', 'evaluator')),
			false_empty  BOOLEAN NOT NULL,
			created_at   TEXT NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (turn_key, run_key, label_source)
		);
	`)
	return err
}

// RecordRecallFeedback validates every label against a result exposed by the
// declared Recall run, then stores only per-install salted turn and Memory
// keys. The raw checkpoint identity and Memory identifier are never written.
func (s *Store) RecordRecallFeedback(p RecordRecallFeedbackParams) (*RecallFeedbackRecordResult, error) {
	if err := validateCheckpointIdentity(p.Identity); err != nil {
		return nil, ErrRecallFeedbackInvalid
	}
	p.RecallID = strings.TrimSpace(p.RecallID)
	if p.RecallID == "" || (len(p.Results) == 0 && p.FalseEmpty == nil) {
		return nil, ErrRecallFeedbackInvalid
	}
	if p.FalseEmpty != nil && !validRecallFeedbackSource(p.FalseEmptySource) {
		return nil, ErrRecallFeedbackInvalid
	}
	for _, result := range p.Results {
		if strings.TrimSpace(result.ResultID) == "" || !validRecallUtility(result.Utility) ||
			!validRecallQuality(result.Quality) || !validRecallFeedbackSource(result.Source) ||
			(result.Utility == "" && result.Quality == "") {
			return nil, ErrRecallFeedbackInvalid
		}
	}

	salt, err := loadOrCreateRecallFeedbackSalt(s.cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("load Recall feedback salt: %w", err)
	}
	turnKey := recallFeedbackDigest(salt, "turn", p.Identity.Host, p.Identity.SessionID, p.Identity.RootTurnID)
	runKey := recallFeedbackDigest(salt, "run", p.RecallID)
	result := &RecallFeedbackRecordResult{}
	err = s.withTx(func(tx *sql.Tx) error {
		var deliveredBytes, elapsedMS, protocolVersion sql.NullInt64
		var binaryVersion, binaryRevision sql.NullString
		var storedTurnKey sql.NullString
		if err := tx.QueryRow(`
			SELECT delivered_utf8_bytes, elapsed_monotonic_ms, protocol_version,
			       binary_version, binary_revision, turn_key
			FROM recall_runs WHERE recall_id = ?`, p.RecallID).Scan(
			&deliveredBytes, &elapsedMS, &protocolVersion, &binaryVersion, &binaryRevision, &storedTurnKey,
		); errors.Is(err, sql.ErrNoRows) {
			return ErrRecallFeedbackResultNotExposed
		} else if err != nil {
			return fmt.Errorf("load Recall run: %w", err)
		}
		if !storedTurnKey.Valid || storedTurnKey.String != turnKey {
			return ErrRecallFeedbackTurnMismatch
		}

		type exposure struct {
			resultID  string
			memoryKey string
			rank      int
		}
		rows, err := tx.Query(`
			SELECT rr.result_id, o.sync_id, rr.result_rank
			FROM recall_results rr
			JOIN observations o ON o.id = rr.observation_id
			WHERE rr.recall_id = ?
			ORDER BY rr.result_rank`, p.RecallID)
		if err != nil {
			return fmt.Errorf("load Recall exposures: %w", err)
		}
		var exposures []exposure
		byResultID := make(map[string]exposure)
		for rows.Next() {
			var exposed exposure
			var memorySyncID string
			if err := rows.Scan(&exposed.resultID, &memorySyncID, &exposed.rank); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan Recall exposure: %w", err)
			}
			exposed.memoryKey = recallFeedbackDigest(salt, "memory", memorySyncID)
			exposures = append(exposures, exposed)
			byResultID[exposed.resultID] = exposed
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("load Recall exposures: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close Recall exposures: %w", err)
		}
		for _, label := range p.Results {
			if _, exposed := byResultID[label.ResultID]; !exposed {
				return ErrRecallFeedbackResultNotExposed
			}
		}
		if p.FalseEmpty != nil && len(exposures) != 0 {
			return ErrRecallFeedbackInvalid
		}

		insertedRun, err := s.execHook(tx, `
			INSERT OR IGNORE INTO recall_feedback_runs (
				run_key, turn_key, result_count, delivered_utf8_bytes, elapsed_monotonic_ms,
				protocol_version, binary_version, binary_revision
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			runKey, turnKey, len(exposures), sqlNullInt64Value(deliveredBytes), sqlNullInt64Value(elapsedMS),
			sqlNullInt64Value(protocolVersion), sqlNullStringValue(binaryVersion), sqlNullStringValue(binaryRevision))
		if err != nil {
			return fmt.Errorf("associate Recall feedback run: %w", err)
		}
		insertedRows, err := insertedRun.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect Recall feedback run: %w", err)
		}
		if insertedRows == 0 {
			var storedTurnKey string
			var storedResultCount int
			if err := tx.QueryRow(`SELECT turn_key, result_count FROM recall_feedback_runs WHERE run_key = ?`, runKey).
				Scan(&storedTurnKey, &storedResultCount); err != nil {
				return fmt.Errorf("load Recall feedback run: %w", err)
			}
			if storedTurnKey != turnKey || storedResultCount != len(exposures) {
				return ErrRecallFeedbackConflict
			}
		}
		for _, exposed := range exposures {
			if _, err := s.execHook(tx, `
				INSERT OR IGNORE INTO recall_feedback_exposures (run_key, memory_key, result_rank)
				VALUES (?, ?, ?)`, runKey, exposed.memoryKey, exposed.rank); err != nil {
				return fmt.Errorf("snapshot Recall exposure: %w", err)
			}
		}

		for _, label := range p.Results {
			exposed := byResultID[label.ResultID]
			inserted, err := s.execHook(tx, `
				INSERT OR IGNORE INTO recall_feedback_labels (
					turn_key, run_key, memory_key, utility, quality, label_source
				) VALUES (?, ?, ?, ?, ?, ?)`,
				turnKey, runKey, exposed.memoryKey, label.Utility, label.Quality, label.Source)
			if err != nil {
				return fmt.Errorf("record Recall feedback: %w", err)
			}
			rows, err := inserted.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect Recall feedback insert: %w", err)
			}
			if rows == 1 {
				result.LabelsRecorded++
				continue
			}
			var storedUtility, storedQuality string
			if err := tx.QueryRow(`
				SELECT utility, quality
				FROM recall_feedback_labels
				WHERE turn_key = ? AND memory_key = ? AND label_source = ?`,
				turnKey, exposed.memoryKey, label.Source).Scan(&storedUtility, &storedQuality); err != nil {
				return fmt.Errorf("load replayed Recall feedback: %w", err)
			}
			if storedUtility != label.Utility || storedQuality != label.Quality {
				return ErrRecallFeedbackConflict
			}
			result.LabelsAlreadyRecorded++
		}
		if p.FalseEmpty != nil {
			inserted, err := s.execHook(tx, `
				INSERT OR IGNORE INTO recall_false_empty_reviews (
					turn_key, run_key, label_source, false_empty
				) VALUES (?, ?, ?, ?)`, turnKey, runKey, p.FalseEmptySource, *p.FalseEmpty)
			if err != nil {
				return fmt.Errorf("record false-empty review: %w", err)
			}
			rows, err := inserted.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect false-empty review: %w", err)
			}
			if rows == 1 {
				result.EmptyReviewsRecorded++
			} else {
				var stored bool
				if err := tx.QueryRow(`
					SELECT false_empty FROM recall_false_empty_reviews
					WHERE turn_key = ? AND run_key = ? AND label_source = ?`,
					turnKey, runKey, p.FalseEmptySource).Scan(&stored); err != nil {
					return fmt.Errorf("load false-empty review: %w", err)
				}
				if stored != *p.FalseEmpty {
					return ErrRecallFeedbackConflict
				}
				result.EmptyReviewsAlreadyRecorded++
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RecallFeedbackReportSnapshot returns content-free local metric rows for the
// aggregate-only Core report. No raw Recall, turn, or Memory identifier leaves
// the Store boundary.
func (s *Store) RecallFeedbackReportSnapshot() (*RecallFeedbackReportSnapshot, error) {
	snapshot := &RecallFeedbackReportSnapshot{}
	runRows, err := s.db.Query(`
		SELECT 'search', COALESCE(run.result_count, COUNT(result.result_id)),
		       run.delivered_utf8_bytes, run.elapsed_monotonic_ms
		FROM recall_runs run
		LEFT JOIN recall_results result ON result.recall_id = run.recall_id
		GROUP BY run.recall_id
		ORDER BY run.created_at, run.recall_id`)
	if err != nil {
		return nil, fmt.Errorf("load Recall operation metrics: %w", err)
	}
	for runRows.Next() {
		var metric RecallFeedbackOperationalMetric
		var delivered, elapsed sql.NullInt64
		if err := runRows.Scan(&metric.Operation, &metric.ExposedResults, &delivered, &elapsed); err != nil {
			_ = runRows.Close()
			return nil, fmt.Errorf("scan Recall operation metric: %w", err)
		}
		metric.DeliveredUTF8Bytes = nullInt64Pointer(delivered)
		metric.ElapsedMonotonicMS = nullInt64Pointer(elapsed)
		snapshot.Operations = append(snapshot.Operations, metric)
	}
	if err := runRows.Err(); err != nil {
		_ = runRows.Close()
		return nil, fmt.Errorf("load Recall operation metrics: %w", err)
	}
	if err := runRows.Close(); err != nil {
		return nil, fmt.Errorf("close Recall operation metrics: %w", err)
	}

	segmentRows, err := s.db.Query(`
		SELECT 'get', 1, delivered_bytes, elapsed_monotonic_ms
		FROM recall_segments
		ORDER BY created_at, recall_id, result_id, position`)
	if err != nil {
		return nil, fmt.Errorf("load Recall content metrics: %w", err)
	}
	for segmentRows.Next() {
		var metric RecallFeedbackOperationalMetric
		var delivered, elapsed sql.NullInt64
		if err := segmentRows.Scan(&metric.Operation, &metric.ExposedResults, &delivered, &elapsed); err != nil {
			_ = segmentRows.Close()
			return nil, fmt.Errorf("scan Recall content metric: %w", err)
		}
		metric.DeliveredUTF8Bytes = nullInt64Pointer(delivered)
		metric.ElapsedMonotonicMS = nullInt64Pointer(elapsed)
		snapshot.Operations = append(snapshot.Operations, metric)
	}
	if err := segmentRows.Err(); err != nil {
		_ = segmentRows.Close()
		return nil, fmt.Errorf("load Recall content metrics: %w", err)
	}
	if err := segmentRows.Close(); err != nil {
		return nil, fmt.Errorf("close Recall content metrics: %w", err)
	}

	boundRunRows, err := s.db.Query(`
		SELECT run.recall_id, run.turn_key,
		       COALESCE(run.result_count, COUNT(result.result_id)),
		       run.elapsed_monotonic_ms, run.started_at_unix_nano,
		       run.completed_at_unix_nano
		FROM recall_runs run
		LEFT JOIN recall_results result ON result.recall_id = run.recall_id
		WHERE run.turn_key IS NOT NULL
		GROUP BY run.recall_id
		ORDER BY run.created_at, run.recall_id`)
	if err != nil {
		return nil, fmt.Errorf("load root-turn Recall runs: %w", err)
	}
	var salt []byte
	for boundRunRows.Next() {
		if salt == nil {
			salt, err = readRecallFeedbackSalt(s.cfg.DataDir)
			if err != nil {
				_ = boundRunRows.Close()
				return nil, fmt.Errorf("load Recall attribution salt: %w", err)
			}
		}
		var metric RecallFeedbackRunMetric
		var recallID string
		var elapsed, started, completed sql.NullInt64
		if err := boundRunRows.Scan(&recallID, &metric.TurnKey, &metric.ResultCount, &elapsed, &started, &completed); err != nil {
			_ = boundRunRows.Close()
			return nil, fmt.Errorf("scan root-turn Recall run: %w", err)
		}
		metric.RunKey = recallFeedbackDigest(salt, "run", recallID)
		metric.ElapsedMonotonicMS = nullInt64Pointer(elapsed)
		metric.StartedAtUnixNano = nullInt64Pointer(started)
		metric.CompletedAtUnixNano = nullInt64Pointer(completed)
		snapshot.Runs = append(snapshot.Runs, metric)
	}
	if err := boundRunRows.Err(); err != nil {
		_ = boundRunRows.Close()
		return nil, fmt.Errorf("load root-turn Recall runs: %w", err)
	}
	if err := boundRunRows.Close(); err != nil {
		return nil, fmt.Errorf("close root-turn Recall runs: %w", err)
	}

	preservedExposureRows, err := s.db.Query(`
		SELECT run.turn_key, exposure.run_key, exposure.memory_key
		FROM recall_feedback_exposures exposure
		JOIN recall_feedback_runs run ON run.run_key = exposure.run_key
		ORDER BY run.created_at, exposure.run_key, exposure.result_rank`)
	if err != nil {
		return nil, fmt.Errorf("load preserved Recall exposures: %w", err)
	}
	for preservedExposureRows.Next() {
		var metric RecallFeedbackExposureMetric
		if err := preservedExposureRows.Scan(&metric.TurnKey, &metric.RunKey, &metric.MemoryKey); err != nil {
			_ = preservedExposureRows.Close()
			return nil, fmt.Errorf("scan preserved Recall exposure: %w", err)
		}
		snapshot.Exposures = append(snapshot.Exposures, metric)
	}
	if err := preservedExposureRows.Err(); err != nil {
		_ = preservedExposureRows.Close()
		return nil, fmt.Errorf("load preserved Recall exposures: %w", err)
	}
	if err := preservedExposureRows.Close(); err != nil {
		return nil, fmt.Errorf("close preserved Recall exposures: %w", err)
	}

	labelRows, err := s.db.Query(`
		SELECT turn_key, run_key, memory_key, utility, quality, label_source
		FROM recall_feedback_labels ORDER BY created_at, turn_key, memory_key, label_source`)
	if err != nil {
		return nil, fmt.Errorf("load Recall labels: %w", err)
	}
	for labelRows.Next() {
		var metric RecallFeedbackLabelMetric
		if err := labelRows.Scan(&metric.TurnKey, &metric.RunKey, &metric.MemoryKey, &metric.Utility, &metric.Quality, &metric.Source); err != nil {
			_ = labelRows.Close()
			return nil, fmt.Errorf("scan Recall label: %w", err)
		}
		snapshot.Labels = append(snapshot.Labels, metric)
	}
	if err := labelRows.Err(); err != nil {
		_ = labelRows.Close()
		return nil, fmt.Errorf("load Recall labels: %w", err)
	}
	if err := labelRows.Close(); err != nil {
		return nil, fmt.Errorf("close Recall labels: %w", err)
	}

	emptyRows, err := s.db.Query(`
		SELECT turn_key, run_key, label_source, false_empty
		FROM recall_false_empty_reviews ORDER BY created_at, turn_key, run_key, label_source`)
	if err != nil {
		return nil, fmt.Errorf("load false-empty reviews: %w", err)
	}
	for emptyRows.Next() {
		var metric RecallFalseEmptyMetric
		if err := emptyRows.Scan(&metric.TurnKey, &metric.RunKey, &metric.Source, &metric.Value); err != nil {
			_ = emptyRows.Close()
			return nil, fmt.Errorf("scan false-empty review: %w", err)
		}
		snapshot.EmptyReviews = append(snapshot.EmptyReviews, metric)
	}
	if err := emptyRows.Err(); err != nil {
		_ = emptyRows.Close()
		return nil, fmt.Errorf("load false-empty reviews: %w", err)
	}
	if err := emptyRows.Close(); err != nil {
		return nil, fmt.Errorf("close false-empty reviews: %w", err)
	}
	return snapshot, nil
}

func nullInt64Pointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}

func sqlNullInt64Value(value sql.NullInt64) any {
	if value.Valid {
		return value.Int64
	}
	return nil
}

func sqlNullStringValue(value sql.NullString) any {
	if value.Valid {
		return value.String
	}
	return nil
}

func validRecallUtility(value string) bool {
	switch value {
	case "", RecallUtilityDecisive, RecallUtilityOrienting, RecallUtilityDuplicate, RecallUtilityUnused:
		return true
	default:
		return false
	}
}

func validRecallQuality(value string) bool {
	switch value {
	case "", RecallQualityCurrent, RecallQualityStale, RecallQualityContradictory, RecallQualityUnknown:
		return true
	default:
		return false
	}
}

func validRecallFeedbackSource(value string) bool {
	switch value {
	case RecallFeedbackSourceAgentExplicit, RecallFeedbackSourceUserExplicit, RecallFeedbackSourceEvaluator:
		return true
	default:
		return false
	}
}

func recallFeedbackDigest(salt []byte, kind string, values ...string) string {
	digest := hmac.New(sha256.New, salt)
	_, _ = io.WriteString(digest, kind)
	for _, value := range values {
		_, _ = fmt.Fprintf(digest, ":%d:", len(value))
		_, _ = io.WriteString(digest, value)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func readRecallFeedbackSalt(dataDir string) ([]byte, error) {
	path := filepath.Join(dataDir, recallFeedbackKeyFilename)
	read := func() ([]byte, error) {
		salt, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(salt) != sha256.Size {
			return nil, errRecallFeedbackSaltMalformed
		}
		if err := os.Chmod(path, 0o600); err != nil {
			return nil, err
		}
		return salt, nil
	}
	readWithRetry := func() ([]byte, error) {
		for attempts := 0; ; attempts++ {
			salt, err := read()
			if err == nil || errors.Is(err, os.ErrNotExist) {
				return salt, err
			}
			if !errors.Is(err, errRecallFeedbackSaltMalformed) || attempts == 9 {
				return nil, err
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return readWithRetry()
}

func loadOrCreateRecallFeedbackSalt(dataDir string) ([]byte, error) {
	path := filepath.Join(dataDir, recallFeedbackKeyFilename)
	if salt, err := readRecallFeedbackSalt(dataDir); err == nil {
		return salt, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	salt := make([]byte, sha256.Size)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return readRecallFeedbackSalt(dataDir)
	}
	if err != nil {
		return nil, err
	}
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(salt); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	remove = false
	return salt, nil
}
