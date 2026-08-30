package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNewStoreHasNoAdmissionSchema(t *testing.T) {
	s := newTestStore(t)

	assertNoAdmissionSchema(t, s)
}

func TestProjectLifecycleResultsExcludeAdmissionAccounting(t *testing.T) {
	for name, result := range map[string]any{
		"rename": MigrateResult{},
		"merge":  MergeResult{},
		"delete": DeleteProjectResult{},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatalf("marshal result: %v", err)
			}
			if strings.Contains(strings.ToLower(string(encoded)), "admission") {
				t.Fatalf("lifecycle result retained Admission accounting: %s", encoded)
			}
		})
	}
}

func TestOpenV2AdmissionFixturePermanentlyDiscardsAdmissionData(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()

	raw := seedAdmissionFixture(t, cfg, "testdata/v2_admission_full.sql")
	assertFullAdmissionFixture(t, raw)
	if err := raw.Close(); err != nil {
		t.Fatalf("close v2 fixture: %v", err)
	}

	for _, scenario := range []string{"upgrade", "repeated v3 open"} {
		t.Run(scenario, func(t *testing.T) {
			s, err := New(cfg)
			if err != nil {
				t.Fatalf("open Store: %v", err)
			}
			assertNoAdmissionSchema(t, s)
			assertAdmissionCanaryWasNotConverted(t, s)
			assertForeignKeysClean(t, s.DB())
			if err := s.Close(); err != nil {
				t.Fatalf("close Store: %v", err)
			}
		})
	}

	raw = seedAdmissionFixture(t, cfg, "testdata/v2_admission_full.sql")
	assertFullAdmissionFixture(t, raw)
	if err := raw.Close(); err != nil {
		t.Fatalf("close recreated v2 fixture: %v", err)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("open Store after v2 recreation: %v", err)
	}
	defer s.Close()
	assertNoAdmissionSchema(t, s)
	assertAdmissionCanaryWasNotConverted(t, s)
	assertForeignKeysClean(t, s.DB())
}

func TestOpenHandlesPartialHistoricalAdmissionSchema(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := seedAdmissionFixture(t, cfg, "testdata/v2_admission_shadow_only.sql")
	if got := admissionSchemaObjectsDB(t, raw); len(got) == 0 {
		t.Fatal("partial v2 fixture has no Admission schema")
	}
	assertForeignKeysClean(t, raw)
	if err := raw.Close(); err != nil {
		t.Fatalf("close partial v2 fixture: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("open partial historical schema: %v", err)
	}
	defer s.Close()
	assertNoAdmissionSchema(t, s)
	assertForeignKeysClean(t, s.DB())
}

func TestAdmissionTombstoneFailureRollsBackStartupAndLaterRetrySucceeds(t *testing.T) {
	s := newUnmigratedAdmissionFixtureStore(t, "testdata/v2_admission_full.sql")
	wantSchema := admissionSchemaObjects(t, s)
	originalExec := s.hooks.exec
	s.hooks.exec = func(db execer, query string, args ...any) (sql.Result, error) {
		if strings.TrimSpace(query) == "DROP TABLE IF EXISTS admission_shadow_proposals" {
			return nil, errors.New("injected Admission drop failure")
		}
		return originalExec(db, query, args...)
	}

	if err := s.migrate(); err == nil || !strings.Contains(err.Error(), "injected Admission drop failure") {
		t.Fatalf("startup migration error = %v, want injected drop failure", err)
	}
	if got := admissionSchemaObjects(t, s); !reflect.DeepEqual(got, wantSchema) {
		t.Fatalf("Admission schema after failed startup = %v, want %v", got, wantSchema)
	}
	assertFullAdmissionFixture(t, s.DB())

	s.hooks.exec = originalExec
	if err := s.migrate(); err != nil {
		t.Fatalf("retry startup migration: %v", err)
	}
	assertNoAdmissionSchema(t, s)
	assertAdmissionCanaryWasNotConverted(t, s)
	assertForeignKeysClean(t, s.DB())
}

func TestAdmissionTombstoneRetriesBusyTransactionFromFirstDrop(t *testing.T) {
	s := newUnmigratedAdmissionFixtureStore(t, "testdata/v2_admission_full.sql")
	originalExec := s.hooks.exec
	dropCalls := []string{}
	busy := true
	reviewsDrops := 0
	reviewsRestoredBeforeRetry := false
	s.hooks.exec = func(db execer, query string, args ...any) (sql.Result, error) {
		query = strings.TrimSpace(query)
		if strings.HasPrefix(query, "DROP TABLE IF EXISTS admission_") {
			dropCalls = append(dropCalls, query)
		}
		if query == "DROP TABLE IF EXISTS admission_shadow_reviews" {
			reviewsDrops++
			if reviewsDrops == 2 {
				tx, ok := db.(*sql.Tx)
				if !ok {
					t.Fatalf("second drop runs on %T, want *sql.Tx", db)
				}
				var exists bool
				if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'admission_shadow_reviews')`).Scan(&exists); err != nil {
					t.Fatalf("inspect retry transaction: %v", err)
				}
				reviewsRestoredBeforeRetry = exists
			}
		}
		if query == "DROP TABLE IF EXISTS admission_study_omissions" && busy {
			busy = false
			return nil, errors.New("database is locked")
		}
		return originalExec(db, query, args...)
	}

	if err := s.migrateAdmissionTombstone(); err != nil {
		t.Fatalf("retry busy tombstone: %v", err)
	}
	wantCalls := []string{
		"DROP TABLE IF EXISTS admission_shadow_reviews",
		"DROP TABLE IF EXISTS admission_study_omissions",
		"DROP TABLE IF EXISTS admission_shadow_reviews",
		"DROP TABLE IF EXISTS admission_study_omissions",
		"DROP TABLE IF EXISTS admission_shadow_proposals",
		"DROP TABLE IF EXISTS admission_shadow_runs",
		"DROP TABLE IF EXISTS admission_studies",
	}
	if !reflect.DeepEqual(dropCalls, wantCalls) {
		t.Fatalf("drop calls = %v, want %v", dropCalls, wantCalls)
	}
	if !reviewsRestoredBeforeRetry {
		t.Fatal("first dropped child table was not restored before retry")
	}
	assertNoAdmissionSchema(t, s)
	assertForeignKeysClean(t, s.DB())
}

func admissionSchemaObjects(t *testing.T, s *Store) []string {
	t.Helper()
	return admissionSchemaObjectsDB(t, s.DB())
}

func admissionSchemaObjectsDB(t *testing.T, db queryer) []string {
	t.Helper()
	rows, err := db.Query(`
		SELECT type || ':' || name
		FROM sqlite_schema
		WHERE type IN ('table', 'index')
		  AND tbl_name LIKE 'admission_%'
		ORDER BY type, name`)
	if err != nil {
		t.Fatalf("query Admission schema: %v", err)
	}
	defer rows.Close()

	objects := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan Admission schema object: %v", err)
		}
		objects = append(objects, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate Admission schema: %v", err)
	}
	return objects
}

func assertNoAdmissionSchema(t *testing.T, s *Store) {
	t.Helper()
	if got := admissionSchemaObjects(t, s); !reflect.DeepEqual(got, []string{}) {
		t.Fatalf("Admission schema objects = %v, want none", got)
	}
}

func seedAdmissionFixture(t *testing.T, cfg Config, fixturePath string) *sql.DB {
	t.Helper()
	fixture, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read fixture %s: %v", fixturePath, err)
	}
	db, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "engram.db"))
	if err != nil {
		t.Fatalf("open fixture database: %v", err)
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{"PRAGMA busy_timeout = 5000", "PRAGMA foreign_keys = ON"} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			t.Fatalf("configure fixture database: %v", err)
		}
	}
	if _, err := db.Exec(string(fixture)); err != nil {
		_ = db.Close()
		t.Fatalf("install fixture %s: %v", fixturePath, err)
	}
	return db
}

func newUnmigratedAdmissionFixtureStore(t *testing.T, fixturePath string) *Store {
	t.Helper()
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	s := &Store{db: seedAdmissionFixture(t, cfg, fixturePath), cfg: cfg, hooks: defaultStoreHooks()}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type admissionFixtureDB interface {
	queryer
	QueryRow(query string, args ...any) *sql.Row
}

func assertFullAdmissionFixture(t *testing.T, db admissionFixtureDB) {
	t.Helper()
	for _, table := range []string{
		"admission_studies",
		"admission_shadow_runs",
		"admission_shadow_proposals",
		"admission_shadow_reviews",
		"admission_study_omissions",
	} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("count fixture table %s: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("fixture table %s count = %d, want 1", table, count)
		}
	}
	for _, index := range []string{
		"idx_admission_shadow_runs_project_created",
		"idx_admission_shadow_proposals_run_ordinal",
		"idx_admission_shadow_reviews_proposal_ordinal",
		"idx_admission_shadow_runs_study_cohort",
		"idx_admission_shadow_runs_study_session",
		"idx_admission_shadow_reviews_reviewer",
		"idx_admission_study_omissions_run_reviewer",
	} {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ?)`, index).Scan(&exists); err != nil {
			t.Fatalf("inspect fixture index %s: %v", index, err)
		}
		if !exists {
			t.Fatalf("fixture index %s is missing", index)
		}
	}
	assertForeignKeysClean(t, db)
}

func assertAdmissionCanaryWasNotConverted(t *testing.T, s *Store) {
	t.Helper()
	var count int
	if err := s.DB().QueryRow(`
		SELECT
			(SELECT COUNT(*) FROM observations
			 WHERE title = 'RETIRED_ADMISSION_CANARY_TITLE'
			    OR content = 'RETIRED_ADMISSION_CANARY_CONTENT') +
			(SELECT COUNT(*) FROM memory_proposals
			 WHERE title = 'RETIRED_ADMISSION_CANARY_TITLE'
			    OR content = 'RETIRED_ADMISSION_CANARY_CONTENT')`).Scan(&count); err != nil {
		t.Fatalf("inspect converted Admission canary: %v", err)
	}
	if count != 0 {
		t.Fatalf("converted Admission canary rows = %d, want 0", count)
	}
}

func assertForeignKeysClean(t *testing.T, db queryer) {
	t.Helper()
	rows, err := db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("foreign key check: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("foreign key check returned a violation")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate foreign key check: %v", err)
	}
}
