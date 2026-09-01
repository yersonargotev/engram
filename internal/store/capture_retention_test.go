package store

import (
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewPurgesExpiredDiagnosticCaptures(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	cfg.diagnosticCaptureNow = func() time.Time { return now }

	seed, err := New(cfg)
	if err != nil {
		t.Fatalf("new seed store: %v", err)
	}
	if _, err := seed.DB().Exec(`INSERT INTO diagnostic_captures (
		project, content_type, session_id, content, created_at, expires_at
	) VALUES
		('engram', 'prompt', 'expired', 'expired Diagnostic content', ?, ?),
		('engram', 'prompt', 'retained', 'unexpired Diagnostic content', ?, ?)`,
		now.Add(-48*time.Hour).Format(time.RFC3339Nano), now.Add(-time.Hour).Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano)); err != nil {
		_ = seed.Close()
		t.Fatalf("seed Diagnostic captures: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var expired, retained int
	if err := s.DB().QueryRow(`SELECT
		COUNT(*) FILTER (WHERE session_id = 'expired'),
		COUNT(*) FILTER (WHERE session_id = 'retained')
		FROM diagnostic_captures`).Scan(&expired, &retained); err != nil {
		t.Fatalf("inspect retained Diagnostic captures: %v", err)
	}
	if expired != 0 || retained != 1 {
		t.Fatalf("captures after reopen = expired:%d retained:%d, want expired:0 retained:1", expired, retained)
	}
}

func TestNewClosesDatabaseWhenInitialDiagnosticCapturePurgeFails(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()

	seed, err := New(cfg)
	if err != nil {
		t.Fatalf("new seed store: %v", err)
	}
	now := time.Now().UTC()
	if _, err := seed.DB().Exec(`
		INSERT INTO diagnostic_captures (
			project, content_type, session_id, content, created_at, expires_at
		) VALUES ('engram', 'prompt', 'expired', 'expired Diagnostic content', ?, ?);
		CREATE TRIGGER reject_diagnostic_capture_delete
		BEFORE DELETE ON diagnostic_captures
		BEGIN
			SELECT RAISE(ABORT, 'forced Diagnostic purge failure');
		END;`, now.Add(-48*time.Hour).Format(time.RFC3339Nano), now.Add(-time.Hour).Format(time.RFC3339Nano)); err != nil {
		_ = seed.Close()
		t.Fatalf("seed failing Diagnostic purge: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	originalOpenDB := openDB
	var opened *sql.DB
	openDB = func(driverName, dataSourceName string) (*sql.DB, error) {
		db, err := originalOpenDB(driverName, dataSourceName)
		opened = db
		return db, err
	}
	t.Cleanup(func() { openDB = originalOpenDB })

	s, err := New(cfg)
	if err == nil || !strings.Contains(err.Error(), "forced Diagnostic purge failure") {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("New error = %v, want initial Diagnostic purge failure", err)
	}
	if s != nil {
		t.Fatalf("New returned Store after purge failure: %#v", s)
	}
	if opened == nil {
		t.Fatal("New did not open a database")
	}
	if err := opened.Ping(); err == nil || !strings.Contains(err.Error(), "closed") {
		_ = opened.Close()
		t.Fatalf("database Ping after failed New = %v, want closed database", err)
	}
}

func TestDiagnosticCaptureJanitorPurgesExpiredRowsWithoutLaterCapture(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	ticks := make(chan time.Time)
	cfg.diagnosticCaptureJanitorTicks = ticks

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	sweepAt := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	if _, err := s.DB().Exec(`INSERT INTO diagnostic_captures (
		project, content_type, session_id, content, created_at, expires_at
	) VALUES ('engram', 'prompt', 'expired-while-open', 'expired Diagnostic content', ?, ?)`,
		sweepAt.Add(-8*24*time.Hour).Format(time.RFC3339Nano),
		sweepAt.Add(-time.Nanosecond).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed Diagnostic capture: %v", err)
	}

	ticks <- sweepAt
	deadline := time.Now().Add(time.Second)
	for {
		var count int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM diagnostic_captures WHERE session_id = 'expired-while-open'`).Scan(&count); err != nil {
			t.Fatalf("inspect Diagnostic capture: %v", err)
		}
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Diagnostic capture remained after the injected janitor tick")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCloseStopsDiagnosticCaptureJanitorAndIsConcurrentSafe(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	cfg.diagnosticCaptureJanitorTicks = make(chan time.Time)

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	janitorDone := s.diagnosticCaptureJanitorDone

	const callers = 8
	errs := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			ready.Done()
			<-start
			errs <- s.Close()
		}()
	}
	ready.Wait()
	close(start)

	deadline := time.After(time.Second)
	for range callers {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("concurrent Close: %v", err)
			}
		case <-deadline:
			t.Fatal("concurrent Close did not stop the Diagnostic capture janitor")
		}
	}
	select {
	case <-janitorDone:
	case <-time.After(time.Second):
		t.Fatal("Diagnostic capture janitor remained alive after Close")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
}
