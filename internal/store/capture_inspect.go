package store

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// CaptureConsentInspection is a content-free snapshot of one capture-consent
// scope. SchemaPresent is false for stores that have not yet run the Content
// capture migration; that state is equivalent to default-off consent.
type CaptureConsentInspection struct {
	SchemaPresent bool
	Consent       *CaptureConsent
}

// CaptureConsentAggregateInspection reports project-wide capture availability
// without returning captured content or opaque session identities.
type CaptureConsentAggregateInspection struct {
	SchemaPresent bool
	Consent       *CaptureConsent
	SessionScoped bool
	Expired       bool
}

// InspectCaptureConsentReadOnly inspects an existing local store without
// creating its directory, running migrations, purging expired captures, or
// reading Diagnostic capture content.
func InspectCaptureConsentReadOnly(dataDir, project, contentType, sessionID string, now time.Time) (*CaptureConsentInspection, error) {
	db, schemaPresent, closeInspection, err := openCaptureConsentInspection(dataDir)
	if err != nil {
		return nil, err
	}
	defer closeInspection()
	if !schemaPresent {
		return &CaptureConsentInspection{}, nil
	}

	project, _ = NormalizeProject(project)
	if now.IsZero() {
		now = time.Now().UTC()
	}
	inspector := &Store{db: db}
	consent, err := inspector.effectiveCaptureConsent(db, project, contentType, sessionID, now.UTC())
	if err != nil {
		return nil, fmt.Errorf("inspect capture consent: %w", err)
	}
	return &CaptureConsentInspection{SchemaPresent: true, Consent: consent}, nil
}

// InspectCaptureConsentAggregateReadOnly distinguishes default-off, active,
// and expired project/type consent without exposing session identifiers.
func InspectCaptureConsentAggregateReadOnly(dataDir, project, contentType string, now time.Time) (*CaptureConsentAggregateInspection, error) {
	db, schemaPresent, closeInspection, err := openCaptureConsentInspection(dataDir)
	if err != nil {
		return nil, err
	}
	defer closeInspection()
	if !schemaPresent {
		return &CaptureConsentAggregateInspection{}, nil
	}
	project, _ = NormalizeProject(project)
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	inspector := &Store{db: db}
	consent, err := inspector.effectiveCaptureConsent(db, project, contentType, "", now)
	if err != nil {
		return nil, fmt.Errorf("inspect project capture consent: %w", err)
	}
	result := &CaptureConsentAggregateInspection{SchemaPresent: true, Consent: consent}
	if consent != nil {
		return result, nil
	}

	var sessionID string
	err = db.QueryRow(`
		SELECT session_id
		FROM capture_consents
		WHERE project = ? AND content_type = ? AND session_id <> '' AND expires_at > ?
		ORDER BY updated_at DESC
		LIMIT 1`, project, contentType, now.Format(time.RFC3339Nano)).Scan(&sessionID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("inspect session capture consent: %w", err)
	}
	if err == nil {
		consent, err = inspector.effectiveCaptureConsent(db, project, contentType, sessionID, now)
		if err != nil {
			return nil, fmt.Errorf("inspect session capture consent: %w", err)
		}
		if consent != nil {
			consent.SessionID = ""
			result.Consent = consent
			result.SessionScoped = true
			return result, nil
		}
	}

	if err := db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM capture_consents
			WHERE project = ? AND content_type = ? AND session_id <> ''
			  AND expires_at IS NOT NULL AND expires_at <= ?
		)`, project, contentType, now.Format(time.RFC3339Nano)).Scan(&result.Expired); err != nil {
		return nil, fmt.Errorf("inspect expired capture consent: %w", err)
	}
	return result, nil
}

func openCaptureConsentInspection(dataDir string) (*sql.DB, bool, func(), error) {
	if !filepath.IsAbs(dataDir) {
		return nil, false, func() {}, fmt.Errorf("engram: data directory must be an absolute path, got %q", dataDir)
	}
	dbPath := filepath.Join(dataDir, "engram.db")
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, func() {}, nil
		}
		return nil, false, func() {}, fmt.Errorf("inspect capture store: %w", err)
	}

	inspectionPath := dbPath
	openQuery := "immutable=1&mode=ro"
	cleanup := func() {}
	if wal, walErr := os.Stat(dbPath + "-wal"); walErr == nil && wal.Size() > 0 {
		// An active writer may hold the newest committed grant in WAL. In that
		// case immutable mode would ignore current state, while opening the
		// source normally can update its shared-memory sidecar. Inspect a
		// private copy so SQLite can consume WAL without touching user files.
		var copyErr error
		inspectionPath, cleanup, copyErr = copyCaptureStoreForInspection(dbPath)
		if copyErr != nil {
			return nil, false, func() {}, copyErr
		}
		openQuery = "mode=rw"
	}
	dsn := (&url.URL{
		Scheme:   "file",
		Path:     filepath.ToSlash(inspectionPath),
		RawQuery: openQuery,
	}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		cleanup()
		return nil, false, func() {}, fmt.Errorf("open capture store read-only: %w", err)
	}
	closeInspection := func() {
		_ = db.Close()
		cleanup()
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA query_only = ON"); err != nil {
		closeInspection()
		return nil, false, func() {}, fmt.Errorf("protect capture inspection connection: %w", err)
	}

	var schemaPresent int
	err = db.QueryRow(`
		SELECT 1
		FROM sqlite_schema
		WHERE type = 'table' AND name = 'capture_consents'
		LIMIT 1`).Scan(&schemaPresent)
	if errors.Is(err, sql.ErrNoRows) {
		return db, false, closeInspection, nil
	}
	if err != nil {
		closeInspection()
		return nil, false, func() {}, fmt.Errorf("inspect capture consent schema: %w", err)
	}
	return db, true, closeInspection, nil
}

func copyCaptureStoreForInspection(dbPath string) (string, func(), error) {
	tempDir, err := os.MkdirTemp("", "engram-capture-inspect-")
	if err != nil {
		return "", nil, fmt.Errorf("create capture inspection directory: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tempDir) }
	tempDBPath := filepath.Join(tempDir, "engram.db")
	for _, suffix := range []string{"", "-wal"} {
		if err := copyCaptureInspectionFile(dbPath+suffix, tempDBPath+suffix); err != nil {
			cleanup()
			return "", nil, err
		}
	}
	return tempDBPath, cleanup, nil
}

func copyCaptureInspectionFile(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open capture inspection source: %w", err)
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create capture inspection copy: %w", err)
	}
	succeeded := false
	defer func() {
		_ = out.Close()
		if !succeeded {
			_ = os.Remove(destination)
		}
	}()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy capture inspection state: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close capture inspection copy: %w", err)
	}
	succeeded = true
	return nil
}
