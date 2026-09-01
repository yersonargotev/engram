package setup

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

func TestInstallCodexRerunNeitherEnablesNorRevokesPromptCaptureConsent(t *testing.T) {
	t.Run("fresh remains default off", func(t *testing.T) {
		resetSetupSeams(t)
		home := useTestHome(t)
		stubCaptureNeutralCodexSetup(t)

		for i := 0; i < 2; i++ {
			if _, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit}); err != nil {
				t.Fatalf("setup run %d: %v", i+1, err)
			}
		}
		dataDir := filepath.Join(home, ".engram")
		status, err := store.InspectCaptureConsentReadOnly(dataDir, "engram", store.CaptureContentTypePrompt, "", time.Now().UTC())
		if err != nil {
			t.Fatalf("inspect fresh consent: %v", err)
		}
		if status.SchemaPresent || status.Consent != nil {
			t.Fatalf("fresh setup created capture consent state: %#v", status)
		}
		if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
			t.Fatalf("setup created Engram data directory: %v", err)
		}
	})

	t.Run("explicit project grant survives rerun", func(t *testing.T) {
		resetSetupSeams(t)
		home := useTestHome(t)
		dataDir := filepath.Join(home, ".engram")
		s, err := store.New(store.FallbackConfig(dataDir))
		if err != nil {
			t.Fatalf("new store: %v", err)
		}
		now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
		if err := s.UpsertCaptureConsent(store.CaptureConsent{
			Project:       "engram",
			ContentType:   store.CaptureContentTypePrompt,
			RetentionDays: 14,
			UpdatedAt:     now,
		}); err != nil {
			t.Fatalf("seed explicit consent: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		stubCaptureNeutralCodexSetup(t)

		for i := 0; i < 2; i++ {
			if _, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit}); err != nil {
				t.Fatalf("setup run %d: %v", i+1, err)
			}
		}
		status, err := store.InspectCaptureConsentReadOnly(dataDir, "engram", store.CaptureContentTypePrompt, "", now)
		if err != nil {
			t.Fatalf("inspect preserved consent: %v", err)
		}
		if status.Consent == nil || status.Consent.RetentionDays != 14 || status.Consent.SessionID != "" {
			t.Fatalf("consent after setup rerun = %#v", status.Consent)
		}
	})
}

func stubCaptureNeutralCodexSetup(t *testing.T) {
	t.Helper()
	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(string, ...string) ([]byte, error) { return nil, nil }
}
