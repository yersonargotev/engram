package obsidian

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

func TestObsidianExportExcludesLegacyAndDiagnosticCaptureContent(t *testing.T) {
	cfg, err := store.DefaultConfig()
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	cfg.DataDir = t.TempDir()
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.DB().Exec(`
		INSERT INTO sessions (id, project, directory) VALUES ('capture-exclusion-session', 'engram', '/synthetic');
		INSERT INTO user_prompts (session_id, content, project) VALUES ('capture-exclusion-session', 'LEGACY-OBSIDIAN-CANARY-102', 'engram');
	`); err != nil {
		t.Fatalf("seed Legacy prompt: %v", err)
	}
	now := time.Now().UTC()
	if err := s.UpsertCaptureConsent(store.CaptureConsent{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
		RetentionDays: store.DefaultDiagnosticRetentionDays, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("enable Diagnostic capture: %v", err)
	}
	result, err := s.CaptureDiagnostic(store.CaptureDiagnosticParams{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
		SessionID: "capture-exclusion-session", Content: "DIAGNOSTIC-OBSIDIAN-CANARY-102", Now: now,
	})
	if err != nil || !result.Captured {
		t.Fatalf("capture Diagnostic content: result=%#v err=%v", result, err)
	}

	vault := t.TempDir()
	if _, err := NewExporter(checkpointStoreReader{store: s}, ExportConfig{
		VaultPath: vault, Force: true, GraphConfig: GraphConfigSkip,
	}).Export(); err != nil {
		t.Fatalf("export to Obsidian: %v", err)
	}

	if err := filepath.WalkDir(vault, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(content)
		if strings.Contains(text, "LEGACY-OBSIDIAN-CANARY-102") || strings.Contains(text, "DIAGNOSTIC-OBSIDIAN-CANARY-102") {
			t.Fatalf("Obsidian file %s exposed captured content: %s", path, text)
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect Obsidian vault: %v", err)
	}
}
