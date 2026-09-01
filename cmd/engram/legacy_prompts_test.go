package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestLegacyPromptCLIInventoryAccessAndPrivateExport(t *testing.T) {
	cfg := testConfig(t)
	seedLegacyPromptCLI(t, cfg, "LEGACY-CLI-CANARY-102")

	withArgs(t, "engram", "legacy-prompts", "inventory", "--project", "Engram", "--json")
	stdout, stderr := captureOutput(t, func() { cmdLegacyPrompts(cfg) })
	if stderr != "" {
		t.Fatalf("inventory stderr=%q", stderr)
	}
	if strings.Contains(stdout, "LEGACY-CLI-CANARY-102") {
		t.Fatalf("inventory exposed prompt content: %s", stdout)
	}
	if got := decodeCLIJSON(t, stdout)["count"]; got != float64(1) {
		t.Fatalf("inventory count=%v", got)
	}

	withArgs(t, "engram", "legacy-prompts", "inventory", "--all", "--json")
	stdout, stderr = captureOutput(t, func() { cmdLegacyPrompts(cfg) })
	if stderr != "" || strings.Contains(stdout, "LEGACY-CLI-CANARY-102") {
		t.Fatalf("all inventory stdout=%q stderr=%q", stdout, stderr)
	}
	allInventory := decodeCLIJSON(t, stdout)
	if allInventory["all"] != true || allInventory["count"] != float64(1) {
		t.Fatalf("all inventory=%v", allInventory)
	}

	withArgs(t, "engram", "legacy-prompts", "access", "--project", "engram", "--limit", "1", "--json")
	stdout, stderr = captureOutput(t, func() { cmdLegacyPrompts(cfg) })
	if stderr != "" || !strings.Contains(stdout, "LEGACY-CLI-CANARY-102") {
		t.Fatalf("access stdout=%q stderr=%q", stdout, stderr)
	}

	output := filepath.Join(t.TempDir(), "legacy.json")
	withArgs(t, "engram", "legacy-prompts", "export", "--project", "engram", "--output", output, "--json")
	stdout, stderr = captureOutput(t, func() { cmdLegacyPrompts(cfg) })
	if stderr != "" || strings.Contains(stdout, "LEGACY-CLI-CANARY-102") {
		t.Fatalf("export stdout=%q stderr=%q", stdout, stderr)
	}
	info, err := os.Stat(output)
	if err != nil {
		t.Fatalf("stat export: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("export mode=%o, want 600", info.Mode().Perm())
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read export: %v", err)
	}
	var envelope legacyPromptExportEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode export: %v", err)
	}
	if envelope.SchemaVersion != 1 || len(envelope.Result.Prompts) != 1 || envelope.Result.Prompts[0].Content != "LEGACY-CLI-CANARY-102" {
		t.Fatalf("export envelope=%+v", envelope)
	}
}

func TestLegacyPromptCLIPurgeRequiresSeparateConfirmation(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	seedLegacyPromptCLI(t, cfg, "LEGACY-PURGE-CANARY-102")

	withArgs(t, "engram", "legacy-prompts", "purge", "--project", "engram", "--json")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdLegacyPrompts(cfg) })
	if recovered == nil || !strings.Contains(stderr, `"code":"confirmation_required"`) || !strings.Contains(stderr, `"count":1`) {
		t.Fatalf("purge without confirmation panic=%v stderr=%q", recovered, stderr)
	}
	assertLegacyPromptCLICount(t, cfg, 1)

	withArgs(t, "engram", "legacy-prompts", "purge", "--project", "engram", "--yes", "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdLegacyPrompts(cfg) })
	if recovered != nil || stderr != "" || decodeCLIJSON(t, stdout)["deleted"] != float64(1) {
		t.Fatalf("confirmed purge stdout=%q stderr=%q panic=%v", stdout, stderr, recovered)
	}
	assertLegacyPromptCLICount(t, cfg, 0)

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open Store: %v", err)
	}
	defer s.Close()
	var tombstones, mutations int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prompt_tombstones`).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM sync_mutations WHERE entity = 'prompt'`).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if tombstones != 0 || mutations != 0 {
		t.Fatalf("purge produced remote evidence: tombstones=%d mutations=%d", tombstones, mutations)
	}
}

func seedLegacyPromptCLI(t *testing.T, cfg store.Config, content string) {
	t.Helper()
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open Store: %v", err)
	}
	defer s.Close()
	if _, err := s.DB().Exec(`INSERT INTO sessions (id, project, directory) VALUES ('legacy-cli-session', 'engram', '/synthetic')`); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := s.DB().Exec(`INSERT INTO user_prompts (session_id, content, project) VALUES ('legacy-cli-session', ?, 'engram')`, content); err != nil {
		t.Fatalf("seed Legacy prompt: %v", err)
	}
}

func assertLegacyPromptCLICount(t *testing.T, cfg store.Config, want int) {
	t.Helper()
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open Store: %v", err)
	}
	defer s.Close()
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM user_prompts`).Scan(&count); err != nil {
		t.Fatalf("count Legacy prompts: %v", err)
	}
	if count != want {
		t.Fatalf("Legacy prompt count=%d, want %d", count, want)
	}
}
