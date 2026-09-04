package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/codexlifecycle"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCodexLifecycleCanaryEmitsCueOnlyAndRegistersEveryNonCompactSource(t *testing.T) {
	stubRuntimeHooks(t)
	dataDir := t.TempDir()
	cfg := store.FallbackConfig(dataDir)
	pluginRoot := writeLifecycleTestPlugin(t)
	t.Setenv("ENGRAM_PROJECT", "engram")
	t.Setenv(codexlifecycle.EnvTreatment, "targeted-recall")
	previousStartImport := startLifecycleImport
	t.Cleanup(func() { startLifecycleImport = previousStartImport })
	startLifecycleImport = func(string, string) { t.Fatal("canary must not auto-import broad project context") }

	for _, source := range []string{"startup", "resume", "clear"} {
		t.Run(source, func(t *testing.T) {
			input := `{"session_id":"session-` + source + `","cwd":` + quoteLifecycleJSON(t, t.TempDir()) + `,"source":"` + source + `"}`
			stdout, stderr := captureOutput(t, func() {
				cmdLifecycleSessionStart(cfg, []string{"--host=codex", "--plugin-root=" + pluginRoot}, strings.NewReader(input))
			})
			if stderr != "" {
				t.Fatalf("stderr = %q", stderr)
			}
			response := decodeLifecycleResponse(t, stdout)
			if response.HookSpecificOutput.AdditionalContext != "canonical cue" {
				t.Fatalf("additionalContext = %q, want cue only", response.HookSpecificOutput.AdditionalContext)
			}
		})
	}

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open lifecycle store: %v", err)
	}
	defer s.Close()
	for _, source := range []string{"startup", "resume", "clear"} {
		if _, err := s.GetSession("session-" + source); err != nil {
			t.Errorf("%s did not register its exact session: %v", source, err)
		}
	}
	for _, table := range []string{"observations", "memory_proposals", "memory_checkpoints", "diagnostic_captures"} {
		var count int
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Errorf("%s count=%d err=%v, want zero", table, count, err)
		}
	}
}

func TestCodexLifecycleDefaultPreservesBoundedBroadContextUntilCanaryIsSelected(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	pluginRoot := writeLifecycleTestPlugin(t)
	t.Setenv("ENGRAM_PROJECT", "engram")
	t.Setenv(codexlifecycle.EnvTreatment, "")
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open broad-context store: %v", err)
	}
	if err := s.CreateSession("prior-session", "engram", "/work/prior"); err != nil {
		t.Fatalf("seed prior session: %v", err)
	}
	if _, err := s.AddObservation(store.AddObservationParams{
		SessionID: "prior-session", Project: "engram", Type: "decision", Title: "Legacy broad sentinel", Content: "preserved until GA selection",
	}); err != nil {
		t.Fatalf("seed broad context: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close broad-context store: %v", err)
	}
	previousStartImport := startLifecycleImport
	t.Cleanup(func() { startLifecycleImport = previousStartImport })
	imports := 0
	startLifecycleImport = func(cwd, project string) {
		imports++
		if cwd != "/work/current" || project != "engram" {
			t.Errorf("legacy import scope = %q/%q", cwd, project)
		}
	}
	stdout, _ := captureOutput(t, func() {
		cmdLifecycleSessionStart(cfg, []string{"--host=codex", "--plugin-root=" + pluginRoot},
			strings.NewReader(`{"session_id":"current-session","cwd":"/work/current","source":"startup"}`))
	})
	context := decodeLifecycleResponse(t, stdout).HookSpecificOutput.AdditionalContext
	if !strings.Contains(context, "Legacy broad sentinel") || imports != 1 {
		t.Fatalf("default context/imports = %q / %d, want preserved broad treatment", context, imports)
	}
	if len(context) > codexlifecycle.MaxInjectedUTF8Bytes {
		t.Fatalf("default context bytes=%d, limit=%d", len(context), codexlifecycle.MaxInjectedUTF8Bytes)
	}
}

func TestCodexLifecycleCompactHasOneCueOnlyPathAndOneDeclaredExactSessionVariant(t *testing.T) {
	stubRuntimeHooks(t)
	dataDir := t.TempDir()
	cfg := store.FallbackConfig(dataDir)
	pluginRoot := writeLifecycleTestPlugin(t)
	t.Setenv("ENGRAM_PROJECT", "engram")

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open lifecycle store: %v", err)
	}
	if err := s.CreateSession("original-session", "engram", "/work/original"); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := s.AddObservation(store.AddObservationParams{
		SessionID: "original-session", Project: "engram", Type: "decision", Title: "Exact session sentinel", Content: strings.Repeat("exact-session-only ", 600),
	}); err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if err := s.CreateSession("foreign-session", "engram", "/work/foreign"); err != nil {
		t.Fatalf("seed foreign session: %v", err)
	}
	if _, err := s.AddObservation(store.AddObservationParams{
		SessionID: "foreign-session", Project: "engram", Type: "decision", Title: "Foreign sentinel", Content: "must never enter exact recovery",
	}); err != nil {
		t.Fatalf("seed foreign observation: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	input := `{"session_id":"original-session","cwd":"/work/original","source":"compact","root_turn_id":"opaque-root-must-not-be-replaced"}`
	t.Setenv(codexlifecycle.EnvTreatment, "targeted-recall")
	stdout, _ := captureOutput(t, func() {
		cmdLifecycleSessionStart(cfg, []string{"--host=codex", "--plugin-root=" + pluginRoot}, strings.NewReader(input))
	})
	if got := decodeLifecycleResponse(t, stdout).HookSpecificOutput.AdditionalContext; got != "canonical cue" {
		t.Fatalf("targeted Recall compact context = %q, want cue only", got)
	}

	t.Setenv(codexlifecycle.EnvTreatment, "targeted-recall-exact-session")
	stdout, _ = captureOutput(t, func() {
		cmdLifecycleSessionStart(cfg, []string{"--host=codex", "--plugin-root=" + pluginRoot}, strings.NewReader(input))
	})
	context := decodeLifecycleResponse(t, stdout).HookSpecificOutput.AdditionalContext
	if !strings.Contains(context, "Exact session sentinel") || strings.Contains(context, "Foreign sentinel") {
		t.Fatalf("exact-session compact context crossed scope:\n%s", context)
	}
	if len(context) > codexlifecycle.MaxInjectedUTF8Bytes {
		t.Fatalf("exact-session compact context bytes=%d, limit=%d", len(context), codexlifecycle.MaxInjectedUTF8Bytes)
	}
	if strings.Contains(context, "opaque-root-must-not-be-replaced") {
		t.Fatalf("compact path invented or reinjected root identity: %s", context)
	}
}

func TestCodexLifecycleInvalidInputNeverInventsSessionIdentity(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	pluginRoot := writeLifecycleTestPlugin(t)
	for _, input := range []string{"{", `{}`, `{"session_id":42,"cwd":"/tmp","source":"startup"}`, `{"session_id":"session","cwd":"/tmp","source":"unknown"}`} {
		stdout, _ := captureOutput(t, func() {
			cmdLifecycleSessionStart(cfg, []string{"--host=codex", "--plugin-root=" + pluginRoot}, strings.NewReader(input))
		})
		if strings.TrimSpace(stdout) != "{}" {
			t.Errorf("invalid input %q output = %q, want {}", input, stdout)
		}
	}
}

func TestCodexLifecyclePreservesNonblankOpaqueSessionIdentity(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	pluginRoot := writeLifecycleTestPlugin(t)
	t.Setenv("ENGRAM_PROJECT", "engram")
	t.Setenv(codexlifecycle.EnvTreatment, "targeted-recall")
	const sessionID = " session:opaque/value "
	stdout, _ := captureOutput(t, func() {
		cmdLifecycleSessionStart(cfg, []string{"--host=codex", "--plugin-root=" + pluginRoot},
			strings.NewReader(`{"session_id":" session:opaque/value ","cwd":"/work/engram","source":"startup"}`))
	})
	decodeLifecycleResponse(t, stdout)
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open exact identity store: %v", err)
	}
	defer s.Close()
	session, err := s.GetSession(sessionID)
	if err != nil || session.ID != sessionID {
		t.Fatalf("persisted session = %+v err=%v, want exact %q", session, err, sessionID)
	}
}

func TestCodexLifecycleSessionEndPreservesOpaqueIdentityAndOnlyClosesExistingState(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	const sessionID = " session:end/opaque "
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open SessionEnd seed store: %v", err)
	}
	if err := s.CreateSession(sessionID, "engram", "/work/engram"); err != nil {
		t.Fatalf("seed SessionEnd session: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close SessionEnd seed store: %v", err)
	}

	stdout, stderr := captureOutput(t, func() {
		cmdLifecycleSessionEnd(cfg, []string{"--host=codex"}, strings.NewReader(`{"session_id":" session:end/opaque ","reason":"user"}`))
	})
	if stdout != "" || stderr != "" {
		t.Fatalf("SessionEnd output stdout=%q stderr=%q, want silent", stdout, stderr)
	}
	s, err = store.New(cfg)
	if err != nil {
		t.Fatalf("reopen SessionEnd store: %v", err)
	}
	defer s.Close()
	session, err := s.GetSession(sessionID)
	if err != nil || session.EndedAt == nil || session.Summary != nil {
		t.Fatalf("SessionEnd session = %+v err=%v, want exact session ended without summary", session, err)
	}
}

func TestCodexLifecycleSessionEndInvalidOrUnknownInputCreatesNoStore(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	for _, input := range []string{
		"{",
		`{}`,
		`{"session_id":42}`,
		`{"session_id":""}`,
		`{"session_id":"unknown-session"}`,
	} {
		stdout, stderr := captureOutput(t, func() {
			cmdLifecycleSessionEnd(cfg, []string{"--host=codex"}, strings.NewReader(input))
		})
		if stdout != "" || stderr != "" {
			t.Errorf("SessionEnd input %q output stdout=%q stderr=%q, want silent", input, stdout, stderr)
		}
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "engram.db")); !os.IsNotExist(err) {
		t.Fatalf("SessionEnd without existing state created a store: %v", err)
	}
}

type lifecycleHookResponse struct {
	HookSpecificOutput struct {
		HookEventName     string `json:"hookEventName"`
		AdditionalContext string `json:"additionalContext"`
	} `json:"hookSpecificOutput"`
}

func decodeLifecycleResponse(t *testing.T, raw string) lifecycleHookResponse {
	t.Helper()
	var response lifecycleHookResponse
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatalf("decode lifecycle response: %v\n%s", err, raw)
	}
	if response.HookSpecificOutput.HookEventName != "SessionStart" {
		t.Fatalf("hookEventName = %q", response.HookSpecificOutput.HookEventName)
	}
	return response
}

func TestCursorLifecycleSessionStartDeliversCanonicalCue(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	pluginRoot := writeCursorLifecycleTestPlugin(t)

	input := `{
  "conversation_id": "conv-cursor-start",
  "session_id": "conv-cursor-start",
  "generation_id": "gen-cursor-start",
  "hook_event_name": "sessionStart",
  "workspace_roots": [` + quoteLifecycleJSON(t, t.TempDir()) + `],
  "composer_mode": "agent"
}`
	stdout, stderr := captureOutput(t, func() {
		cmdLifecycleSessionStart(cfg, []string{"--host=cursor", "--plugin-root=" + pluginRoot}, strings.NewReader(input))
	})
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	var response map[string]any
	if err := json.Unmarshal([]byte(stdout), &response); err != nil {
		t.Fatalf("decode Cursor sessionStart response: %v\n%s", err, stdout)
	}
	if got, _ := response["additional_context"].(string); got != "canonical cue" {
		t.Fatalf("additional_context = %#v, want canonical cue", response["additional_context"])
	}
	if _, ok := response["hookSpecificOutput"]; ok {
		t.Fatalf("Cursor sessionStart used Codex hookSpecificOutput: %s", stdout)
	}

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open Cursor lifecycle store: %v", err)
	}
	defer s.Close()
	if _, err := s.GetSession("conv-cursor-start"); err == nil {
		t.Fatal("Cursor sessionStart registered a session; cue delivery must not own durability")
	}
	for _, table := range []string{"observations", "memory_proposals", "memory_checkpoints", "diagnostic_captures"} {
		var count int
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Errorf("%s count=%d err=%v, want zero", table, count, err)
		}
	}
}

func TestCursorLifecycleSessionStartInvalidInputReturnsEmptyObject(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	pluginRoot := writeCursorLifecycleTestPlugin(t)
	stdout, stderr := captureOutput(t, func() {
		cmdLifecycleSessionStart(cfg, []string{"--host=cursor", "--plugin-root=" + pluginRoot}, strings.NewReader(`{`))
	})
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	if strings.TrimSpace(stdout) != "{}" {
		t.Fatalf("invalid Cursor sessionStart stdout = %q, want empty object", stdout)
	}
}

func writeCursorLifecycleTestPlugin(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "skills", "engram-memory", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create Cursor lifecycle test plugin: %v", err)
	}
	content := "before\n<!-- engram:checkpoint-cue:start -->\ncanonical cue\n<!-- engram:checkpoint-cue:end -->\nafter\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write Cursor lifecycle test skill: %v", err)
	}
	return root
}

func writeLifecycleTestPlugin(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "skills", "memory", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create lifecycle test plugin: %v", err)
	}
	content := "before\n<!-- engram:checkpoint-cue:start -->\ncanonical cue\n<!-- engram:checkpoint-cue:end -->\nafter\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write lifecycle test skill: %v", err)
	}
	return root
}

func quoteLifecycleJSON(t *testing.T, value string) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("quote lifecycle JSON: %v", err)
	}
	return string(raw)
}
