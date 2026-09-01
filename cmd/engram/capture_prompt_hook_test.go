package main

import (
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

func TestCapturePromptHookAlwaysForwardsExactOpaqueIdentityAndKeepsCaptureDefaultOff(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	useSynchronousPromptPersistence(t, cfg)
	t.Setenv("ENGRAM_PROJECT", "engram")
	seedPromptHookSession(t, cfg, "session:opaque/value")
	input := `{"session_id":"session:opaque/value","turn_id":"turn:opaque/value","cwd":"/work/engram","prompt":"PRIVATE-PROMPT-MUST-STAY-DIAGNOSTIC"}`

	firstOut, firstErr := captureOutput(t, func() {
		cmdCapturePromptHook(cfg, []string{"--host=codex"}, strings.NewReader(input))
	})
	secondOut, secondErr := captureOutput(t, func() {
		cmdCapturePromptHook(cfg, []string{"--host=codex"}, strings.NewReader(input))
	})
	if firstErr != "" || secondErr != "" || firstOut != secondOut {
		t.Fatalf("prompt identity changed: first=%q/%q second=%q/%q", firstOut, firstErr, secondOut, secondErr)
	}

	var response struct {
		HookSpecificOutput struct {
			HookEventName     string `json:"hookEventName"`
			AdditionalContext string `json:"additionalContext"`
		} `json:"hookSpecificOutput"`
	}
	if err := json.Unmarshal([]byte(firstOut), &response); err != nil {
		t.Fatalf("decode prompt hook response: %v\n%s", err, firstOut)
	}
	wantIdentity := `{"host":"codex","session_id":"session:opaque/value","root_turn_id":"turn:opaque/value"}`
	if response.HookSpecificOutput.HookEventName != "UserPromptSubmit" || !strings.Contains(response.HookSpecificOutput.AdditionalContext, wantIdentity) {
		t.Fatalf("prompt hook lost exact identity: %#v", response)
	}
	for _, forbidden := range []string{"PRIVATE-PROMPT-MUST-STAY-DIAGNOSTIC", "checkpoint-cue", "disposition", "no_durable_knowledge"} {
		if strings.Contains(firstOut, forbidden) {
			t.Fatalf("prompt hook output leaked or invented %q: %s", forbidden, firstOut)
		}
	}
	assertPromptHookPersistence(t, cfg, 0)
}

func TestCapturePromptHookPersistsOnlySeparatelyConsentedDiagnosticPrompt(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	useSynchronousPromptPersistence(t, cfg)
	t.Setenv("ENGRAM_PROJECT", "engram")
	seedPromptHookSession(t, cfg, "consented-session")
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open prompt hook store: %v", err)
	}
	if err := s.UpsertCaptureConsent(store.CaptureConsent{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
		RetentionDays: store.DefaultDiagnosticRetentionDays, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("enable prompt capture: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close prompt hook store: %v", err)
	}

	input := `{"session_id":"consented-session","turn_id":"root-turn","cwd":"/work/engram","prompt":"CONSENTED-DIAGNOSTIC-PROMPT"}`
	stdout, stderr := captureOutput(t, func() {
		cmdCapturePromptHook(cfg, []string{"--host=codex"}, strings.NewReader(input))
	})
	if stderr != "" || !strings.Contains(stdout, `"root_turn_id\":\"root-turn`) {
		t.Fatalf("consented prompt hook response stdout=%q stderr=%q", stdout, stderr)
	}
	assertPromptHookPersistence(t, cfg, 1)
}

func TestCapturePromptHookDoesNotApplyConsentGrantedAfterEventObservation(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	useSynchronousPromptPersistence(t, cfg)
	t.Setenv("ENGRAM_PROJECT", "engram")
	seedPromptHookSession(t, cfg, "late-consent-session")

	input := &callbackReader{
		Reader: strings.NewReader(`{"session_id":"late-consent-session","turn_id":"root-turn","cwd":"/work/engram","prompt":"MUST-NOT-BE-RETROACTIVE"}`),
		callback: func() {
			time.Sleep(2 * time.Millisecond)
			s, err := store.New(cfg)
			if err != nil {
				t.Fatalf("open late-consent store: %v", err)
			}
			defer s.Close()
			if err := s.UpsertCaptureConsent(store.CaptureConsent{
				Project: "engram", ContentType: store.CaptureContentTypePrompt,
				RetentionDays: store.DefaultDiagnosticRetentionDays, UpdatedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("grant late prompt consent: %v", err)
			}
		},
	}
	stdout, stderr := captureOutput(t, func() {
		cmdCapturePromptHook(cfg, []string{"--host=codex"}, input)
	})
	if stderr != "" || !strings.Contains(stdout, `"root_turn_id\":\"root-turn`) {
		t.Fatalf("late-consent prompt hook response stdout=%q stderr=%q", stdout, stderr)
	}
	assertPromptHookPersistence(t, cfg, 0)
}

func TestCapturePromptHookInvalidInputNeverInventsIdentity(t *testing.T) {
	stubRuntimeHooks(t)
	cfg := store.FallbackConfig(t.TempDir())
	for _, input := range []string{
		"{",
		`{}`,
		`{"session_id":42,"turn_id":"turn","prompt":"hello"}`,
		`{"session_id":"session","turn_id":42,"prompt":"hello"}`,
		`{"session_id":"session","prompt":"hello"}`,
		`{"turn_id":"turn","prompt":"hello"}`,
		`{"session_id":"` + strings.Repeat("s", maxPromptHookAdditionalContextBytes) + `","turn_id":"turn","prompt":"hello"}`,
	} {
		stdout, _ := captureOutput(t, func() {
			cmdCapturePromptHook(cfg, []string{"--host=codex"}, strings.NewReader(input))
		})
		if strings.TrimSpace(stdout) != "{}" {
			t.Errorf("invalid input %q output=%q, want {}", input, stdout)
		}
	}
}

func seedPromptHookSession(t *testing.T, cfg store.Config, sessionID string) {
	t.Helper()
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open prompt hook seed store: %v", err)
	}
	if err := s.CreateSession(sessionID, "engram", "/work/engram"); err != nil {
		t.Fatalf("seed prompt hook session: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close prompt hook seed store: %v", err)
	}
}

func assertPromptHookPersistence(t *testing.T, cfg store.Config, captures int) {
	t.Helper()
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("inspect prompt hook store: %v", err)
	}
	defer s.Close()
	for table, want := range map[string]int{
		"diagnostic_captures": captures,
		"observations":        0,
		"memory_proposals":    0,
		"memory_checkpoints":  0,
	} {
		var got int
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != want {
			t.Errorf("%s count=%d err=%v, want %d", table, got, err, want)
		}
	}
}

func useSynchronousPromptPersistence(t *testing.T, cfg store.Config) {
	t.Helper()
	previous := startPromptCapturePersistence
	t.Cleanup(func() { startPromptCapturePersistence = previous })
	startPromptCapturePersistence = func(raw []byte) {
		cmdCapturePromptPersist(cfg, []string{"--host=codex"}, strings.NewReader(string(raw)))
	}
}

type callbackReader struct {
	io.Reader
	once     sync.Once
	callback func()
}

func (r *callbackReader) Read(p []byte) (int, error) {
	r.once.Do(r.callback)
	return r.Reader.Read(p)
}
