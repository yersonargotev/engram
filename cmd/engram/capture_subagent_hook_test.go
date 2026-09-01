package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/recallbaseline"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCaptureSubagentHookDefaultsOffAndAcceptsOnlyConsentedEnvelope(t *testing.T) {
	cfg := testConfig(t)
	t.Setenv("ENGRAM_PROJECT", "engram")

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	service := memoryops.New(s)
	if _, err := service.EnableCapture(memoryops.CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
	}); err != nil {
		t.Fatalf("enable independent prompt capture: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	run := func(input string) map[string]any {
		t.Helper()
		stdout, stderr := captureOutput(t, func() {
			cmdCaptureSubagentHook(cfg, []string{"--host=codex"}, strings.NewReader(input))
		})
		if stderr != "" {
			t.Fatalf("hook stderr = %q", stderr)
		}
		var response map[string]any
		if err := json.Unmarshal([]byte(stdout), &response); err != nil {
			t.Fatalf("hook output %q is not valid JSON: %v", stdout, err)
		}
		return response
	}

	const raw = "ordinary subagent answer that must remain transient"
	if response := run(`{"turn_id":"turn-103","last_assistant_message":"` + raw + `"}`); len(response) != 0 {
		t.Fatalf("default-disabled hook response = %v, want empty object", response)
	}
	assertSubagentDiagnosticRowCount(t, cfg, 0)

	s, err = store.New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	service = memoryops.New(s)
	if _, err := service.EnableCapture(memoryops.CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
	}); err != nil {
		t.Fatalf("enable subagent capture: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if response := run(`{"turn_id":"turn-103","stdout":"{\"kind\":\"engram_diagnostic\",\"title\":\"Forbidden fallback\",\"learning\":\"stdout must be ignored\"}"}`); len(response) != 0 {
		t.Fatalf("stdout-only hook response = %v, want empty object", response)
	}
	assertSubagentDiagnosticRowCount(t, cfg, 0)

	malformed := run(`{"turn_id":"turn-103","last_assistant_message":"ordinary raw answer"}`)
	if !strings.Contains(fmt.Sprint(malformed["systemMessage"]), "rejected") {
		t.Fatalf("malformed hook response = %v", malformed)
	}
	assertSubagentDiagnosticRowCount(t, cfg, 0)

	invalidUTF8Input := append([]byte(`{"turn_id":"turn-103","last_assistant_message":"{\"kind\":\"engram_diagnostic\",\"title\":\"`), 0xff)
	invalidUTF8Input = append(invalidUTF8Input, []byte(`\",\"learning\":\"invalid hook bytes must be rejected\"}"}`)...)
	invalidUTF8 := run(string(invalidUTF8Input))
	if !strings.Contains(fmt.Sprint(invalidUTF8["systemMessage"]), "rejected") {
		t.Fatalf("invalid UTF-8 hook response = %v", invalidUTF8)
	}
	assertSubagentDiagnosticRowCount(t, cfg, 0)

	const envelope = `{"kind":"engram_diagnostic","title":"Validated boundary","learning":"Only this bounded diagnostic may be retained.","evidence_ref":"cmd/engram/capture_subagent_hook_test.go"}`
	encodedEnvelope, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if response := run(`{"turn_id":"turn-103","last_assistant_message":` + string(encodedEnvelope) + `}`); len(response) != 0 {
		t.Fatalf("consented hook response = %v, want empty object", response)
	}
	assertSubagentDiagnosticRowCount(t, cfg, 1)
}

func TestCaptureSubagentHookUsesHostScopedSessionIdentity(t *testing.T) {
	cfg := testConfig(t)
	t.Setenv("ENGRAM_PROJECT", "engram")
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	service := memoryops.New(s)
	if _, err := service.EnableCapture(memoryops.CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
		SessionID: "claude-session", ExpiresAt: pointerTime(time.Now().UTC().Add(time.Hour)),
	}); err != nil {
		t.Fatalf("enable session capture: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	const input = `{"session_id":"claude-session","last_assistant_message":"{\"kind\":\"engram_diagnostic\",\"title\":\"Claude boundary\",\"learning\":\"Session consent is exact.\"}"}`
	stdout, stderr := captureOutput(t, func() {
		cmdCaptureSubagentHook(cfg, []string{"--host=claude-code"}, strings.NewReader(input))
	})
	if stderr != "" || strings.TrimSpace(stdout) != "{}" {
		t.Fatalf("Claude hook stdout=%q stderr=%q", stdout, stderr)
	}
	assertSubagentDiagnosticRowCount(t, cfg, 1)
}

func TestCaptureSubagentHookRecordsOnlyContentFreeBaselineEvents(t *testing.T) {
	cfg := testConfig(t)
	t.Setenv("ENGRAM_PROJECT", "engram")
	t.Setenv("ENGRAM_RECALL_BASELINE", "1")
	const sentinel = "ASSISTANT-CONTENT-MUST-NOT-LEAK"

	stdout, stderr := captureOutput(t, func() {
		cmdCaptureSubagentHook(cfg, []string{"--host=codex"}, strings.NewReader(
			`{"turn_id":"turn-secret","last_assistant_message":"`+sentinel+`"}`,
		))
	})
	if stderr != "" || strings.TrimSpace(stdout) != "{}" {
		t.Fatalf("hook stdout=%q stderr=%q", stdout, stderr)
	}

	ledger, err := recallbaseline.Open(recallbaseline.Config{DataDir: cfg.DataDir})
	if err != nil {
		t.Fatalf("open baseline ledger: %v", err)
	}
	defer ledger.Close()
	report, err := ledger.Report(protocolcontract.CompatibilityReport{})
	if err != nil {
		t.Fatalf("baseline report: %v", err)
	}
	if report.Lifecycle.SubagentStop.Events != 1 || report.Lifecycle.SubagentStop.Observed != 1 ||
		report.Lifecycle.Capture.Events != 1 || report.Lifecycle.Capture.Disabled != 1 {
		t.Fatalf("subagent baseline = %+v", report.Lifecycle)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	for _, forbidden := range []string{sentinel, "turn-secret"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("baseline report leaked %q: %s", forbidden, raw)
		}
	}
}

func assertSubagentDiagnosticRowCount(t *testing.T, cfg store.Config, want int) {
	t.Helper()
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	var got int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM diagnostic_captures WHERE content_type = ?`, store.CaptureContentTypeSubagentOutput).Scan(&got); err != nil {
		t.Fatalf("count subagent captures: %v", err)
	}
	if got != want {
		t.Fatalf("subagent Diagnostic captures = %d, want %d", got, want)
	}
}

func pointerTime(value time.Time) *time.Time { return &value }
