//go:build !windows

package plugin_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

func TestSubagentCaptureUnixManifestsDelegateToConsentBoundary(t *testing.T) {
	root := repoRoot(t)
	codexManifest := readCodexHooksManifest(t, filepath.Join(root, "plugin", "codex", "hooks", "hooks.json"))
	claudeManifest := readCodexHooksManifest(t, filepath.Join(root, "plugin", "claude-code", "hooks", "hooks.json"))
	codexCommand := singleCodexHookCommand(t, codexManifest, "SubagentStop")
	claudeCommand := singleCodexHookCommand(t, claudeManifest, "SubagentStop")
	if codexCommand != "engram capture subagent-hook --host=codex" {
		t.Fatalf("Codex SubagentStop command = %q, want direct core boundary", codexCommand)
	}
	if claudeCommand != "engram capture subagent-hook --host=claude-code" {
		t.Fatalf("Claude Code SubagentStop command = %q, want direct core boundary", claudeCommand)
	}

	binDir := t.TempDir()
	binary := filepath.Join(binDir, "engram")
	build := exec.Command("go", "build", "-o", binary, "./cmd/engram")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Engram CLI: %v\n%s", err, output)
	}
	dataDir := t.TempDir()
	env := append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"ENGRAM_DATA_DIR="+dataDir,
		"ENGRAM_PROJECT=engram",
	)

	defaultOutput := runUnixSubagentManifest(t, codexCommand,
		`{"turn_id":"turn-default","last_assistant_message":"ordinary transient draft"}`, env)
	assertEmptyHookJSON(t, defaultOutput)
	assertSubagentCaptureStoreCounts(t, dataDir, 0, 0, 0, 0)

	runEngramCLI(t, binary, env, "capture", "enable", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--json")
	assertEmptyHookJSON(t, runUnixSubagentManifest(t, codexCommand,
		`{"turn_id":"turn-prompt-only","last_assistant_message":"ordinary transient draft"}`, env))
	assertSubagentCaptureStoreCounts(t, dataDir, 0, 0, 0, 0)

	runEngramCLI(t, binary, env, "capture", "enable", "--project", "engram", "--type", store.CaptureContentTypeSubagentOutput, "--json")
	malformed := runUnixSubagentManifest(t, codexCommand,
		`{"turn_id":"turn-malformed","last_assistant_message":"ordinary transient draft","stdout":"{\"kind\":\"engram_diagnostic\",\"title\":\"fallback\",\"learning\":\"forbidden\"}"}`, env)
	var rejected map[string]any
	if err := json.Unmarshal([]byte(malformed), &rejected); err != nil || !strings.Contains(rejected["systemMessage"].(string), "rejected") {
		t.Fatalf("malformed envelope response = %q, err=%v", malformed, err)
	}
	assertSubagentCaptureStoreCounts(t, dataDir, 0, 0, 0, 0)
	invalidUTF8Input := append([]byte(`{"turn_id":"turn-invalid-utf8","last_assistant_message":"{\"kind\":\"engram_diagnostic\",\"title\":\"`), 0xff)
	invalidUTF8Input = append(invalidUTF8Input, []byte(`\",\"learning\":\"invalid manifest bytes must be rejected\"}"}`)...)
	invalidUTF8 := runUnixSubagentManifest(t, codexCommand, string(invalidUTF8Input), env)
	var invalidRejected map[string]any
	if err := json.Unmarshal([]byte(invalidUTF8), &invalidRejected); err != nil || !strings.Contains(invalidRejected["systemMessage"].(string), "rejected") {
		t.Fatalf("invalid UTF-8 response = %q, err=%v", invalidUTF8, err)
	}
	assertSubagentCaptureStoreCounts(t, dataDir, 0, 0, 0, 0)

	const codexEnvelope = `{"kind":"engram_diagnostic","title":"Unix manifest","learning":"The real manifest reaches the bounded core adapter.","evidence_ref":"plugin/subagent_capture_unix_test.go"}`
	assertEmptyHookJSON(t, runUnixSubagentManifest(t, codexCommand,
		`{"turn_id":"turn-consented","last_assistant_message":`+quoteJSON(t, codexEnvelope)+`}`, env))
	assertSubagentCaptureStoreCounts(t, dataDir, 1, 0, 0, 0)
	assertDefaultSubagentRetention(t, dataDir)

	runEngramCLI(t, binary, env, "capture", "disable", "--project", "engram", "--type", store.CaptureContentTypeSubagentOutput, "--json")
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	runEngramCLI(t, binary, env, "capture", "enable", "--project", "engram", "--type", store.CaptureContentTypeSubagentOutput,
		"--session-id", "claude-exact", "--expires-at", expiresAt.Format(time.RFC3339), "--json")
	const claudeEnvelope = `{"kind":"engram_diagnostic","title":"Claude manifest","learning":"Session grants do not widen to another session."}`
	assertEmptyHookJSON(t, runUnixSubagentManifest(t, claudeCommand,
		`{"session_id":"claude-other","last_assistant_message":`+quoteJSON(t, claudeEnvelope)+`}`, env))
	assertSubagentCaptureStoreCounts(t, dataDir, 1, 0, 0, 0)
	assertEmptyHookJSON(t, runUnixSubagentManifest(t, claudeCommand,
		`{"session_id":"claude-exact","last_assistant_message":`+quoteJSON(t, claudeEnvelope)+`}`, env))
	assertSubagentCaptureStoreCounts(t, dataDir, 2, 0, 0, 0)

	runEngramCLI(t, binary, env, "capture", "purge", "--project", "engram", "--type", store.CaptureContentTypeSubagentOutput, "--yes", "--json")
	assertSubagentCaptureStoreCounts(t, dataDir, 0, 0, 0, 0)
	s, err := store.New(store.FallbackConfig(dataDir))
	if err != nil {
		t.Fatalf("open capture store: %v", err)
	}
	expiredAt := time.Now().UTC().Add(-time.Minute)
	if err := s.UpsertCaptureConsent(store.CaptureConsent{
		Project: "engram", ContentType: store.CaptureContentTypeSubagentOutput,
		SessionID: "turn-expired", RetentionDays: store.DefaultDiagnosticRetentionDays,
		ExpiresAt: &expiredAt, UpdatedAt: expiredAt.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("seed expired consent: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close capture store: %v", err)
	}
	assertEmptyHookJSON(t, runUnixSubagentManifest(t, codexCommand,
		`{"turn_id":"turn-expired","last_assistant_message":`+quoteJSON(t, codexEnvelope)+`}`, env))
	statusOutput := runEngramCLI(t, binary, env, "capture", "status", "--project", "engram", "--type", store.CaptureContentTypeSubagentOutput,
		"--session-id", "turn-expired", "--json")
	var statusPayload map[string]any
	if err := json.Unmarshal([]byte(statusOutput), &statusPayload); err != nil || statusPayload["state"] != "expired" {
		t.Fatalf("expired status = %q, err=%v", statusOutput, err)
	}
	assertSubagentCaptureStoreCounts(t, dataDir, 0, 0, 0, 0)
}

func runUnixSubagentManifest(t *testing.T, command, input string, env []string) string {
	t.Helper()
	run := exec.Command(codexTestBash(t), "-c", command)
	run.Env = env
	run.Stdin = strings.NewReader(input)
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("run SubagentStop manifest command: %v\n%s", err, output)
	}
	return string(output)
}

func runEngramCLI(t *testing.T, binary string, env []string, args ...string) string {
	t.Helper()
	run := exec.Command(binary, args...)
	run.Env = env
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("run engram %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func assertEmptyHookJSON(t *testing.T, output string) {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		t.Fatalf("hook output %q is not valid JSON: %v", output, err)
	}
	if len(response) != 0 {
		t.Fatalf("hook output = %v, want empty object", response)
	}
}

func assertSubagentCaptureStoreCounts(t *testing.T, dataDir string, captures, observations, proposals, checkpoints int) {
	t.Helper()
	s, err := store.New(store.FallbackConfig(dataDir))
	if err != nil {
		t.Fatalf("open capture store: %v", err)
	}
	defer s.Close()
	for table, want := range map[string]int{
		"diagnostic_captures": captures,
		"observations":        observations,
		"memory_proposals":    proposals,
		"memory_checkpoints":  checkpoints,
		"sessions":            0,
	} {
		var got int
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if got != want {
			t.Fatalf("%s count = %d, want %d", table, got, want)
		}
	}
	var mutations int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM sync_mutations`).Scan(&mutations); err != nil {
		t.Fatalf("count sync mutations: %v", err)
	}
	if mutations != 0 {
		t.Fatalf("subagent capture created %d sync mutations", mutations)
	}
	var retiredEvaluationObjects int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name LIKE 'admission_%'`).Scan(&retiredEvaluationObjects); err != nil {
		t.Fatalf("count retired evaluation objects: %v", err)
	}
	if retiredEvaluationObjects != 0 {
		t.Fatalf("subagent capture created %d retired evaluation objects", retiredEvaluationObjects)
	}
}

func assertDefaultSubagentRetention(t *testing.T, dataDir string) {
	t.Helper()
	s, err := store.New(store.FallbackConfig(dataDir))
	if err != nil {
		t.Fatalf("open capture store: %v", err)
	}
	defer s.Close()
	var createdRaw, expiresRaw string
	if err := s.DB().QueryRow(`SELECT created_at, expires_at FROM diagnostic_captures ORDER BY id DESC LIMIT 1`).Scan(&createdRaw, &expiresRaw); err != nil {
		t.Fatalf("read capture retention: %v", err)
	}
	created, err := time.Parse(time.RFC3339Nano, createdRaw)
	if err != nil {
		t.Fatalf("parse created_at: %v", err)
	}
	expires, err := time.Parse(time.RFC3339Nano, expiresRaw)
	if err != nil {
		t.Fatalf("parse expires_at: %v", err)
	}
	if got := expires.Sub(created); got != 7*24*time.Hour {
		t.Fatalf("default retention = %s, want 168h", got)
	}
}
