//go:build windows

package plugin_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

func TestCodexWindowsSubagentCaptureManifestRuntime(t *testing.T) {
	root := repoRoot(t)
	manifest := readCodexHooksManifest(t, filepath.Join(root, "plugin", "codex", "hooks", "hooks.json"))
	command := codexWindowsSubagentCommand(t, manifest)
	if command != "engram capture subagent-hook --host=codex" || strings.Contains(command, `"`) {
		t.Fatalf("Windows SubagentStop command must be quote-free and delegate directly to core: %q", command)
	}

	binDir := t.TempDir()
	binary := filepath.Join(binDir, "engram.exe")
	build := exec.Command("go", "build", "-o", binary, "./cmd/engram")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Engram CLI: %v\n%s", err, output)
	}
	dataDir := t.TempDir()
	env := cleanWindowsSubagentEnv(binDir, dataDir)

	defaultOutput := runWindowsSubagentManifest(t, command,
		`{"turn_id":"turn-default","last_assistant_message":"ordinary transient draft"}`, env)
	assertWindowsEmptyHookJSON(t, defaultOutput)
	assertWindowsSubagentStoreCounts(t, dataDir, 0)

	runWindowsEngramCLI(t, binary, env, "capture", "enable", "--project", "engram", "--type", store.CaptureContentTypeSubagentOutput, "--json")
	malformed := runWindowsSubagentManifest(t, command,
		`{"turn_id":"turn-malformed","last_assistant_message":"ordinary transient draft","stdout":"{\"kind\":\"engram_diagnostic\",\"title\":\"fallback\",\"learning\":\"forbidden\"}"}`, env)
	var rejected map[string]any
	if err := json.Unmarshal([]byte(malformed), &rejected); err != nil || !strings.Contains(rejected["systemMessage"].(string), "rejected") {
		t.Fatalf("malformed envelope response = %q, err=%v", malformed, err)
	}
	assertWindowsSubagentStoreCounts(t, dataDir, 0)
	invalidUTF8Input := append([]byte(`{"turn_id":"turn-invalid-utf8","last_assistant_message":"{\"kind\":\"engram_diagnostic\",\"title\":\"`), 0xff)
	invalidUTF8Input = append(invalidUTF8Input, []byte(`\",\"learning\":\"invalid manifest bytes must be rejected\"}"}`)...)
	invalidUTF8 := runWindowsSubagentManifest(t, command, string(invalidUTF8Input), env)
	var invalidRejected map[string]any
	if err := json.Unmarshal([]byte(invalidUTF8), &invalidRejected); err != nil || !strings.Contains(invalidRejected["systemMessage"].(string), "rejected") {
		t.Fatalf("invalid UTF-8 response = %q, err=%v", invalidUTF8, err)
	}
	assertWindowsSubagentStoreCounts(t, dataDir, 0)

	const envelope = `{"kind":"engram_diagnostic","title":"Windows manifest","learning":"The cmd.exe boundary reaches the bounded core adapter."}`
	assertWindowsEmptyHookJSON(t, runWindowsSubagentManifest(t, command,
		`{"turn_id":"turn-consented","last_assistant_message":`+quoteJSON(t, envelope)+`}`, env))
	assertWindowsSubagentStoreCounts(t, dataDir, 1)

	runWindowsEngramCLI(t, binary, env, "capture", "purge", "--project", "engram", "--type", store.CaptureContentTypeSubagentOutput, "--yes", "--json")
	runWindowsEngramCLI(t, binary, env, "capture", "disable", "--project", "engram", "--type", store.CaptureContentTypeSubagentOutput, "--json")
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
	assertWindowsEmptyHookJSON(t, runWindowsSubagentManifest(t, command,
		`{"turn_id":"turn-expired","last_assistant_message":`+quoteJSON(t, envelope)+`}`, env))
	status := runWindowsEngramCLI(t, binary, env, "capture", "status", "--project", "engram", "--type", store.CaptureContentTypeSubagentOutput,
		"--session-id", "turn-expired", "--json")
	var statusPayload map[string]any
	if err := json.Unmarshal([]byte(status), &statusPayload); err != nil || statusPayload["state"] != "expired" {
		t.Fatalf("expired status = %q, err=%v", status, err)
	}
	assertWindowsSubagentStoreCounts(t, dataDir, 0)
}

func codexWindowsSubagentCommand(t *testing.T, manifest hooksJSON) string {
	t.Helper()
	var commands []string
	for _, group := range manifest.Hooks["SubagentStop"] {
		for _, hook := range group.Hooks {
			if hook.Type == "command" && hook.CommandWindows != "" {
				commands = append(commands, hook.CommandWindows)
			}
		}
	}
	if len(commands) != 1 {
		t.Fatalf("SubagentStop has %d Windows command hooks, want exactly one", len(commands))
	}
	return commands[0]
}

func runWindowsSubagentManifest(t *testing.T, command, input string, env []string) string {
	t.Helper()
	run := exec.Command("cmd.exe")
	run.SysProcAttr = &syscall.SysProcAttr{CmdLine: `cmd.exe /C "` + command + `"`}
	run.Env = env
	run.Stdin = strings.NewReader(input)
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("run Windows SubagentStop command: %v\n%s", err, output)
	}
	return string(output)
}

func runWindowsEngramCLI(t *testing.T, binary string, env []string, args ...string) string {
	t.Helper()
	run := exec.Command(binary, args...)
	run.Env = env
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("run engram %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func cleanWindowsSubagentEnv(binDir, dataDir string) []string {
	env := make([]string, 0, len(os.Environ())+3)
	for _, item := range os.Environ() {
		upper := strings.ToUpper(item)
		if strings.HasPrefix(upper, "PATH=") || strings.HasPrefix(upper, "ENGRAM_DATA_DIR=") || strings.HasPrefix(upper, "ENGRAM_PROJECT=") {
			continue
		}
		env = append(env, item)
	}
	return append(env,
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"ENGRAM_DATA_DIR="+dataDir,
		"ENGRAM_PROJECT=engram",
	)
}

func assertWindowsEmptyHookJSON(t *testing.T, output string) {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal([]byte(output), &response); err != nil || len(response) != 0 {
		t.Fatalf("hook output = %q, want empty JSON object (err=%v)", output, err)
	}
}

func assertWindowsSubagentStoreCounts(t *testing.T, dataDir string, captures int) {
	t.Helper()
	s, err := store.New(store.FallbackConfig(dataDir))
	if err != nil {
		t.Fatalf("open capture store: %v", err)
	}
	defer s.Close()
	for table, want := range map[string]int{
		"diagnostic_captures": captures,
		"observations":        0,
		"memory_proposals":    0,
		"memory_checkpoints":  0,
		"sessions":            0,
		"sync_mutations":      0,
	} {
		var got int
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if got != want {
			t.Fatalf("%s count = %d, want %d", table, got, want)
		}
	}
	var retiredEvaluationObjects int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name LIKE 'admission_%'`).Scan(&retiredEvaluationObjects); err != nil {
		t.Fatalf("count retired evaluation objects: %v", err)
	}
	if retiredEvaluationObjects != 0 {
		t.Fatalf("subagent capture created %d retired evaluation objects", retiredEvaluationObjects)
	}
}
