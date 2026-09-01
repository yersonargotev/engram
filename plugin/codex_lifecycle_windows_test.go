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

	"github.com/yersonargotev/engram/internal/store"
)

func TestCodexWindowsLifecycleManifestRuntime(t *testing.T) {
	root := repoRoot(t)
	manifest := readCodexHooksManifest(t, filepath.Join(root, "plugin", "codex", "hooks", "hooks.json"))
	pluginRoot := filepath.Join(t.TempDir(), "plugin root with spaces")
	skillSource, err := os.ReadFile(filepath.Join(root, "plugin", "codex", "skills", "memory", "SKILL.md"))
	if err != nil {
		t.Fatalf("read canonical skill: %v", err)
	}
	skillPath := filepath.Join(pluginRoot, "skills", "memory", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(skillPath), 0o755); err != nil {
		t.Fatalf("create plugin fixture: %v", err)
	}
	if err := os.WriteFile(skillPath, skillSource, 0o644); err != nil {
		t.Fatalf("write plugin fixture: %v", err)
	}

	binDir := t.TempDir()
	binary := filepath.Join(binDir, "engram.exe")
	build := exec.Command("go", "build", "-o", binary, "./cmd/engram")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Engram CLI: %v\n%s", err, output)
	}
	dataDir := t.TempDir()
	env := append(cleanWindowsSubagentEnv(binDir, dataDir), "ENGRAM_CODEX_RECALL_CANARY=targeted-recall")

	sessionCommand := strings.ReplaceAll(matchingSessionStartHook(t, manifest, "startup").CommandWindows, "${PLUGIN_ROOT}", pluginRoot)
	sessionOutput := runWindowsLifecycleManifest(t, sessionCommand,
		`{"session_id":"windows-session","cwd":"C:\\work\\engram","source":"startup"}`, env)
	var sessionResponse codexHookResponse
	if err := json.Unmarshal([]byte(sessionOutput), &sessionResponse); err != nil {
		t.Fatalf("decode Windows SessionStart output: %v\n%s", err, sessionOutput)
	}
	cue := readCanonicalCheckpointCue(t, skillPath)
	if sessionResponse.HookSpecificOutput.AdditionalContext != cue {
		t.Fatalf("Windows canary context is not cue-only: %q", sessionResponse.HookSpecificOutput.AdditionalContext)
	}

	promptCommand := singleCodexHook(t, manifest, "UserPromptSubmit").CommandWindows
	promptOutput := runWindowsLifecycleManifest(t, promptCommand,
		`{"session_id":"windows-session","turn_id":"opaque:windows/value","cwd":"C:\\work\\engram","prompt":"private prompt"}`, env)
	var promptResponse codexHookResponse
	if err := json.Unmarshal([]byte(promptOutput), &promptResponse); err != nil {
		t.Fatalf("decode Windows UserPromptSubmit output: %v\n%s", err, promptOutput)
	}
	if !strings.Contains(promptResponse.HookSpecificOutput.AdditionalContext,
		`{"host":"codex","session_id":"windows-session","root_turn_id":"opaque:windows/value"}`) {
		t.Fatalf("Windows prompt hook lost exact identity: %s", promptOutput)
	}
	if strings.Contains(promptOutput, "private prompt") {
		t.Fatalf("Windows prompt hook leaked prompt content: %s", promptOutput)
	}

	s, err := store.New(store.FallbackConfig(dataDir))
	if err != nil {
		t.Fatalf("open Windows lifecycle store: %v", err)
	}
	defer s.Close()
	if _, err := s.GetSession("windows-session"); err != nil {
		t.Fatalf("Windows lifecycle did not register exact session: %v", err)
	}
}

func runWindowsLifecycleManifest(t *testing.T, command, input string, env []string) string {
	t.Helper()
	run := exec.Command("cmd.exe")
	run.SysProcAttr = &syscall.SysProcAttr{CmdLine: `cmd.exe /C "` + command + `"`}
	run.Env = env
	run.Stdin = strings.NewReader(input)
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("run Windows lifecycle manifest command: %v\n%s", err, output)
	}
	return string(output)
}
