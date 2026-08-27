package plugin_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCodexStopVerifierAllowsTerminalCheckpoint(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "Stop")

	for _, disposition := range []string{"saved", "skipped", "needs_review"} {
		t.Run(disposition, func(t *testing.T) {
			status := `{"checkpoint":{"identity":{"host":"codex","session_id":"session-47","root_turn_id":"turn-47"},"disposition":` + quoteJSON(t, disposition) + `}}`
			fakeBin, _ := writeCodexStopFakeEngram(t, status, "", 0)
			output := runCodexHook(t, command, `{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":false}`, pluginRoot, fakeBin)
			var response map[string]any
			if err := json.Unmarshal([]byte(output), &response); err != nil {
				t.Fatalf("Stop output is not valid JSON: %v\noutput: %s", err, output)
			}
			if len(response) != 0 {
				t.Fatalf("terminal checkpoint produced Stop feedback: %s", output)
			}
		})
	}
}

func TestCodexStopVerifierDeclaresWindowsAdapter(t *testing.T) {
	root := repoRoot(t)
	manifestPath := filepath.Join(root, "plugin", "codex", "hooks", "hooks.json")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read Codex hooks manifest: %v", err)
	}
	var manifest codexHooksManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parse Codex hooks manifest: %v", err)
	}
	const wantUnix = `"${PLUGIN_ROOT}/scripts/stop.sh"`
	const wantWindows = `powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${PLUGIN_ROOT}\scripts\stop.ps1"`
	for _, group := range manifest.Hooks["Stop"] {
		for _, hook := range group.Hooks {
			if hook.Type == "command" && hook.Command == wantUnix && hook.CommandWindows == wantWindows && hook.Timeout == 3 {
				windowsPath := filepath.Join(root, "plugin", "codex", "scripts", "stop.ps1")
				windowsRaw, err := os.ReadFile(windowsPath)
				if err != nil {
					t.Fatalf("read Windows Stop adapter: %v", err)
				}
				windowsScript := string(windowsRaw)
				for _, required := range []string{
					"[Console]::In.ReadToEnd()",
					"ConvertFrom-Json",
					"session_id",
					"turn_id",
					"stop_hook_active",
					"--host=codex",
					"--session-id=",
					"--root-turn-id=",
					"WaitForExit(2000)",
					"checkpoint_not_found",
					"decision = 'block'",
					"systemMessage",
				} {
					if !strings.Contains(windowsScript, required) {
						t.Errorf("Windows Stop adapter must contain %q", required)
					}
				}
				if strings.Contains(windowsScript, "no_durable_knowledge") {
					t.Error("Windows Stop adapter must not select the skipped disposition")
				}
				return
			}
		}
	}
	t.Fatalf("Stop hook must declare command %q, commandWindows %q, and timeout 3", wantUnix, wantWindows)
}

func TestCodexStopVerifierRequestsOneContinuationForMissingCheckpoint(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "Stop")
	sessionID := "-session/47:opaque"
	rootTurnID := "-turn/47:opaque"
	fakeBin, argsPath := writeCodexStopFakeEngram(t, "", `{"code":"checkpoint_not_found","message":"inspect checkpoint: checkpoint not found"}`, 1)

	input := `{"session_id":` + quoteJSON(t, sessionID) + `,"turn_id":` + quoteJSON(t, rootTurnID) + `,"stop_hook_active":false}`
	output := runCodexHook(t, command, input, pluginRoot, fakeBin)
	var response struct {
		Decision      string `json:"decision"`
		Reason        string `json:"reason"`
		SystemMessage string `json:"systemMessage"`
	}
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		t.Fatalf("Stop output is not valid JSON: %v\noutput: %s", err, output)
	}
	if response.Decision != "block" || response.SystemMessage != "" {
		t.Fatalf("missing checkpoint response = %+v, want one continuation without integration failure", response)
	}
	wantIdentity := `{"host":"codex","session_id":` + quoteJSON(t, sessionID) + `,"root_turn_id":` + quoteJSON(t, rootTurnID) + `}`
	if !strings.Contains(response.Reason, wantIdentity) {
		t.Fatalf("continuation does not preserve original identity %s\nreason: %s", wantIdentity, response.Reason)
	}
	if words := len(strings.Fields(response.Reason)); words == 0 || words > 45 {
		t.Fatalf("continuation has %d words, want a minimal instruction of 1..45 words: %s", words, response.Reason)
	}
	if strings.Contains(response.Reason, "no_durable_knowledge") {
		t.Fatalf("continuation selected a checkpoint disposition: %s", response.Reason)
	}

	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read fake engram arguments: %v", err)
	}
	wantArgs := "checkpoint\nstatus\n--host=codex\n--session-id=" + sessionID + "\n--root-turn-id=" + rootTurnID + "\n--json\n"
	if string(args) != wantArgs {
		t.Fatalf("checkpoint status arguments = %q, want %q", args, wantArgs)
	}
}

func TestCodexStopVerifierDoesNotLoopWhenCheckpointIsStillMissing(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "Stop")
	fakeBin, _ := writeCodexStopFakeEngram(t, "", `{"code":"checkpoint_not_found","message":"inspect checkpoint: checkpoint not found"}`, 1)

	output := runCodexHook(t, command, `{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":true}`, pluginRoot, fakeBin)
	var response struct {
		Decision      string `json:"decision"`
		Reason        string `json:"reason"`
		SystemMessage string `json:"systemMessage"`
	}
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		t.Fatalf("Stop output is not valid JSON: %v\noutput: %s", err, output)
	}
	if response.Decision != "" || response.Reason != "" {
		t.Fatalf("replayed Stop requested a second continuation: %s", output)
	}
	if !strings.Contains(response.SystemMessage, "still missing after the single recovery continuation") {
		t.Fatalf("replayed missing checkpoint was not reported visibly: %s", output)
	}
	if strings.Contains(output, "skipped") || strings.Contains(output, "no_durable_knowledge") {
		t.Fatalf("loop prevention invented a checkpoint disposition: %s", output)
	}
}

func TestCodexStopVerifierAllowsRecoveredCheckpointOnReplay(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "Stop")
	status := `{"checkpoint":{"identity":{"host":"codex","session_id":"session-47","root_turn_id":"turn-47"},"disposition":"skipped"}}`
	fakeBin, _ := writeCodexStopFakeEngram(t, status, "", 0)

	output := runCodexHook(t, command, `{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":true}`, pluginRoot, fakeBin)
	var response map[string]any
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		t.Fatalf("replayed Stop output is not valid JSON: %v\noutput: %s", err, output)
	}
	if len(response) != 0 {
		t.Fatalf("recovered checkpoint produced replay feedback: %s", output)
	}
}

func TestCodexStopVerifierReportsIntegrationFailures(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "Stop")

	tests := []struct {
		name     string
		stdout   string
		stderr   string
		exitCode int
	}{
		{name: "executable failure", stderr: "engram: command not found", exitCode: 127},
		{name: "transport failure", stderr: `{"code":"checkpoint_failed","message":"open store: unavailable"}`, exitCode: 1},
		{name: "malformed response", stdout: "not-json", exitCode: 0},
		{name: "multiple responses", stdout: `{"checkpoint":{"identity":{"host":"codex","session_id":"session-47","root_turn_id":"turn-47"},"disposition":"saved"}} {"checkpoint":{"identity":{"host":"codex","session_id":"session-47","root_turn_id":"turn-47"},"disposition":"saved"}}`, exitCode: 0},
		{name: "wrong identity", stdout: `{"checkpoint":{"identity":{"host":"codex","session_id":"other","root_turn_id":"turn-47"},"disposition":"saved"}}`, exitCode: 0},
		{name: "unexpected disposition", stdout: `{"checkpoint":{"identity":{"host":"codex","session_id":"session-47","root_turn_id":"turn-47"},"disposition":"pending"}}`, exitCode: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeBin, _ := writeCodexStopFakeEngram(t, tc.stdout, tc.stderr, tc.exitCode)
			output := runCodexHook(t, command, `{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":false}`, pluginRoot, fakeBin)
			var response struct {
				Decision      string `json:"decision"`
				SystemMessage string `json:"systemMessage"`
			}
			if err := json.Unmarshal([]byte(output), &response); err != nil {
				t.Fatalf("integration failure output is not valid JSON: %v\noutput: %s", err, output)
			}
			if response.Decision != "" || !strings.HasPrefix(response.SystemMessage, "Engram checkpoint verifier integration failure:") {
				t.Fatalf("integration failure was not surfaced without continuation: %s", output)
			}
			if strings.Contains(output, "skipped") || strings.Contains(output, "no_durable_knowledge") {
				t.Fatalf("integration failure invented a checkpoint disposition: %s", output)
			}
		})
	}
}

func TestCodexStopVerifierReportsTimeoutBeforeCodexKillsTheHook(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "Stop")
	fakeBin := writeCodexStopTimeoutEngram(t)

	started := time.Now()
	output := runCodexHook(t, command, `{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":false}`, pluginRoot, fakeBin)
	elapsed := time.Since(started)
	var response struct {
		Decision      string `json:"decision"`
		SystemMessage string `json:"systemMessage"`
	}
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		t.Fatalf("timeout output is not valid JSON: %v\noutput: %s", err, output)
	}
	if response.Decision != "" || !strings.Contains(response.SystemMessage, "timed out after 2 seconds") {
		t.Fatalf("checkpoint timeout was not surfaced as an integration failure: %s", output)
	}
	if elapsed >= 4*time.Second {
		t.Fatalf("internal timeout took %s, want less than the three-second Codex deadline plus process overhead", elapsed)
	}
	if strings.Contains(output, "skipped") || strings.Contains(output, "no_durable_knowledge") {
		t.Fatalf("timeout invented a checkpoint disposition: %s", output)
	}
}

func TestCodexStopVerifierRejectsInvalidInputWithoutCallingCore(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "Stop")

	for _, tc := range []struct {
		name  string
		input string
	}{
		{name: "malformed", input: "{"},
		{name: "missing session", input: `{"turn_id":"turn-47","stop_hook_active":false}`},
		{name: "missing turn", input: `{"session_id":"session-47","stop_hook_active":false}`},
		{name: "missing loop state", input: `{"session_id":"session-47","turn_id":"turn-47"}`},
		{name: "non-string identity", input: `{"session_id":47,"turn_id":[],"stop_hook_active":false}`},
		{name: "non-boolean loop state", input: `{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":"false"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeBin, argsPath := writeCodexStopFakeEngram(t, "", "", 0)
			output := runCodexHook(t, command, tc.input, pluginRoot, fakeBin)
			var response struct {
				Decision      string `json:"decision"`
				SystemMessage string `json:"systemMessage"`
			}
			if err := json.Unmarshal([]byte(output), &response); err != nil {
				t.Fatalf("invalid input output is not valid JSON: %v\noutput: %s", err, output)
			}
			if response.Decision != "" || !strings.Contains(response.SystemMessage, "Stop input") {
				t.Fatalf("invalid Stop input was not reported: %s", output)
			}
			if _, err := os.Stat(argsPath); !os.IsNotExist(err) {
				t.Fatalf("invalid Stop input called the checkpoint core: %v", err)
			}
		})
	}
}

func writeCodexStopFakeEngram(t *testing.T, stdout, stderr string, exitCode int) (string, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "engram")
	argsPath := filepath.Join(dir, "args")
	script := "#!/bin/bash\nprintf '%s\\n' \"$@\" > " + strconv.Quote(argsPath) + "\n"
	if stdout != "" {
		script += "printf '%s\\n' " + strconv.Quote(stdout) + "\n"
	}
	if stderr != "" {
		script += "printf '%s\\n' " + strconv.Quote(stderr) + " >&2\n"
	}
	script += "exit " + strconv.Itoa(exitCode) + "\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake engram: %v", err)
	}
	return dir, argsPath
}

func writeCodexStopTimeoutEngram(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "engram")
	if err := os.WriteFile(path, []byte("#!/bin/bash\nexec sleep 5\n"), 0o755); err != nil {
		t.Fatalf("write timeout fake engram: %v", err)
	}
	return dir
}
