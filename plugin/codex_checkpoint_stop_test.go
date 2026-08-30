//go:build !windows

package plugin_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCodexStopVerifierDelegatesToCoreWithoutRuntimeDependencies(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "Stop")
	windowsCommand := singleCodexHookWindowsCommand(t, manifest, "Stop")
	if windowsCommand != "engram checkpoint verify-stop --host=codex" || strings.Contains(windowsCommand, `"`) {
		t.Fatalf("Windows Stop command must be quote-free and delegate directly to core: %q", windowsCommand)
	}
	input := `{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":false}`
	fakeBin, argsPath, inputPath := writeCodexStopDelegate(t, `{}`+"\n", "", 0)

	stdout, stderr, err := runCodexStopLauncher(t, nil, command, input, pluginRoot, fakeBin)
	if err != nil || stderr != "" || strings.TrimSpace(stdout) != "{}" {
		t.Fatalf("delegate exit=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	args, readErr := os.ReadFile(argsPath)
	if readErr != nil {
		t.Fatalf("read delegated arguments: %v", readErr)
	}
	if string(args) != "checkpoint\nverify-stop\n--host=codex\n" {
		t.Fatalf("delegated arguments = %q", args)
	}
	forwarded, readErr := os.ReadFile(inputPath)
	if readErr != nil {
		t.Fatalf("read delegated input: %v", readErr)
	}
	if string(forwarded) != input {
		t.Fatalf("delegated input = %q, want %q", forwarded, input)
	}

	for _, relative := range []string{"scripts/stop.sh"} {
		raw, readErr := os.ReadFile(filepath.Join(pluginRoot, filepath.FromSlash(relative)))
		if readErr != nil {
			t.Fatalf("read %s: %v", relative, readErr)
		}
		for _, forbidden := range []string{"jq", "ConvertFrom-Json", "checkpoint_not_found", "decision", "systemMessage", "no_durable_knowledge"} {
			if strings.Contains(string(raw), forbidden) {
				t.Errorf("%s owns core behavior %q", relative, forbidden)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(pluginRoot, "scripts", "stop.ps1")); !os.IsNotExist(err) {
		t.Fatalf("obsolete Windows Stop adapter must not remain: %v", err)
	}
}

func singleCodexHookWindowsCommand(t *testing.T, manifest hooksJSON, event string) string {
	t.Helper()
	var commands []string
	for _, group := range manifest.Hooks[event] {
		for _, hook := range group.Hooks {
			if hook.Type == "command" && hook.CommandWindows != "" {
				commands = append(commands, hook.CommandWindows)
			}
		}
	}
	if len(commands) != 1 {
		t.Fatalf("%s has %d Windows command hooks, want exactly one", event, len(commands))
	}
	return commands[0]
}

func TestCodexStopVerifierLeavesIntegrationFailuresVisible(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "Stop")
	input := `{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":false}`

	t.Run("missing executable", func(t *testing.T) {
		stdout, stderr, err := runCodexStopLauncher(t, nil, command, input, pluginRoot, t.TempDir())
		if err == nil || stdout != "" || !strings.Contains(stderr, "engram") {
			t.Fatalf("missing executable exit=%v stdout=%q stderr=%q", err, stdout, stderr)
		}
	})

	t.Run("transport failure", func(t *testing.T) {
		fakeBin, _, _ := writeCodexStopDelegate(t, "", "checkpoint store unavailable\n", 1)
		stdout, stderr, err := runCodexStopLauncher(t, nil, command, input, pluginRoot, fakeBin)
		if err == nil || stdout != "" || !strings.Contains(stderr, "checkpoint store unavailable") {
			t.Fatalf("transport failure exit=%v stdout=%q stderr=%q", err, stdout, stderr)
		}
		if strings.Contains(stdout+stderr, "skipped") {
			t.Fatalf("transport failure invented a disposition: stdout=%q stderr=%q", stdout, stderr)
		}
	})

	t.Run("malformed response", func(t *testing.T) {
		fakeBin, _, _ := writeCodexStopDelegate(t, "not-json\n", "", 0)
		stdout, stderr, err := runCodexStopLauncher(t, nil, command, input, pluginRoot, fakeBin)
		if err != nil || stderr != "" || strings.TrimSpace(stdout) != "not-json" {
			t.Fatalf("malformed response exit=%v stdout=%q stderr=%q", err, stdout, stderr)
		}
		if strings.Contains(stdout, "skipped") {
			t.Fatalf("malformed response invented a disposition: %q", stdout)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		fakeBin, _, _ := writeCodexStopDelegate(t, "", "", 0)
		path := filepath.Join(fakeBin, "engram")
		if err := os.WriteFile(path, []byte("#!/bin/bash\nexec /bin/sleep 5\n"), 0o755); err != nil {
			t.Fatalf("write timeout delegate: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		started := time.Now()
		stdout, stderr, err := runCodexStopLauncher(t, ctx, command, input, pluginRoot, fakeBin)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) >= 2*time.Second {
			t.Fatalf("timeout exit=%v elapsed=%s stdout=%q stderr=%q", err, time.Since(started), stdout, stderr)
		}
		if strings.Contains(stdout+stderr, "skipped") {
			t.Fatalf("timeout invented a disposition: stdout=%q stderr=%q", stdout, stderr)
		}
	})
}

func TestCodexCheckpointAcceptanceUsesRealCLIAndScripts(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	stopCommand := singleCodexHookCommand(t, manifest, "Stop")
	binDir := t.TempDir()
	engramPath := filepath.Join(binDir, "engram")
	build := exec.Command("go", "build", "-o", engramPath, "./cmd/engram")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build real Engram CLI: %v\n%s", err, output)
	}
	writeCodexAcceptanceCurl(t, binDir)
	dataDir := t.TempDir()
	baseEnv := append(os.Environ(), "ENGRAM_DATA_DIR="+dataDir)

	cue := readCanonicalCheckpointCue(t, filepath.Join(pluginRoot, "skills", "memory", "SKILL.md"))
	sessionCommand := matchingSessionStartCommand(t, manifest, "startup")
	sessionInput := `{"session_id":"session-47-acceptance","cwd":` + quoteJSON(t, root) + `,"source":"startup"}`
	sessionOutput := runCodexHook(t, sessionCommand, sessionInput, pluginRoot, binDir)
	var sessionResponse codexHookResponse
	if err := json.Unmarshal([]byte(sessionOutput), &sessionResponse); err != nil {
		t.Fatalf("parse SessionStart acceptance output: %v\n%s", err, sessionOutput)
	}
	if count := strings.Count(sessionResponse.HookSpecificOutput.AdditionalContext, cue); count != 1 {
		t.Fatalf("acceptance cue count = %d, want 1", count)
	}

	stopInput := `{"session_id":"session-47-acceptance","turn_id":"turn-47-acceptance","stop_hook_active":false}`
	missingOut, missingErr, err := runCodexStopLauncher(t, nil, stopCommand, stopInput, pluginRoot, binDir, "ENGRAM_DATA_DIR="+dataDir)
	if err != nil || missingErr != "" {
		t.Fatalf("missing Stop exit=%v stdout=%q stderr=%q", err, missingOut, missingErr)
	}
	var missing map[string]any
	if err := json.Unmarshal([]byte(missingOut), &missing); err != nil || missing["decision"] != "block" {
		t.Fatalf("missing Stop response=%#v parse=%v", missing, err)
	}
	if !strings.Contains(missing["reason"].(string), `{"host":"codex","session_id":"session-47-acceptance","root_turn_id":"turn-47-acceptance"}`) {
		t.Fatalf("missing Stop lost original identity: %s", missingOut)
	}

	cases := []struct {
		name       string
		sessionID  string
		rootTurnID string
		recordArgs []string
	}{
		{
			name:       "skipped",
			sessionID:  "session-47-acceptance",
			rootTurnID: "turn-47-acceptance",
			recordArgs: []string{"--disposition=skipped", "--reason=no_durable_knowledge"},
		},
		{
			name:       "saved",
			sessionID:  "session-48-saved",
			rootTurnID: "turn-48-saved",
			recordArgs: []string{
				"--disposition=saved",
				"--project=engram",
				`--memory-json={"type":"decision","title":"Codex acceptance memory","content":"Retire only exact-owned legacy activation after replacement verification."}`,
			},
		},
		{
			name:       "needs_review",
			sessionID:  "session-48-review",
			rootTurnID: "turn-48-review",
			recordArgs: []string{
				"--disposition=needs_review",
				"--project=engram",
				`--proposal-json={"title":"Review Codex acceptance proposal","content":"Keep this proposal local until review."}`,
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			args := []string{
				"checkpoint", "record", "--host=codex",
				"--session-id=" + testCase.sessionID,
				"--root-turn-id=" + testCase.rootTurnID,
			}
			args = append(args, testCase.recordArgs...)
			args = append(args, "--json")
			for attempt, want := range []string{"created", "already_recorded"} {
				record := exec.Command(engramPath, args...)
				record.Env = baseEnv
				output, err := record.CombinedOutput()
				if err != nil {
					t.Fatalf("record attempt %d through real CLI: %v\n%s", attempt+1, err, output)
				}
				var response map[string]any
				if err := json.Unmarshal(output, &response); err != nil || response["idempotency"] != want {
					t.Fatalf("record attempt %d response=%#v parse=%v, want idempotency %q", attempt+1, response, err, want)
				}
			}

			input := `{"session_id":` + quoteJSON(t, testCase.sessionID) + `,"turn_id":` + quoteJSON(t, testCase.rootTurnID) + `,"stop_hook_active":false}`
			terminalOut, terminalErr, err := runCodexStopLauncher(t, nil, stopCommand, input, pluginRoot, binDir, "ENGRAM_DATA_DIR="+dataDir)
			if err != nil || terminalErr != "" || strings.TrimSpace(terminalOut) != "{}" {
				t.Fatalf("terminal Stop exit=%v stdout=%q stderr=%q", err, terminalOut, terminalErr)
			}
		})
	}

	loopInput := `{"session_id":"session-47-acceptance","turn_id":"turn-47-still-missing","stop_hook_active":true}`
	loopOut, loopErr, err := runCodexStopLauncher(t, nil, stopCommand, loopInput, pluginRoot, binDir, "ENGRAM_DATA_DIR="+dataDir)
	if err != nil || loopErr != "" {
		t.Fatalf("loop Stop exit=%v stdout=%q stderr=%q", err, loopOut, loopErr)
	}
	var loop map[string]any
	if err := json.Unmarshal([]byte(loopOut), &loop); err != nil || loop["decision"] != nil || !strings.Contains(loop["systemMessage"].(string), "single recovery continuation") {
		t.Fatalf("loop Stop response=%#v parse=%v", loop, err)
	}
}

func runCodexStopLauncher(t *testing.T, ctx context.Context, command, input, pluginRoot, binDir string, extraEnv ...string) (string, string, error) {
	t.Helper()
	if ctx == nil {
		ctx = context.Background()
	}
	run := exec.CommandContext(ctx, codexTestBash(t), "-c", command)
	run.Env = environmentWithOverrides(append([]string{
		"PLUGIN_ROOT=" + pluginRoot,
		"PATH=" + binDir,
	}, extraEnv...)...)
	run.Stdin = strings.NewReader(input)
	var stdout, stderr strings.Builder
	run.Stdout = &stdout
	run.Stderr = &stderr
	err := run.Run()
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return stdout.String(), stderr.String(), err
}

func environmentWithOverrides(overrides ...string) []string {
	keys := make(map[string]struct{}, len(overrides))
	for _, item := range overrides {
		if key, _, ok := strings.Cut(item, "="); ok {
			keys[key] = struct{}{}
		}
	}
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, item := range os.Environ() {
		key, _, ok := strings.Cut(item, "=")
		if _, replaced := keys[key]; ok && replaced {
			continue
		}
		environment = append(environment, item)
	}
	return append(environment, overrides...)
}

func writeCodexStopDelegate(t *testing.T, stdout, stderr string, exitCode int) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args")
	inputPath := filepath.Join(dir, "input")
	script := "#!/bin/bash\nIFS= read -r input || true\nprintf '%s' \"$input\" > " + shellSingleQuote(inputPath) + "\nprintf '%s\\n' \"$@\" > " + shellSingleQuote(argsPath) + "\n"
	if stdout != "" {
		script += "printf '%s' " + shellSingleQuote(stdout) + "\n"
	}
	if stderr != "" {
		script += "printf '%s' " + shellSingleQuote(stderr) + " >&2\n"
	}
	script += "exit " + string(rune('0'+exitCode)) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "engram"), []byte(script), 0o755); err != nil {
		t.Fatalf("write delegate: %v", err)
	}
	return dir, argsPath, inputPath
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\"'\"'`) + "'"
}

func writeCodexAcceptanceCurl(t *testing.T, binDir string) {
	t.Helper()
	script := `#!/bin/bash
case "$*" in
  *"/context?project="*) printf '%s\n' '{"context":"Acceptance context."}' ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(binDir, "curl"), []byte(script), 0o755); err != nil {
		t.Fatalf("write acceptance curl: %v", err)
	}
}
