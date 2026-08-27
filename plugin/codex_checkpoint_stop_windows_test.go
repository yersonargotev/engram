//go:build windows

package plugin_test

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	if os.Getenv("ENGRAM_STOP_WINDOWS_HELPER") == "1" {
		input, _ := io.ReadAll(os.Stdin)
		if string(input) != os.Getenv("ENGRAM_STOP_WINDOWS_INPUT") || strings.Join(os.Args[1:], "\n") != "checkpoint\nverify-stop\n--host=codex" {
			fmt.Fprintln(os.Stderr, "Stop adapter did not preserve input and core command")
			os.Exit(97)
		}
		if stdout := os.Getenv("ENGRAM_STOP_WINDOWS_STDOUT"); stdout != "" {
			fmt.Fprintln(os.Stdout, stdout)
		}
		if stderr := os.Getenv("ENGRAM_STOP_WINDOWS_STDERR"); stderr != "" {
			fmt.Fprintln(os.Stderr, stderr)
		}
		code, _ := strconv.Atoi(os.Getenv("ENGRAM_STOP_WINDOWS_EXIT"))
		os.Exit(code)
	}
	os.Exit(m.Run())
}

func TestCodexWindowsStopVerifierRuntime(t *testing.T) {
	root := repoRoot(t)
	adapterPath := filepath.Join(root, "plugin", "codex", "scripts", "stop.ps1")
	fakeBin := copyWindowsStopHelper(t)

	t.Run("allows a terminal checkpoint", func(t *testing.T) {
		output, stderr, code := runCodexWindowsStop(t, adapterPath, fakeBin, `{}`, "", 0,
			`{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":false}`)
		if code != 0 || stderr != "" || strings.TrimSpace(output) != "{}" {
			t.Fatalf("exit=%d stdout=%q stderr=%q, want terminal Stop success", code, output, stderr)
		}
	})

	t.Run("requests one continuation for a missing checkpoint", func(t *testing.T) {
		input := `{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":false}`
		responseJSON := `{"decision":"block","reason":"Finalize the missing Engram checkpoint for {\"host\":\"codex\",\"session_id\":\"session-47\",\"root_turn_id\":\"turn-47\"}."}`
		output, stderr, code := runCodexWindowsStop(t, adapterPath, fakeBin, responseJSON, "", 0, input)
		if code != 0 || stderr != "" {
			t.Fatalf("exit=%d stdout=%q stderr=%q", code, output, stderr)
		}
		var response struct {
			Decision string `json:"decision"`
			Reason   string `json:"reason"`
		}
		if err := json.Unmarshal([]byte(output), &response); err != nil {
			t.Fatalf("parse missing response: %v: %s", err, output)
		}
		if response.Decision != "block" || !strings.Contains(response.Reason, `{"host":"codex","session_id":"session-47","root_turn_id":"turn-47"}`) {
			t.Fatalf("missing checkpoint response = %+v", response)
		}
	})

	t.Run("does not request a second continuation", func(t *testing.T) {
		responseJSON := `{"systemMessage":"Engram checkpoint verifier integration failure: checkpoint is still missing after the single recovery continuation."}`
		output, stderr, code := runCodexWindowsStop(t, adapterPath, fakeBin, responseJSON, "", 0,
			`{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":true}`)
		if code != 0 || stderr != "" {
			t.Fatalf("exit=%d stdout=%q stderr=%q", code, output, stderr)
		}
		var response struct {
			Decision      string `json:"decision"`
			SystemMessage string `json:"systemMessage"`
		}
		if err := json.Unmarshal([]byte(output), &response); err != nil {
			t.Fatalf("parse loop-prevention response: %v: %s", err, output)
		}
		if response.Decision != "" || !strings.Contains(response.SystemMessage, "single recovery continuation") {
			t.Fatalf("loop-prevention response = %+v", response)
		}
	})

	t.Run("preserves integration failure", func(t *testing.T) {
		output, stderr, code := runCodexWindowsStop(t, adapterPath, fakeBin, "", "checkpoint store unavailable", 1,
			`{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":false}`)
		if code != 1 || output != "" || !strings.Contains(stderr, "checkpoint store unavailable") {
			t.Fatalf("exit=%d stdout=%q stderr=%q", code, output, stderr)
		}
	})

	t.Run("preserves malformed response for the host to reject", func(t *testing.T) {
		output, stderr, code := runCodexWindowsStop(t, adapterPath, fakeBin, "not-json", "", 0,
			`{"session_id":"session-47","turn_id":"turn-47","stop_hook_active":false}`)
		if code != 0 || stderr != "" || strings.TrimSpace(output) != "not-json" {
			t.Fatalf("exit=%d stdout=%q stderr=%q", code, output, stderr)
		}
	})
}

func copyWindowsStopHelper(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	raw, err := os.ReadFile(executable)
	if err != nil {
		t.Fatalf("read test executable: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "engram.exe"), raw, 0o755); err != nil {
		t.Fatalf("write fake engram executable: %v", err)
	}
	return dir
}

func runCodexWindowsStop(t *testing.T, adapterPath, fakeBin, stdout, stderr string, exitCode int, input string) (string, string, int) {
	t.Helper()
	run := exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", adapterPath)
	run.Env = append(os.Environ(),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"ENGRAM_STOP_WINDOWS_HELPER=1",
		"ENGRAM_STOP_WINDOWS_STDOUT="+stdout,
		"ENGRAM_STOP_WINDOWS_STDERR="+stderr,
		"ENGRAM_STOP_WINDOWS_EXIT="+strconv.Itoa(exitCode),
		"ENGRAM_STOP_WINDOWS_INPUT="+input,
	)
	run.Stdin = strings.NewReader(input)
	var output, errorOutput strings.Builder
	run.Stdout = &output
	run.Stderr = &errorOutput
	err := run.Run()
	if err == nil {
		return output.String(), errorOutput.String(), 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return output.String(), errorOutput.String(), exitErr.ExitCode()
	}
	t.Fatalf("run Windows Stop adapter: %v", err)
	return "", "", -1
}
