//go:build !windows

package plugin_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCodexCaptureHooksEmitOnlyBoundedBaselineMetadataWhenEnabled(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	binDir := t.TempDir()
	logPath := filepath.Join(binDir, "baseline-args.log")

	writeExecutable(t, filepath.Join(binDir, "curl"), `#!/bin/bash
case "$*" in
  *'/project/current'*) printf '%s' '{"project":"engram","project_source":"config","project_strength":"strong","implicit_write_allowed":true}' ;;
esac
exit 0
`)
	writeExecutable(t, filepath.Join(binDir, "engram"), `#!/bin/bash
printf '%s\n' "$*" >> "$BASELINE_ARGS_LOG"
`)

	run := func(script, input string) string {
		t.Helper()
		command := exec.Command(codexTestBash(t), filepath.Join(pluginRoot, "scripts", script))
		command.Stdin = strings.NewReader(input)
		command.Env = append(os.Environ(),
			"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
			"ENGRAM_RECALL_BASELINE=1",
			"BASELINE_ARGS_LOG="+logPath,
		)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("run %s: %v\n%s", script, err, output)
		} else {
			return string(output)
		}
		return ""
	}

	run("user-prompt-submit.sh", `{"cwd":"/tmp/repo","session_id":"session-secret","turn_id":"turn-secret","prompt":"PROMPT-CONTENT-MUST-NOT-LEAK"}`)
	run("subagent-stop.sh", `{"cwd":"/tmp/repo","session_id":"session-secret","last_assistant_message":"ASSISTANT-CONTENT-MUST-NOT-LEAK"}`)
	sessionStartOutput := run("session-start.sh", `{"cwd":"/tmp/repo","session_id":"session-secret","source":"startup"}`)

	deadline := time.Now().Add(2 * time.Second)
	var logged string
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(logPath)
		if err == nil {
			logged = string(raw)
			if strings.Contains(logged, "--kind capture") && strings.Contains(logged, "--kind subagent_stop") && strings.Contains(logged, "--operation session_start") {
				break
			}
		} else if !os.IsNotExist(err) {
			t.Fatalf("read baseline args: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, want := range []string{
		"recall-baseline record --kind capture --surface lifecycle --operation prompt --outcome unknown",
		"recall-baseline record --kind subagent_stop --surface lifecycle --operation subagent_stop --outcome observed",
		"recall-baseline record --kind capture --surface lifecycle --operation subagent --outcome enabled",
		"recall-baseline record --kind operation --surface lifecycle --operation session_start --outcome success --delivered-bytes ",
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("baseline calls do not contain %q\n%s", want, logged)
		}
	}
	if !strings.Contains(logged, "--delivered-bytes "+strconv.Itoa(len(sessionStartOutput))) {
		t.Fatalf("SessionStart byte count does not match %d delivered UTF-8 bytes\n%s", len(sessionStartOutput), logged)
	}
	for _, forbidden := range []string{"PROMPT-CONTENT-MUST-NOT-LEAK", "ASSISTANT-CONTENT-MUST-NOT-LEAK", "/tmp/repo"} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("baseline call leaked %q\n%s", forbidden, logged)
		}
	}
	for _, unnecessaryIdentity := range []string{"session-secret", "turn-secret", "--host"} {
		if strings.Contains(logged, unnecessaryIdentity) {
			t.Fatalf("baseline call retained unnecessary identity %q\n%s", unnecessaryIdentity, logged)
		}
	}

	if err := os.Remove(logPath); err != nil {
		t.Fatalf("remove enabled baseline log: %v", err)
	}
	command := exec.Command(codexTestBash(t), filepath.Join(pluginRoot, "scripts", "user-prompt-submit.sh"))
	command.Stdin = strings.NewReader(`{"cwd":"/tmp/repo","session_id":"session-secret","turn_id":"turn-secret","prompt":"disabled baseline"}`)
	command.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"ENGRAM_RECALL_BASELINE=",
		"BASELINE_ARGS_LOG="+logPath,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run disabled baseline hook: %v\n%s", err, output)
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("disabled hook invoked baseline recorder: %v", err)
	}
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}
