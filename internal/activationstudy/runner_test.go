package activationstudy

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRunUsesDisposableStateAndReturnsOnlyBoundedEvents(t *testing.T) {
	rootCommand := exec.Command("git", "rev-parse", "--show-toplevel")
	rootOutput, err := rootCommand.Output()
	if err != nil {
		t.Fatal(err)
	}
	repo := strings.TrimSpace(string(rootOutput))
	revisionCommand := exec.Command("git", "-C", repo, "rev-parse", "HEAD^{commit}")
	revisionOutput, err := revisionCommand.Output()
	if err != nil {
		t.Fatal(err)
	}
	revision := strings.TrimSpace(string(revisionOutput))

	skillDir := filepath.Join(t.TempDir(), "engram-memory-cli")
	writeTestFile(t, skillDir, "SKILL.md", "---\nname: engram-memory-cli\ndescription: test\n---\nUse the CLI.\n")
	skillHash, err := HashTree(skillDir)
	if err != nil {
		t.Fatal(err)
	}
	contractPath, hashPath := writeFrozenContract(t, validContractJSON())
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	study.Contract.SourceRevision = revision
	study.Contract.Engram.SourceRevision = revision
	study.Contract.Codex.GoVersion = runtime.Version()
	study.Contract.UserSkill.TreeSHA256 = skillHash
	study.Contract.Repetitions = 1

	fakeCodex := filepath.Join(t.TempDir(), "codex")
	fakeScript := `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "codex-cli 0.151.0"
  exit 0
fi
for argument in "$@"; do
  if [ "$argument" = "debug" ]; then
    guidance=""
    if [ -f AGENTS.md ] && grep -q 'engram-memory-protocol' AGENTS.md; then
      guidance=' skills/memory-protocol/SKILL.md'
    fi
    printf '[{"role":"developer","content":[{"type":"input_text","text":"### Available skills\\n- engram-memory-cli: test\\n</skills_instructions>%s"}]}]\n' "$guidance"
    exit 0
  fi
done
if [ "$1" != "exec" ]; then
  exit 64
fi
output=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ] || [ "$1" = "--output-last-message" ]; then
    shift
    output="$1"
  fi
  shift
done
engram current-project --json >/dev/null || exit 70
engram context activation-study-fixture --brief --task "synthetic recall" --scope project --limit 5 --json >/dev/null || exit 71
engram save --title "Synthetic preference" --content "What: EVAL-PRESERVE-731" --project activation-study-fixture --json >/dev/null || exit 72
printf '%s\n' '{"type":"item.completed","item":{"type":"command_execution","command":"sed -n 1,120p /private/.agents/skills/engram-memory-cli/SKILL.md","exit_code":0}}'
printf '%s\n' '{"type":"turn.completed"}'
printf '%s\n' 'COBALT-MAPLE-731' > "$output"
`
	if err := os.WriteFile(fakeCodex, []byte(fakeScript), 0o700); err != nil {
		t.Fatal(err)
	}
	authFile := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(authFile, []byte(`{"synthetic":"credential"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	eventsPath := filepath.Join(t.TempDir(), "events.json")
	runOptions := RunOptions{
		SourceRepo:  repo,
		UserSkill:   skillDir,
		AuthFile:    authFile,
		CodexBinary: fakeCodex,
		TempRoot:    t.TempDir(),
		OutputPath:  eventsPath,
	}
	eventSet, err := study.Run(ctx, runOptions)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(eventSet.Records) != 18 {
		t.Fatalf("records = %d, want 18", len(eventSet.Records))
	}
	if !eventSet.Verification.CodexPromptInputVerified {
		t.Fatal("Codex prompt-input discovery was not verified")
	}
	for _, record := range eventSet.Records {
		if !record.Events["user_skill_read"] || !record.Events["current_project_invoked"] || !record.Events["task_brief_invoked"] || !record.Events["memory_write_succeeded"] {
			t.Fatalf("cell %s events = %#v", record.CellID, record.Events)
		}
		if record.Events["integration_failure"] {
			t.Fatalf("cell %s unexpectedly failed: %#v", record.CellID, record)
		}
	}
	encoded, err := json.Marshal(eventSet)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{repo, fakeCodex, authFile, "synthetic\":\"credential", "/private/"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("event set retained private material %q", forbidden)
		}
	}

	noRetryScript := `#!/bin/sh
if [ "$1" = "--version" ]; then echo "codex-cli 0.151.0"; exit 0; fi
exit 99
`
	if err := os.WriteFile(fakeCodex, []byte(noRetryScript), 0o700); err != nil {
		t.Fatal(err)
	}
	resumed, err := study.Run(ctx, runOptions)
	if err != nil {
		t.Fatalf("idempotent Run() error = %v", err)
	}
	if len(resumed.Records) != len(eventSet.Records) {
		t.Fatalf("resumed records = %d, want %d", len(resumed.Records), len(eventSet.Records))
	}
}
