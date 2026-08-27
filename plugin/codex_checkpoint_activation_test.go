package plugin_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	checkpointCueStart = "<!-- engram:checkpoint-cue:start -->"
	checkpointCueEnd   = "<!-- engram:checkpoint-cue:end -->"
)

func TestCodexSessionStartEmitsOneCanonicalCueAsModelContextForEverySource(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	cue := readCanonicalCheckpointCue(t, filepath.Join(pluginRoot, "skills", "memory", "SKILL.md"))
	if words := len(strings.Fields(cue)); words == 0 || words > 60 {
		t.Fatalf("canonical activation cue has %d words, want 1..60", words)
	}

	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	workspace := t.TempDir()
	fakeBin := writeCodexActivationFakeBin(t)

	for _, source := range []string{"startup", "resume", "clear", "compact"} {
		t.Run(source, func(t *testing.T) {
			command := matchingSessionStartCommand(t, manifest, source)
			input := `{"session_id":"session-46","cwd":` + quoteJSON(t, workspace) + `,"source":` + quoteJSON(t, source) + `}`
			output := runCodexHook(t, command, input, pluginRoot, fakeBin)

			var response codexHookResponse
			if err := json.Unmarshal([]byte(output), &response); err != nil {
				t.Fatalf("SessionStart output is not valid JSON: %v\noutput: %s", err, output)
			}
			if response.SystemMessage != "" {
				t.Fatalf("cue was emitted as UI-only systemMessage: %q", response.SystemMessage)
			}
			if response.HookSpecificOutput.HookEventName != "SessionStart" {
				t.Fatalf("hook event name = %q, want SessionStart", response.HookSpecificOutput.HookEventName)
			}
			context := response.HookSpecificOutput.AdditionalContext
			if count := strings.Count(context, cue); count != 1 {
				t.Fatalf("canonical cue appears %d times in model context, want exactly once\ncontext: %s", count, context)
			}
			for _, rubricHeading := range []string{"## Choose a disposition", "## Finalize idempotently"} {
				if strings.Contains(context, rubricHeading) {
					t.Fatalf("SessionStart repeated detailed rubric heading %q\ncontext: %s", rubricHeading, context)
				}
			}
		})
	}
}

func TestCodexUserPromptSubmitForwardsStableRootTurnIdentityAsModelContext(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	cue := readCanonicalCheckpointCue(t, filepath.Join(pluginRoot, "skills", "memory", "SKILL.md"))
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "UserPromptSubmit")
	workspace := t.TempDir()
	sessionID := "session-" + filepath.Base(t.TempDir())
	rootTurnID := "turn-46:opaque/value"
	input := `{"session_id":` + quoteJSON(t, sessionID) +
		`,"turn_id":` + quoteJSON(t, rootTurnID) +
		`,"cwd":` + quoteJSON(t, workspace) +
		`,"prompt":"Implement issue 46"}`
	fakeBin := writeCodexActivationFakeBin(t)

	first := runCodexHook(t, command, input, pluginRoot, fakeBin)
	second := runCodexHook(t, command, input, pluginRoot, fakeBin)
	if first != second {
		t.Fatalf("identity context changed for the same root turn\nfirst:  %s\nsecond: %s", first, second)
	}

	var response codexHookResponse
	if err := json.Unmarshal([]byte(first), &response); err != nil {
		t.Fatalf("UserPromptSubmit output is not valid JSON: %v\noutput: %s", err, first)
	}
	if response.SystemMessage != "" {
		t.Fatalf("root identity was emitted as UI-only systemMessage: %q", response.SystemMessage)
	}
	if response.HookSpecificOutput.HookEventName != "UserPromptSubmit" {
		t.Fatalf("hook event name = %q, want UserPromptSubmit", response.HookSpecificOutput.HookEventName)
	}
	context := response.HookSpecificOutput.AdditionalContext
	wantIdentity := `{"host":"codex","session_id":` + quoteJSON(t, sessionID) + `,"root_turn_id":` + quoteJSON(t, rootTurnID) + `}`
	if !strings.Contains(context, wantIdentity) {
		t.Fatalf("model context does not carry the exact root-turn identity %s\ncontext: %s", wantIdentity, context)
	}
	if strings.Contains(context, cue) {
		t.Fatalf("UserPromptSubmit repeated the SessionStart activation cue\ncontext: %s", context)
	}
	if strings.Contains(first, `"disposition"`) || strings.Contains(first, `"reason":"no_durable_knowledge"`) {
		t.Fatalf("identity adapter attempted to finalize a checkpoint\noutput: %s", first)
	}
}

func TestCodexUserPromptSubmitDoesNotInventCheckpointIdentity(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	command := singleCodexHookCommand(t, manifest, "UserPromptSubmit")
	fakeBin := writeCodexActivationFakeBin(t)

	for _, tc := range []struct {
		name  string
		input string
	}{
		{name: "malformed input", input: "{"},
		{name: "missing session", input: `{"turn_id":"turn-46","prompt":"hello"}`},
		{name: "missing turn", input: `{"session_id":"session-46","prompt":"hello"}`},
		{name: "numeric session", input: `{"session_id":46,"turn_id":"turn-46","prompt":"hello"}`},
		{name: "numeric turn", input: `{"session_id":"session-46","turn_id":46,"prompt":"hello"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := runCodexHook(t, command, tc.input, pluginRoot, fakeBin)
			var response map[string]any
			if err := json.Unmarshal([]byte(output), &response); err != nil {
				t.Fatalf("invalid input produced invalid hook JSON: %v\noutput: %s", err, output)
			}
			if len(response) != 0 {
				t.Fatalf("invalid input produced checkpoint identity context: %s", output)
			}
		})
	}
}

type codexHookResponse struct {
	SystemMessage      string `json:"systemMessage"`
	HookSpecificOutput struct {
		HookEventName     string `json:"hookEventName"`
		AdditionalContext string `json:"additionalContext"`
	} `json:"hookSpecificOutput"`
}

func readCanonicalCheckpointCue(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read canonical checkpoint skill: %v", err)
	}
	content := string(raw)
	if strings.Count(content, checkpointCueStart) != 1 || strings.Count(content, checkpointCueEnd) != 1 {
		t.Fatalf("canonical checkpoint skill must contain exactly one cue marker pair")
	}
	after, _ := strings.CutPrefix(content[strings.Index(content, checkpointCueStart):], checkpointCueStart)
	cue, found := strings.CutSuffix(strings.SplitN(after, checkpointCueEnd, 2)[0], "\n")
	if !found {
		cue = strings.SplitN(after, checkpointCueEnd, 2)[0]
	}
	return strings.TrimSpace(cue)
}

func readCodexHooksManifest(t *testing.T, path string) hooksJSON {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Codex hooks manifest: %v", err)
	}
	var manifest hooksJSON
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parse Codex hooks manifest: %v", err)
	}
	return manifest
}

func matchingSessionStartCommand(t *testing.T, manifest hooksJSON, source string) string {
	t.Helper()
	var commands []string
	for _, group := range manifest.Hooks["SessionStart"] {
		matcher := group.Matcher
		if matcher == "" {
			matcher = ".*"
		}
		matched, err := regexp.MatchString("^(?:"+matcher+")$", source)
		if err != nil {
			t.Fatalf("invalid SessionStart matcher %q: %v", group.Matcher, err)
		}
		if !matched {
			continue
		}
		for _, hook := range group.Hooks {
			if hook.Type == "command" {
				commands = append(commands, hook.Command)
			}
		}
	}
	if len(commands) != 1 {
		t.Fatalf("SessionStart source %q matched %d command hooks, want exactly one", source, len(commands))
	}
	return commands[0]
}

func singleCodexHookCommand(t *testing.T, manifest hooksJSON, event string) string {
	t.Helper()
	var commands []string
	for _, group := range manifest.Hooks[event] {
		for _, hook := range group.Hooks {
			if hook.Type == "command" {
				commands = append(commands, hook.Command)
			}
		}
	}
	if len(commands) != 1 {
		t.Fatalf("%s has %d command hooks, want exactly one", event, len(commands))
	}
	return commands[0]
}

func quoteJSON(t *testing.T, value string) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("quote JSON: %v", err)
	}
	return string(raw)
}

func runCodexHook(t *testing.T, command, input, pluginRoot, fakeBin string) string {
	t.Helper()
	bashPath := codexTestBash(t)
	run := exec.Command(bashPath, "-c", command)
	run.Env = append(os.Environ(),
		"PLUGIN_ROOT="+pluginRoot,
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	run.Stdin = strings.NewReader(input)
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("run Codex hook: %v\noutput: %s", err, output)
	}
	return string(output)
}

func writeCodexActivationFakeBin(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "curl")
	script := `#!/bin/bash
case "$*" in
  *"/context?project="*) printf '%s\n' '{"context":"Prior Engram context."}' ;;
esac
exit 0
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake curl: %v", err)
	}
	return dir
}
