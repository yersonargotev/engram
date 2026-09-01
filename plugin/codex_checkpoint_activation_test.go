package plugin_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
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
	binDir := buildCodexActivationCLI(t, root)
	t.Setenv("ENGRAM_DATA_DIR", t.TempDir())
	t.Setenv("ENGRAM_PROJECT", "engram")
	t.Setenv("ENGRAM_CODEX_RECALL_CANARY", "targeted-recall")

	for _, source := range []string{"startup", "resume", "clear", "compact"} {
		t.Run(source, func(t *testing.T) {
			command := matchingSessionStartCommand(t, manifest, source)
			input := `{"session_id":"session-46","cwd":` + quoteJSON(t, workspace) + `,"source":` + quoteJSON(t, source) + `}`
			output := runCodexHook(t, command, input, pluginRoot, binDir)

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
			if context != cue {
				t.Fatalf("canary %s injected historical context instead of cue only:\n%s", source, context)
			}
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
	binDir := buildCodexActivationCLI(t, root)
	t.Setenv("ENGRAM_DATA_DIR", t.TempDir())
	t.Setenv("ENGRAM_PROJECT", "engram")

	first := runCodexHook(t, command, input, pluginRoot, binDir)
	second := runCodexHook(t, command, input, pluginRoot, binDir)
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
	binDir := buildCodexActivationCLI(t, root)
	t.Setenv("ENGRAM_DATA_DIR", t.TempDir())

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
			output := runCodexHook(t, command, tc.input, pluginRoot, binDir)
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

func TestCodexSessionEndClosesTheCoreRegisteredSessionWithoutDurableArtifacts(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	binDir := buildCodexActivationCLI(t, root)
	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)
	t.Setenv("ENGRAM_PROJECT", "engram")
	t.Setenv("ENGRAM_CODEX_RECALL_CANARY", "targeted-recall")

	const sessionID = "session-end-core-acceptance"
	runCodexHook(t, matchingSessionStartCommand(t, manifest, "startup"),
		`{"session_id":"`+sessionID+`","cwd":`+quoteJSON(t, root)+`,"source":"startup"}`,
		pluginRoot, binDir)
	runCodexHook(t, singleCodexHookCommand(t, manifest, "SessionEnd"),
		`{"session_id":"`+sessionID+`","cwd":`+quoteJSON(t, root)+`,"reason":"user"}`,
		pluginRoot, binDir)

	s, err := store.New(store.FallbackConfig(dataDir))
	if err != nil {
		t.Fatalf("open SessionEnd acceptance store: %v", err)
	}
	defer s.Close()
	session, err := s.GetSession(sessionID)
	if err != nil || session.EndedAt == nil || session.Summary != nil {
		t.Fatalf("SessionEnd session = %+v err=%v, want ended without summary", session, err)
	}
	for _, table := range []string{
		"observations", "memory_proposals", "memory_checkpoints", "diagnostic_captures",
		"recall_feedback_runs", "recall_feedback_exposures", "recall_feedback_labels", "recall_false_empty_reviews",
	} {
		var count int
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Errorf("SessionEnd %s count=%d err=%v, want zero", table, count, err)
		}
	}
}

func TestCodexLifecycleManifestUsesTheSameBoundedCoreCommandsOnUnixAndWindows(t *testing.T) {
	root := repoRoot(t)
	manifest := readCodexHooksManifest(t, filepath.Join(root, "plugin", "codex", "hooks", "hooks.json"))
	const sessionCommand = `engram lifecycle session-start --host=codex --plugin-root="${PLUGIN_ROOT}"`
	const promptCommand = "engram capture prompt-hook --host=codex"
	const sessionEndCommand = "engram lifecycle session-end --host=codex"
	for _, duplicateCompactEvent := range []string{"PreCompact", "PostCompact"} {
		if len(manifest.Hooks[duplicateCompactEvent]) != 0 {
			t.Fatalf("%s must not introduce a second compact-recovery path", duplicateCompactEvent)
		}
	}

	for _, source := range []string{"startup", "resume", "clear", "compact"} {
		matched := matchingSessionStartHook(t, manifest, source)
		if matched.Command != sessionCommand || matched.CommandWindows != sessionCommand {
			t.Errorf("SessionStart %s commands = %q / %q, want exact shared core command", source, matched.Command, matched.CommandWindows)
		}
		if matched.Timeout != 10 || matched.AdditionalContextLimit != 4096 {
			t.Errorf("SessionStart %s timeout/limit = %d/%d, want 10/4096", source, matched.Timeout, matched.AdditionalContextLimit)
		}
	}
	prompt := singleCodexHook(t, manifest, "UserPromptSubmit")
	if prompt.Command != promptCommand || prompt.CommandWindows != promptCommand {
		t.Fatalf("UserPromptSubmit commands = %q / %q, want exact shared core command", prompt.Command, prompt.CommandWindows)
	}
	if prompt.Timeout != 2 || prompt.AdditionalContextLimit != 1024 {
		t.Fatalf("UserPromptSubmit timeout/limit = %d/%d, want 2/1024", prompt.Timeout, prompt.AdditionalContextLimit)
	}
	sessionEnd := singleCodexHook(t, manifest, "SessionEnd")
	if sessionEnd.Command != sessionEndCommand || sessionEnd.CommandWindows != sessionEndCommand || sessionEnd.Timeout != 3 {
		t.Fatalf("SessionEnd command/Windows/timeout = %q / %q / %d, want exact shared core command and timeout 3",
			sessionEnd.Command, sessionEnd.CommandWindows, sessionEnd.Timeout)
	}
	stop := singleCodexHook(t, manifest, "Stop")
	if strings.Contains(stop.Command, "session-end") || strings.Contains(stop.CommandWindows, "session-end") {
		t.Fatalf("Stop must not close sessions: command=%q commandWindows=%q", stop.Command, stop.CommandWindows)
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
	return matchingSessionStartHook(t, manifest, source).Command
}

func matchingSessionStartHook(t *testing.T, manifest hooksJSON, source string) hookEntry {
	t.Helper()
	var hooks []hookEntry
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
				hooks = append(hooks, hook)
			}
		}
	}
	if len(hooks) != 1 {
		t.Fatalf("SessionStart source %q matched %d command hooks, want exactly one", source, len(hooks))
	}
	return hooks[0]
}

func singleCodexHookCommand(t *testing.T, manifest hooksJSON, event string) string {
	return singleCodexHook(t, manifest, event).Command
}

func singleCodexHook(t *testing.T, manifest hooksJSON, event string) hookEntry {
	t.Helper()
	var hooks []hookEntry
	for _, group := range manifest.Hooks[event] {
		for _, hook := range group.Hooks {
			if hook.Type == "command" {
				hooks = append(hooks, hook)
			}
		}
	}
	if len(hooks) != 1 {
		t.Fatalf("%s has %d command hooks, want exactly one", event, len(hooks))
	}
	return hooks[0]
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

func buildCodexActivationCLI(t *testing.T, root string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "engram")
	build := exec.Command("go", "build", "-o", path, "./cmd/engram")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Engram lifecycle CLI: %v\n%s", err, output)
	}
	return dir
}
