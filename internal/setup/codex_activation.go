package setup

import (
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	codexCheckpointCueStart = "<!-- engram:checkpoint-cue:start -->"
	codexCheckpointCueEnd   = "<!-- engram:checkpoint-cue:end -->"
)

type codexActivationHooksManifest struct {
	Hooks map[string][]struct {
		Matcher string `json:"matcher"`
		Hooks   []struct {
			Type           string `json:"type"`
			Command        string `json:"command"`
			CommandWindows string `json:"commandWindows"`
			Timeout        int    `json:"timeout"`
			Async          bool   `json:"async"`
		} `json:"hooks"`
	} `json:"hooks"`
}

// verifyInstalledCodexActivation checks the installed plugin's public asset
// contract without executing hooks. The skill is the canonical source for both
// the detailed rubric and the cue; SessionStart adapters may only project it.
func verifyInstalledCodexActivation(installedPath string, hooksRaw []byte) bool {
	return verifyInstalledCodexActivationCue(installedPath) && verifyInstalledCodexSessionStartHooks(installedPath, hooksRaw)
}

func verifyInstalledCodexActivationCue(installedPath string) bool {
	skillRaw, err := readFileFn(filepath.Join(installedPath, "skills", "memory", "SKILL.md"))
	return err == nil && validCodexCheckpointSkill(string(skillRaw))
}

func verifyInstalledCodexSessionHooks(installedPath string, hooksRaw []byte) bool {
	if !verifyInstalledCodexSessionStartHooks(installedPath, hooksRaw) {
		return false
	}
	var manifest codexActivationHooksManifest
	if json.Unmarshal(hooksRaw, &manifest) != nil {
		return false
	}
	groups := manifest.Hooks["SessionEnd"]
	if len(groups) != 1 || groups[0].Matcher != "" || len(groups[0].Hooks) != 1 {
		return false
	}
	end := groups[0].Hooks[0]
	if end.Type != "command" || end.Command != `"${PLUGIN_ROOT}/scripts/session-end.sh"` ||
		end.CommandWindows != `powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${PLUGIN_ROOT}\scripts\session-end.ps1"` ||
		end.Timeout != 3 || end.Async {
		return false
	}
	for _, relative := range []string{"scripts/session-end.sh", "scripts/session-end.ps1"} {
		if _, err := readFileFn(filepath.Join(installedPath, filepath.FromSlash(relative))); err != nil {
			return false
		}
	}
	return true
}

func verifyInstalledCodexSessionStartHooks(installedPath string, hooksRaw []byte) bool {
	helperRaw, err := readFileFn(filepath.Join(installedPath, "scripts", "_checkpoint.sh"))
	if err != nil || !validCodexCheckpointHelper(string(helperRaw)) {
		return false
	}

	var manifest codexActivationHooksManifest
	if json.Unmarshal(hooksRaw, &manifest) != nil {
		return false
	}
	for _, source := range []string{"startup", "resume", "clear", "compact"} {
		commands := matchingCodexSessionStartCommands(manifest, source)
		if len(commands) != 1 {
			return false
		}
		scriptPath, ok := installedCodexPluginScriptPath(installedPath, commands[0])
		if !ok {
			return false
		}
		scriptRaw, err := readFileFn(scriptPath)
		if err != nil || !validCodexSessionStartAdapter(string(scriptRaw)) {
			return false
		}
	}
	return true
}

func verifyInstalledCodexPromptHook(installedPath string, hooksRaw []byte) bool {
	var manifest codexActivationHooksManifest
	if json.Unmarshal(hooksRaw, &manifest) != nil {
		return false
	}
	groups := manifest.Hooks["UserPromptSubmit"]
	if len(groups) != 1 || groups[0].Matcher != "" || len(groups[0].Hooks) != 1 {
		return false
	}
	hook := groups[0].Hooks[0]
	if hook.Type != "command" || hook.Command != `"${PLUGIN_ROOT}/scripts/user-prompt-submit.sh"` || hook.Timeout != 2 || hook.Async {
		return false
	}
	scriptRaw, err := readFileFn(filepath.Join(installedPath, "scripts", "user-prompt-submit.sh"))
	if err != nil {
		return false
	}
	script := string(scriptRaw)
	return strings.Contains(script, `"${ENGRAM_URL}/prompts"`) &&
		strings.Contains(script, "session_id") && strings.Contains(script, "turn_id")
}

func validCodexCheckpointSkill(skill string) bool {
	cue, ok := extractCodexCheckpointCue(skill)
	if !ok || len(strings.Fields(cue)) > 60 {
		return false
	}
	for _, required := range []string{
		"root user turn",
		"engram-memory",
		"`saved`",
		"`skipped(no_durable_knowledge)`",
		"`needs_review`",
		"continuations",
		"subagents",
	} {
		if !strings.Contains(cue, required) {
			return false
		}
	}
	for _, required := range []string{
		"## Root user turn boundary",
		"## Choose a disposition",
		"### `saved`",
		"### `skipped(no_durable_knowledge)`",
		"### `needs_review`",
		"## Finalize idempotently",
		"`already_recorded`",
		"`mem_checkpoint`",
		"`root_turn_id`",
	} {
		if !strings.Contains(skill, required) {
			return false
		}
	}
	return true
}

func extractCodexCheckpointCue(skill string) (string, bool) {
	if strings.Count(skill, codexCheckpointCueStart) != 1 || strings.Count(skill, codexCheckpointCueEnd) != 1 {
		return "", false
	}
	start := strings.Index(skill, codexCheckpointCueStart) + len(codexCheckpointCueStart)
	end := strings.Index(skill[start:], codexCheckpointCueEnd)
	if end < 0 {
		return "", false
	}
	cue := strings.TrimSpace(skill[start : start+end])
	return cue, cue != ""
}

func validCodexCheckpointHelper(helper string) bool {
	for _, required := range []string{
		codexCheckpointCueStart,
		codexCheckpointCueEnd,
		"checkpoint_activation_cue",
		"hookSpecificOutput",
		"additionalContext",
		`hookEventName:"SessionStart"`,
	} {
		if !strings.Contains(helper, required) {
			return false
		}
	}
	return true
}

func matchingCodexSessionStartCommands(manifest codexActivationHooksManifest, source string) []string {
	var commands []string
	for _, group := range manifest.Hooks["SessionStart"] {
		matcher := group.Matcher
		if matcher == "" {
			matcher = ".*"
		}
		matched, err := regexp.MatchString("^(?:"+matcher+")$", source)
		if err != nil || !matched {
			continue
		}
		for _, hook := range group.Hooks {
			if hook.Type == "command" {
				commands = append(commands, hook.Command)
			}
		}
	}
	return commands
}

func installedCodexPluginScriptPath(installedPath, command string) (string, bool) {
	const prefix = `"${PLUGIN_ROOT}/scripts/`
	trimmed := strings.TrimSpace(command)
	if !strings.HasPrefix(trimmed, prefix) || !strings.HasSuffix(trimmed, `"`) {
		return "", false
	}
	relative := strings.TrimSuffix(strings.TrimPrefix(trimmed, prefix), `"`)
	if relative == "" || filepath.Base(relative) != relative {
		return "", false
	}
	return filepath.Join(installedPath, "scripts", relative), true
}

func validCodexSessionStartAdapter(script string) bool {
	if !strings.Contains(script, `source "${SCRIPT_DIR}/_checkpoint.sh"`) ||
		!strings.Contains(script, "emit_session_start_context") {
		return false
	}
	for _, rubricHeading := range []string{"## Choose a disposition", "## Finalize idempotently"} {
		if strings.Contains(script, rubricHeading) {
			return false
		}
	}
	return true
}
