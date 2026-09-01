package mcp

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestAgentPolicyProjectionsDoNotReintroduceLegacyMandates(t *testing.T) {
	root := filepath.Join("..", "..")
	paths := []string{
		"plugin/codex/skills/memory/SKILL.md",
		"plugin/claude-code/skills/memory/SKILL.md",
		"plugin/claude-code/scripts/session-start.sh",
		"plugin/claude-code/scripts/post-compaction.sh",
		"plugin/claude-code/scripts/user-prompt-submit.sh",
		"plugin/claude-code/scripts/user-prompt-submit.ps1",
		"plugin/opencode/engram.ts",
		"internal/setup/plugins/opencode/engram.ts",
		"plugin/pi/index.ts",
		"plugin/pi/compaction-recovery.js",
		"skills/memory-protocol/SKILL.md",
		"skills/engram-memory-cli/SKILL.md",
		"internal/setup/setup.go",
		"README.md",
		"DOCS.md",
		"docs/intended-usage.md",
		"docs/PLUGINS.md",
		"docs/AGENT-SETUP.md",
		"docs/ARCHITECTURE.md",
		"docs/COMPARISON.md",
		"plugin/pi/README.md",
	}
	legacyMandates := []*regexp.Regexp{
		regexp.MustCompile(`(?:mandatory|must|required before|not optional|first action required).{0,180}mem_session_summary`),
		regexp.MustCompile(`mem_session_summary.{0,180}(?:mandatory|must|not optional|before (?:ending|saying|finish))`),
		regexp.MustCompile(`proactive(?:ly)?[^.]{0,30}(?:call|use|save)[^.]{0,30}mem_save`),
		regexp.MustCompile(`(?:call|use|save)[^.]{0,35}mem_save[^.]{0,35}proactive(?:ly)?`),
		regexp.MustCompile(`(?:immediately|after (?:any|every))[^.]{0,45}(?:with|call|use)[^.]{0,25}mem_save`),
		regexp.MustCompile(`mem_save[^.]{0,30}(?:immediately after|after (?:any|every)|call now)`),
		regexp.MustCompile(`next session starts blind`),
		regexp.MustCompile(`auto-saves? checkpoint`),
		regexp.MustCompile(`after compaction[^.]{0,45}(?:call|use)[^.]{0,20}mem_context`),
	}

	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			t.Fatalf("read policy projection %s: %v", path, err)
		}
		normalized := strings.ToLower(strings.Join(strings.Fields(string(raw)), " "))
		for _, mandate := range legacyMandates {
			if match := mandate.FindString(normalized); match != "" {
				t.Errorf("%s retains legacy Memory mandate %q", path, match)
			}
		}
	}
}

func TestCanonicalAndRepositorySkillsShareTerminalMemoryAuthority(t *testing.T) {
	root := filepath.Join("..", "..")
	canonical := readPolicyProjection(t, root, "plugin/codex/skills/memory/SKILL.md")
	for _, required := range []string{"Terminal Memory commit", "explicit curation", "material loss-risk handoff"} {
		if !strings.Contains(canonical, required) {
			t.Errorf("canonical skill missing %q", required)
		}
	}
	const cueStart = "<!-- engram:checkpoint-cue:start -->"
	const cueEnd = "<!-- engram:checkpoint-cue:end -->"
	start := strings.Index(canonical, cueStart)
	end := strings.Index(canonical, cueEnd)
	if start == -1 || end <= start {
		t.Fatal("canonical skill is missing bounded activation cue markers")
	}
	activationCue := canonical[start:end]
	for _, required := range []string{
		"Terminal Memory commit",
		"saved",
		"needs_review",
		"skipped(no_durable_knowledge)",
		"Current user intent, maintained source, and runtime evidence override Memory",
	} {
		if !strings.Contains(activationCue, required) {
			t.Errorf("canonical Activation cue missing %q", required)
		}
	}

	repositoryPolicy := readPolicyProjection(t, root, "skills/memory-protocol/SKILL.md")
	if !strings.Contains(repositoryPolicy, "engram-memory") || !strings.Contains(repositoryPolicy, "canonical") {
		t.Error("repository Memory guidance must point to the canonical engram-memory skill")
	}
	for _, competingOperation := range []string{"`mem_save`", "`mem_session_summary`"} {
		if strings.Contains(repositoryPolicy, competingOperation) {
			t.Errorf("repository Memory guidance retains competing operation %s", competingOperation)
		}
	}

	cliSkill := readPolicyProjection(t, root, "skills/engram-memory-cli/SKILL.md")
	for _, required := range []string{"Terminal Memory commit", "explicit curation", "material loss-risk handoff"} {
		if !strings.Contains(cliSkill, required) {
			t.Errorf("CLI skill missing %q", required)
		}
	}
}

func TestAgentPolicyProjectionsPublishBoundedAuthorityAwareRecall(t *testing.T) {
	root := filepath.Join("..", "..")
	paths := []string{
		"plugin/codex/skills/memory/SKILL.md",
		"plugin/claude-code/skills/memory/SKILL.md",
		"skills/engram-memory-cli/SKILL.md",
		"internal/setup/setup.go",
		"plugin/opencode/engram.ts",
		"internal/setup/plugins/opencode/engram.ts",
		"plugin/pi/index.ts",
	}
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?:five|5) candidate`),
		regexp.MustCompile(`4 kib`),
		regexp.MustCompile(`strong.{0,10}explicit`),
		regexp.MustCompile(`(?:reformulat.{0,40}(?:once|one)|(?:once|one).{0,40}reformulat)`),
		regexp.MustCompile(`6.{0,20}10`),
		regexp.MustCompile(`self-contained`),
		regexp.MustCompile(`(?:recall.{0,20}unavailable|unavailable.{0,20}recall).{0,100}(?:not block|without blocking|continue|fails open)`),
	}
	for _, path := range paths {
		content := strings.ToLower(strings.Join(strings.Fields(readPolicyProjection(t, root, path)), " "))
		for _, pattern := range patterns {
			if !pattern.MatchString(content) {
				t.Errorf("%s is missing Recall policy pattern %q", path, pattern)
			}
		}
	}
}

func TestMaintainedDocsDescribeFiveToolTerminalPolicy(t *testing.T) {
	root := filepath.Join("..", "..")
	wantTools := []string{
		"mem_current_project",
		"mem_search",
		"mem_get_observation",
		"mem_checkpoint",
		"mem_checkpoint_status",
	}
	for _, path := range []string{
		"README.md",
		"DOCS.md",
		"docs/AGENT-SETUP.md",
		"docs/ARCHITECTURE.md",
		"plugin/pi/README.md",
	} {
		content := readPolicyProjection(t, root, path)
		optionalSummary := regexp.MustCompile(`(?i)(?:optional(?:ly)?[^.\n]{0,60}session summary|session summary[^.\n]{0,60}optional)`)
		if !optionalSummary.MatchString(content) {
			t.Errorf("%s does not describe Session summary as optional", path)
		}
		for _, tool := range wantTools {
			if !strings.Contains(content, tool) {
				t.Errorf("%s missing default tool %s", path, tool)
			}
		}
	}
	for _, path := range []string{"README.md", "DOCS.md", "plugin/pi/README.md"} {
		if content := readPolicyProjection(t, root, path); !strings.Contains(content, "Terminal Memory") {
			t.Errorf("%s missing Terminal Memory workflow", path)
		}
	}

	for _, path := range []string{"README.md", "DOCS.md", "docs/AGENT-SETUP.md", "docs/ARCHITECTURE.md"} {
		content := readPolicyProjection(t, root, path)
		for _, required := range []string{"Memory operation", "agent lifecycle operation"} {
			if !strings.Contains(content, required) {
				t.Errorf("%s missing operation-boundary vocabulary %q", path, required)
			}
		}
	}

	agentSetup := readPolicyProjection(t, root, "docs/AGENT-SETUP.md")
	section := func(start, end string) string {
		t.Helper()
		from := strings.Index(agentSetup, start)
		if from == -1 {
			t.Fatalf("docs/AGENT-SETUP.md missing section %q", start)
		}
		to := strings.Index(agentSetup[from+len(start):], end)
		if to == -1 {
			t.Fatalf("docs/AGENT-SETUP.md section %q missing boundary %q", start, end)
		}
		return agentSetup[from : from+len(start)+to]
	}
	for name, content := range map[string]string{
		"Gemini":  section("## Gemini CLI", "## Codex"),
		"VS Code": section("## VS Code", "## Cursor"),
	} {
		for _, bareArgs := range []string{`"args": ["mcp"]`, `\"args\":[\"mcp\"]`} {
			if strings.Contains(content, bareArgs) {
				t.Errorf("%s manual setup retains bare all-tools arguments %q", name, bareArgs)
			}
		}
		if !strings.Contains(content, "--tools=agent") {
			t.Errorf("%s manual setup does not select the agent profile", name)
		}
	}
	gemini := section("## Gemini CLI", "## Codex")
	if strings.Contains(gemini, "gemini mcp add engram engram mcp\n") {
		t.Error("Gemini CLI command retains bare all-tools arguments")
	}
	for _, staleLifecycleGuidance := range []string{
		"when to save, search, and close sessions",
		"when to save and search memories",
	} {
		if strings.Contains(strings.ToLower(agentSetup), staleLifecycleGuidance) {
			t.Errorf("docs/AGENT-SETUP.md retains stale lifecycle guidance %q", staleLifecycleGuidance)
		}
	}
}

func TestMaintainedDocsDescribeExactMemoryDedupeIdentity(t *testing.T) {
	root := filepath.Join("..", "..")
	const identity = "normalized content hash + project + scope + type + title + tool_name + normalized topic_key"
	for _, path := range []string{"DOCS.md", "docs/ARCHITECTURE.md"} {
		content := strings.Join(strings.Fields(readPolicyProjection(t, root, path)), " ")
		if !strings.Contains(content, identity) {
			t.Errorf("%s does not document the exact Memory dedupe identity %q", path, identity)
		}
		if strings.Contains(content, "Without a `topic_key`, every `mem_save` creates a new observation") {
			t.Errorf("%s contradicts exact dedupe for Memories without a topic key", path)
		}
	}
}

func readPolicyProjection(t *testing.T, root, path string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		t.Fatalf("read policy projection %s: %v", path, err)
	}
	return string(raw)
}
