package mcp

import (
	"maps"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

// TestServerInstructionsStaysUnderClientTruncationLimit enforces the
// boundary documented for the Claude Code MCP client: it truncates the
// `instructions` field returned during handshake at 2048 runes (UTF-8
// chars after decode). Keeping the constant under that ceiling avoids
// a class of handshake-validity regressions reported against Claude
// Code sessions where `mem_*` tools stopped responding shortly after
// connect.
//
// Source-side `len(serverInstructions)` is BYTES (Go strings are UTF-8
// byte slices). The figure the client uses is the RUNE count —
// em-dashes, ⏤ arrows, etc. each decode as one rune but several
// bytes. Asserting on runes here matches the client's truncation rule.
//
// The reported `Server instructions truncated from 2539 to 2048 chars`
// log line is the empirical evidence for the 2048-rune ceiling; the
// client-side handling after truncation (rejection, reconnect, or
// SIGINT) is documented in the field reports but not reproduced in
// this repo.
func TestServerInstructionsStaysUnderClientTruncationLimit(t *testing.T) {
	const clientTruncationCeiling = 2048
	runes := utf8.RuneCountInString(serverInstructions)
	t.Logf("serverInstructions rune count: %d (byte count: %d)", runes, len(serverInstructions))
	if runes >= clientTruncationCeiling {
		t.Errorf("serverInstructions is %d runes (>=%d) — exceeds the documented 2048-rune MCP client truncation ceiling. Trim prose.",
			runes, clientTruncationCeiling)
	}
}

func TestCurationJudgeDescriptionUsesCandidateJudgmentIDs(t *testing.T) {
	const candidateInstruction = "once per entry using that entry's judgment_id"
	s := newMCPTestStore(t)
	tool := NewServerWithTools(s, ResolveTools("mem_judge")).ListTools()["mem_judge"]
	if tool == nil || !strings.Contains(tool.Tool.Description, candidateInstruction) {
		t.Errorf("specialized mem_judge description must require each candidate's judgment_id")
	}
}

func TestServerInstructionsExposeOnlyProtocolToolsAndTerminalPolicy(t *testing.T) {
	wantTools := make(map[string]bool)
	for _, name := range protocolcontract.MinimumTools() {
		wantTools[name] = true
	}
	gotTools := make(map[string]bool)
	for _, name := range regexp.MustCompile(`\bmem_[a-z_]+\b`).FindAllString(serverInstructions, -1) {
		gotTools[name] = true
	}
	if !maps.Equal(gotTools, wantTools) {
		t.Fatalf("MCP instructions mention tools %#v, want only Protocol tools %#v", gotTools, wantTools)
	}

	for _, required := range []string{
		"Terminal Memory commit",
		"one terminal Memory checkpoint",
		"settled root user turn",
		"saved", "needs_review", "skipped",
		"Current user intent, maintained source, and runtime evidence override Memory",
		"curation", "lifecycle", "admin",
	} {
		if !strings.Contains(serverInstructions, required) {
			t.Errorf("MCP instructions missing %q", required)
		}
	}
	for _, legacyMandate := range []string{"MANDATORY", "PROACTIVE SAVE", "immediately after ANY"} {
		if strings.Contains(serverInstructions, legacyMandate) {
			t.Errorf("MCP instructions retain legacy mandate %q", legacyMandate)
		}
	}
}
