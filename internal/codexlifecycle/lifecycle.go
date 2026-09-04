// Package codexlifecycle owns the bounded Codex lifecycle treatment contract.
package codexlifecycle

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const (
	// EnvTreatment selects an explicitly declared Codex lifecycle canary.
	EnvTreatment = "ENGRAM_CODEX_RECALL_CANARY"

	// MaxInjectedUTF8Bytes is the complete SessionStart.additionalContext budget.
	MaxInjectedUTF8Bytes = 4 * 1024

	checkpointCueStart = "<!-- engram:checkpoint-cue:start -->"
	checkpointCueEnd   = "<!-- engram:checkpoint-cue:end -->"
)

// Treatment identifies one declared lifecycle injection treatment.
type Treatment string

const (
	TreatmentBroadProjectContext               Treatment = "broad_project_context"
	TreatmentCueOnlyTargetedRecall             Treatment = "cue_only_targeted_recall"
	TreatmentCueOnlyTargetedRecallExactSession Treatment = "cue_only_targeted_recall_exact_session"
)

// Selection is the complete, content-free result of resolving EnvTreatment.
type Selection struct {
	Treatment  Treatment `json:"treatment"`
	Enabled    bool      `json:"enabled"`
	Valid      bool      `json:"valid"`
	ReasonCode string    `json:"reason_code"`
}

// SelectTreatment keeps the existing broad treatment as the default and only
// enables one of the two closed canary values.
func SelectTreatment(raw string) Selection {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return Selection{Treatment: TreatmentBroadProjectContext, Valid: true, ReasonCode: "canary_disabled"}
	case "targeted-recall":
		return Selection{Treatment: TreatmentCueOnlyTargetedRecall, Enabled: true, Valid: true, ReasonCode: "canary_targeted_recall"}
	case "targeted-recall-exact-session":
		return Selection{Treatment: TreatmentCueOnlyTargetedRecallExactSession, Enabled: true, Valid: true, ReasonCode: "canary_targeted_recall_exact_session"}
	default:
		return Selection{Treatment: TreatmentBroadProjectContext, ReasonCode: "canary_treatment_invalid"}
	}
}

// ReadCanonicalCue loads the single canonical cue from an installed plugin.
func ReadCanonicalCue(pluginRoot string) (string, error) {
	pluginRoot = strings.TrimSpace(pluginRoot)
	if pluginRoot == "" {
		return "", fmt.Errorf("plugin root is required")
	}
	return ReadCanonicalCueFile(filepath.Join(pluginRoot, "skills", "memory", "SKILL.md"))
}

// ReadCanonicalCueFile loads the single canonical cue from a skill file.
func ReadCanonicalCueFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read canonical checkpoint skill: %w", err)
	}
	content := string(raw)
	if strings.Count(content, checkpointCueStart) != 1 || strings.Count(content, checkpointCueEnd) != 1 {
		return "", fmt.Errorf("canonical checkpoint skill must contain exactly one cue marker pair")
	}
	start := strings.Index(content, checkpointCueStart) + len(checkpointCueStart)
	end := strings.Index(content[start:], checkpointCueEnd)
	if end < 0 {
		return "", fmt.Errorf("canonical checkpoint cue end marker is missing")
	}
	cue := strings.TrimSpace(content[start : start+end])
	if cue == "" {
		return "", fmt.Errorf("canonical checkpoint cue is empty")
	}
	return cue, nil
}

// BuildModelContext renders one cue plus optional exact-session context while
// preserving valid UTF-8 within a complete injected-byte budget.
func BuildModelContext(cue, extra string, limit int) (string, bool) {
	context := strings.TrimSpace(cue)
	if strings.TrimSpace(extra) != "" {
		context += "\n\n" + strings.TrimSpace(extra)
	}
	if limit <= 0 || len(context) <= limit {
		return context, false
	}
	end := limit
	for end > 0 && !utf8.RuneStart(context[end]) {
		end--
	}
	return context[:end], true
}
