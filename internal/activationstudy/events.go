package activationstudy

import (
	"bufio"
	"encoding/json"
	"io"
	"sort"
	"strings"
)

const syntheticRecallCanary = "COBALT-MAPLE-731"

type RawEvidence struct {
	CodexJSONL           io.Reader
	CodexExitCode        int
	FinalMessage         string
	AvailableSkills      []string
	ShimEvents           []ShimEvent
	PreservationVerified bool
}

type ShimEvent struct {
	Operation string `json:"operation"`
	ExitCode  int    `json:"exit_code"`
}

type OperationOutcome struct {
	Operation string `json:"operation"`
	Succeeded bool   `json:"succeeded"`
}

// RunRecord contains only protocol cell identity and bounded classifications.
type RunRecord struct {
	SchemaVersion   string             `json:"schema_version"`
	Sequence        int                `json:"sequence"`
	CellID          string             `json:"cell_id"`
	PromptID        string             `json:"prompt_id"`
	PromptClass     string             `json:"prompt_class"`
	Treatment       string             `json:"treatment"`
	Repetition      int                `json:"repetition"`
	SessionShape    string             `json:"session_shape"`
	AvailableSkills []string           `json:"available_skills"`
	SkillReads      []string           `json:"skill_reads"`
	Operations      []OperationOutcome `json:"operations"`
	Events          map[string]bool    `json:"events"`
	Omissions       []string           `json:"omissions"`
	Deviations      []string           `json:"protocol_deviations"`
}

// Classify discards raw Codex evidence after reducing it to frozen event names.
func Classify(run PlannedRun, evidence RawEvidence) RunRecord {
	record := RunRecord{
		SchemaVersion: "codex-activation-events-v1",
		Sequence:      run.Sequence, CellID: run.CellID, PromptID: run.PromptID,
		PromptClass: run.PromptClass, Treatment: run.Treatment, Repetition: run.Repetition,
		SessionShape: run.SessionShape, Events: make(map[string]bool),
	}
	for _, event := range requiredActivationEvents() {
		record.Events[event] = false
	}
	record.AvailableSkills = sortedUnique(evidence.AvailableSkills)
	record.Events["skill_description_available"] = containsString(record.AvailableSkills, "engram-memory-cli")

	readSkills, parseFailed := classifySkillReads(evidence.CodexJSONL)
	record.SkillReads = readSkills
	for _, skill := range readSkills {
		switch skill {
		case "user:engram-memory-cli":
			record.Events["user_skill_read"] = true
		case "project:engram-memory-protocol":
			record.Events["project_memory_protocol_read"] = true
		case "project:engram-memory-cli":
			record.Events["project_memory_cli_read"] = true
		}
	}
	record.Events["overlapping_memory_skills_read"] = len(readSkills) > 1
	if parseFailed {
		record.Omissions = append(record.Omissions, "codex_event_parse_failed")
	}

	operationSucceeded := make(map[string]bool)
	for _, event := range evidence.ShimEvents {
		operation := canonicalOperation(event.Operation)
		if operation == "" {
			operation = "other"
		}
		succeeded := event.ExitCode == 0
		record.Operations = append(record.Operations, OperationOutcome{Operation: operation, Succeeded: succeeded})
		operationSucceeded[operation] = operationSucceeded[operation] || succeeded
		switch operation {
		case "current_project":
			record.Events["current_project_invoked"] = true
		case "task_brief":
			record.Events["task_brief_invoked"] = true
		case "targeted_search":
			record.Events["targeted_search_invoked"] = true
		case "save":
			record.Events["memory_write_attempted"] = true
			if succeeded {
				record.Events["memory_write_succeeded"] = true
			}
		case "checkpoint":
			record.Events["checkpoint_attempted"] = true
			if succeeded {
				record.Events["checkpoint_succeeded"] = true
			}
		}
		if !succeeded {
			record.Events["integration_failure"] = true
		}
	}
	sort.Slice(record.Operations, func(i, j int) bool {
		if record.Operations[i].Operation != record.Operations[j].Operation {
			return record.Operations[i].Operation < record.Operations[j].Operation
		}
		return !record.Operations[i].Succeeded && record.Operations[j].Succeeded
	})

	if run.PromptClass == "explicit_preservation" && len(readSkills) > 0 && !record.Events["memory_write_attempted"] && evidence.CodexExitCode == 0 && !parseFailed {
		record.Events["memory_write_skipped"] = true
	}
	if (operationSucceeded["task_brief"] || operationSucceeded["targeted_search"]) && strings.Contains(strings.ToUpper(evidence.FinalMessage), syntheticRecallCanary) {
		record.Events["useful_recall"] = true
	}
	if evidence.PreservationVerified {
		record.Events["useful_preservation"] = true
	}
	if len(readSkills) == 0 && len(evidence.ShimEvents) == 0 {
		record.Events["engram_not_invoked"] = true
	}
	if evidence.CodexExitCode != 0 || parseFailed {
		record.Events["integration_failure"] = true
	}
	if record.Events["integration_failure"] {
		record.Events["engram_not_invoked"] = false
	}
	record.Omissions = sortedUnique(record.Omissions)
	return record
}

func requiredActivationEvents() []string {
	return []string{
		"skill_description_available", "user_skill_read", "project_memory_protocol_read",
		"project_memory_cli_read", "overlapping_memory_skills_read", "current_project_invoked",
		"task_brief_invoked", "targeted_search_invoked", "memory_write_attempted",
		"memory_write_succeeded", "memory_write_skipped", "checkpoint_attempted",
		"checkpoint_succeeded", "engram_not_invoked", "integration_failure",
		"useful_recall", "useful_preservation",
	}
}

func classifySkillReads(reader io.Reader) ([]string, bool) {
	if reader == nil {
		return nil, false
	}
	seen := make(map[string]bool)
	parseFailed := false
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) != nil {
			parseFailed = true
			continue
		}
		item, ok := event["item"].(map[string]any)
		if !ok || !successfulEvidenceItem(item) {
			continue
		}
		encoded, err := json.Marshal(item)
		if err != nil {
			parseFailed = true
			continue
		}
		content := strings.ToLower(filepathSlashes(string(encoded)))
		switch {
		case strings.Contains(content, ".agents/skills/engram-memory-cli/skill.md"):
			seen["user:engram-memory-cli"] = true
		case strings.Contains(content, "skills/engram-memory-cli/skill.md"):
			seen["project:engram-memory-cli"] = true
		}
		if strings.Contains(content, "skills/memory-protocol/skill.md") {
			seen["project:engram-memory-protocol"] = true
		}
	}
	if scanner.Err() != nil {
		parseFailed = true
	}
	reads := make([]string, 0, len(seen))
	for skill := range seen {
		reads = append(reads, skill)
	}
	sort.Strings(reads)
	return reads, parseFailed
}

func successfulEvidenceItem(item map[string]any) bool {
	typeName, _ := item["type"].(string)
	switch typeName {
	case "command_execution", "file_read", "tool_call":
	default:
		return false
	}
	if exitCode, ok := item["exit_code"].(float64); ok && exitCode != 0 {
		return false
	}
	return true
}

func filepathSlashes(value string) string {
	return strings.ReplaceAll(value, "\\\\", "/")
}

func canonicalOperation(value string) string {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "current_project", "task_brief", "targeted_search", "save", "checkpoint":
		return strings.TrimSpace(strings.ToLower(value))
	default:
		return ""
	}
}

func sortedUnique(values []string) []string {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			seen[value] = true
		}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
