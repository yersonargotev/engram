package activationstudy

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestClassifyReducesRawEvidenceToBoundedActivationEvents(t *testing.T) {
	t.Parallel()

	run := PlannedRun{
		Sequence: 1, CellID: "explicit-recall-r01-engram-normal", PromptID: "explicit-recall",
		PromptClass: "explicit_recall", Treatment: "engram-normal", Repetition: 1, SessionShape: "fresh",
	}
	raw := strings.Join([]string{
		`{"type":"thread.started","thread_id":"local-secret-id"}`,
		`{"type":"item.completed","item":{"type":"command_execution","command":"sed -n '1,220p' /tmp/private-home/.agents/skills/engram-memory-cli/SKILL.md","aggregated_output":"secret raw skill","exit_code":0}}`,
		`{"type":"item.completed","item":{"type":"command_execution","command":"sed -n '1,220p' skills/memory-protocol/SKILL.md","aggregated_output":"private protocol","exit_code":0}}`,
	}, "\n")
	record := Classify(run, RawEvidence{
		CodexJSONL:      strings.NewReader(raw),
		CodexExitCode:   0,
		FinalMessage:    "The retained answer is COBALT-MAPLE-731 from /tmp/private-home.",
		AvailableSkills: []string{"engram-memory-cli", "engram-memory-protocol"},
		ShimEvents: []ShimEvent{
			{Operation: "current_project", ExitCode: 0},
			{Operation: "task_brief", ExitCode: 0},
			{Operation: "save", ExitCode: 0},
			{Operation: "checkpoint", ExitCode: 0},
		},
		PreservationVerified: true,
	})

	for _, event := range []string{
		"skill_description_available", "user_skill_read", "project_memory_protocol_read",
		"overlapping_memory_skills_read", "current_project_invoked", "task_brief_invoked",
		"memory_write_attempted", "memory_write_succeeded", "checkpoint_attempted",
		"checkpoint_succeeded", "useful_recall", "useful_preservation",
	} {
		if !record.Events[event] {
			t.Errorf("event %q = false, want true", event)
		}
	}
	if record.Events["engram_not_invoked"] || record.Events["integration_failure"] {
		t.Fatalf("unexpected terminal events: %#v", record.Events)
	}

	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"/tmp/", "private-home", "local-secret-id", "secret raw skill", "COBALT-MAPLE-731"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("bounded record retained %q: %s", forbidden, encoded)
		}
	}
}

func TestClassifySeparatesSkipNonInvocationAndIntegrationFailure(t *testing.T) {
	t.Parallel()

	preservationRun := PlannedRun{CellID: "preserve-r01-neutral", PromptID: "preserve", PromptClass: "explicit_preservation", Treatment: "neutral", Repetition: 1, SessionShape: "fresh"}
	skillRead := `{"type":"item.completed","item":{"type":"command_execution","command":"cat /private/.agents/skills/engram-memory-cli/SKILL.md","exit_code":0}}`
	skipped := Classify(preservationRun, RawEvidence{CodexJSONL: strings.NewReader(skillRead), AvailableSkills: []string{"engram-memory-cli"}})
	if !skipped.Events["memory_write_skipped"] || skipped.Events["engram_not_invoked"] || skipped.Events["integration_failure"] {
		t.Fatalf("skipped events = %#v", skipped.Events)
	}

	routineRun := PlannedRun{CellID: "routine-r01-neutral", PromptID: "routine", PromptClass: "routine_non_durable", Treatment: "neutral", Repetition: 1, SessionShape: "fresh"}
	notInvoked := Classify(routineRun, RawEvidence{CodexJSONL: strings.NewReader(`{"type":"turn.completed"}`), AvailableSkills: []string{"engram-memory-cli"}})
	if !notInvoked.Events["engram_not_invoked"] || notInvoked.Events["memory_write_skipped"] {
		t.Fatalf("non-invocation events = %#v", notInvoked.Events)
	}

	failed := Classify(routineRun, RawEvidence{
		CodexJSONL: strings.NewReader("not-json\n"), CodexExitCode: 1,
		ShimEvents: []ShimEvent{{Operation: "targeted_search", ExitCode: 2}},
	})
	if !failed.Events["integration_failure"] || !failed.Events["targeted_search_invoked"] {
		t.Fatalf("failure events = %#v", failed.Events)
	}
	if failed.Events["engram_not_invoked"] {
		t.Fatalf("integration failure was also classified as non-invocation: %#v", failed.Events)
	}
	if len(failed.Omissions) != 1 || failed.Omissions[0] != "codex_event_parse_failed" {
		t.Fatalf("failure omissions = %#v", failed.Omissions)
	}
}
