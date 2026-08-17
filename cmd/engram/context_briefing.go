package main

import (
	"fmt"
	"strings"

	"github.com/yersonargotev/engram/internal/store"
	"github.com/yersonargotev/engram/internal/taskbriefing"
)

type contextBriefingMemory struct {
	Memory   store.Observation                `json:"memory"`
	PinBoost int                              `json:"pin_boost"`
	Evidence []taskbriefing.SelectionEvidence `json:"evidence"`
}

type contextBriefingOutput struct {
	Mode                 string                       `json:"mode"`
	Project              string                       `json:"project"`
	Scope                string                       `json:"scope,omitempty"`
	Memories             []contextBriefingMemory      `json:"memories"`
	Diagnostics          []taskbriefing.Diagnostic    `json:"diagnostics"`
	BaseResolution       *taskbriefing.BaseResolution `json:"base_resolution,omitempty"`
	ResultLimitOmissions int                          `json:"result_limit_omissions"`
	BudgetOmissions      int                          `json:"budget_omissions"`
}

func newContextBriefingOutput(project, scope string, result taskbriefing.Result) contextBriefingOutput {
	memories := make([]contextBriefingMemory, 0, len(result.Memories))
	for _, selected := range result.Memories {
		memories = append(memories, contextBriefingMemory{
			Memory:   selected.Memory,
			PinBoost: selected.PinBoost,
			Evidence: selected.Evidence,
		})
	}
	diagnostics := result.Diagnostics
	if diagnostics == nil {
		diagnostics = []taskbriefing.Diagnostic{}
	}
	return contextBriefingOutput{
		Mode:                 "brief",
		Project:              project,
		Scope:                scope,
		Memories:             memories,
		Diagnostics:          diagnostics,
		BaseResolution:       result.BaseResolution,
		ResultLimitOmissions: result.ResultLimitOmissions,
		BudgetOmissions:      result.BudgetOmissions,
	}
}

func renderContextBriefing(output contextBriefingOutput, taskProvided bool) {
	fmt.Println("## Task Briefing")
	fmt.Printf("Project: %s\n", output.Project)
	if output.BaseResolution != nil {
		fmt.Printf("Base: %s (%s)\n", output.BaseResolution.Ref, output.BaseResolution.Source)
	}
	fmt.Println()
	if len(output.Memories) == 0 {
		fmt.Println("No relevant durable memories found for this task.")
		if !taskProvided {
			fmt.Println("Provide --task \"<intent>\" to guide selection.")
		}
	} else {
		for _, selected := range output.Memories {
			memory := selected.Memory
			fmt.Printf("### #%d [%s] %s\n", memory.ID, memory.Type, memory.Title)
			fmt.Printf("Scope: %s\n\n%s\n\n", memory.Scope, memory.Content)
			fmt.Println("Selection evidence:")
			for _, evidence := range selected.Evidence {
				fmt.Printf("- %s: matched %s in %s\n", evidence.Signal, strings.Join(evidence.MatchedTerms, ", "), strings.Join(evidence.MatchedFields, ", "))
			}
			if selected.PinBoost > 0 {
				fmt.Printf("- pin: relevant pinned memory received +%d\n", selected.PinBoost)
			}
			fmt.Println()
		}
	}
	if len(output.Diagnostics) > 0 {
		fmt.Println("Diagnostics:")
		for _, diagnostic := range output.Diagnostics {
			fmt.Printf("- %s: %s%s\n", diagnostic.Code, contextBriefingDiagnosticMessage(diagnostic), contextBriefingDiagnosticDetails(diagnostic))
		}
	}
	if output.ResultLimitOmissions > 0 || output.BudgetOmissions > 0 {
		fmt.Printf("Omitted: %d by result limit, %d by output budget\n", output.ResultLimitOmissions, output.BudgetOmissions)
	}
}

func contextBriefingDiagnosticDetails(diagnostic taskbriefing.Diagnostic) string {
	switch diagnostic.Code {
	case taskbriefing.DiagnosticGitOperationFailed:
		if len(diagnostic.Sources) == 0 {
			return ""
		}
		sources := make([]string, len(diagnostic.Sources))
		for index, source := range diagnostic.Sources {
			sources[index] = string(source)
		}
		return fmt.Sprintf(" Failed sources: %s; remaining usable signals were retained.", strings.Join(sources, ", "))
	case taskbriefing.DiagnosticTaskInputTruncated, taskbriefing.DiagnosticRepositoryInputTruncated:
		if len(diagnostic.Truncations) == 0 {
			return ""
		}
		truncations := make([]string, len(diagnostic.Truncations))
		for index, truncation := range diagnostic.Truncations {
			truncations[index] = fmt.Sprintf("%s: %d total, %d analyzed, %d omitted", truncation.Signal, truncation.TotalTerms, truncation.AnalyzedTerms, truncation.OmittedTerms)
		}
		return " Truncation: " + strings.Join(truncations, "; ")
	default:
		return ""
	}
}

func contextBriefingDiagnosticMessage(diagnostic taskbriefing.Diagnostic) string {
	switch diagnostic.Code {
	case taskbriefing.DiagnosticNoUsableSignals:
		return "No usable task or repository signal was available."
	case taskbriefing.DiagnosticRepositoryProjectUnresolved:
		return "Repository evidence was ignored because its project was unresolved."
	case taskbriefing.DiagnosticRepositoryProjectMismatch:
		return "Repository evidence was ignored because it belongs to another project."
	case taskbriefing.DiagnosticBranchBaseUnresolved:
		return "Committed branch evidence was unavailable because no base resolved."
	case taskbriefing.DiagnosticGitOperationFailed:
		return "Some repository evidence was unavailable."
	case taskbriefing.DiagnosticTaskInputTruncated:
		return "Task intent exceeded the deterministic input bound."
	case taskbriefing.DiagnosticRepositoryInputTruncated:
		return "Repository evidence exceeded deterministic input bounds."
	case taskbriefing.DiagnosticResultLimitReached:
		return "Lower-ranked relevant memories were omitted by the result limit."
	case taskbriefing.DiagnosticOutputBudgetExhausted:
		return "Lower-ranked memories were omitted whole to preserve the output budget."
	case taskbriefing.DiagnosticSelectedMemoryConflict:
		return "Selected memories have a judged conflict; review both before acting."
	default:
		return "Task briefing completed with a typed degradation."
	}
}
