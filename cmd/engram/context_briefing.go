package main

import (
	"bytes"
	"encoding/json"
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
	Mode                    string                            `json:"mode"`
	Project                 string                            `json:"project"`
	Scope                   string                            `json:"scope,omitempty"`
	Memories                []contextBriefingMemory           `json:"memories"`
	Diagnostics             []taskbriefing.Diagnostic         `json:"diagnostics"`
	BaseResolution          *taskbriefing.BaseResolution      `json:"base_resolution,omitempty"`
	ResultLimitOmissions    int                               `json:"result_limit_omissions"`
	BudgetOmissions         int                               `json:"budget_omissions"`
	Pipeline                taskbriefing.PipelineAccounting   `json:"pipeline"`
	EmptyResultReason       taskbriefing.EmptyResultReason    `json:"empty_result_reason,omitempty"`
	Rejections              []taskbriefing.CandidateRejection `json:"rejections"`
	RejectionDetailsOmitted int                               `json:"rejection_details_omitted"`
	Fallback                *taskbriefing.SearchFallback      `json:"fallback,omitempty"`
	FallbackOmitted         bool                              `json:"fallback_omitted,omitempty"`
	FallbackOmissionReason  string                            `json:"fallback_omission_reason,omitempty"`
	conflictPairs           []taskbriefing.ConflictPair
	fallbackCandidate       *taskbriefing.SearchFallback
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
	rejections := result.Rejections
	if rejections == nil {
		rejections = []taskbriefing.CandidateRejection{}
	}
	return contextBriefingOutput{
		Mode:                    "brief",
		Project:                 project,
		Scope:                   scope,
		Memories:                memories,
		Diagnostics:             diagnostics,
		BaseResolution:          result.BaseResolution,
		ResultLimitOmissions:    result.ResultLimitOmissions,
		Pipeline:                result.Pipeline,
		EmptyResultReason:       result.EmptyResultReason,
		Rejections:              rejections,
		RejectionDetailsOmitted: result.RejectionDetailsOmitted,
		Fallback:                result.Fallback,
		conflictPairs:           result.ConflictPairs,
		fallbackCandidate:       result.FallbackCandidate,
	}
}

func encodeContextBriefing(output contextBriefingOutput, jsonMode, taskProvided bool, budget int) ([]byte, error) {
	for {
		var encoded []byte
		if jsonMode {
			var err error
			encoded, err = json.Marshal(output)
			if err != nil {
				return nil, err
			}
			encoded = append(encoded, '\n')
		} else {
			encoded = formatContextBriefing(output, taskProvided)
		}
		if len(encoded) <= budget {
			return encoded, nil
		}
		if len(output.Rejections) > 0 {
			output.Rejections = output.Rejections[:len(output.Rejections)-1]
			output.RejectionDetailsOmitted++
			continue
		}
		if output.Fallback != nil {
			output.Fallback = nil
			output.FallbackOmitted = true
			output.FallbackOmissionReason = "output_budget"
			continue
		}
		if len(output.Memories) == 0 {
			return nil, fmt.Errorf("task briefing metadata exceeds %d-byte output budget", budget)
		}
		output.Memories = output.Memories[:len(output.Memories)-1]
		output.BudgetOmissions++
		output.Diagnostics = withoutContextBriefingDiagnostic(output.Diagnostics, taskbriefing.DiagnosticSelectedMemoryConflict)
		if contextBriefingHasSelectedConflict(output) {
			output.Diagnostics = append(output.Diagnostics, taskbriefing.Diagnostic{Code: taskbriefing.DiagnosticSelectedMemoryConflict})
		}
		if !hasContextBriefingDiagnostic(output.Diagnostics, taskbriefing.DiagnosticOutputBudgetExhausted) {
			output.Diagnostics = append(output.Diagnostics, taskbriefing.Diagnostic{Code: taskbriefing.DiagnosticOutputBudgetExhausted})
		}
		if output.Fallback == nil && output.fallbackCandidate != nil && !output.FallbackOmitted {
			fallback := *output.fallbackCandidate
			fallback.ReasonCode = taskbriefing.FallbackOutputBudgetExhausted
			output.Fallback = &fallback
		}
	}
}

func contextBriefingHasSelectedConflict(output contextBriefingOutput) bool {
	selected := make(map[string]struct{}, len(output.Memories))
	for _, memory := range output.Memories {
		selected[memory.Memory.SyncID] = struct{}{}
	}
	for _, pair := range output.conflictPairs {
		_, sourceSelected := selected[pair.SourceID]
		_, targetSelected := selected[pair.TargetID]
		if sourceSelected && targetSelected {
			return true
		}
	}
	return false
}

func formatContextBriefing(output contextBriefingOutput, taskProvided bool) []byte {
	var buffer bytes.Buffer
	fmt.Fprintln(&buffer, "## Task Briefing")
	fmt.Fprintf(&buffer, "Project: %s\n", output.Project)
	if output.BaseResolution != nil {
		fmt.Fprintf(&buffer, "Base: %s (%s)\n", output.BaseResolution.Ref, output.BaseResolution.Source)
	}
	retrieved := fmt.Sprintf("%d (complete)", output.Pipeline.RetrievedCandidates)
	if !output.Pipeline.RetrievalCountComplete {
		retrieved = fmt.Sprintf("at least %d (incomplete)", output.Pipeline.RetrievedCandidates)
	}
	fmt.Fprintf(&buffer, "Pipeline: eligible inventory: %d; retrieved candidates: %s; task gate: %d; repository gate: %d; lifecycle: %d; threshold: %d; qualified: %d\n",
		output.Pipeline.EligibleInventory, retrieved, output.Pipeline.TaskGateRejections,
		output.Pipeline.RepositoryRejections, output.Pipeline.LifecycleRejections,
		output.Pipeline.ThresholdRejections, output.Pipeline.QualifiedCandidates)
	if len(output.Pipeline.Retrievals) > 0 {
		fmt.Fprintln(&buffer, "Retrieval ceilings:")
		for _, retrieval := range output.Pipeline.Retrievals {
			completeness := "complete"
			if !retrieval.CountComplete {
				completeness = "incomplete"
			}
			signals := make([]string, len(retrieval.Signals))
			for index, signal := range retrieval.Signals {
				signals[index] = string(signal)
			}
			fmt.Fprintf(&buffer, "- %s: %d retrieved, ceiling %d (%s)\n", strings.Join(signals, ", "), retrieval.Retrieved, retrieval.Limit, completeness)
		}
	}
	fmt.Fprintln(&buffer)
	if len(output.Memories) == 0 {
		fmt.Fprintln(&buffer, "No relevant durable memories found for this task.")
		if !taskProvided {
			fmt.Fprintln(&buffer, "Provide --task \"<intent>\" to guide selection.")
		}
	} else {
		for _, selected := range output.Memories {
			memory := selected.Memory
			fmt.Fprintf(&buffer, "### #%d [%s] %s\n", memory.ID, memory.Type, memory.Title)
			fmt.Fprintf(&buffer, "Scope: %s\n\n%s\n\n", memory.Scope, memory.Content)
			fmt.Fprintln(&buffer, "Selection evidence:")
			for _, evidence := range selected.Evidence {
				fmt.Fprintf(&buffer, "- %s: matched %s in %s", evidence.Signal, strings.Join(evidence.MatchedTerms, ", "), strings.Join(evidence.MatchedFields, ", "))
				if len(evidence.MatchedIdentifiers) > 0 {
					fmt.Fprintf(&buffer, "; identifiers %s", strings.Join(evidence.MatchedIdentifiers, ", "))
				}
				if len(evidence.MatchedDistinctiveTerms) > 0 {
					fmt.Fprintf(&buffer, "; distinctive %s", strings.Join(evidence.MatchedDistinctiveTerms, ", "))
				}
				fmt.Fprintln(&buffer)
			}
			if selected.PinBoost > 0 {
				fmt.Fprintf(&buffer, "- pin: relevant pinned memory received +%d\n", selected.PinBoost)
			}
			fmt.Fprintln(&buffer)
		}
	}
	if len(output.Diagnostics) > 0 {
		fmt.Fprintln(&buffer, "Diagnostics:")
		for _, diagnostic := range output.Diagnostics {
			fmt.Fprintf(&buffer, "- %s: %s\n", diagnostic.Code, formatContextBriefingDiagnostic(diagnostic))
		}
	}
	if len(output.Rejections) > 0 || output.RejectionDetailsOmitted > 0 {
		fmt.Fprintln(&buffer, "Rejections:")
		for _, rejection := range output.Rejections {
			fmt.Fprintf(&buffer, "- memory #%d: %s/%s", rejection.MemoryID, rejection.Stage, rejection.ReasonCode)
			if rejection.TaskEvidence != nil {
				fmt.Fprintf(&buffer, "; task evidence %d/%d", rejection.TaskEvidence.Matched, rejection.TaskEvidence.Required)
			}
			if rejection.Score != nil && rejection.Threshold != nil {
				fmt.Fprintf(&buffer, "; score %d/%d", *rejection.Score, *rejection.Threshold)
			}
			fmt.Fprintln(&buffer)
		}
		if output.RejectionDetailsOmitted > 0 {
			fmt.Fprintf(&buffer, "- %d additional rejection details omitted\n", output.RejectionDetailsOmitted)
		}
	}
	if output.EmptyResultReason != "" {
		fmt.Fprintf(&buffer, "Empty result: %s\n", output.EmptyResultReason)
	}
	if output.Fallback != nil {
		fmt.Fprintf(&buffer, "Targeted search fallback (%s): anchors %s\n", output.Fallback.ReasonCode, strings.Join(output.Fallback.Anchors, ", "))
		fmt.Fprintf(&buffer, "Command: %s\n", formatSearchInvocation(output.Fallback.Invocation))
	}
	if output.FallbackOmitted {
		fmt.Fprintf(&buffer, "Targeted search fallback omitted: %s\n", output.FallbackOmissionReason)
	}
	fmt.Fprintf(&buffer, "Omitted: %d by result limit, %d by output budget\n", output.ResultLimitOmissions, output.BudgetOmissions)
	return buffer.Bytes()
}

func formatSearchInvocation(invocation taskbriefing.SearchInvocation) string {
	parts := []string{invocation.Command}
	for _, arg := range invocation.Args {
		parts = append(parts, shellQuote(arg))
	}
	return strings.Join(parts, " ")
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func formatContextBriefingDiagnostic(diagnostic taskbriefing.Diagnostic) string {
	switch diagnostic.Code {
	case taskbriefing.DiagnosticGitOperationFailed:
		if len(diagnostic.Sources) == 0 {
			return "Some repository evidence was unavailable."
		}
		sources := make([]string, len(diagnostic.Sources))
		for index, source := range diagnostic.Sources {
			sources[index] = string(source)
		}
		return fmt.Sprintf("Some repository evidence was unavailable. Failed sources: %s; remaining usable signals were retained.", strings.Join(sources, ", "))
	case taskbriefing.DiagnosticTaskInputTruncated, taskbriefing.DiagnosticRepositoryInputTruncated:
		message := "Repository evidence exceeded deterministic input bounds."
		if diagnostic.Code == taskbriefing.DiagnosticTaskInputTruncated {
			message = "Task intent exceeded the deterministic input bound."
		}
		if len(diagnostic.Truncations) == 0 {
			return message
		}
		truncations := make([]string, len(diagnostic.Truncations))
		for index, truncation := range diagnostic.Truncations {
			truncations[index] = fmt.Sprintf("%s: %d total, %d analyzed, %d omitted", truncation.Signal, truncation.TotalTerms, truncation.AnalyzedTerms, truncation.OmittedTerms)
			if !truncation.CountComplete {
				truncations[index] += " (prefix count; acquisition cutoff reached)"
			}
		}
		return message + " Truncation: " + strings.Join(truncations, "; ")
	case taskbriefing.DiagnosticNoUsableSignals:
		return "No usable task or repository signal was available."
	case taskbriefing.DiagnosticRepositoryProjectUnresolved:
		return "Repository evidence was ignored because its project was unresolved."
	case taskbriefing.DiagnosticRepositoryProjectMismatch:
		return "Repository evidence was ignored because it belongs to another project."
	case taskbriefing.DiagnosticBranchBaseUnresolved:
		return "Committed branch evidence was unavailable because no base resolved."
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

func hasContextBriefingDiagnostic(diagnostics []taskbriefing.Diagnostic, code taskbriefing.DiagnosticCode) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

func withoutContextBriefingDiagnostic(diagnostics []taskbriefing.Diagnostic, code taskbriefing.DiagnosticCode) []taskbriefing.Diagnostic {
	filtered := make([]taskbriefing.Diagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if diagnostic.Code != code {
			filtered = append(filtered, diagnostic)
		}
	}
	return filtered
}
