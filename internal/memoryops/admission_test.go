package memoryops

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestPreviewAdmissionUsesLocalMemoriesWithoutPersisting(t *testing.T) {
	service := newTestService(t)
	saveObservation(t, service, "engram", "SQLite authority", "Local SQLite remains the source of truth.")

	before, err := service.store.Stats()
	if err != nil {
		t.Fatalf("stats before preview: %v", err)
	}

	result, err := service.PreviewAdmission(AdmissionPreviewInput{
		Project: " ENGRAM ",
		Evidence: EvidenceBundle{
			Version: EvidenceBundleVersion,
			Items: []EvidenceItem{
				{Reference: "prompt-1", Source: EvidenceSourceUserPrompt, Content: "Remember this: Explicit saves remain authoritative."},
				{Reference: "summary-1", Source: EvidenceSourceSessionSummary, Content: "## Key Learnings\n- Local SQLite remains the source of truth."},
			},
		},
	})
	if err != nil {
		t.Fatalf("preview admission: %v", err)
	}
	if result.Project != "engram" {
		t.Fatalf("project = %q, want engram", result.Project)
	}
	if len(result.Proposals) != 2 {
		t.Fatalf("proposals = %#v, want 2", result.Proposals)
	}
	if got := result.Proposals[0].Assessment.Recommendation; got != AdmissionAdmit {
		t.Fatalf("explicit assessment = %q, want admit", got)
	}
	if got := result.Proposals[1].Assessment.Recommendation; got != AdmissionReject {
		t.Fatalf("duplicate assessment = %q, want reject", got)
	}
	if !reflect.DeepEqual(result.Proposals[1].Assessment.ReasonCodes, []string{ReasonNormalizedExactDuplicate}) {
		t.Fatalf("duplicate reasons = %#v", result.Proposals[1].Assessment.ReasonCodes)
	}

	after, err := service.store.Stats()
	if err != nil {
		t.Fatalf("stats after preview: %v", err)
	}
	if after.TotalObservations != before.TotalObservations || after.TotalSessions != before.TotalSessions || after.TotalPrompts != before.TotalPrompts {
		t.Fatalf("preview mutated memory state: before=%#v after=%#v", before, after)
	}
}

func TestPreviewAdmissionIsDeterministicAndRedactsPrivateEvidence(t *testing.T) {
	service := newTestService(t)
	input := AdmissionPreviewInput{
		Project: "engram",
		Evidence: EvidenceBundle{
			Version: EvidenceBundleVersion,
			Items: []EvidenceItem{{
				Reference: "summary-1",
				Source:    EvidenceSourceSessionSummary,
				Content:   "## Key Learnings\n- The token <private>secret-value</private> must never appear.",
			}},
		},
	}

	first, err := service.PreviewAdmission(input)
	if err != nil {
		t.Fatalf("first preview: %v", err)
	}
	second, err := service.PreviewAdmission(input)
	if err != nil {
		t.Fatalf("second preview: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("preview is not deterministic:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if got := first.Proposals[0].Proposal.Content; got != "The token [REDACTED] must never appear." {
		t.Fatalf("redacted content = %q", got)
	}
}

func TestPreviewAdmissionValidatesBoundedEvidence(t *testing.T) {
	service := newTestService(t)
	tests := []struct {
		name     string
		input    AdmissionPreviewInput
		contains string
	}{
		{
			name:     "missing project",
			input:    AdmissionPreviewInput{Evidence: EvidenceBundle{Version: EvidenceBundleVersion}},
			contains: "project is required",
		},
		{
			name:     "unsupported version",
			input:    AdmissionPreviewInput{Project: "engram", Evidence: EvidenceBundle{Version: "v2"}},
			contains: "unsupported evidence bundle version",
		},
		{
			name:     "unknown source",
			input:    AdmissionPreviewInput{Project: "engram", Evidence: EvidenceBundle{Version: EvidenceBundleVersion, Items: []EvidenceItem{{Reference: "e1", Source: "transcript", Content: "content"}}}},
			contains: "unsupported evidence source",
		},
		{
			name:     "missing reference",
			input:    AdmissionPreviewInput{Project: "engram", Evidence: EvidenceBundle{Version: EvidenceBundleVersion, Items: []EvidenceItem{{Source: EvidenceSourceUserPrompt, Content: "Remember this: content"}}}},
			contains: "evidence reference is required",
		},
		{
			name:     "duplicate reference",
			input:    AdmissionPreviewInput{Project: "engram", Evidence: EvidenceBundle{Version: EvidenceBundleVersion, Items: []EvidenceItem{{Reference: "e1", Source: EvidenceSourceUserPrompt}, {Reference: "e1", Source: EvidenceSourceAgentNote}}}},
			contains: "duplicate evidence reference",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.PreviewAdmission(tc.input)
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("error = %v, want containing %q", err, tc.contains)
			}
		})
	}
}

func TestPreviewAdmissionEnforcesEvidenceSizeBounds(t *testing.T) {
	service := newTestService(t)
	tooMany := make([]EvidenceItem, MaxEvidenceItems+1)
	for index := range tooMany {
		tooMany[index] = EvidenceItem{Reference: string(rune('a' + index)), Source: EvidenceSourceAgentNote}
	}
	totalTooLarge := make([]EvidenceItem, 5)
	for index := range totalTooLarge {
		totalTooLarge[index] = EvidenceItem{
			Reference: string(rune('a' + index)),
			Source:    EvidenceSourceAgentNote,
			Content:   strings.Repeat("x", MaxEvidenceItemBytes),
		}
	}
	tests := []struct {
		name     string
		evidence EvidenceBundle
		contains string
	}{
		{
			name:     "too many items",
			evidence: EvidenceBundle{Version: EvidenceBundleVersion, Items: tooMany},
			contains: "maximum is 32",
		},
		{
			name: "item too large",
			evidence: EvidenceBundle{Version: EvidenceBundleVersion, Items: []EvidenceItem{{
				Reference: "large",
				Source:    EvidenceSourceAgentNote,
				Content:   strings.Repeat("x", MaxEvidenceItemBytes+1),
			}}},
			contains: "maximum is 16384",
		},
		{
			name:     "bundle too large",
			evidence: EvidenceBundle{Version: EvidenceBundleVersion, Items: totalTooLarge},
			contains: "maximum is 65536",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.PreviewAdmission(AdmissionPreviewInput{Project: "engram", Evidence: tc.evidence})
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("error = %v, want containing %q", err, tc.contains)
			}
		})
	}
}

func TestProtectedCategoryCannotBeRejectedWhenFlagIsMissing(t *testing.T) {
	service := newTestService(t)
	saveObservation(t, service, "engram", "Existing decision", "Keep SQLite authoritative.")
	assessment, err := service.assessMemoryProposal("engram", MemoryProposal{
		Type:         "decision",
		Title:        "Existing decision",
		Content:      "keep sqlite authoritative.",
		Scope:        "project",
		Category:     ProposalDecision,
		Protected:    false,
		EvidenceRefs: []string{"summary-1"},
	})
	if err != nil {
		t.Fatalf("assess protected category: %v", err)
	}
	if assessment.Recommendation != AdmissionReview {
		t.Fatalf("recommendation = %q, want review", assessment.Recommendation)
	}
}

func TestStructuredGenerationStopsAtUnrecognizedHeading(t *testing.T) {
	proposals, err := generateMemoryProposals(EvidenceBundle{
		Version: EvidenceBundleVersion,
		Items: []EvidenceItem{{
			Reference: "summary-1",
			Source:    EvidenceSourceSessionSummary,
			Content:   "## Decisions\n- Keep SQLite authoritative.\n\nNotes:\n- Finished three files.",
		}},
	})
	if err != nil {
		t.Fatalf("generate proposals: %v", err)
	}
	if len(proposals) != 1 || proposals[0].Content != "Keep SQLite authoritative." {
		t.Fatalf("proposals = %#v", proposals)
	}
}

func TestPreviewAdmissionAcquiresPersistedSessionEvidenceWithoutPersisting(t *testing.T) {
	service := newTestService(t)
	if err := service.store.CreateSession("session-v2", "engram", "/work/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	for _, content := range []string{
		"Investigate admission quality.",
		"Remember this: Explicit session requests remain authoritative.",
	} {
		if _, err := service.store.AddPrompt(store.AddPromptParams{SessionID: "session-v2", Project: "engram", Content: content}); err != nil {
			t.Fatalf("add prompt: %v", err)
		}
	}
	if err := service.store.EndSession("session-v2", "## Decisions\n- Session evidence is acquired in memoryops."); err != nil {
		t.Fatalf("end session: %v", err)
	}
	before, err := service.store.Stats()
	if err != nil {
		t.Fatalf("stats before: %v", err)
	}

	result, err := service.PreviewAdmission(AdmissionPreviewInput{Project: "engram", SessionID: "session-v2"})
	if err != nil {
		t.Fatalf("preview session: %v", err)
	}
	if result.Acquisition == nil || result.Acquisition.SessionID != "session-v2" {
		t.Fatalf("acquisition = %#v", result.Acquisition)
	}
	if len(result.Acquisition.Sources) != 2 {
		t.Fatalf("coverage = %#v", result.Acquisition.Sources)
	}
	if result.Acquisition.Sources[0].Source != EvidenceSourceUserPrompt || result.Acquisition.Sources[0].AvailableItems != 2 || result.Acquisition.Sources[0].IncludedItems != 2 {
		t.Fatalf("prompt coverage = %#v", result.Acquisition.Sources[0])
	}
	if result.Acquisition.Sources[1].Source != EvidenceSourceSessionSummary || result.Acquisition.Sources[1].AvailableItems != 1 || result.Acquisition.Sources[1].IncludedItems != 1 {
		t.Fatalf("summary coverage = %#v", result.Acquisition.Sources[1])
	}
	if len(result.Proposals) != 2 || result.Proposals[0].Assessment.Recommendation != AdmissionAdmit || result.Proposals[1].Proposal.Category != ProposalDecision {
		t.Fatalf("proposals = %#v", result.Proposals)
	}
	after, err := service.store.Stats()
	if err != nil {
		t.Fatalf("stats after: %v", err)
	}
	if before.TotalObservations != after.TotalObservations || before.TotalSessions != after.TotalSessions || before.TotalPrompts != after.TotalPrompts {
		t.Fatalf("session preview mutated memory state: before=%#v after=%#v", before, after)
	}
}

func TestPreviewAdmissionFallsBackToPersistedSessionSummaryMemory(t *testing.T) {
	service := newTestService(t)
	if err := service.store.CreateSession("session-summary-memory", "engram", "/work/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := service.store.AddObservation(store.AddObservationParams{
		SessionID: "session-summary-memory", Type: "session_summary", Title: "Summary", Content: "## Key Learnings\n- Session summaries may be persisted as Memories.", Project: "engram", Scope: "project",
	}); err != nil {
		t.Fatalf("add session summary memory: %v", err)
	}

	result, err := service.PreviewAdmission(AdmissionPreviewInput{Project: "engram", SessionID: "session-summary-memory"})
	if err != nil {
		t.Fatalf("preview session: %v", err)
	}
	if len(result.Proposals) != 1 || result.Proposals[0].Proposal.Content != "Session summaries may be persisted as Memories." {
		t.Fatalf("proposals = %#v", result.Proposals)
	}
}

func TestPreviewAdmissionSessionResolutionErrors(t *testing.T) {
	service := newTestService(t)
	if err := service.store.CreateSession("session-other", "other", "/work/other"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	tests := []struct {
		name  string
		input AdmissionPreviewInput
		want  error
	}{
		{name: "neither mode", input: AdmissionPreviewInput{Project: "engram"}, want: ErrAdmissionInputMode},
		{name: "missing session", input: AdmissionPreviewInput{Project: "engram", SessionID: "missing"}, want: ErrAdmissionSessionNotFound},
		{name: "project mismatch", input: AdmissionPreviewInput{Project: "engram", SessionID: "session-other"}, want: ErrAdmissionSessionProjectMismatch},
		{name: "both modes", input: AdmissionPreviewInput{Project: "engram", SessionID: "session-other", Evidence: EvidenceBundle{Version: EvidenceBundleVersion}}, want: ErrAdmissionInputMode},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.PreviewAdmission(tc.input)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestPreviewAdmissionSessionReportsEmptyAndBoundedEvidence(t *testing.T) {
	service := newTestService(t)
	if err := service.store.CreateSession("session-empty", "engram", "/work/engram"); err != nil {
		t.Fatalf("create empty session: %v", err)
	}
	empty, err := service.PreviewAdmission(AdmissionPreviewInput{Project: "engram", SessionID: "session-empty"})
	if err != nil {
		t.Fatalf("preview empty session: %v", err)
	}
	if !hasAdmissionDiagnostic(empty.Diagnostics, "session_evidence_empty") || !hasAdmissionDiagnostic(empty.Diagnostics, "no_memory_proposals") {
		t.Fatalf("diagnostics = %#v", empty.Diagnostics)
	}

	if err := service.store.CreateSession("session-bounded", "engram", "/work/engram"); err != nil {
		t.Fatalf("create bounded session: %v", err)
	}
	for index := 0; index < MaxEvidenceItems+5; index++ {
		content := strings.Repeat("é", MaxEvidenceItemBytes)
		if index == MaxEvidenceItems+4 {
			content = "Remember this: Keep the latest bounded prompt. " + content
		}
		if _, err := service.store.AddPrompt(store.AddPromptParams{SessionID: "session-bounded", Project: "engram", Content: content}); err != nil {
			t.Fatalf("add bounded prompt %d: %v", index, err)
		}
	}
	bounded, err := service.PreviewAdmission(AdmissionPreviewInput{Project: "engram", SessionID: "session-bounded"})
	if err != nil {
		t.Fatalf("preview bounded session: %v", err)
	}
	coverage := bounded.Acquisition.Sources[0]
	if coverage.AvailableItems != MaxEvidenceItems+5 || coverage.OmittedItems == 0 || coverage.TruncatedItems == 0 {
		t.Fatalf("coverage = %#v", coverage)
	}
	if bounded.Acquisition.IncludedItems > MaxEvidenceItems || bounded.Acquisition.IncludedContentBytes > MaxEvidenceBundleBytes {
		t.Fatalf("acquisition exceeds bounds: %#v", bounded.Acquisition)
	}
	if !hasAdmissionDiagnostic(bounded.Diagnostics, "session_evidence_omitted") {
		t.Fatalf("diagnostics = %#v", bounded.Diagnostics)
	}
}

func hasAdmissionDiagnostic(diagnostics []AdmissionDiagnostic, code string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}
