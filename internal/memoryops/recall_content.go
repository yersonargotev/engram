package memoryops

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/store"
)

type RecallContentInput struct {
	RecallID        string
	ResultID        string
	Project         string
	Scope           string
	AllProjects     bool
	ProjectStrength project.IdentityStrength
	DeliberateScope bool
	Position        int
	BinaryVersion   string
	BinaryRevision  string
}

type RecallMemoryContent struct {
	ID      int64  `json:"id"`
	SyncID  string `json:"sync_id"`
	Title   string `json:"title"`
	Type    string `json:"type"`
	Project string `json:"project,omitempty"`
	Scope   string `json:"scope"`
	Content string `json:"content"`
}

type RecallContentResult struct {
	RecallID             string              `json:"recall_id"`
	ResultID             string              `json:"result_id"`
	Memory               RecallMemoryContent `json:"memory"`
	Position             int                 `json:"position"`
	OriginalBytes        int                 `json:"original_bytes"`
	DeliveredUTF8Bytes   int                 `json:"delivered_utf8_bytes"`
	LimitBytes           int                 `json:"limit_bytes"`
	Truncated            bool                `json:"truncated"`
	ContinuationPosition *int                `json:"continuation_position,omitempty"`
	Replayed             bool                `json:"replayed"`
	ElapsedMonotonicMS   int64               `json:"elapsed_monotonic_ms"`
	Provenance           RecallProvenance    `json:"provenance"`
	Warning              *RecallWarning      `json:"warning,omitempty"`
	Diagnostics          []RecallDiagnostic  `json:"diagnostics,omitempty"`
}

func (s *Service) RecallContent(input RecallContentInput) (*RecallContentResult, error) {
	return s.RecallContentContext(context.Background(), input)
}

func (s *Service) RecallContentContext(ctx context.Context, input RecallContentInput) (*RecallContentResult, error) {
	started := s.recallStartedAt()
	result := &RecallContentResult{
		RecallID: strings.TrimSpace(input.RecallID), ResultID: strings.TrimSpace(input.ResultID),
		Position: input.Position, LimitBytes: RecallContentBudgetBytes,
		Provenance: RecallProvenance{
			ProtocolVersion: protocolcontract.Version,
			BinaryVersion:   fallback(input.BinaryVersion, "unknown"),
			BinaryRevision:  strings.TrimSpace(input.BinaryRevision),
		},
	}
	defer func() { result.ElapsedMonotonicMS = s.recallElapsed(started).Milliseconds() }()

	var err error
	input.Scope, err = NormalizeRecallScope(input.Scope)
	if err != nil {
		setRecallContentFailure(result, "recall_selection_invalid", "recall_scope_invalid", err.Error())
		return result, nil
	}
	input.Project, _ = store.NormalizeProject(input.Project)
	if input.AllProjects && input.Project != "" {
		setRecallContentFailure(result, "recall_selection_invalid", "recall_scope_invalid", "project and all projects cannot be used together")
		return result, nil
	}
	if input.Scope != "project" && input.Project == "" && !input.AllProjects {
		setRecallContentFailure(result, "recall_selection_invalid", "recall_scope_invalid", "all projects is required for broad recall without an explicit project")
		return result, nil
	}
	if result.RecallID == "" || result.ResultID == "" {
		setRecallContentFailure(result, "recall_selection_invalid", "recall_selection_required", "recall_id and result_id are required")
		return result, nil
	}
	if input.Position < 0 {
		setRecallContentFailure(result, "recall_position_invalid", "recall_position_invalid", "position must be a non-negative UTF-8 byte position")
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		setRecallContentFailure(result, "recall_canceled", "recall_canceled", err.Error())
		return result, nil
	}
	if !recallAuthorityAllows(RecallInput{
		Project: input.Project, Scope: input.Scope, AllProjects: input.AllProjects,
		ProjectStrength: input.ProjectStrength, DeliberateScope: input.DeliberateScope,
	}) {
		if input.AllProjects || input.Scope != "project" {
			setRecallContentFailure(result, "recall_scope_relevance_required", "recall_scope_relevance_required", "broad scope requires explicit task relevance")
		} else {
			setRecallContentFailure(result, "recall_project_authority_required", project.WriteAuthorityErrorCode, "current project identity must be strong or explicit")
		}
		return result, nil
	}
	if err := s.requireStore(); err != nil {
		setRecallContentFailure(result, "recall_unavailable", "recall_store_failure", err.Error())
		return result, nil
	}

	selection, err := s.store.RecallSelectionContext(ctx, result.RecallID, result.ResultID, input.Project, input.Scope, input.AllProjects)
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			setRecallContentFailure(result, "recall_canceled", "recall_canceled", err.Error())
		case errors.Is(err, store.ErrRecallSelectionUnavailable):
			setRecallContentFailure(result, "recall_selection_invalid", "recall_selection_unavailable", err.Error())
		default:
			setRecallContentFailure(result, "recall_unavailable", "recall_store_failure", err.Error())
		}
		return result, nil
	}
	content := selection.Observation.Content
	if selection.Observation.RevisionCount != selection.RevisionCount || selection.CurrentLocalRevisionCount != selection.LocalRevisionCount {
		setRecallContentFailure(result, "recall_selection_invalid", "recall_memory_changed", "selected Memory changed after candidate Recall")
		return result, nil
	}
	if !utf8.ValidString(content) {
		setRecallContentFailure(result, "recall_selection_invalid", "recall_content_invalid_utf8", "selected Memory content is not valid UTF-8")
		return result, nil
	}
	if input.Position > 0 {
		authorized, continuationErr := s.store.RecallContinuationAuthorizedContext(ctx, result.RecallID, result.ResultID, input.Position)
		if continuationErr != nil {
			switch {
			case errors.Is(continuationErr, context.Canceled), errors.Is(continuationErr, context.DeadlineExceeded):
				setRecallContentFailure(result, "recall_canceled", "recall_canceled", continuationErr.Error())
			default:
				setRecallContentFailure(result, "recall_unavailable", "recall_store_failure", continuationErr.Error())
			}
			return result, nil
		}
		if !authorized {
			setRecallContentFailure(result, "recall_position_invalid", "recall_position_invalid", "position was not returned by a preceding segment for this Recall selection")
			return result, nil
		}
	}
	if input.Position >= len(content) || !utf8.RuneStart(content[input.Position]) {
		setRecallContentFailure(result, "recall_position_invalid", "recall_position_invalid", "position must identify the start of a UTF-8 code point within the selected Memory")
		return result, nil
	}

	end := input.Position + RecallContentBudgetBytes
	if end > len(content) {
		end = len(content)
	}
	for end < len(content) && end > input.Position && !utf8.RuneStart(content[end]) {
		end--
	}
	segment := content[input.Position:end]
	result.OriginalBytes = len(content)
	result.DeliveredUTF8Bytes = len(segment)
	result.Truncated = end < len(content)
	if result.Truncated {
		continuation := end
		result.ContinuationPosition = &continuation
	}
	observation := selection.Observation
	result.Memory = RecallMemoryContent{
		ID: observation.ID, SyncID: observation.SyncID, Title: observation.Title,
		Type: observation.Type, Scope: observation.Scope, Content: segment,
	}
	if observation.Project != nil {
		result.Memory.Project = *observation.Project
	}

	replayed, err := s.store.RecordRecallSegmentContext(ctx, store.RecallSegmentRecord{
		RecallID: result.RecallID, ResultID: result.ResultID, ObservationID: observation.ID,
		RevisionCount: selection.RevisionCount, LocalRevisionCount: selection.LocalRevisionCount,
		Position: input.Position, OriginalBytes: result.OriginalBytes,
		DeliveredBytes: result.DeliveredUTF8Bytes, LimitBytes: result.LimitBytes,
		Truncated: result.Truncated, ContinuationPosition: result.ContinuationPosition,
	})
	if err != nil {
		result.Memory = RecallMemoryContent{}
		result.OriginalBytes = 0
		result.DeliveredUTF8Bytes = 0
		result.Truncated = false
		result.ContinuationPosition = nil
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			setRecallContentFailure(result, "recall_canceled", "recall_canceled", err.Error())
		} else if errors.Is(err, store.ErrRecallSelectionUnavailable) {
			setRecallContentFailure(result, "recall_selection_invalid", "recall_selection_unavailable", err.Error())
		} else {
			setRecallContentFailure(result, "recall_unavailable", "recall_store_failure", err.Error())
		}
		return result, nil
	}
	result.Replayed = replayed
	return result, nil
}

func setRecallContentFailure(result *RecallContentResult, warningCode, diagnosticCode, detail string) {
	messages := map[string][2]string{
		"recall_canceled":                   {"Complete Memory retrieval was canceled; continuing without content.", "Retry the same positioned request only if the Memory remains material."},
		"recall_unavailable":                {"Complete Memory retrieval is unavailable; continuing without content.", "Retry once only if the selected Memory remains material."},
		"recall_selection_invalid":          {"The selected Recall result is unavailable; continuing without content.", "Run a new narrow Recall and select one of its returned result identities."},
		"recall_position_invalid":           {"The Recall continuation position is invalid; continuing without content.", "Use only the continuation_position returned by the preceding segment."},
		"recall_project_authority_required": {"Complete Memory retrieval requires strong or explicit current project authority.", "Provide the exact current project and retry the selected result."},
		"recall_scope_relevance_required":   {"Complete Memory retrieval skipped because broad scope was not explicitly relevant.", "Retry only when the original broader Recall scope remains material."},
	}
	message := messages[warningCode]
	result.Warning = &RecallWarning{Code: warningCode, Message: message[0], NextAction: message[1]}
	result.Diagnostics = []RecallDiagnostic{{Code: diagnosticCode, Operation: "recall_content", Detail: detail}}
}
