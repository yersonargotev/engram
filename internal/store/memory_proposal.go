package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var memoryProposalReasonCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

const MaxMemoryProposalReasonCodes = 32

type MemoryProposalCategory string

const (
	MemoryProposalExplicitRequest MemoryProposalCategory = "explicit_request"
	MemoryProposalDecision        MemoryProposalCategory = "decision"
	MemoryProposalRootCause       MemoryProposalCategory = "root_cause"
	MemoryProposalInvariant       MemoryProposalCategory = "invariant"
	MemoryProposalConstraint      MemoryProposalCategory = "constraint"
	MemoryProposalPreference      MemoryProposalCategory = "preference"
	MemoryProposalLearning        MemoryProposalCategory = "learning"
)

const (
	MemoryProposalReasonExplicitUserRequest      = "explicit_user_request"
	MemoryProposalReasonStructuredSection        = "structured_section"
	MemoryProposalReasonProtectedProposal        = "protected_proposal"
	MemoryProposalReasonEmptyContent             = "empty_content"
	MemoryProposalReasonNormalizedExactDuplicate = "normalized_exact_duplicate"
	MemoryProposalReasonRedactedOnly             = "redacted_only"
	MemoryProposalReasonRequiresReview           = "requires_review"
)

var validMemoryProposalTypes = map[string]struct{}{
	"manual": {}, "decision": {}, "bugfix": {}, "architecture": {},
	"policy": {}, "preference": {}, "discovery": {},
}

var validMemoryProposalScopes = map[string]struct{}{
	"project": {}, "personal": {}, "global": {},
}

var validMemoryProposalCategories = map[string]struct{}{
	string(MemoryProposalExplicitRequest): {}, string(MemoryProposalDecision): {},
	string(MemoryProposalRootCause): {}, string(MemoryProposalInvariant): {},
	string(MemoryProposalConstraint): {}, string(MemoryProposalPreference): {},
	string(MemoryProposalLearning): {},
}

var validMemoryProposalReasonCodes = map[string]struct{}{
	MemoryProposalReasonExplicitUserRequest: {}, MemoryProposalReasonStructuredSection: {},
	MemoryProposalReasonProtectedProposal: {}, MemoryProposalReasonEmptyContent: {},
	MemoryProposalReasonNormalizedExactDuplicate: {}, MemoryProposalReasonRedactedOnly: {},
	MemoryProposalReasonRequiresReview: {},
}

// MemoryProposal is potentially durable knowledge retained for explicit review.
// It is local-only and remains distinct from both a Memory and a Shadow admission
// assessment.
type MemoryProposal struct {
	ID           string   `json:"id"`
	Project      string   `json:"project"`
	Type         string   `json:"type"`
	Title        string   `json:"title"`
	Content      string   `json:"content"`
	Scope        string   `json:"scope"`
	Category     string   `json:"category"`
	Protected    bool     `json:"protected"`
	EvidenceRefs []string `json:"evidence_refs"`
	ReasonCodes  []string `json:"reason_codes"`
	CreatedAt    string   `json:"created_at"`
}

type MemoryProposalInput struct {
	Type         string
	Title        string
	Content      string
	Scope        string
	Category     string
	Protected    bool
	EvidenceRefs []string
	ReasonCodes  []string
}

// CreateMemoryProposal retains one local proposal without assessing or
// promoting it. Checkpoint finalization uses the same transaction helper when
// creation and attachment must commit atomically.
func (s *Store) CreateMemoryProposal(project string, input MemoryProposalInput) (*MemoryProposal, error) {
	var proposal *MemoryProposal
	err := s.withTx(func(tx *sql.Tx) error {
		var err error
		proposal, err = createMemoryProposalTx(tx, project, input)
		return err
	})
	if err != nil {
		return nil, err
	}
	return proposal, nil
}

func createMemoryProposalTx(tx *sql.Tx, project string, input MemoryProposalInput) (*MemoryProposal, error) {
	project, _ = NormalizeProject(project)
	input.Type = RedactPrivateBlocks(input.Type)
	input.Title = RedactPrivateBlocks(input.Title)
	input.Content = RedactPrivateBlocks(input.Content)
	input.Scope = RedactPrivateBlocks(input.Scope)
	input.Category = RedactPrivateBlocks(input.Category)
	var err error
	input.ReasonCodes, err = normalizeMemoryProposalReasonCodes(input.ReasonCodes)
	if err != nil {
		return nil, err
	}
	evidenceRefs, err := normalizeLocalEvidenceReferences(input.EvidenceRefs, "Memory proposal")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCheckpointInvalidReferences, err)
	}
	_, validType := validMemoryProposalTypes[input.Type]
	_, validScope := validMemoryProposalScopes[input.Scope]
	_, validCategory := validMemoryProposalCategories[input.Category]
	if project == "" || !validType || !validScope || !validCategory ||
		strings.TrimSpace(input.Title) == "" || strings.TrimSpace(input.Content) == "" {
		return nil, ErrCheckpointInvalidReferences
	}
	evidenceJSON, err := json.Marshal(evidenceRefs)
	if err != nil {
		return nil, fmt.Errorf("marshal Memory proposal evidence references: %w", err)
	}
	reasonJSON, err := json.Marshal(input.ReasonCodes)
	if err != nil {
		return nil, fmt.Errorf("marshal Memory proposal reason codes: %w", err)
	}

	proposal := &MemoryProposal{
		ID:           newSyncID("proposal"),
		Project:      project,
		Type:         input.Type,
		Title:        input.Title,
		Content:      input.Content,
		Scope:        input.Scope,
		Category:     input.Category,
		Protected:    input.Protected,
		EvidenceRefs: evidenceRefs,
		ReasonCodes:  input.ReasonCodes,
	}
	if _, err := tx.Exec(`
		INSERT INTO memory_proposals (
			id, project, type, title, content, scope, category, protected,
			evidence_refs, reason_codes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		proposal.ID, proposal.Project, proposal.Type, proposal.Title, proposal.Content,
		proposal.Scope, proposal.Category, proposal.Protected, string(evidenceJSON), string(reasonJSON),
	); err != nil {
		return nil, fmt.Errorf("insert Memory proposal: %w", err)
	}
	if err := tx.QueryRow(`SELECT created_at FROM memory_proposals WHERE id = ?`, proposal.ID).Scan(&proposal.CreatedAt); err != nil {
		return nil, fmt.Errorf("read Memory proposal timestamp: %w", err)
	}
	return proposal, nil
}

func normalizeMemoryProposalReasonCodes(values []string) ([]string, error) {
	if len(values) > MaxMemoryProposalReasonCodes {
		return nil, ErrCheckpointInvalidReferences
	}
	result := make([]string, 0, len(values))
	for _, raw := range values {
		value := RedactPrivateBlocks(raw)
		_, valid := validMemoryProposalReasonCodes[value]
		if value != raw || !memoryProposalReasonCodePattern.MatchString(value) || !valid {
			return nil, ErrCheckpointInvalidReferences
		}
		result = append(result, value)
	}
	return result, nil
}

// GetMemoryProposal returns one local Memory proposal for an explicit review
// workflow. It never consults Memory search or Admission shadow state.
func (s *Store) GetMemoryProposal(id string) (*MemoryProposal, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, ErrCheckpointProposalNotFound
	}
	proposal, err := loadMemoryProposal(s.db, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCheckpointProposalNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get Memory proposal: %w", err)
	}
	return proposal, nil
}

type memoryProposalQuerier interface {
	QueryRow(query string, args ...any) *sql.Row
}

func loadMemoryProposal(q memoryProposalQuerier, id string) (*MemoryProposal, error) {
	var proposal MemoryProposal
	var evidenceJSON, reasonJSON string
	err := q.QueryRow(`
		SELECT id, project, type, title, content, scope, category, protected,
		       evidence_refs, reason_codes, created_at
		FROM memory_proposals WHERE id = ?`, id).Scan(
		&proposal.ID, &proposal.Project, &proposal.Type, &proposal.Title,
		&proposal.Content, &proposal.Scope, &proposal.Category, &proposal.Protected,
		&evidenceJSON, &reasonJSON, &proposal.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(evidenceJSON), &proposal.EvidenceRefs); err != nil {
		return nil, fmt.Errorf("decode Memory proposal evidence references: %w", err)
	}
	if err := json.Unmarshal([]byte(reasonJSON), &proposal.ReasonCodes); err != nil {
		return nil, fmt.Errorf("decode Memory proposal reason codes: %w", err)
	}
	return &proposal, nil
}
