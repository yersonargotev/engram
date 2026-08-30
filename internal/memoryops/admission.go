package memoryops

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/store"
)

const (
	// EvidenceBundleVersion identifies the deterministic preview input contract.
	EvidenceBundleVersion = "v1"
	// AdmissionGeneratorVersion identifies the deterministic proposal grammar.
	AdmissionGeneratorVersion = "v1"
	// AdmissionPolicyVersion identifies the deterministic assessment policy.
	AdmissionPolicyVersion = "v1"

	MaxEvidenceItems       = 32
	MaxEvidenceItemBytes   = 16 * 1024
	MaxEvidenceBundleBytes = 64 * 1024
)

var (
	ErrAdmissionInputMode              = errors.New("admission preview requires exactly one evidence source")
	ErrAdmissionSessionNotFound        = errors.New("admission preview session not found")
	ErrAdmissionSessionProjectMismatch = errors.New("admission preview session belongs to a different project")
)

type EvidenceSource string

const (
	EvidenceSourceUserPrompt       EvidenceSource = "user_prompt"
	EvidenceSourceSessionSummary   EvidenceSource = "session_summary"
	EvidenceSourceAgentNote        EvidenceSource = "agent_note"
	EvidenceSourceRepositorySignal EvidenceSource = "repository_signal"
	EvidenceSourceToolOutput       EvidenceSource = "tool_output"
)

// EvidenceItem is one bounded, provenance-bearing fragment. Repository signals
// and tool output may be supplied as context, but never create proposals in v1.
type EvidenceItem struct {
	Reference string         `json:"reference"`
	Source    EvidenceSource `json:"source"`
	Content   string         `json:"content"`
}

// EvidenceBundle is the complete, bounded input to one admission preview.
type EvidenceBundle struct {
	Version string         `json:"version"`
	Items   []EvidenceItem `json:"items"`
}

// ProposalCategory is Admission-owned vocabulary retained only while the
// experimental runtime remains present.
type ProposalCategory string

const (
	ProposalExplicitRequest ProposalCategory = "explicit_request"
	ProposalDecision        ProposalCategory = "decision"
	ProposalRootCause       ProposalCategory = "root_cause"
	ProposalInvariant       ProposalCategory = "invariant"
	ProposalConstraint      ProposalCategory = "constraint"
	ProposalPreference      ProposalCategory = "preference"
	ProposalLearning        ProposalCategory = "learning"
)

const (
	ReasonExplicitUserRequest      = "explicit_user_request"
	ReasonStructuredSection        = "structured_section"
	ReasonProtectedProposal        = "protected_proposal"
	ReasonEmptyContent             = "empty_content"
	ReasonNormalizedExactDuplicate = "normalized_exact_duplicate"
	ReasonRedactedOnly             = "redacted_only"
	ReasonRequiresReview           = "requires_review"
)

// MemoryProposal is potentially durable knowledge. It is never persisted by a
// preview and remains distinct from store.Observation.
type MemoryProposal struct {
	Type         string           `json:"type"`
	Title        string           `json:"title"`
	Content      string           `json:"content"`
	Scope        string           `json:"scope"`
	Category     ProposalCategory `json:"category"`
	Protected    bool             `json:"protected"`
	EvidenceRefs []string         `json:"evidence_refs"`
	ReasonCodes  []string         `json:"reason_codes"`
}

type AdmissionRecommendation string

const (
	AdmissionAdmit  AdmissionRecommendation = "admit"
	AdmissionReview AdmissionRecommendation = "review"
	AdmissionReject AdmissionRecommendation = "reject"
)

// AdmissionAssessment is advisory. It neither creates nor discards a Memory.
type AdmissionAssessment struct {
	Recommendation AdmissionRecommendation `json:"recommendation"`
	ReasonCodes    []string                `json:"reason_codes"`
	EvidenceRefs   []string                `json:"evidence_refs"`
}

type AssessedMemoryProposal struct {
	Proposal   MemoryProposal      `json:"proposal"`
	Assessment AdmissionAssessment `json:"assessment"`
}

type AdmissionDiagnostic struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type AdmissionPreviewInput struct {
	Project   string
	Evidence  EvidenceBundle
	SessionID string
}

type EvidenceSourceCoverage struct {
	Source         EvidenceSource `json:"source"`
	AvailableItems int            `json:"available_items"`
	IncludedItems  int            `json:"included_items"`
	OmittedItems   int            `json:"omitted_items"`
	TruncatedItems int            `json:"truncated_items"`
}

type AdmissionAcquisition struct {
	Mode                 string                   `json:"mode"`
	SessionID            string                   `json:"session_id"`
	EvidenceVersion      string                   `json:"evidence_version"`
	IncludedItems        int                      `json:"included_items"`
	IncludedContentBytes int                      `json:"included_content_bytes"`
	Sources              []EvidenceSourceCoverage `json:"sources"`
}

type AdmissionSessionProjectMismatchError struct {
	SessionID        string
	RequestedProject string
	SessionProject   string
}

func (e *AdmissionSessionProjectMismatchError) Error() string {
	return fmt.Sprintf("%s: session %q belongs to project %q, not %q", ErrAdmissionSessionProjectMismatch, e.SessionID, e.SessionProject, e.RequestedProject)
}

func (e *AdmissionSessionProjectMismatchError) Unwrap() error {
	return ErrAdmissionSessionProjectMismatch
}

type AdmissionPreviewResult struct {
	Mode        string                   `json:"mode"`
	Project     string                   `json:"project"`
	Acquisition *AdmissionAcquisition    `json:"acquisition,omitempty"`
	Proposals   []AssessedMemoryProposal `json:"proposals"`
	Diagnostics []AdmissionDiagnostic    `json:"diagnostics"`
}

// PreviewAdmission generates and assesses proposals without persisting any
// proposal or assessment. It consults existing local Memories only for exact
// normalized duplicate advice.
func (s *Service) PreviewAdmission(input AdmissionPreviewInput) (*AdmissionPreviewResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	project, _ := store.NormalizeProject(input.Project)
	if project == "" {
		return nil, ErrProjectRequired
	}
	sessionID := strings.TrimSpace(input.SessionID)
	evidenceProvided := input.Evidence.Version != "" || input.Evidence.Items != nil
	if (sessionID != "") == evidenceProvided {
		return nil, ErrAdmissionInputMode
	}

	bundle := input.Evidence
	var acquisition *AdmissionAcquisition
	var diagnostics []AdmissionDiagnostic
	if sessionID != "" {
		var err error
		bundle, acquisition, diagnostics, err = s.acquireSessionAdmissionEvidence(project, sessionID)
		if err != nil {
			return nil, err
		}
	}

	proposals, err := generateMemoryProposals(bundle)
	if err != nil {
		return nil, err
	}
	result := &AdmissionPreviewResult{
		Mode:        "shadow_preview",
		Project:     project,
		Acquisition: acquisition,
		Proposals:   make([]AssessedMemoryProposal, 0, len(proposals)),
		Diagnostics: diagnostics,
	}
	for _, proposal := range proposals {
		assessment, err := s.assessMemoryProposal(project, proposal)
		if err != nil {
			return nil, err
		}
		result.Proposals = append(result.Proposals, AssessedMemoryProposal{
			Proposal:   proposal,
			Assessment: assessment,
		})
	}
	if len(result.Proposals) == 0 {
		result.Diagnostics = append(result.Diagnostics, AdmissionDiagnostic{
			Code:    "no_memory_proposals",
			Message: "The bounded evidence did not match the deterministic v1 proposal grammar.",
		})
	}
	return result, nil
}

type acquiredEvidenceItem struct {
	reference string
	source    EvidenceSource
	content   string
}

func (s *Service) acquireSessionAdmissionEvidence(project, sessionID string) (EvidenceBundle, *AdmissionAcquisition, []AdmissionDiagnostic, error) {
	session, err := s.store.GetSession(sessionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, store.ErrSessionNotFound) {
			return EvidenceBundle{}, nil, nil, fmt.Errorf("%w: %q", ErrAdmissionSessionNotFound, sessionID)
		}
		return EvidenceBundle{}, nil, nil, fmt.Errorf("load admission preview session: %w", err)
	}
	sessionProject, _ := store.NormalizeProject(session.Project)
	if sessionProject != project {
		return EvidenceBundle{}, nil, nil, &AdmissionSessionProjectMismatchError{
			SessionID: sessionID, RequestedProject: project, SessionProject: sessionProject,
		}
	}

	summaryItem, err := s.sessionSummaryEvidence(session)
	if err != nil {
		return EvidenceBundle{}, nil, nil, err
	}
	summaryAvailable := 0
	if summaryItem != nil {
		summaryAvailable = 1
	}
	promptLimit := MaxEvidenceItems - summaryAvailable
	prompts, totalPrompts, err := s.store.SessionPrompts(sessionID, promptLimit)
	if err != nil {
		return EvidenceBundle{}, nil, nil, fmt.Errorf("load admission preview prompts: %w", err)
	}

	acquisition := &AdmissionAcquisition{
		Mode:            "session",
		SessionID:       sessionID,
		EvidenceVersion: EvidenceBundleVersion,
		Sources: []EvidenceSourceCoverage{
			{Source: EvidenceSourceUserPrompt, AvailableItems: totalPrompts},
			{Source: EvidenceSourceSessionSummary, AvailableItems: summaryAvailable},
		},
	}
	diagnostics := make([]AdmissionDiagnostic, 0, 4)
	if totalPrompts == 0 {
		diagnostics = append(diagnostics, AdmissionDiagnostic{Code: "session_prompts_unavailable", Message: "The session has no persisted user prompts."})
	}
	if summaryAvailable == 0 {
		diagnostics = append(diagnostics, AdmissionDiagnostic{Code: "session_summary_unavailable", Message: "The session has no persisted session summary."})
	}

	items := make([]EvidenceItem, 0, len(prompts)+summaryAvailable)
	remainingBytes := MaxEvidenceBundleBytes
	var boundedSummary *EvidenceItem
	if summaryItem != nil {
		item, truncated := boundAcquiredEvidence(*summaryItem, min(MaxEvidenceItemBytes, remainingBytes))
		if item.Content != "" {
			boundedSummary = &item
			remainingBytes -= len([]byte(item.Content))
			acquisition.Sources[1].IncludedItems = 1
			if truncated {
				acquisition.Sources[1].TruncatedItems = 1
			}
		} else {
			acquisition.Sources[1].OmittedItems = 1
		}
	}

	boundedPrompts := make([]EvidenceItem, 0, len(prompts))
	for index := len(prompts) - 1; index >= 0; index-- {
		if remainingBytes <= 0 {
			break
		}
		prompt := prompts[index]
		reference := "prompt:" + strconv.FormatInt(prompt.ID, 10)
		item, truncated := boundAcquiredEvidence(acquiredEvidenceItem{
			reference: reference,
			source:    EvidenceSourceUserPrompt,
			content:   prompt.Content,
		}, min(MaxEvidenceItemBytes, remainingBytes))
		if item.Content == "" {
			continue
		}
		boundedPrompts = append(boundedPrompts, item)
		remainingBytes -= len([]byte(item.Content))
		acquisition.Sources[0].IncludedItems++
		if truncated {
			acquisition.Sources[0].TruncatedItems++
		}
	}
	for index := len(boundedPrompts) - 1; index >= 0; index-- {
		items = append(items, boundedPrompts[index])
	}
	if boundedSummary != nil {
		items = append(items, *boundedSummary)
	}

	for index := range acquisition.Sources {
		coverage := &acquisition.Sources[index]
		coverage.OmittedItems = max(coverage.OmittedItems, coverage.AvailableItems-coverage.IncludedItems)
		acquisition.IncludedItems += coverage.IncludedItems
	}
	for _, item := range items {
		acquisition.IncludedContentBytes += len([]byte(item.Content))
	}
	if acquisition.IncludedItems == 0 {
		diagnostics = append(diagnostics, AdmissionDiagnostic{Code: "session_evidence_empty", Message: "The session has no persisted evidence available for admission preview."})
	}
	if acquisition.Sources[0].OmittedItems > 0 || acquisition.Sources[0].TruncatedItems > 0 || acquisition.Sources[1].OmittedItems > 0 || acquisition.Sources[1].TruncatedItems > 0 {
		diagnostics = append(diagnostics, AdmissionDiagnostic{Code: "session_evidence_omitted", Message: "Session evidence was bounded by the v1 item or byte limits; coverage reports omissions and truncation."})
	}

	return EvidenceBundle{Version: EvidenceBundleVersion, Items: items}, acquisition, diagnostics, nil
}

func (s *Service) sessionSummaryEvidence(session *store.Session) (*acquiredEvidenceItem, error) {
	observation, err := s.store.LatestSessionObservationByType(session.ID, "session_summary")
	if err != nil {
		return nil, fmt.Errorf("load admission preview session summary: %w", err)
	}
	if observation != nil && strings.TrimSpace(observation.Content) != "" {
		reference := "summary:" + strconv.FormatInt(observation.ID, 10)
		return &acquiredEvidenceItem{reference: reference, source: EvidenceSourceSessionSummary, content: observation.Content}, nil
	}
	if session.Summary == nil || strings.TrimSpace(*session.Summary) == "" {
		return nil, nil
	}
	return &acquiredEvidenceItem{
		reference: "session-summary",
		source:    EvidenceSourceSessionSummary,
		content:   *session.Summary,
	}, nil
}

func boundAcquiredEvidence(input acquiredEvidenceItem, maxBytes int) (EvidenceItem, bool) {
	content := strings.TrimSpace(redactPrivateContent(input.content))
	bounded, truncated := truncateUTF8Bytes(content, maxBytes)
	return EvidenceItem{Reference: input.reference, Source: input.source, Content: bounded}, truncated
}

func truncateUTF8Bytes(value string, maxBytes int) (string, bool) {
	if maxBytes <= 0 {
		return "", value != ""
	}
	if len([]byte(value)) <= maxBytes {
		return value, false
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return strings.TrimSpace(value[:end]), true
}

type sectionDefinition struct {
	category  ProposalCategory
	typ       string
	protected bool
}

var sectionDefinitions = map[string]sectionDefinition{
	"decisions":          {category: ProposalDecision, typ: "decision", protected: true},
	"decisiones":         {category: ProposalDecision, typ: "decision", protected: true},
	"root causes":        {category: ProposalRootCause, typ: "bugfix", protected: true},
	"causas raíz":        {category: ProposalRootCause, typ: "bugfix", protected: true},
	"causas raiz":        {category: ProposalRootCause, typ: "bugfix", protected: true},
	"invariants":         {category: ProposalInvariant, typ: "architecture", protected: true},
	"invariantes":        {category: ProposalInvariant, typ: "architecture", protected: true},
	"constraints":        {category: ProposalConstraint, typ: "policy", protected: true},
	"restricciones":      {category: ProposalConstraint, typ: "policy", protected: true},
	"preferences":        {category: ProposalPreference, typ: "preference", protected: true},
	"preferencias":       {category: ProposalPreference, typ: "preference", protected: true},
	"key learning":       {category: ProposalLearning, typ: "discovery", protected: false},
	"key learnings":      {category: ProposalLearning, typ: "discovery", protected: false},
	"learning":           {category: ProposalLearning, typ: "discovery", protected: false},
	"learnings":          {category: ProposalLearning, typ: "discovery", protected: false},
	"aprendizaje clave":  {category: ProposalLearning, typ: "discovery", protected: false},
	"aprendizajes clave": {category: ProposalLearning, typ: "discovery", protected: false},
	"aprendizaje":        {category: ProposalLearning, typ: "discovery", protected: false},
	"aprendizajes":       {category: ProposalLearning, typ: "discovery", protected: false},
}

var (
	listItemPattern  = regexp.MustCompile(`^\s*(?:[-*+]|\d+[.)])\s+(.+?)\s*$`)
	redactionPattern = regexp.MustCompile(`(?i)\[redacted\]|<redacted>|\bredacted\b|\*+`)
	boldPattern      = regexp.MustCompile(`\*\*([^*]+)\*\*`)
	italicPattern    = regexp.MustCompile(`\*([^*]+)\*`)
	codePattern      = regexp.MustCompile("`([^`]+)`")
)

func generateMemoryProposals(bundle EvidenceBundle) ([]MemoryProposal, error) {
	if err := validateEvidenceBundle(bundle); err != nil {
		return nil, err
	}
	proposals := make([]MemoryProposal, 0)
	seen := make(map[string]int)
	for _, item := range bundle.Items {
		content := redactPrivateContent(item.Content)
		switch item.Source {
		case EvidenceSourceUserPrompt:
			for _, explicit := range explicitMemoryRequests(content) {
				appendMemoryProposal(&proposals, seen, newMemoryProposal(
					explicit,
					"manual",
					ProposalExplicitRequest,
					true,
					strings.TrimSpace(item.Reference),
					ReasonExplicitUserRequest,
				))
			}
		case EvidenceSourceSessionSummary, EvidenceSourceAgentNote:
			for _, proposal := range structuredMemoryProposals(content, strings.TrimSpace(item.Reference)) {
				appendMemoryProposal(&proposals, seen, proposal)
			}
		case EvidenceSourceRepositorySignal, EvidenceSourceToolOutput:
			// Provenance-only sources in v1. They cannot formulate a proposal.
		}
	}
	return proposals, nil
}

func validateEvidenceBundle(bundle EvidenceBundle) error {
	if strings.TrimSpace(bundle.Version) != EvidenceBundleVersion {
		return fmt.Errorf("unsupported evidence bundle version %q: want %q", bundle.Version, EvidenceBundleVersion)
	}
	if len(bundle.Items) > MaxEvidenceItems {
		return fmt.Errorf("evidence bundle contains %d items: maximum is %d", len(bundle.Items), MaxEvidenceItems)
	}
	seenReferences := make(map[string]struct{}, len(bundle.Items))
	totalBytes := 0
	for index, item := range bundle.Items {
		reference := strings.TrimSpace(item.Reference)
		if reference == "" {
			return fmt.Errorf("evidence reference is required at item %d", index)
		}
		if _, exists := seenReferences[reference]; exists {
			return fmt.Errorf("duplicate evidence reference %q", reference)
		}
		seenReferences[reference] = struct{}{}
		switch item.Source {
		case EvidenceSourceUserPrompt, EvidenceSourceSessionSummary, EvidenceSourceAgentNote, EvidenceSourceRepositorySignal, EvidenceSourceToolOutput:
		default:
			return fmt.Errorf("unsupported evidence source %q", item.Source)
		}
		itemBytes := len([]byte(item.Content))
		if itemBytes > MaxEvidenceItemBytes {
			return fmt.Errorf("evidence item %q contains %d bytes: maximum is %d", reference, itemBytes, MaxEvidenceItemBytes)
		}
		totalBytes += itemBytes
		if totalBytes > MaxEvidenceBundleBytes {
			return fmt.Errorf("evidence bundle contains %d content bytes: maximum is %d", totalBytes, MaxEvidenceBundleBytes)
		}
	}
	return nil
}

func explicitMemoryRequests(content string) []string {
	requests := make([]string, 0)
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		lower := strings.ToLower(trimmed)
		for _, prefix := range []string{"remember this:", "recuerda esto:"} {
			if !strings.HasPrefix(lower, prefix) {
				continue
			}
			request := cleanProposalContent(trimmed[len(prefix):])
			if request != "" {
				requests = append(requests, request)
			}
			break
		}
	}
	return requests
}

func structuredMemoryProposals(content, evidenceRef string) []MemoryProposal {
	proposals := make([]MemoryProposal, 0)
	var active *sectionDefinition
	for _, line := range strings.Split(content, "\n") {
		if definition, recognized, heading := parseSectionHeading(line); heading {
			if recognized {
				copy := definition
				active = &copy
			} else {
				active = nil
			}
			continue
		}
		if active == nil {
			continue
		}
		match := listItemPattern.FindStringSubmatch(line)
		if len(match) != 2 {
			continue
		}
		item := cleanProposalContent(match[1])
		if item == "" {
			continue
		}
		proposals = append(proposals, newMemoryProposal(
			item,
			active.typ,
			active.category,
			active.protected,
			evidenceRef,
			ReasonStructuredSection,
		))
	}
	return proposals
}

func parseSectionHeading(line string) (sectionDefinition, bool, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return sectionDefinition{}, false, false
	}
	hasMarkdownHeading := strings.HasPrefix(trimmed, "#")
	hasPlainHeading := strings.HasSuffix(trimmed, ":")
	if hasMarkdownHeading {
		trimmed = strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
	}
	trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ":"))
	definition, recognized := sectionDefinitions[strings.ToLower(trimmed)]
	if recognized {
		return definition, true, true
	}
	return sectionDefinition{}, false, hasMarkdownHeading || hasPlainHeading
}

func newMemoryProposal(content, typ string, category ProposalCategory, protected bool, evidenceRef, reason string) MemoryProposal {
	return MemoryProposal{
		Type:         typ,
		Title:        proposalTitle(content),
		Content:      content,
		Scope:        "project",
		Category:     category,
		Protected:    protected,
		EvidenceRefs: []string{evidenceRef},
		ReasonCodes:  []string{reason},
	}
}

func appendMemoryProposal(proposals *[]MemoryProposal, seen map[string]int, proposal MemoryProposal) {
	key := normalizeProposalContent(proposal.Content)
	if key == "" {
		return
	}
	if index, exists := seen[key]; exists {
		existing := &(*proposals)[index]
		for _, ref := range proposal.EvidenceRefs {
			if !containsString(existing.EvidenceRefs, ref) {
				existing.EvidenceRefs = append(existing.EvidenceRefs, ref)
			}
		}
		return
	}
	seen[key] = len(*proposals)
	*proposals = append(*proposals, proposal)
}

func proposalTitle(content string) string {
	const maximumRunes = 80
	if utf8.RuneCountInString(content) <= maximumRunes {
		return content
	}
	runes := []rune(content)
	return strings.TrimSpace(string(runes[:maximumRunes])) + "..."
}

func cleanProposalContent(content string) string {
	content = redactPrivateContent(content)
	content = boldPattern.ReplaceAllString(content, "$1")
	content = codePattern.ReplaceAllString(content, "$1")
	content = italicPattern.ReplaceAllString(content, "$1")
	return strings.TrimSpace(strings.Join(strings.Fields(content), " "))
}

func redactPrivateContent(content string) string {
	return store.RedactPrivateBlocks(content)
}

func normalizeProposalContent(content string) string {
	return strings.ToLower(strings.Join(strings.Fields(content), " "))
}

func (s *Service) assessMemoryProposal(project string, proposal MemoryProposal) (AdmissionAssessment, error) {
	if err := s.requireStore(); err != nil {
		return AdmissionAssessment{}, err
	}
	assessment := AdmissionAssessment{EvidenceRefs: append([]string{}, proposal.EvidenceRefs...)}
	if proposal.Category == ProposalExplicitRequest || containsString(proposal.ReasonCodes, ReasonExplicitUserRequest) {
		assessment.Recommendation = AdmissionAdmit
		assessment.ReasonCodes = []string{ReasonExplicitUserRequest}
		return assessment, nil
	}

	issueReason := ""
	content := strings.TrimSpace(proposal.Content)
	switch {
	case content == "":
		issueReason = ReasonEmptyContent
	case isRedactedOnly(content):
		issueReason = ReasonRedactedOnly
	default:
		exists, err := s.store.ObservationContentExists(content, project, proposal.Scope)
		if err != nil {
			return AdmissionAssessment{}, fmt.Errorf("check normalized exact duplicate: %w", err)
		}
		if exists {
			issueReason = ReasonNormalizedExactDuplicate
		}
	}

	if isProtectedProposal(proposal) {
		assessment.Recommendation = AdmissionReview
		assessment.ReasonCodes = []string{ReasonProtectedProposal}
		if issueReason != "" {
			assessment.ReasonCodes = append(assessment.ReasonCodes, issueReason)
		}
		assessment.ReasonCodes = append(assessment.ReasonCodes, ReasonRequiresReview)
		return assessment, nil
	}
	if issueReason != "" {
		assessment.Recommendation = AdmissionReject
		assessment.ReasonCodes = []string{issueReason}
		return assessment, nil
	}
	assessment.Recommendation = AdmissionReview
	assessment.ReasonCodes = []string{ReasonRequiresReview}
	return assessment, nil
}

func isProtectedProposal(proposal MemoryProposal) bool {
	if proposal.Protected {
		return true
	}
	switch proposal.Category {
	case ProposalExplicitRequest, ProposalDecision, ProposalRootCause, ProposalInvariant, ProposalConstraint, ProposalPreference:
		return true
	default:
		return false
	}
}

func isRedactedOnly(content string) bool {
	withoutMarkers := redactionPattern.ReplaceAllString(content, "")
	for _, character := range withoutMarkers {
		if unicode.IsLetter(character) || unicode.IsNumber(character) {
			return false
		}
	}
	return true
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
