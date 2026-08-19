package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const MaxAdmissionShadowReviewNoteLength = 4096

var ErrAdmissionShadowProposalNotFound = errors.New("admission shadow proposal not found")

type AdmissionShadowRun struct {
	ID                   string                    `json:"id"`
	Project              string                    `json:"project"`
	SessionID            string                    `json:"session_id,omitempty"`
	Mode                 string                    `json:"mode"`
	AdmissionVersion     string                    `json:"admission_version"`
	IncludedItems        int                       `json:"included_items"`
	IncludedContentBytes int                       `json:"included_content_bytes"`
	CreatedAt            string                    `json:"created_at"`
	Proposals            []AdmissionShadowProposal `json:"proposals,omitempty"`
}

type AdmissionShadowProposal struct {
	ID                    string                  `json:"id"`
	RunID                 string                  `json:"run_id"`
	Ordinal               int                     `json:"ordinal"`
	Type                  string                  `json:"type"`
	Title                 string                  `json:"title"`
	Content               string                  `json:"content"`
	Scope                 string                  `json:"scope"`
	Category              string                  `json:"category"`
	Protected             bool                    `json:"protected"`
	Recommendation        string                  `json:"recommendation"`
	ProposalReasonCodes   []string                `json:"proposal_reason_codes"`
	AssessmentReasonCodes []string                `json:"assessment_reason_codes"`
	EvidenceRefs          []string                `json:"evidence_refs"`
	CreatedAt             string                  `json:"created_at"`
	Reviews               []AdmissionShadowReview `json:"reviews"`
}

type AdmissionShadowReview struct {
	ID          string `json:"id"`
	ProposalID  string `json:"proposal_id"`
	Verdict     string `json:"verdict"`
	Note        string `json:"note"`
	Unsupported bool   `json:"unsupported"`
	PrivacyLeak bool   `json:"privacy_leak"`
	CreatedAt   string `json:"created_at"`
}

type AdmissionShadowProposalInput struct {
	Type                  string
	Title                 string
	Content               string
	Scope                 string
	Category              string
	Protected             bool
	Recommendation        string
	ProposalReasonCodes   []string
	AssessmentReasonCodes []string
	EvidenceRefs          []string
}

type CreateAdmissionShadowRunParams struct {
	Project              string
	SessionID            string
	Mode                 string
	AdmissionVersion     string
	IncludedItems        int
	IncludedContentBytes int
	Proposals            []AdmissionShadowProposalInput
}

type AddAdmissionShadowReviewParams struct {
	ProposalID  string
	Verdict     string
	Note        string
	Unsupported bool
	PrivacyLeak bool
}

// migrateAdmissionShadow creates the local-only persistence boundary for
// admission experiments. These tables intentionally have no sync triggers and
// are not part of ExportData.
func (s *Store) migrateAdmissionShadow() error {
	_, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS admission_shadow_runs (
			id                     TEXT PRIMARY KEY,
			project                TEXT NOT NULL,
			session_id             TEXT,
			mode                   TEXT NOT NULL,
			admission_version      TEXT NOT NULL,
			included_items         INTEGER NOT NULL DEFAULT 0 CHECK (included_items >= 0),
			included_content_bytes INTEGER NOT NULL DEFAULT 0 CHECK (included_content_bytes >= 0),
			created_at             TEXT NOT NULL DEFAULT (datetime('now'))
		);

		CREATE TABLE IF NOT EXISTS admission_shadow_proposals (
			id                      TEXT PRIMARY KEY,
			run_id                  TEXT NOT NULL,
			ordinal                 INTEGER NOT NULL CHECK (ordinal >= 0),
			type                    TEXT NOT NULL,
			title                   TEXT NOT NULL,
			content                 TEXT NOT NULL,
			scope                   TEXT NOT NULL,
			category                TEXT NOT NULL,
			protected               BOOLEAN NOT NULL DEFAULT 0,
			recommendation          TEXT NOT NULL,
			proposal_reason_codes   TEXT NOT NULL DEFAULT '[]',
			assessment_reason_codes TEXT NOT NULL DEFAULT '[]',
			evidence_refs           TEXT NOT NULL DEFAULT '[]',
			created_at              TEXT NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (run_id) REFERENCES admission_shadow_runs(id) ON DELETE CASCADE,
			UNIQUE (run_id, ordinal)
		);

		CREATE TABLE IF NOT EXISTS admission_shadow_reviews (
			id           TEXT PRIMARY KEY,
			proposal_id  TEXT NOT NULL,
			verdict      TEXT NOT NULL,
			note         TEXT NOT NULL DEFAULT '',
			unsupported  BOOLEAN NOT NULL DEFAULT 0,
			privacy_leak BOOLEAN NOT NULL DEFAULT 0,
			created_at   TEXT NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (proposal_id) REFERENCES admission_shadow_proposals(id) ON DELETE CASCADE
		);

		CREATE INDEX IF NOT EXISTS idx_admission_shadow_runs_project_created
			ON admission_shadow_runs(project, created_at, id);
		CREATE INDEX IF NOT EXISTS idx_admission_shadow_proposals_run_ordinal
			ON admission_shadow_proposals(run_id, ordinal);
		CREATE INDEX IF NOT EXISTS idx_admission_shadow_reviews_proposal_created
			ON admission_shadow_reviews(proposal_id, created_at, id);
	`)
	return err
}

// CreateAdmissionShadowRun atomically persists one immutable run and all of
// its proposal snapshots. Its input deliberately cannot carry raw evidence,
// prompts, or summaries.
func (s *Store) CreateAdmissionShadowRun(p CreateAdmissionShadowRunParams) (*AdmissionShadowRun, error) {
	p.Project = stripPrivateTags(p.Project)
	p.Project, _ = NormalizeProject(p.Project)
	p.Project = strings.TrimSpace(p.Project)
	p.SessionID = stripPrivateTags(p.SessionID)
	p.Mode = stripPrivateTags(p.Mode)
	p.AdmissionVersion = stripPrivateTags(p.AdmissionVersion)
	if p.Project == "" {
		return nil, fmt.Errorf("admission shadow run requires project")
	}
	if p.Mode == "" {
		return nil, fmt.Errorf("admission shadow run requires mode")
	}
	if p.AdmissionVersion == "" {
		return nil, fmt.Errorf("admission shadow run requires admission version")
	}
	if p.IncludedItems < 0 || p.IncludedContentBytes < 0 {
		return nil, fmt.Errorf("admission shadow acquisition counts must not be negative")
	}

	runID := newSyncID("shadow-run")
	err := s.withTx(func(tx *sql.Tx) error {
		if _, err := s.execHook(tx, `
			INSERT INTO admission_shadow_runs (
				id, project, session_id, mode, admission_version,
				included_items, included_content_bytes
			) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			runID, p.Project, nullableString(p.SessionID), p.Mode,
			p.AdmissionVersion, p.IncludedItems, p.IncludedContentBytes,
		); err != nil {
			return fmt.Errorf("insert admission shadow run: %w", err)
		}

		for ordinal, input := range p.Proposals {
			proposal, err := normalizeAdmissionShadowProposalInput(input)
			if err != nil {
				return fmt.Errorf("proposal %d: %w", ordinal, err)
			}
			proposalReasonCodes, err := marshalAdmissionShadowStrings(proposal.ProposalReasonCodes)
			if err != nil {
				return err
			}
			assessmentReasonCodes, err := marshalAdmissionShadowStrings(proposal.AssessmentReasonCodes)
			if err != nil {
				return err
			}
			evidenceRefs, err := marshalAdmissionShadowStrings(proposal.EvidenceRefs)
			if err != nil {
				return err
			}
			proposalID := newSyncID("shadow-proposal")
			if _, err := s.execHook(tx, `
				INSERT INTO admission_shadow_proposals (
					id, run_id, ordinal, type, title, content, scope, category,
					protected, recommendation, proposal_reason_codes,
					assessment_reason_codes, evidence_refs
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				proposalID, runID, ordinal, proposal.Type, proposal.Title,
				proposal.Content, proposal.Scope, proposal.Category,
				proposal.Protected, proposal.Recommendation, proposalReasonCodes,
				assessmentReasonCodes, evidenceRefs,
			); err != nil {
				return fmt.Errorf("insert admission shadow proposal %d: %w", ordinal, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	run, err := s.admissionShadowRunByID(runID)
	if err != nil {
		return nil, err
	}
	proposals, err := s.admissionShadowProposalsByRun(runID)
	if err != nil {
		return nil, err
	}
	run.Proposals = proposals
	return run, nil
}

// ListAdmissionShadowRuns returns project runs in stable oldest-first order.
func (s *Store) ListAdmissionShadowRuns(project string) ([]AdmissionShadowRun, error) {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, fmt.Errorf("admission shadow run listing requires project")
	}
	rows, err := s.queryItHook(s.db, `
		SELECT id, project, ifnull(session_id, ''), mode, admission_version,
		       included_items, included_content_bytes, created_at
		FROM admission_shadow_runs
		WHERE project = ?
		ORDER BY datetime(created_at), rowid`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := make([]AdmissionShadowRun, 0)
	for rows.Next() {
		var run AdmissionShadowRun
		if err := rows.Scan(&run.ID, &run.Project, &run.SessionID, &run.Mode,
			&run.AdmissionVersion, &run.IncludedItems, &run.IncludedContentBytes,
			&run.CreatedAt); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// ListAdmissionShadowProposals returns immutable proposal snapshots together
// with append-only review history. pendingOnly excludes any proposal that has
// at least one review event.
func (s *Store) ListAdmissionShadowProposals(project string, pendingOnly bool) ([]AdmissionShadowProposal, error) {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, fmt.Errorf("admission shadow proposal listing requires project")
	}
	query := `
		SELECT p.id, p.run_id, p.ordinal, p.type, p.title, p.content, p.scope,
		       p.category, p.protected, p.recommendation, p.proposal_reason_codes,
		       p.assessment_reason_codes, p.evidence_refs, p.created_at
		FROM admission_shadow_proposals p
		JOIN admission_shadow_runs r ON r.id = p.run_id
		WHERE r.project = ?`
	if pendingOnly {
		query += ` AND NOT EXISTS (
			SELECT 1 FROM admission_shadow_reviews rv WHERE rv.proposal_id = p.id
		)`
	}
	query += ` ORDER BY datetime(r.created_at), r.rowid, p.ordinal`

	proposals, err := s.queryAdmissionShadowProposals(query, project)
	if err != nil {
		return nil, err
	}
	if len(proposals) == 0 {
		return proposals, nil
	}
	reviews, err := s.listAdmissionShadowReviews(project)
	if err != nil {
		return nil, err
	}
	for i := range proposals {
		proposals[i].Reviews = reviews[proposals[i].ID]
		if proposals[i].Reviews == nil {
			proposals[i].Reviews = []AdmissionShadowReview{}
		}
	}
	return proposals, nil
}

// AddAdmissionShadowReview appends a normalized correction. Repeating the
// latest correction is idempotent; returning to an earlier correction after a
// different event appends a new audit event.
func (s *Store) AddAdmissionShadowReview(p AddAdmissionShadowReviewParams) (*AdmissionShadowReview, bool, error) {
	p.ProposalID = strings.TrimSpace(p.ProposalID)
	p.Verdict = strings.ToLower(strings.TrimSpace(p.Verdict))
	p.Note = normalizeAdmissionShadowReviewNote(p.Note)
	if p.ProposalID == "" {
		return nil, false, fmt.Errorf("admission shadow review requires proposal id")
	}
	if !validAdmissionShadowVerdict(p.Verdict) {
		return nil, false, fmt.Errorf("invalid admission shadow review verdict %q", p.Verdict)
	}

	var result AdmissionShadowReview
	alreadyRecorded := false
	err := s.withTx(func(tx *sql.Tx) error {
		var exists bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM admission_shadow_proposals WHERE id = ?)`, p.ProposalID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrAdmissionShadowProposalNotFound
		}

		latest, err := admissionShadowLatestReviewTx(tx, p.ProposalID)
		if err != nil {
			return err
		}
		if latest != nil && latest.Verdict == p.Verdict && latest.Note == p.Note &&
			latest.Unsupported == p.Unsupported && latest.PrivacyLeak == p.PrivacyLeak {
			result = *latest
			alreadyRecorded = true
			return nil
		}

		result = AdmissionShadowReview{
			ID:          newSyncID("shadow-review"),
			ProposalID:  p.ProposalID,
			Verdict:     p.Verdict,
			Note:        p.Note,
			Unsupported: p.Unsupported,
			PrivacyLeak: p.PrivacyLeak,
		}
		if _, err := s.execHook(tx, `
			INSERT INTO admission_shadow_reviews (
				id, proposal_id, verdict, note, unsupported, privacy_leak
			) VALUES (?, ?, ?, ?, ?, ?)`,
			result.ID, result.ProposalID, result.Verdict, result.Note,
			result.Unsupported, result.PrivacyLeak,
		); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT created_at FROM admission_shadow_reviews WHERE id = ?`, result.ID).Scan(&result.CreatedAt)
	})
	if err != nil {
		return nil, false, err
	}
	return &result, alreadyRecorded, nil
}

func normalizeAdmissionShadowProposalInput(input AdmissionShadowProposalInput) (AdmissionShadowProposalInput, error) {
	input.Type = stripPrivateTags(input.Type)
	input.Title = stripPrivateTags(input.Title)
	input.Content = stripPrivateTags(input.Content)
	input.Scope = stripPrivateTags(input.Scope)
	input.Category = stripPrivateTags(input.Category)
	input.Recommendation = strings.ToLower(stripPrivateTags(input.Recommendation))
	input.ProposalReasonCodes = redactAdmissionShadowStrings(input.ProposalReasonCodes)
	input.AssessmentReasonCodes = redactAdmissionShadowStrings(input.AssessmentReasonCodes)
	input.EvidenceRefs = redactAdmissionShadowStrings(input.EvidenceRefs)
	if input.Type == "" || input.Title == "" || input.Scope == "" || input.Category == "" {
		return input, fmt.Errorf("type, title, scope, and category are required")
	}
	if !validAdmissionShadowVerdict(input.Recommendation) {
		return input, fmt.Errorf("invalid recommendation %q", input.Recommendation)
	}
	return input, nil
}

func validAdmissionShadowVerdict(verdict string) bool {
	switch verdict {
	case "admit", "review", "reject":
		return true
	default:
		return false
	}
}

func normalizeAdmissionShadowReviewNote(note string) string {
	note = stripPrivateTags(note)
	runes := []rune(note)
	if len(runes) > MaxAdmissionShadowReviewNoteLength {
		note = string(runes[:MaxAdmissionShadowReviewNoteLength])
	}
	return note
}

func redactAdmissionShadowStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, stripPrivateTags(value))
	}
	return result
}

func marshalAdmissionShadowStrings(values []string) (string, error) {
	if values == nil {
		values = []string{}
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", fmt.Errorf("marshal admission shadow string list: %w", err)
	}
	return string(encoded), nil
}

func unmarshalAdmissionShadowStrings(encoded string) ([]string, error) {
	values := []string{}
	if err := json.Unmarshal([]byte(encoded), &values); err != nil {
		return nil, fmt.Errorf("decode admission shadow string list: %w", err)
	}
	return values, nil
}

func (s *Store) admissionShadowRunByID(runID string) (*AdmissionShadowRun, error) {
	var run AdmissionShadowRun
	err := s.db.QueryRow(`
		SELECT id, project, ifnull(session_id, ''), mode, admission_version,
		       included_items, included_content_bytes, created_at
		FROM admission_shadow_runs WHERE id = ?`, runID).Scan(
		&run.ID, &run.Project, &run.SessionID, &run.Mode, &run.AdmissionVersion,
		&run.IncludedItems, &run.IncludedContentBytes, &run.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &run, nil
}

func (s *Store) admissionShadowProposalsByRun(runID string) ([]AdmissionShadowProposal, error) {
	proposals, err := s.queryAdmissionShadowProposals(`
		SELECT id, run_id, ordinal, type, title, content, scope, category,
		       protected, recommendation, proposal_reason_codes,
		       assessment_reason_codes, evidence_refs, created_at
		FROM admission_shadow_proposals
		WHERE run_id = ?
		ORDER BY ordinal`, runID)
	if err != nil {
		return nil, err
	}
	for i := range proposals {
		proposals[i].Reviews = []AdmissionShadowReview{}
	}
	return proposals, nil
}

func (s *Store) queryAdmissionShadowProposals(query string, args ...any) ([]AdmissionShadowProposal, error) {
	rows, err := s.queryItHook(s.db, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	proposals := make([]AdmissionShadowProposal, 0)
	for rows.Next() {
		var proposal AdmissionShadowProposal
		var proposalReasons, assessmentReasons, evidenceRefs string
		if err := rows.Scan(
			&proposal.ID, &proposal.RunID, &proposal.Ordinal, &proposal.Type,
			&proposal.Title, &proposal.Content, &proposal.Scope, &proposal.Category,
			&proposal.Protected, &proposal.Recommendation, &proposalReasons,
			&assessmentReasons, &evidenceRefs, &proposal.CreatedAt,
		); err != nil {
			return nil, err
		}
		if proposal.ProposalReasonCodes, err = unmarshalAdmissionShadowStrings(proposalReasons); err != nil {
			return nil, err
		}
		if proposal.AssessmentReasonCodes, err = unmarshalAdmissionShadowStrings(assessmentReasons); err != nil {
			return nil, err
		}
		if proposal.EvidenceRefs, err = unmarshalAdmissionShadowStrings(evidenceRefs); err != nil {
			return nil, err
		}
		proposals = append(proposals, proposal)
	}
	return proposals, rows.Err()
}

func (s *Store) listAdmissionShadowReviews(project string) (map[string][]AdmissionShadowReview, error) {
	rows, err := s.queryItHook(s.db, `
		SELECT rv.id, rv.proposal_id, rv.verdict, rv.note, rv.unsupported,
		       rv.privacy_leak, rv.created_at
		FROM admission_shadow_reviews rv
		JOIN admission_shadow_proposals p ON p.id = rv.proposal_id
		JOIN admission_shadow_runs r ON r.id = p.run_id
		WHERE r.project = ?
		ORDER BY datetime(r.created_at), r.rowid, p.ordinal,
		         datetime(rv.created_at), rv.rowid`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string][]AdmissionShadowReview)
	for rows.Next() {
		var review AdmissionShadowReview
		if err := rows.Scan(&review.ID, &review.ProposalID, &review.Verdict,
			&review.Note, &review.Unsupported, &review.PrivacyLeak,
			&review.CreatedAt); err != nil {
			return nil, err
		}
		result[review.ProposalID] = append(result[review.ProposalID], review)
	}
	return result, rows.Err()
}

func admissionShadowLatestReviewTx(tx *sql.Tx, proposalID string) (*AdmissionShadowReview, error) {
	var review AdmissionShadowReview
	err := tx.QueryRow(`
		SELECT id, proposal_id, verdict, note, unsupported, privacy_leak, created_at
		FROM admission_shadow_reviews
		WHERE proposal_id = ?
		ORDER BY datetime(created_at) DESC, rowid DESC
		LIMIT 1`, proposalID).Scan(
		&review.ID, &review.ProposalID, &review.Verdict, &review.Note,
		&review.Unsupported, &review.PrivacyLeak, &review.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &review, nil
}
