package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	MaxAdmissionShadowReviewNoteLength       = 4096
	MaxAdmissionShadowEvidenceReferences     = MaxLocalEvidenceReferences
	MaxAdmissionShadowEvidenceReferenceBytes = MaxLocalEvidenceReferenceBytes
)

var (
	ErrAdmissionShadowProposalNotFound = errors.New("admission shadow proposal not found")
	ErrAdmissionShadowRunNotFound      = errors.New("admission shadow run not found")
)

type AdmissionShadowRun struct {
	ID                        string                    `json:"id"`
	Project                   string                    `json:"project"`
	SessionID                 string                    `json:"session_id,omitempty"`
	Mode                      string                    `json:"mode"`
	EvidenceVersion           string                    `json:"evidence_version"`
	GeneratorVersion          string                    `json:"generator_version"`
	PolicyVersion             string                    `json:"policy_version"`
	StudyID                   string                    `json:"study_id,omitempty"`
	StudyVersion              string                    `json:"study_version,omitempty"`
	StudyContractHash         string                    `json:"study_contract_hash,omitempty"`
	Cohort                    string                    `json:"cohort,omitempty"`
	CohortKind                string                    `json:"cohort_kind,omitempty"`
	Adapter                   string                    `json:"adapter,omitempty"`
	ProjectType               string                    `json:"project_type,omitempty"`
	SessionShape              string                    `json:"session_shape,omitempty"`
	ConsentAttestation        string                    `json:"consent_attestation,omitempty"`
	IndependentReviewRequired bool                      `json:"independent_review_required,omitempty"`
	DiagnosticCodes           []string                  `json:"diagnostic_codes"`
	IncludedItems             int                       `json:"included_items"`
	IncludedContentBytes      int                       `json:"included_content_bytes"`
	CreatedAt                 string                    `json:"created_at"`
	Proposals                 []AdmissionShadowProposal `json:"proposals,omitempty"`
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
	ReviewerID  string `json:"reviewer_id,omitempty"`
	Ordinal     int    `json:"ordinal"`
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
	EvidenceVersion      string
	GeneratorVersion     string
	PolicyVersion        string
	Study                *AdmissionStudyRunMetadata
	DiagnosticCodes      []string
	IncludedItems        int
	IncludedContentBytes int
	Proposals            []AdmissionShadowProposalInput
}

type AddAdmissionShadowReviewParams struct {
	ProposalID  string
	ReviewerID  string
	Verdict     string
	Note        string
	Unsupported *bool
	PrivacyLeak *bool
}

// migrateAdmissionShadow creates the local-only persistence boundary for
// admission experiments. These tables intentionally have no sync triggers and
// are not part of ExportData.
func (s *Store) migrateAdmissionShadow() error {
	_, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS admission_studies (
			study_id        TEXT NOT NULL,
			study_version   TEXT NOT NULL,
			contract_version TEXT NOT NULL,
			metrics_version TEXT NOT NULL,
			contract_hash   TEXT NOT NULL,
			contract_json   TEXT NOT NULL,
			created_at      TEXT NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (study_id, study_version)
		);

		CREATE TABLE IF NOT EXISTS admission_shadow_runs (
			id                     TEXT PRIMARY KEY,
			project                TEXT NOT NULL,
			session_id             TEXT,
			mode                   TEXT NOT NULL,
			evidence_version       TEXT NOT NULL,
			generator_version      TEXT NOT NULL,
			policy_version         TEXT NOT NULL,
			diagnostic_codes       TEXT NOT NULL DEFAULT '[]',
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
			ordinal      INTEGER NOT NULL CHECK (ordinal >= 0),
			verdict      TEXT NOT NULL,
			note         TEXT NOT NULL DEFAULT '',
			unsupported  BOOLEAN NOT NULL DEFAULT 0,
			privacy_leak BOOLEAN NOT NULL DEFAULT 0,
			created_at   TEXT NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (proposal_id) REFERENCES admission_shadow_proposals(id) ON DELETE CASCADE,
			UNIQUE (proposal_id, ordinal)
		);

		CREATE INDEX IF NOT EXISTS idx_admission_shadow_runs_project_created
			ON admission_shadow_runs(project, created_at, id);
		CREATE INDEX IF NOT EXISTS idx_admission_shadow_proposals_run_ordinal
			ON admission_shadow_proposals(run_id, ordinal);
		CREATE INDEX IF NOT EXISTS idx_admission_shadow_reviews_proposal_ordinal
			ON admission_shadow_reviews(proposal_id, ordinal);
	`)
	if err != nil {
		return err
	}
	for _, column := range []struct {
		name       string
		definition string
	}{
		{"study_id", "TEXT"},
		{"study_version", "TEXT"},
		{"study_contract_hash", "TEXT"},
		{"cohort", "TEXT"},
		{"cohort_kind", "TEXT"},
		{"adapter", "TEXT"},
		{"project_type", "TEXT"},
		{"session_shape", "TEXT"},
		{"consent_attestation", "TEXT"},
		{"independent_review_required", "BOOLEAN NOT NULL DEFAULT 0"},
	} {
		if err := s.addColumnIfNotExists("admission_shadow_runs", column.name, column.definition); err != nil {
			return err
		}
	}
	if err := s.addColumnIfNotExists("admission_shadow_reviews", "reviewer_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	_, err = s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS admission_study_omissions (
			id          TEXT PRIMARY KEY,
			run_id      TEXT NOT NULL,
			reviewer_id TEXT NOT NULL,
			ordinal     INTEGER NOT NULL CHECK (ordinal >= 0),
			category    TEXT NOT NULL,
			reason_code TEXT NOT NULL,
			annotation  TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (run_id) REFERENCES admission_shadow_runs(id) ON DELETE CASCADE,
			UNIQUE (run_id, reviewer_id, ordinal)
		);
		CREATE INDEX IF NOT EXISTS idx_admission_shadow_runs_study_cohort
			ON admission_shadow_runs(study_id, study_version, cohort, created_at, id);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_admission_shadow_runs_study_session
			ON admission_shadow_runs(study_id, study_version, session_id)
			WHERE study_id IS NOT NULL AND session_id IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_admission_shadow_reviews_reviewer
			ON admission_shadow_reviews(proposal_id, reviewer_id, ordinal);
		CREATE INDEX IF NOT EXISTS idx_admission_study_omissions_run_reviewer
			ON admission_study_omissions(run_id, reviewer_id, ordinal);
	`)
	return err
}

// CreateAdmissionShadowRun atomically persists one immutable run and all of
// its proposal snapshots. Its input deliberately cannot carry raw evidence,
// prompts, or summaries.
func (s *Store) CreateAdmissionShadowRun(p CreateAdmissionShadowRunParams) (*AdmissionShadowRun, error) {
	run, _, err := s.createAdmissionShadowRun(p)
	return run, err
}

// CreateAdmissionStudyShadowRun adds idempotency for the frozen study/session
// identity while leaving legacy shadow behavior unchanged.
func (s *Store) CreateAdmissionStudyShadowRun(p CreateAdmissionShadowRunParams) (*AdmissionShadowRun, bool, error) {
	if p.Study == nil {
		return nil, false, fmt.Errorf("admission study shadow run requires study metadata")
	}
	return s.createAdmissionShadowRun(p)
}

func (s *Store) createAdmissionShadowRun(p CreateAdmissionShadowRunParams) (*AdmissionShadowRun, bool, error) {
	p.Project = RedactPrivateBlocks(p.Project)
	p.Project, _ = NormalizeProject(p.Project)
	p.Project = strings.TrimSpace(p.Project)
	p.SessionID = RedactPrivateBlocks(p.SessionID)
	p.Mode = RedactPrivateBlocks(p.Mode)
	p.EvidenceVersion = RedactPrivateBlocks(p.EvidenceVersion)
	p.GeneratorVersion = RedactPrivateBlocks(p.GeneratorVersion)
	p.PolicyVersion = RedactPrivateBlocks(p.PolicyVersion)
	p.DiagnosticCodes = redactAdmissionShadowStrings(p.DiagnosticCodes)
	if p.Project == "" {
		return nil, false, fmt.Errorf("admission shadow run requires project")
	}
	if p.Mode == "" {
		return nil, false, fmt.Errorf("admission shadow run requires mode")
	}
	if p.EvidenceVersion == "" || p.GeneratorVersion == "" || p.PolicyVersion == "" {
		return nil, false, fmt.Errorf("admission shadow run requires evidence, generator, and policy versions")
	}
	if p.IncludedItems < 0 || p.IncludedContentBytes < 0 {
		return nil, false, fmt.Errorf("admission shadow acquisition counts must not be negative")
	}
	if p.Study != nil && strings.TrimSpace(p.SessionID) == "" {
		return nil, false, fmt.Errorf("%w: attributed runs require a session id", ErrAdmissionStudyMetadataMismatch)
	}

	runID := newSyncID("shadow-run")
	alreadyRecorded := false
	diagnosticCodes, err := marshalAdmissionShadowStrings(p.DiagnosticCodes)
	if err != nil {
		return nil, false, err
	}
	err = s.withTx(func(tx *sql.Tx) error {
		var study AdmissionStudyRunMetadata
		var studyHash, cohortKind string
		if p.Study != nil {
			study = normalizeAdmissionStudyRunMetadata(*p.Study)
			frozen, err := admissionStudyByIdentity(tx, study.StudyID, study.StudyVersion)
			if err != nil {
				if errors.Is(err, ErrAdmissionStudyNotFound) {
					return fmt.Errorf("%w: unknown study %s/%s", ErrAdmissionStudyMetadataMismatch, study.StudyID, study.StudyVersion)
				}
				return err
			}
			if err := validateAdmissionStudyMetadataAgainstContract(study, strings.TrimSpace(p.SessionID), frozen.Contract); err != nil {
				return err
			}
			studyHash = frozen.ContractHash
			for _, cohort := range frozen.Contract.Cohorts {
				if cohort.ID == study.Cohort {
					cohortKind = cohort.Kind
					break
				}
			}
			var existing struct {
				ID, Project, EvidenceVersion, GeneratorVersion, PolicyVersion string
				ContractHash, Cohort, Adapter, ProjectType, SessionShape      string
				ConsentAttestation                                            string
				IndependentReviewRequired                                     bool
			}
			err = tx.QueryRow(`
				SELECT id, project, evidence_version, generator_version, policy_version,
				       study_contract_hash, cohort, adapter, project_type, session_shape,
				       consent_attestation, independent_review_required
				FROM admission_shadow_runs
				WHERE study_id = ? AND study_version = ? AND session_id = ?`,
				study.StudyID, study.StudyVersion, p.SessionID,
			).Scan(&existing.ID, &existing.Project, &existing.EvidenceVersion,
				&existing.GeneratorVersion, &existing.PolicyVersion, &existing.ContractHash,
				&existing.Cohort, &existing.Adapter, &existing.ProjectType,
				&existing.SessionShape, &existing.ConsentAttestation,
				&existing.IndependentReviewRequired)
			if err == nil {
				if existing.Project != p.Project || existing.EvidenceVersion != p.EvidenceVersion ||
					existing.GeneratorVersion != p.GeneratorVersion || existing.PolicyVersion != p.PolicyVersion ||
					existing.ContractHash != studyHash || existing.Cohort != study.Cohort ||
					existing.Adapter != study.Adapter || existing.ProjectType != study.ProjectType ||
					existing.SessionShape != study.SessionShape || existing.ConsentAttestation != study.ConsentAttestation ||
					existing.IndependentReviewRequired != study.IndependentReviewRequired {
					return fmt.Errorf("%w: session already belongs to different frozen run metadata", ErrAdmissionStudyMetadataMismatch)
				}
				runID = existing.ID
				alreadyRecorded = true
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		if _, err := s.execHook(tx, `
			INSERT INTO admission_shadow_runs (
				id, project, session_id, mode, evidence_version, generator_version,
				policy_version, study_id, study_version, study_contract_hash, cohort,
				cohort_kind, adapter, project_type, session_shape, consent_attestation,
				independent_review_required, diagnostic_codes, included_items, included_content_bytes
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			runID, p.Project, nullableString(p.SessionID), p.Mode,
			p.EvidenceVersion, p.GeneratorVersion, p.PolicyVersion,
			nullableString(study.StudyID), nullableString(study.StudyVersion), nullableString(studyHash),
			nullableString(study.Cohort), nullableString(cohortKind), nullableString(study.Adapter),
			nullableString(study.ProjectType), nullableString(study.SessionShape),
			nullableString(study.ConsentAttestation), study.IndependentReviewRequired, diagnosticCodes,
			p.IncludedItems, p.IncludedContentBytes,
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
		return nil, false, err
	}

	run, err := s.admissionShadowRunByID(runID)
	if err != nil {
		return nil, false, err
	}
	proposals, err := s.admissionShadowProposalsByRun(runID)
	if err != nil {
		return nil, false, err
	}
	run.Proposals = proposals
	return run, alreadyRecorded, nil
}

// ListAdmissionShadowRuns returns project runs in stable oldest-first order.
func (s *Store) ListAdmissionShadowRuns(project string) ([]AdmissionShadowRun, error) {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, fmt.Errorf("admission shadow run listing requires project")
	}
	rows, err := s.queryItHook(s.db, `
		SELECT id, project, ifnull(session_id, ''), mode, evidence_version,
		       generator_version, policy_version, ifnull(study_id, ''),
		       ifnull(study_version, ''), ifnull(study_contract_hash, ''),
		       ifnull(cohort, ''), ifnull(cohort_kind, ''), ifnull(adapter, ''),
		       ifnull(project_type, ''), ifnull(session_shape, ''),
		       ifnull(consent_attestation, ''), independent_review_required, diagnostic_codes,
		       included_items, included_content_bytes, created_at
		FROM admission_shadow_runs
		WHERE project = ?
		ORDER BY datetime(created_at), id`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := make([]AdmissionShadowRun, 0)
	for rows.Next() {
		var run AdmissionShadowRun
		var diagnosticCodes string
		if err := rows.Scan(&run.ID, &run.Project, &run.SessionID, &run.Mode,
			&run.EvidenceVersion, &run.GeneratorVersion, &run.PolicyVersion,
			&run.StudyID, &run.StudyVersion, &run.StudyContractHash, &run.Cohort,
			&run.CohortKind, &run.Adapter, &run.ProjectType, &run.SessionShape,
			&run.ConsentAttestation, &run.IndependentReviewRequired,
			&diagnosticCodes, &run.IncludedItems, &run.IncludedContentBytes,
			&run.CreatedAt); err != nil {
			return nil, err
		}
		if run.DiagnosticCodes, err = unmarshalAdmissionShadowStrings(diagnosticCodes); err != nil {
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
	query += ` ORDER BY datetime(r.created_at), r.id, p.ordinal`

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
	p.ReviewerID = strings.ToLower(strings.TrimSpace(p.ReviewerID))
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
		var studyID string
		if err := tx.QueryRow(`
			SELECT ifnull(r.study_id, '')
			FROM admission_shadow_proposals p
			JOIN admission_shadow_runs r ON r.id = p.run_id
			WHERE p.id = ?`, p.ProposalID).Scan(&studyID); errors.Is(err, sql.ErrNoRows) {
			return ErrAdmissionShadowProposalNotFound
		} else if err != nil {
			return err
		}
		if studyID != "" && !validAdmissionStudyIdentifier(p.ReviewerID) {
			return fmt.Errorf("%w: attributed reviews require a bounded reviewer id", ErrAdmissionStudyMetadataMismatch)
		}
		if studyID == "" && p.ReviewerID != "" {
			return fmt.Errorf("%w: reviewer id requires an attributed study run", ErrAdmissionStudyMetadataMismatch)
		}

		latest, err := admissionShadowLatestReviewTx(tx, p.ProposalID, p.ReviewerID)
		if err != nil {
			return err
		}
		unsupported := false
		privacyLeak := false
		if latest != nil {
			unsupported = latest.Unsupported
			privacyLeak = latest.PrivacyLeak
		}
		if p.Unsupported != nil {
			unsupported = *p.Unsupported
		}
		if p.PrivacyLeak != nil {
			privacyLeak = *p.PrivacyLeak
		}
		if latest != nil && latest.Verdict == p.Verdict && latest.Note == p.Note &&
			latest.Unsupported == unsupported && latest.PrivacyLeak == privacyLeak {
			result = *latest
			alreadyRecorded = true
			return nil
		}
		var nextOrdinal int
		if err := tx.QueryRow(`
			SELECT ifnull(MAX(ordinal) + 1, 0)
			FROM admission_shadow_reviews
			WHERE proposal_id = ?`, p.ProposalID).Scan(&nextOrdinal); err != nil {
			return err
		}

		result = AdmissionShadowReview{
			ID:          newSyncID("shadow-review"),
			ProposalID:  p.ProposalID,
			ReviewerID:  p.ReviewerID,
			Ordinal:     nextOrdinal,
			Verdict:     p.Verdict,
			Note:        p.Note,
			Unsupported: unsupported,
			PrivacyLeak: privacyLeak,
		}
		if _, err := s.execHook(tx, `
			INSERT INTO admission_shadow_reviews (
				id, proposal_id, reviewer_id, ordinal, verdict, note, unsupported, privacy_leak
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			result.ID, result.ProposalID, result.ReviewerID, result.Ordinal, result.Verdict, result.Note,
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
	input.Type = RedactPrivateBlocks(input.Type)
	input.Title = RedactPrivateBlocks(input.Title)
	input.Content = RedactPrivateBlocks(input.Content)
	input.Scope = RedactPrivateBlocks(input.Scope)
	input.Category = RedactPrivateBlocks(input.Category)
	input.Recommendation = strings.ToLower(RedactPrivateBlocks(input.Recommendation))
	input.ProposalReasonCodes = redactAdmissionShadowStrings(input.ProposalReasonCodes)
	input.AssessmentReasonCodes = redactAdmissionShadowStrings(input.AssessmentReasonCodes)
	var err error
	input.EvidenceRefs, err = normalizeLocalEvidenceReferences(input.EvidenceRefs, "admission shadow proposal")
	if err != nil {
		return input, err
	}
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
	note = RedactPrivateBlocks(note)
	runes := []rune(note)
	if len(runes) > MaxAdmissionShadowReviewNoteLength {
		note = string(runes[:MaxAdmissionShadowReviewNoteLength])
	}
	return note
}

func redactAdmissionShadowStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, RedactPrivateBlocks(value))
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
	var diagnosticCodes string
	err := s.db.QueryRow(`
		SELECT id, project, ifnull(session_id, ''), mode, evidence_version,
		       generator_version, policy_version, ifnull(study_id, ''),
		       ifnull(study_version, ''), ifnull(study_contract_hash, ''),
		       ifnull(cohort, ''), ifnull(cohort_kind, ''), ifnull(adapter, ''),
		       ifnull(project_type, ''), ifnull(session_shape, ''),
		       ifnull(consent_attestation, ''), independent_review_required, diagnostic_codes,
		       included_items, included_content_bytes, created_at
		FROM admission_shadow_runs WHERE id = ?`, runID).Scan(
		&run.ID, &run.Project, &run.SessionID, &run.Mode, &run.EvidenceVersion,
		&run.GeneratorVersion, &run.PolicyVersion, &run.StudyID, &run.StudyVersion,
		&run.StudyContractHash, &run.Cohort, &run.CohortKind, &run.Adapter,
		&run.ProjectType, &run.SessionShape, &run.ConsentAttestation,
		&run.IndependentReviewRequired, &diagnosticCodes,
		&run.IncludedItems, &run.IncludedContentBytes, &run.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	if run.DiagnosticCodes, err = unmarshalAdmissionShadowStrings(diagnosticCodes); err != nil {
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
		SELECT rv.id, rv.proposal_id, rv.reviewer_id, rv.ordinal, rv.verdict, rv.note, rv.unsupported,
		       rv.privacy_leak, rv.created_at
		FROM admission_shadow_reviews rv
		JOIN admission_shadow_proposals p ON p.id = rv.proposal_id
		JOIN admission_shadow_runs r ON r.id = p.run_id
		WHERE r.project = ?
		ORDER BY datetime(r.created_at), r.id, p.ordinal, rv.ordinal, rv.id`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string][]AdmissionShadowReview)
	for rows.Next() {
		var review AdmissionShadowReview
		if err := rows.Scan(&review.ID, &review.ProposalID, &review.ReviewerID, &review.Ordinal, &review.Verdict,
			&review.Note, &review.Unsupported, &review.PrivacyLeak,
			&review.CreatedAt); err != nil {
			return nil, err
		}
		result[review.ProposalID] = append(result[review.ProposalID], review)
	}
	return result, rows.Err()
}

func admissionShadowLatestReviewTx(tx *sql.Tx, proposalID, reviewerID string) (*AdmissionShadowReview, error) {
	var review AdmissionShadowReview
	err := tx.QueryRow(`
		SELECT id, proposal_id, reviewer_id, ordinal, verdict, note, unsupported, privacy_leak, created_at
		FROM admission_shadow_reviews
		WHERE proposal_id = ? AND reviewer_id = ?
		ORDER BY ordinal DESC, id DESC
		LIMIT 1`, proposalID, reviewerID).Scan(
		&review.ID, &review.ProposalID, &review.ReviewerID, &review.Ordinal, &review.Verdict, &review.Note,
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
