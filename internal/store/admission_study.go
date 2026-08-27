package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const (
	AdmissionStudyContractVersion             = "admission-study-v1"
	AdmissionStudyCleanupExplicit             = "explicit_study_cleanup"
	MaxAdmissionStudyOmissionAnnotationLength = 1024
)

var (
	ErrAdmissionStudyNotFound         = errors.New("admission study not found")
	ErrAdmissionStudyContractChanged  = errors.New("admission study contract changed")
	ErrAdmissionStudyMetadataMismatch = errors.New("admission study metadata mismatch")

	admissionStudyIdentifierPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	admissionStudySessionIDPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

type AdmissionStudyContract struct {
	ContractVersion         string                    `json:"contract_version"`
	StudyID                 string                    `json:"study_id"`
	StudyVersion            string                    `json:"study_version"`
	MetricsVersion          string                    `json:"metrics_version"`
	Cohorts                 []AdmissionStudyCohort    `json:"cohorts"`
	Adapters                []string                  `json:"adapters"`
	ProjectTypes            []string                  `json:"project_types"`
	SessionShapes           []string                  `json:"session_shapes"`
	LabelSchema             AdmissionStudyLabelSchema `json:"label_schema"`
	Thresholds              AdmissionStudyThresholds  `json:"thresholds"`
	Consent                 AdmissionStudyConsent     `json:"consent"`
	Retention               AdmissionStudyRetention   `json:"retention"`
	AllowedAggregateOutputs []string                  `json:"allowed_aggregate_outputs"`
}

type AdmissionStudyCohort struct {
	ID                                  string   `json:"id"`
	Kind                                string   `json:"kind"`
	SessionIDs                          []string `json:"session_ids"`
	MinimumRuns                         int      `json:"minimum_runs"`
	MinimumProposals                    int      `json:"minimum_proposals"`
	MinimumIndependentReviewedProposals int      `json:"minimum_independent_reviewed_proposals"`
}

type AdmissionStudyLabelSchema struct {
	Version            string   `json:"version"`
	Verdicts           []string `json:"verdicts"`
	OmissionCategories []string `json:"omission_categories"`
	ReasonCodes        []string `json:"reason_codes"`
}

type AdmissionStudyThresholds struct {
	MinimumPromotionPrecision     float64 `json:"minimum_promotion_precision"`
	MaximumReviewRate             float64 `json:"maximum_review_rate"`
	MinimumReviewCoverage         float64 `json:"minimum_review_coverage"`
	MinimumInterReviewerAgreement float64 `json:"minimum_inter_reviewer_agreement"`
	MaximumProtectedFalseRejects  int     `json:"maximum_protected_false_rejects"`
	MaximumUnsupportedProposals   int     `json:"maximum_unsupported_proposals"`
	MaximumPrivacyLeaks           int     `json:"maximum_privacy_leaks"`
}

type AdmissionStudyConsent struct {
	Required    bool   `json:"required"`
	Attestation string `json:"attestation"`
}

type AdmissionStudyRetention struct {
	Days    int    `json:"days"`
	Cleanup string `json:"cleanup"`
}

type AdmissionStudy struct {
	Contract     AdmissionStudyContract `json:"contract"`
	ContractHash string                 `json:"contract_hash"`
	CreatedAt    string                 `json:"created_at"`
}

type AdmissionStudyRunMetadata struct {
	StudyID                   string `json:"study_id"`
	StudyVersion              string `json:"study_version"`
	Cohort                    string `json:"cohort"`
	Adapter                   string `json:"adapter"`
	ProjectType               string `json:"project_type"`
	SessionShape              string `json:"session_shape"`
	ConsentAttestation        string `json:"consent_attestation"`
	IndependentReviewRequired bool   `json:"independent_review_required"`
}

type AdmissionStudyOmission struct {
	ID         string `json:"id"`
	RunID      string `json:"run_id"`
	ReviewerID string `json:"reviewer_id"`
	Ordinal    int    `json:"ordinal"`
	Category   string `json:"category"`
	ReasonCode string `json:"reason_code"`
	Annotation string `json:"annotation"`
	CreatedAt  string `json:"created_at"`
}

type AddAdmissionStudyOmissionParams struct {
	RunID      string
	ReviewerID string
	Category   string
	ReasonCode string
	Annotation string
}

type AdmissionStudyCleanupResult struct {
	StudyID       string `json:"study_id"`
	StudyVersion  string `json:"study_version"`
	RunCount      int    `json:"run_count"`
	ProposalCount int    `json:"proposal_count"`
	ReviewCount   int    `json:"review_count"`
	OmissionCount int    `json:"omission_count"`
}

// FreezeAdmissionStudy validates and freezes one immutable versioned study
// contract. An identical retry is idempotent; changing a frozen version is an
// explicit error and requires a new study version.
func (s *Store) FreezeAdmissionStudy(contract AdmissionStudyContract) (*AdmissionStudy, bool, error) {
	contract, err := normalizeAdmissionStudyContract(contract)
	if err != nil {
		return nil, false, err
	}
	encoded, err := json.Marshal(contract)
	if err != nil {
		return nil, false, fmt.Errorf("marshal admission study contract: %w", err)
	}
	digest := sha256.Sum256(encoded)
	contractHash := hex.EncodeToString(digest[:])

	var result AdmissionStudy
	alreadyFrozen := false
	err = s.withTx(func(tx *sql.Tx) error {
		stored, err := admissionStudyByIdentity(tx, contract.StudyID, contract.StudyVersion)
		if err == nil {
			if stored.ContractHash != contractHash {
				return ErrAdmissionStudyContractChanged
			}
			result = *stored
			alreadyFrozen = true
			return nil
		}
		if !errors.Is(err, ErrAdmissionStudyNotFound) {
			return err
		}
		if _, err := s.execHook(tx, `
			INSERT INTO admission_studies (
				study_id, study_version, contract_version, metrics_version,
				contract_hash, contract_json
			) VALUES (?, ?, ?, ?, ?, ?)`,
			contract.StudyID, contract.StudyVersion, contract.ContractVersion,
			contract.MetricsVersion, contractHash, string(encoded),
		); err != nil {
			return fmt.Errorf("insert admission study: %w", err)
		}
		stored, err = admissionStudyByIdentity(tx, contract.StudyID, contract.StudyVersion)
		if err != nil {
			return err
		}
		result = *stored
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return &result, alreadyFrozen, nil
}

func (s *Store) GetAdmissionStudy(studyID, studyVersion string) (*AdmissionStudy, error) {
	studyID = strings.ToLower(strings.TrimSpace(studyID))
	studyVersion = strings.ToLower(strings.TrimSpace(studyVersion))
	if !validAdmissionStudyIdentifier(studyID) || !validAdmissionStudyIdentifier(studyVersion) {
		return nil, fmt.Errorf("invalid admission study identity")
	}
	return admissionStudyByIdentity(s.db, studyID, studyVersion)
}

func (s *Store) DeleteAdmissionStudy(studyID, studyVersion string) (*AdmissionStudyCleanupResult, error) {
	studyID = strings.ToLower(strings.TrimSpace(studyID))
	studyVersion = strings.ToLower(strings.TrimSpace(studyVersion))
	if !validAdmissionStudyIdentifier(studyID) || !validAdmissionStudyIdentifier(studyVersion) {
		return nil, fmt.Errorf("invalid admission study identity")
	}
	result := AdmissionStudyCleanupResult{StudyID: studyID, StudyVersion: studyVersion}
	err := s.withTx(func(tx *sql.Tx) error {
		if _, err := admissionStudyByIdentity(tx, studyID, studyVersion); err != nil {
			return err
		}
		queries := []struct {
			destination *int
			query       string
		}{
			{&result.RunCount, `SELECT COUNT(*) FROM admission_shadow_runs WHERE study_id = ? AND study_version = ?`},
			{&result.ProposalCount, `
				SELECT COUNT(*) FROM admission_shadow_proposals p
				JOIN admission_shadow_runs r ON r.id = p.run_id
				WHERE r.study_id = ? AND r.study_version = ?`},
			{&result.ReviewCount, `
				SELECT COUNT(*) FROM admission_shadow_reviews v
				JOIN admission_shadow_proposals p ON p.id = v.proposal_id
				JOIN admission_shadow_runs r ON r.id = p.run_id
				WHERE r.study_id = ? AND r.study_version = ?`},
			{&result.OmissionCount, `
				SELECT COUNT(*) FROM admission_study_omissions o
				JOIN admission_shadow_runs r ON r.id = o.run_id
				WHERE r.study_id = ? AND r.study_version = ?`},
		}
		for _, count := range queries {
			if err := tx.QueryRow(count.query, studyID, studyVersion).Scan(count.destination); err != nil {
				return err
			}
		}
		if _, err := s.execHook(tx, `DELETE FROM admission_shadow_runs WHERE study_id = ? AND study_version = ?`, studyID, studyVersion); err != nil {
			return err
		}
		if _, err := s.execHook(tx, `DELETE FROM admission_studies WHERE study_id = ? AND study_version = ?`, studyID, studyVersion); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *Store) ValidateAdmissionStudyRunMetadata(metadata AdmissionStudyRunMetadata, sessionID string) (*AdmissionStudyRunMetadata, *AdmissionStudy, error) {
	metadata = normalizeAdmissionStudyRunMetadata(metadata)
	sessionID = strings.TrimSpace(sessionID)
	study, err := s.GetAdmissionStudy(metadata.StudyID, metadata.StudyVersion)
	if err != nil {
		if errors.Is(err, ErrAdmissionStudyNotFound) {
			return nil, nil, fmt.Errorf("%w: unknown study %s/%s", ErrAdmissionStudyMetadataMismatch, metadata.StudyID, metadata.StudyVersion)
		}
		return nil, nil, err
	}
	if err := validateAdmissionStudyMetadataAgainstContract(metadata, sessionID, study.Contract); err != nil {
		return nil, nil, err
	}
	return &metadata, study, nil
}

func admissionStudyByIdentity(q interface {
	QueryRow(query string, args ...any) *sql.Row
}, studyID, studyVersion string) (*AdmissionStudy, error) {
	var result AdmissionStudy
	var encoded string
	err := q.QueryRow(`
		SELECT contract_hash, contract_json, created_at
		FROM admission_studies
		WHERE study_id = ? AND study_version = ?`, studyID, studyVersion).Scan(
		&result.ContractHash, &encoded, &result.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrAdmissionStudyNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(encoded), &result.Contract); err != nil {
		return nil, fmt.Errorf("decode admission study contract: %w", err)
	}
	return &result, nil
}

func normalizeAdmissionStudyContract(contract AdmissionStudyContract) (AdmissionStudyContract, error) {
	contract.ContractVersion = strings.ToLower(strings.TrimSpace(contract.ContractVersion))
	contract.StudyID = strings.ToLower(strings.TrimSpace(contract.StudyID))
	contract.StudyVersion = strings.ToLower(strings.TrimSpace(contract.StudyVersion))
	contract.MetricsVersion = strings.ToLower(strings.TrimSpace(contract.MetricsVersion))
	contract.LabelSchema.Version = strings.ToLower(strings.TrimSpace(contract.LabelSchema.Version))
	contract.Consent.Attestation = strings.ToLower(strings.TrimSpace(contract.Consent.Attestation))
	contract.Retention.Cleanup = strings.ToLower(strings.TrimSpace(contract.Retention.Cleanup))
	if contract.ContractVersion != AdmissionStudyContractVersion {
		return contract, fmt.Errorf("unsupported admission study contract version %q", contract.ContractVersion)
	}
	for field, value := range map[string]string{
		"study_id": contract.StudyID, "study_version": contract.StudyVersion,
		"metrics_version": contract.MetricsVersion, "label_schema.version": contract.LabelSchema.Version,
		"consent.attestation": contract.Consent.Attestation,
	} {
		if !validAdmissionStudyIdentifier(value) {
			return contract, fmt.Errorf("invalid admission study %s %q", field, value)
		}
	}

	var err error
	if contract.Adapters, err = normalizeAdmissionStudyIdentifiers("adapter", contract.Adapters); err != nil {
		return contract, err
	}
	if contract.ProjectTypes, err = normalizeAdmissionStudyIdentifiers("project type", contract.ProjectTypes); err != nil {
		return contract, err
	}
	if contract.SessionShapes, err = normalizeAdmissionStudyIdentifiers("session shape", contract.SessionShapes); err != nil {
		return contract, err
	}
	if contract.LabelSchema.Verdicts, err = normalizeAdmissionStudyIdentifiers("verdict", contract.LabelSchema.Verdicts); err != nil {
		return contract, err
	}
	if strings.Join(contract.LabelSchema.Verdicts, ",") != "admit,reject,review" {
		return contract, fmt.Errorf("admission study verdict schema must contain admit, review, and reject")
	}
	if contract.LabelSchema.OmissionCategories, err = normalizeAdmissionStudyIdentifiers("omission category", contract.LabelSchema.OmissionCategories); err != nil {
		return contract, err
	}
	requiredOmissionCategories := []string{"constraint", "decision", "invariant", "preference", "root_cause"}
	if strings.Join(contract.LabelSchema.OmissionCategories, ",") != strings.Join(requiredOmissionCategories, ",") {
		return contract, fmt.Errorf("admission study omission categories must contain exactly %s", strings.Join(requiredOmissionCategories, ", "))
	}
	if contract.LabelSchema.ReasonCodes, err = normalizeAdmissionStudyIdentifiers("reason code", contract.LabelSchema.ReasonCodes); err != nil {
		return contract, err
	}
	if contract.AllowedAggregateOutputs, err = normalizeAdmissionStudyIdentifiers("aggregate output", contract.AllowedAggregateOutputs); err != nil {
		return contract, err
	}
	requiredOutputs := []string{"counts", "distributions", "gates", "quality", "sufficiency", "uncertainty"}
	if strings.Join(contract.AllowedAggregateOutputs, ",") != strings.Join(requiredOutputs, ",") {
		return contract, fmt.Errorf("admission study aggregate outputs must contain exactly %s", strings.Join(requiredOutputs, ", "))
	}

	if len(contract.Cohorts) == 0 {
		return contract, fmt.Errorf("admission study requires cohorts")
	}
	seenCohorts := map[string]bool{}
	seenKinds := map[string]bool{}
	seenSessionIDs := map[string]string{}
	for index := range contract.Cohorts {
		cohort := &contract.Cohorts[index]
		cohort.ID = strings.ToLower(strings.TrimSpace(cohort.ID))
		cohort.Kind = strings.ToLower(strings.TrimSpace(cohort.Kind))
		if !validAdmissionStudyIdentifier(cohort.ID) || (cohort.Kind != "calibration" && cohort.Kind != "held_out") {
			return contract, fmt.Errorf("invalid admission study cohort %q kind %q", cohort.ID, cohort.Kind)
		}
		if seenCohorts[cohort.ID] || seenKinds[cohort.Kind] {
			return contract, fmt.Errorf("duplicate admission study cohort %q or kind %q", cohort.ID, cohort.Kind)
		}
		seenCohorts[cohort.ID] = true
		seenKinds[cohort.Kind] = true
		if cohort.SessionIDs, err = normalizeAdmissionStudySessionIDs(cohort.SessionIDs); err != nil {
			return contract, fmt.Errorf("cohort %q: %w", cohort.ID, err)
		}
		for _, sessionID := range cohort.SessionIDs {
			if owner, exists := seenSessionIDs[sessionID]; exists {
				return contract, fmt.Errorf("admission study session %q belongs to both %q and %q", sessionID, owner, cohort.ID)
			}
			seenSessionIDs[sessionID] = cohort.ID
		}
		if cohort.MinimumRuns <= 0 || cohort.MinimumProposals <= 0 ||
			cohort.MinimumIndependentReviewedProposals <= 0 ||
			cohort.MinimumIndependentReviewedProposals > cohort.MinimumProposals ||
			cohort.MinimumRuns > len(cohort.SessionIDs) {
			return contract, fmt.Errorf("invalid sampling requirements for cohort %q", cohort.ID)
		}
	}
	if !seenKinds["calibration"] || !seenKinds["held_out"] {
		return contract, fmt.Errorf("admission study requires calibration and held_out cohorts")
	}
	sort.Slice(contract.Cohorts, func(i, j int) bool { return contract.Cohorts[i].ID < contract.Cohorts[j].ID })

	thresholds := contract.Thresholds
	for name, value := range map[string]float64{
		"minimum_promotion_precision":      thresholds.MinimumPromotionPrecision,
		"maximum_review_rate":              thresholds.MaximumReviewRate,
		"minimum_review_coverage":          thresholds.MinimumReviewCoverage,
		"minimum_inter_reviewer_agreement": thresholds.MinimumInterReviewerAgreement,
	} {
		if value < 0 || value > 1 {
			return contract, fmt.Errorf("admission study threshold %s must be between 0 and 1", name)
		}
	}
	if thresholds.MaximumProtectedFalseRejects < 0 || thresholds.MaximumUnsupportedProposals < 0 || thresholds.MaximumPrivacyLeaks < 0 {
		return contract, fmt.Errorf("admission study count thresholds must not be negative")
	}
	if thresholds.MinimumReviewCoverage != 1 {
		return contract, fmt.Errorf("admission study minimum_review_coverage must be 1")
	}
	if thresholds.MaximumProtectedFalseRejects != 0 || thresholds.MaximumUnsupportedProposals != 0 || thresholds.MaximumPrivacyLeaks != 0 {
		return contract, fmt.Errorf("admission study safety thresholds must be zero")
	}
	if !contract.Consent.Required {
		return contract, fmt.Errorf("admission study consent must be required")
	}
	if contract.Retention.Days <= 0 || contract.Retention.Cleanup != AdmissionStudyCleanupExplicit {
		return contract, fmt.Errorf("admission study retention requires positive days and %q cleanup", AdmissionStudyCleanupExplicit)
	}
	return contract, nil
}

func normalizeAdmissionStudyIdentifiers(label string, values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("admission study requires at least one %s", label)
	}
	result := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, raw := range values {
		value := strings.ToLower(strings.TrimSpace(raw))
		if !validAdmissionStudyIdentifier(value) {
			return nil, fmt.Errorf("invalid admission study %s %q", label, value)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("duplicate admission study %s %q", label, value)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func validAdmissionStudyIdentifier(value string) bool {
	return admissionStudyIdentifierPattern.MatchString(value)
}

func normalizeAdmissionStudySessionIDs(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("admission study cohort requires frozen session ids")
	}
	result := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if !admissionStudySessionIDPattern.MatchString(value) {
			return nil, fmt.Errorf("invalid admission study session id %q", value)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("duplicate admission study session id %q", value)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func NormalizeAdmissionStudyReviewerID(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if !validAdmissionStudyIdentifier(value) {
		return "", fmt.Errorf("invalid admission study reviewer id %q", value)
	}
	return value, nil
}

func normalizeAdmissionStudyRunMetadata(metadata AdmissionStudyRunMetadata) AdmissionStudyRunMetadata {
	metadata.StudyID = strings.ToLower(strings.TrimSpace(metadata.StudyID))
	metadata.StudyVersion = strings.ToLower(strings.TrimSpace(metadata.StudyVersion))
	metadata.Cohort = strings.ToLower(strings.TrimSpace(metadata.Cohort))
	metadata.Adapter = strings.ToLower(strings.TrimSpace(metadata.Adapter))
	metadata.ProjectType = strings.ToLower(strings.TrimSpace(metadata.ProjectType))
	metadata.SessionShape = strings.ToLower(strings.TrimSpace(metadata.SessionShape))
	metadata.ConsentAttestation = strings.ToLower(strings.TrimSpace(metadata.ConsentAttestation))
	return metadata
}

func validateAdmissionStudyMetadataAgainstContract(metadata AdmissionStudyRunMetadata, sessionID string, contract AdmissionStudyContract) error {
	for field, value := range map[string]string{
		"study_id": metadata.StudyID, "study_version": metadata.StudyVersion,
		"cohort": metadata.Cohort, "adapter": metadata.Adapter,
		"project_type": metadata.ProjectType, "session_shape": metadata.SessionShape,
		"consent_attestation": metadata.ConsentAttestation,
	} {
		if !validAdmissionStudyIdentifier(value) {
			return fmt.Errorf("%w: invalid %s %q", ErrAdmissionStudyMetadataMismatch, field, value)
		}
	}
	if metadata.StudyID != contract.StudyID || metadata.StudyVersion != contract.StudyVersion ||
		metadata.ConsentAttestation != contract.Consent.Attestation ||
		!admissionStudyContains(contract.Adapters, metadata.Adapter) ||
		!admissionStudyContains(contract.ProjectTypes, metadata.ProjectType) ||
		!admissionStudyContains(contract.SessionShapes, metadata.SessionShape) {
		return fmt.Errorf("%w: run dimensions are not declared by frozen contract", ErrAdmissionStudyMetadataMismatch)
	}
	for _, cohort := range contract.Cohorts {
		if cohort.ID == metadata.Cohort {
			if !admissionStudyContains(cohort.SessionIDs, sessionID) {
				return fmt.Errorf("%w: session %q is not frozen in cohort %q", ErrAdmissionStudyMetadataMismatch, sessionID, metadata.Cohort)
			}
			return nil
		}
	}
	return fmt.Errorf("%w: unknown cohort %q", ErrAdmissionStudyMetadataMismatch, metadata.Cohort)
}

func admissionStudyContains(values []string, target string) bool {
	index := sort.SearchStrings(values, target)
	return index < len(values) && values[index] == target
}

func (s *Store) AddAdmissionStudyOmission(p AddAdmissionStudyOmissionParams) (*AdmissionStudyOmission, bool, error) {
	p.RunID = strings.TrimSpace(p.RunID)
	p.ReviewerID = strings.ToLower(strings.TrimSpace(p.ReviewerID))
	p.Category = strings.ToLower(strings.TrimSpace(p.Category))
	p.ReasonCode = strings.ToLower(strings.TrimSpace(p.ReasonCode))
	p.Annotation = normalizeAdmissionStudyOmissionAnnotation(p.Annotation)
	if p.RunID == "" {
		return nil, false, fmt.Errorf("admission study omission requires run id")
	}
	if strings.TrimSpace(p.Annotation) == "" {
		return nil, false, fmt.Errorf("admission study omission requires annotation")
	}
	var result AdmissionStudyOmission
	alreadyRecorded := false
	err := s.withTx(func(tx *sql.Tx) error {
		var studyID, studyVersion string
		if err := tx.QueryRow(`
			SELECT ifnull(study_id, ''), ifnull(study_version, '')
			FROM admission_shadow_runs WHERE id = ?`, p.RunID).Scan(&studyID, &studyVersion); errors.Is(err, sql.ErrNoRows) {
			return ErrAdmissionShadowRunNotFound
		} else if err != nil {
			return err
		}
		if studyID == "" {
			return fmt.Errorf("%w: omissions require an attributed study run", ErrAdmissionStudyMetadataMismatch)
		}
		study, err := admissionStudyByIdentity(tx, studyID, studyVersion)
		if err != nil {
			return err
		}
		if !validAdmissionStudyIdentifier(p.ReviewerID) ||
			!admissionStudyContains(study.Contract.LabelSchema.OmissionCategories, p.Category) ||
			!admissionStudyContains(study.Contract.LabelSchema.ReasonCodes, p.ReasonCode) {
			return fmt.Errorf("%w: omission labels are not declared by frozen contract", ErrAdmissionStudyMetadataMismatch)
		}
		existing, err := admissionStudyMatchingOmissionTx(tx, p)
		if err != nil {
			return err
		}
		if existing != nil {
			result = *existing
			alreadyRecorded = true
			return nil
		}
		var nextOrdinal int
		if err := tx.QueryRow(`
			SELECT ifnull(MAX(ordinal) + 1, 0)
			FROM admission_study_omissions
			WHERE run_id = ? AND reviewer_id = ?`, p.RunID, p.ReviewerID).Scan(&nextOrdinal); err != nil {
			return err
		}
		result = AdmissionStudyOmission{
			ID: newSyncID("study-omission"), RunID: p.RunID, ReviewerID: p.ReviewerID,
			Ordinal: nextOrdinal, Category: p.Category, ReasonCode: p.ReasonCode, Annotation: p.Annotation,
		}
		if _, err := s.execHook(tx, `
			INSERT INTO admission_study_omissions (
				id, run_id, reviewer_id, ordinal, category, reason_code, annotation
			) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			result.ID, result.RunID, result.ReviewerID, result.Ordinal,
			result.Category, result.ReasonCode, result.Annotation,
		); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT created_at FROM admission_study_omissions WHERE id = ?`, result.ID).Scan(&result.CreatedAt)
	})
	if err != nil {
		return nil, false, err
	}
	return &result, alreadyRecorded, nil
}

func (s *Store) ListAdmissionStudyOmissions(studyID, studyVersion string) ([]AdmissionStudyOmission, error) {
	studyID = strings.ToLower(strings.TrimSpace(studyID))
	studyVersion = strings.ToLower(strings.TrimSpace(studyVersion))
	if !validAdmissionStudyIdentifier(studyID) || !validAdmissionStudyIdentifier(studyVersion) {
		return nil, fmt.Errorf("invalid admission study identity")
	}
	rows, err := s.queryItHook(s.db, `
		SELECT o.id, o.run_id, o.reviewer_id, o.ordinal, o.category,
		       o.reason_code, o.annotation, o.created_at
		FROM admission_study_omissions o
		JOIN admission_shadow_runs r ON r.id = o.run_id
		WHERE r.study_id = ? AND r.study_version = ?
		ORDER BY datetime(r.created_at), r.id, o.reviewer_id, o.ordinal, o.id`, studyID, studyVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]AdmissionStudyOmission, 0)
	for rows.Next() {
		var omission AdmissionStudyOmission
		if err := rows.Scan(&omission.ID, &omission.RunID, &omission.ReviewerID,
			&omission.Ordinal, &omission.Category, &omission.ReasonCode,
			&omission.Annotation, &omission.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, omission)
	}
	return result, rows.Err()
}

func (s *Store) ListAdmissionStudyRuns(studyID, studyVersion string) ([]AdmissionShadowRun, error) {
	studyID = strings.ToLower(strings.TrimSpace(studyID))
	studyVersion = strings.ToLower(strings.TrimSpace(studyVersion))
	if !validAdmissionStudyIdentifier(studyID) || !validAdmissionStudyIdentifier(studyVersion) {
		return nil, fmt.Errorf("invalid admission study identity")
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
		WHERE study_id = ? AND study_version = ?
		ORDER BY datetime(created_at), id`, studyID, studyVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]AdmissionShadowRun, 0)
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
		result = append(result, run)
	}
	return result, rows.Err()
}

func (s *Store) ListAdmissionStudyProposals(studyID, studyVersion string) ([]AdmissionShadowProposal, error) {
	studyID = strings.ToLower(strings.TrimSpace(studyID))
	studyVersion = strings.ToLower(strings.TrimSpace(studyVersion))
	if !validAdmissionStudyIdentifier(studyID) || !validAdmissionStudyIdentifier(studyVersion) {
		return nil, fmt.Errorf("invalid admission study identity")
	}
	proposals, err := s.queryAdmissionShadowProposals(`
		SELECT p.id, p.run_id, p.ordinal, p.type, p.title, p.content, p.scope,
		       p.category, p.protected, p.recommendation, p.proposal_reason_codes,
		       p.assessment_reason_codes, p.evidence_refs, p.created_at
		FROM admission_shadow_proposals p
		JOIN admission_shadow_runs r ON r.id = p.run_id
		WHERE r.study_id = ? AND r.study_version = ?
		ORDER BY datetime(r.created_at), r.id, p.ordinal`, studyID, studyVersion)
	if err != nil || len(proposals) == 0 {
		return proposals, err
	}
	reviews, err := s.listAdmissionStudyReviews(studyID, studyVersion)
	if err != nil {
		return nil, err
	}
	for index := range proposals {
		proposals[index].Reviews = reviews[proposals[index].ID]
		if proposals[index].Reviews == nil {
			proposals[index].Reviews = []AdmissionShadowReview{}
		}
	}
	return proposals, nil
}

func (s *Store) listAdmissionStudyReviews(studyID, studyVersion string) (map[string][]AdmissionShadowReview, error) {
	rows, err := s.queryItHook(s.db, `
		SELECT v.id, v.proposal_id, v.reviewer_id, v.ordinal, v.verdict, v.note,
		       v.unsupported, v.privacy_leak, v.created_at
		FROM admission_shadow_reviews v
		JOIN admission_shadow_proposals p ON p.id = v.proposal_id
		JOIN admission_shadow_runs r ON r.id = p.run_id
		WHERE r.study_id = ? AND r.study_version = ?
		ORDER BY datetime(r.created_at), r.id, p.ordinal, v.ordinal, v.id`, studyID, studyVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string][]AdmissionShadowReview)
	for rows.Next() {
		var review AdmissionShadowReview
		if err := rows.Scan(&review.ID, &review.ProposalID, &review.ReviewerID,
			&review.Ordinal, &review.Verdict, &review.Note, &review.Unsupported,
			&review.PrivacyLeak, &review.CreatedAt); err != nil {
			return nil, err
		}
		result[review.ProposalID] = append(result[review.ProposalID], review)
	}
	return result, rows.Err()
}

func admissionStudyMatchingOmissionTx(tx *sql.Tx, p AddAdmissionStudyOmissionParams) (*AdmissionStudyOmission, error) {
	var omission AdmissionStudyOmission
	err := tx.QueryRow(`
		SELECT id, run_id, reviewer_id, ordinal, category, reason_code, annotation, created_at
		FROM admission_study_omissions
		WHERE run_id = ? AND reviewer_id = ? AND category = ? AND reason_code = ? AND annotation = ?
		ORDER BY ordinal, id LIMIT 1`, p.RunID, p.ReviewerID, p.Category, p.ReasonCode, p.Annotation).Scan(
		&omission.ID, &omission.RunID, &omission.ReviewerID, &omission.Ordinal,
		&omission.Category, &omission.ReasonCode, &omission.Annotation, &omission.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &omission, nil
}

func normalizeAdmissionStudyOmissionAnnotation(annotation string) string {
	annotation = RedactPrivateBlocks(annotation)
	runes := []rune(annotation)
	if len(runes) > MaxAdmissionStudyOmissionAnnotationLength {
		annotation = string(runes[:MaxAdmissionStudyOmissionAnnotationLength])
	}
	return annotation
}
