package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestAdmissionStudyMigrationUpgradesLegacyShadowTables(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	initial, err := New(cfg)
	if err != nil {
		t.Fatalf("create initial database: %v", err)
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("close initial database: %v", err)
	}

	raw, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "engram.db"))
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	if _, err := raw.Exec(`
		PRAGMA foreign_keys = OFF;
		DROP TABLE admission_study_omissions;
		DROP TABLE admission_studies;
		DROP TABLE admission_shadow_reviews;
		DROP TABLE admission_shadow_proposals;
		DROP TABLE admission_shadow_runs;
		CREATE TABLE admission_shadow_runs (
			id TEXT PRIMARY KEY, project TEXT NOT NULL, session_id TEXT, mode TEXT NOT NULL,
			evidence_version TEXT NOT NULL, generator_version TEXT NOT NULL, policy_version TEXT NOT NULL,
			diagnostic_codes TEXT NOT NULL DEFAULT '[]', included_items INTEGER NOT NULL DEFAULT 0,
			included_content_bytes INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT (datetime('now'))
		);
		CREATE TABLE admission_shadow_proposals (
			id TEXT PRIMARY KEY, run_id TEXT NOT NULL, ordinal INTEGER NOT NULL, type TEXT NOT NULL,
			title TEXT NOT NULL, content TEXT NOT NULL, scope TEXT NOT NULL, category TEXT NOT NULL,
			protected BOOLEAN NOT NULL DEFAULT 0, recommendation TEXT NOT NULL,
			proposal_reason_codes TEXT NOT NULL DEFAULT '[]', assessment_reason_codes TEXT NOT NULL DEFAULT '[]',
			evidence_refs TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (run_id) REFERENCES admission_shadow_runs(id) ON DELETE CASCADE,
			UNIQUE (run_id, ordinal)
		);
		CREATE TABLE admission_shadow_reviews (
			id TEXT PRIMARY KEY, proposal_id TEXT NOT NULL, ordinal INTEGER NOT NULL, verdict TEXT NOT NULL,
			note TEXT NOT NULL DEFAULT '', unsupported BOOLEAN NOT NULL DEFAULT 0,
			privacy_leak BOOLEAN NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (proposal_id) REFERENCES admission_shadow_proposals(id) ON DELETE CASCADE,
			UNIQUE (proposal_id, ordinal)
		);
		INSERT INTO admission_shadow_runs (
			id, project, session_id, mode, evidence_version, generator_version, policy_version
		) VALUES ('legacy-run', 'engram', 'legacy-session', 'session', 'v1', 'v1', 'v1');
		INSERT INTO admission_shadow_proposals (
			id, run_id, ordinal, type, title, content, scope, category, recommendation
		) VALUES ('legacy-proposal', 'legacy-run', 0, 'decision', 'Legacy', 'Legacy content', 'project', 'decision', 'admit');
		INSERT INTO admission_shadow_reviews (
			id, proposal_id, ordinal, verdict
		) VALUES ('legacy-review', 'legacy-proposal', 0, 'admit');
	`); err != nil {
		raw.Close()
		t.Fatalf("prepare legacy shadow schema: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}

	upgraded, err := New(cfg)
	if err != nil {
		t.Fatalf("upgrade legacy shadow schema: %v", err)
	}
	t.Cleanup(func() { _ = upgraded.Close() })
	runs, err := upgraded.ListAdmissionShadowRuns("engram")
	if err != nil || len(runs) != 1 || runs[0].StudyID != "" {
		t.Fatalf("upgraded legacy runs = %#v err=%v", runs, err)
	}
	proposals, err := upgraded.ListAdmissionShadowProposals("engram", false)
	if err != nil || len(proposals) != 1 || len(proposals[0].Reviews) != 1 || proposals[0].Reviews[0].ReviewerID != "" {
		t.Fatalf("upgraded legacy reviews = %#v err=%v", proposals, err)
	}
	if _, _, err := upgraded.FreezeAdmissionStudy(validAdmissionStudyContract()); err != nil {
		t.Fatalf("freeze study after migration: %v", err)
	}
}

func TestFreezeAdmissionStudyIsVersionedImmutableAndIdempotent(t *testing.T) {
	s := newTestStore(t)
	contract := validAdmissionStudyContract()

	first, alreadyFrozen, err := s.FreezeAdmissionStudy(contract)
	if err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}
	if alreadyFrozen {
		t.Fatal("first freeze reported already frozen")
	}
	if first.ContractHash == "" || first.Contract.StudyID != contract.StudyID ||
		first.Contract.StudyVersion != contract.StudyVersion {
		t.Fatalf("frozen study = %#v", first)
	}

	repeated, alreadyFrozen, err := s.FreezeAdmissionStudy(contract)
	if err != nil {
		t.Fatalf("repeat admission study freeze: %v", err)
	}
	if !alreadyFrozen || repeated.ContractHash != first.ContractHash ||
		!reflect.DeepEqual(repeated.Contract, first.Contract) {
		t.Fatalf("repeated freeze = %#v, want %#v", repeated, first)
	}

	changed := contract
	changed.Thresholds.MinimumPromotionPrecision = 0.99
	if _, _, err := s.FreezeAdmissionStudy(changed); !errors.Is(err, ErrAdmissionStudyContractChanged) {
		t.Fatalf("changed frozen contract error = %v, want %v", err, ErrAdmissionStudyContractChanged)
	}

	unsafe := contract
	unsafe.StudyVersion = "v2"
	unsafe.Thresholds.MaximumPrivacyLeaks = 1
	if _, _, err := s.FreezeAdmissionStudy(unsafe); err == nil {
		t.Fatal("study with non-zero safety tolerance unexpectedly frozen")
	}
}

func TestFreezeAdmissionStudyRejectsIncompleteEvaluationContracts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*AdmissionStudyContract)
	}{
		{name: "partial review coverage", mutate: func(contract *AdmissionStudyContract) {
			contract.Thresholds.MinimumReviewCoverage = 0.99
		}},
		{name: "non-zero safety tolerance", mutate: func(contract *AdmissionStudyContract) {
			contract.Thresholds.MaximumUnsupportedProposals = 1
		}},
		{name: "missing durable omission category", mutate: func(contract *AdmissionStudyContract) {
			contract.LabelSchema.OmissionCategories = []string{"decision", "root_cause", "invariant", "constraint"}
		}},
		{name: "no independent review sample", mutate: func(contract *AdmissionStudyContract) {
			contract.Cohorts[0].MinimumIndependentReviewedProposals = 0
		}},
		{name: "overlapping cohort manifests", mutate: func(contract *AdmissionStudyContract) {
			contract.Cohorts[1].SessionIDs = append(contract.Cohorts[1].SessionIDs, "session-1")
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newTestStore(t)
			contract := validAdmissionStudyContract()
			test.mutate(&contract)
			if _, _, err := s.FreezeAdmissionStudy(contract); err == nil {
				t.Fatal("incomplete evaluation contract unexpectedly frozen")
			}
		})
	}
}

func TestCreateAdmissionShadowRunValidatesAndRetainsFrozenStudyMetadata(t *testing.T) {
	s := newTestStore(t)
	contract := validAdmissionStudyContract()
	study, _, err := s.FreezeAdmissionStudy(contract)
	if err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}

	params := validAdmissionShadowRunParams("study proposal")
	params.Study = &AdmissionStudyRunMetadata{
		StudyID:                   contract.StudyID,
		StudyVersion:              contract.StudyVersion,
		Cohort:                    "calibration",
		Adapter:                   "codex",
		ProjectType:               "cli",
		SessionShape:              "feature",
		ConsentAttestation:        "consent-v1",
		IndependentReviewRequired: true,
	}
	run, err := s.CreateAdmissionShadowRun(params)
	if err != nil {
		t.Fatalf("create attributed shadow run: %v", err)
	}
	if run.StudyID != contract.StudyID || run.StudyVersion != contract.StudyVersion ||
		run.StudyContractHash != study.ContractHash || run.Cohort != "calibration" ||
		run.CohortKind != "calibration" || run.Adapter != "codex" ||
		run.ProjectType != "cli" || run.SessionShape != "feature" ||
		run.ConsentAttestation != "consent-v1" || !run.IndependentReviewRequired {
		t.Fatalf("attributed shadow run = %#v", run)
	}

	invalid := validAdmissionShadowRunParams("invalid proposal")
	invalid.Study = &AdmissionStudyRunMetadata{
		StudyID:            contract.StudyID,
		StudyVersion:       contract.StudyVersion,
		Cohort:             "held-out",
		Adapter:            "unknown-adapter",
		ProjectType:        "cli",
		SessionShape:       "feature",
		ConsentAttestation: "consent-v1",
	}
	if _, err := s.CreateAdmissionShadowRun(invalid); !errors.Is(err, ErrAdmissionStudyMetadataMismatch) {
		t.Fatalf("unknown study metadata error = %v, want %v", err, ErrAdmissionStudyMetadataMismatch)
	}
	if got := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM admission_shadow_runs`); got != 1 {
		t.Fatalf("shadow run count after rejected metadata = %d, want 1", got)
	}

	undeclaredSession := validAdmissionShadowRunParams("undeclared session")
	undeclaredSession.SessionID = "session-unlisted"
	undeclaredSession.Study = validAdmissionStudyRunMetadata("calibration")
	if _, err := s.CreateAdmissionShadowRun(undeclaredSession); !errors.Is(err, ErrAdmissionStudyMetadataMismatch) {
		t.Fatalf("undeclared study session error = %v, want %v", err, ErrAdmissionStudyMetadataMismatch)
	}

	missingSession := validAdmissionShadowRunParams("missing session")
	missingSession.SessionID = ""
	missingSession.Study = validAdmissionStudyRunMetadata("held-out")
	if _, err := s.CreateAdmissionShadowRun(missingSession); !errors.Is(err, ErrAdmissionStudyMetadataMismatch) {
		t.Fatalf("missing study session error = %v, want %v", err, ErrAdmissionStudyMetadataMismatch)
	}
}

func TestAdmissionStudyReviewsKeepIndependentReviewerStreams(t *testing.T) {
	s := newTestStore(t)
	contract := validAdmissionStudyContract()
	if _, _, err := s.FreezeAdmissionStudy(contract); err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}
	params := validAdmissionShadowRunParams("independent labels")
	params.Study = validAdmissionStudyRunMetadata("calibration")
	run, err := s.CreateAdmissionShadowRun(params)
	if err != nil {
		t.Fatalf("create attributed shadow run: %v", err)
	}
	proposalID := run.Proposals[0].ID

	first, alreadyRecorded, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID: proposalID, ReviewerID: "reviewer-a", Verdict: "admit",
	})
	if err != nil || alreadyRecorded {
		t.Fatalf("first reviewer label = %#v already=%t err=%v", first, alreadyRecorded, err)
	}
	second, alreadyRecorded, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID: proposalID, ReviewerID: "reviewer-b", Verdict: "reject",
	})
	if err != nil || alreadyRecorded || second.ReviewerID != "reviewer-b" || second.ID == first.ID {
		t.Fatalf("second reviewer label = %#v already=%t err=%v", second, alreadyRecorded, err)
	}
	repeated, alreadyRecorded, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID: proposalID, ReviewerID: "reviewer-a", Verdict: "admit",
	})
	if err != nil || !alreadyRecorded || repeated.ID != first.ID {
		t.Fatalf("first reviewer retry = %#v already=%t err=%v", repeated, alreadyRecorded, err)
	}

	proposals, err := s.ListAdmissionShadowProposals("engram", false)
	if err != nil || len(proposals) != 1 || len(proposals[0].Reviews) != 2 {
		t.Fatalf("independent labels = %#v err=%v", proposals, err)
	}
	if proposals[0].Reviews[0].ReviewerID != "reviewer-a" || proposals[0].Reviews[1].ReviewerID != "reviewer-b" {
		t.Fatalf("reviewer streams = %#v", proposals[0].Reviews)
	}
}

func TestAdmissionStudyOmissionIsBoundedRedactedAndIdempotent(t *testing.T) {
	s := newTestStore(t)
	contract := validAdmissionStudyContract()
	if _, _, err := s.FreezeAdmissionStudy(contract); err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}
	params := validAdmissionShadowRunParams("omission run")
	params.SessionID = "session-2"
	params.Study = validAdmissionStudyRunMetadata("held-out")
	run, err := s.CreateAdmissionShadowRun(params)
	if err != nil {
		t.Fatalf("create attributed shadow run: %v", err)
	}
	annotation := "Missed invariant <private>secret material</private> " + strings.Repeat("x", MaxAdmissionStudyOmissionAnnotationLength+20)
	first, alreadyRecorded, err := s.AddAdmissionStudyOmission(AddAdmissionStudyOmissionParams{
		RunID: run.ID, ReviewerID: "reviewer-a", Category: "invariant",
		ReasonCode: "not_proposed", Annotation: annotation,
	})
	if err != nil || alreadyRecorded {
		t.Fatalf("first omission = %#v already=%t err=%v", first, alreadyRecorded, err)
	}
	if strings.Contains(first.Annotation, "secret material") || len([]rune(first.Annotation)) > MaxAdmissionStudyOmissionAnnotationLength {
		t.Fatalf("unsafe omission annotation = %q", first.Annotation)
	}
	second, alreadyRecorded, err := s.AddAdmissionStudyOmission(AddAdmissionStudyOmissionParams{
		RunID: run.ID, ReviewerID: "reviewer-a", Category: "decision",
		ReasonCode: "wrong_category", Annotation: "Missed decision.",
	})
	if err != nil || alreadyRecorded || second.ID == first.ID {
		t.Fatalf("second omission = %#v already=%t err=%v", second, alreadyRecorded, err)
	}
	repeated, alreadyRecorded, err := s.AddAdmissionStudyOmission(AddAdmissionStudyOmissionParams{
		RunID: run.ID, ReviewerID: "reviewer-a", Category: "invariant",
		ReasonCode: "not_proposed", Annotation: annotation,
	})
	if err != nil || !alreadyRecorded || repeated.ID != first.ID {
		t.Fatalf("repeated omission = %#v already=%t err=%v", repeated, alreadyRecorded, err)
	}
	if _, _, err := s.AddAdmissionStudyOmission(AddAdmissionStudyOmissionParams{
		RunID: run.ID, ReviewerID: "reviewer-b", Category: "unknown",
		ReasonCode: "not_proposed", Annotation: "bounded",
	}); !errors.Is(err, ErrAdmissionStudyMetadataMismatch) {
		t.Fatalf("unknown omission label error = %v", err)
	}
	if _, _, err := s.AddAdmissionStudyOmission(AddAdmissionStudyOmissionParams{
		RunID: run.ID, ReviewerID: "reviewer-b", Category: "decision",
		ReasonCode: "not_proposed", Annotation: "",
	}); err == nil {
		t.Fatal("empty omission annotation unexpectedly succeeded")
	}
	omissions, err := s.ListAdmissionStudyOmissions(contract.StudyID, contract.StudyVersion)
	if err != nil || len(omissions) != 2 || omissions[0].ReviewerID != "reviewer-a" {
		t.Fatalf("omissions = %#v err=%v", omissions, err)
	}
}

func TestDeleteAdmissionStudyRemovesOnlyOwnedLocalRows(t *testing.T) {
	s := newTestStore(t)
	contract := validAdmissionStudyContract()
	if _, _, err := s.FreezeAdmissionStudy(contract); err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}
	params := validAdmissionShadowRunParams("owned study proposal")
	params.Study = validAdmissionStudyRunMetadata("calibration")
	run, err := s.CreateAdmissionShadowRun(params)
	if err != nil {
		t.Fatalf("create attributed run: %v", err)
	}
	if _, _, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID: run.Proposals[0].ID, ReviewerID: "reviewer-a", Verdict: "admit",
	}); err != nil {
		t.Fatalf("add attributed review: %v", err)
	}
	if _, _, err := s.AddAdmissionStudyOmission(AddAdmissionStudyOmissionParams{
		RunID: run.ID, ReviewerID: "reviewer-a", Category: "decision",
		ReasonCode: "not_proposed", Annotation: "Missed durable decision.",
	}); err != nil {
		t.Fatalf("add admission study omission: %v", err)
	}
	legacy := mustCreateAdmissionShadowRun(t, s, "engram", "legacy proposal")

	deleted, err := s.DeleteAdmissionStudy(contract.StudyID, contract.StudyVersion)
	if err != nil {
		t.Fatalf("delete admission study: %v", err)
	}
	if deleted.RunCount != 1 || deleted.ProposalCount != 1 || deleted.ReviewCount != 1 || deleted.OmissionCount != 1 {
		t.Fatalf("cleanup result = %#v", deleted)
	}
	if _, err := s.GetAdmissionStudy(contract.StudyID, contract.StudyVersion); !errors.Is(err, ErrAdmissionStudyNotFound) {
		t.Fatalf("study after cleanup error = %v", err)
	}
	proposals, err := s.ListAdmissionShadowProposals("engram", false)
	if err != nil || len(proposals) != 1 || proposals[0].RunID != legacy.ID {
		t.Fatalf("remaining legacy shadow rows = %#v err=%v", proposals, err)
	}
}

func TestAdmissionStudyRowsRemainOutsideExportAndSync(t *testing.T) {
	s := newTestStore(t)
	beforeMutations := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM sync_mutations`)
	contract := validAdmissionStudyContract()
	contract.StudyID = "private-study-marker"
	if _, _, err := s.FreezeAdmissionStudy(contract); err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}
	params := validAdmissionShadowRunParams("private-proposal-marker")
	metadata := validAdmissionStudyRunMetadata("calibration")
	metadata.StudyID = contract.StudyID
	params.Study = metadata
	run, err := s.CreateAdmissionShadowRun(params)
	if err != nil {
		t.Fatalf("create attributed run: %v", err)
	}
	if _, _, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID: run.Proposals[0].ID, ReviewerID: "private-reviewer-marker", Verdict: "admit",
	}); err != nil {
		t.Fatalf("add attributed review: %v", err)
	}
	if _, _, err := s.AddAdmissionStudyOmission(AddAdmissionStudyOmissionParams{
		RunID: run.ID, ReviewerID: "private-reviewer-marker", Category: "decision",
		ReasonCode: "not_proposed", Annotation: "private-omission-marker",
	}); err != nil {
		t.Fatalf("add attributed omission: %v", err)
	}

	exported, err := s.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	encoded, err := json.Marshal(exported)
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}
	for _, forbidden := range []string{"private-study-marker", "private-proposal-marker", "private-reviewer-marker", "private-omission-marker"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("normal export leaked %q: %s", forbidden, encoded)
		}
	}
	searchResults, err := s.Search("private-study-marker", SearchOptions{Project: "engram", Limit: 10})
	if err != nil || len(searchResults) != 0 {
		t.Fatalf("Memory search exposed study rows = %#v err=%v", searchResults, err)
	}
	contextOutput, err := s.FormatContext("engram", "project")
	if err != nil {
		t.Fatalf("format Memory context: %v", err)
	}
	if strings.Contains(contextOutput, "private-study-marker") || strings.Contains(contextOutput, "private-proposal-marker") ||
		strings.Contains(contextOutput, "private-reviewer-marker") || strings.Contains(contextOutput, "private-omission-marker") {
		t.Fatalf("Memory context exposed study rows: %q", contextOutput)
	}
	if got := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM sync_mutations`); got != beforeMutations {
		t.Fatalf("study operations created sync mutations: before=%d after=%d", beforeMutations, got)
	}
}

func validAdmissionStudyContract() AdmissionStudyContract {
	return AdmissionStudyContract{
		ContractVersion: "admission-study-v1",
		StudyID:         "real-session-v1",
		StudyVersion:    "v1",
		MetricsVersion:  "admission-study-metrics-v1",
		Cohorts: []AdmissionStudyCohort{
			{ID: "calibration", Kind: "calibration", SessionIDs: []string{"session-1", "session-calibration-2"}, MinimumRuns: 2, MinimumProposals: 2, MinimumIndependentReviewedProposals: 1},
			{ID: "held-out", Kind: "held_out", SessionIDs: []string{"session-2", "session-held-out-2"}, MinimumRuns: 2, MinimumProposals: 2, MinimumIndependentReviewedProposals: 1},
		},
		Adapters:      []string{"codex", "claude-code"},
		ProjectTypes:  []string{"cli", "library"},
		SessionShapes: []string{"feature", "bugfix"},
		LabelSchema: AdmissionStudyLabelSchema{
			Version:            "admission-study-labels-v1",
			Verdicts:           []string{"admit", "review", "reject"},
			OmissionCategories: []string{"decision", "root_cause", "invariant", "constraint", "preference"},
			ReasonCodes:        []string{"not_proposed", "wrong_category", "insufficient_evidence"},
		},
		Thresholds: AdmissionStudyThresholds{
			MinimumPromotionPrecision:     0.90,
			MaximumReviewRate:             0.40,
			MinimumReviewCoverage:         1,
			MinimumInterReviewerAgreement: 0.80,
			MaximumProtectedFalseRejects:  0,
			MaximumUnsupportedProposals:   0,
			MaximumPrivacyLeaks:           0,
		},
		Consent:   AdmissionStudyConsent{Required: true, Attestation: "consent-v1"},
		Retention: AdmissionStudyRetention{Days: 30, Cleanup: "explicit_study_cleanup"},
		AllowedAggregateOutputs: []string{
			"counts", "distributions", "quality", "uncertainty", "sufficiency", "gates",
		},
	}
}

func validAdmissionStudyRunMetadata(cohort string) *AdmissionStudyRunMetadata {
	return &AdmissionStudyRunMetadata{
		StudyID:                   "real-session-v1",
		StudyVersion:              "v1",
		Cohort:                    cohort,
		Adapter:                   "codex",
		ProjectType:               "cli",
		SessionShape:              "feature",
		ConsentAttestation:        "consent-v1",
		IndependentReviewRequired: true,
	}
}

func validAdmissionShadowRunParams(title string) CreateAdmissionShadowRunParams {
	return CreateAdmissionShadowRunParams{
		Project:          "engram",
		SessionID:        "session-1",
		Mode:             "session",
		EvidenceVersion:  "v1",
		GeneratorVersion: "v1",
		PolicyVersion:    "v1",
		Proposals: []AdmissionShadowProposalInput{{
			Type:                  "decision",
			Title:                 title,
			Content:               "Keep the study attributable.",
			Scope:                 "project",
			Category:              "decision",
			Protected:             true,
			Recommendation:        "admit",
			AssessmentReasonCodes: []string{"explicit_user_request"},
		}},
	}
}
