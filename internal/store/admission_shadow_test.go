package store

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestRedactPrivateBlocksHandlesNestedUnclosedAndCaseInsensitiveTags(t *testing.T) {
	tests := map[string]string{
		"unicode prefix": "café <PRIVATE>credential</PRIVATE> remains",
		"nested":         "before <private>outer <private>inner</private> tail</private> after",
		"unclosed":       "before <private>credential tail",
	}
	want := map[string]string{
		"unicode prefix": "café [REDACTED] remains",
		"nested":         "before [REDACTED] after",
		"unclosed":       "before [REDACTED]",
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if got := RedactPrivateBlocks(input); got != want[name] {
				t.Fatalf("RedactPrivateBlocks(%q) = %q, want %q", input, got, want[name])
			}
		})
	}
}

func TestAdmissionShadowMigrationFreshAndReopened(t *testing.T) {
	dir := t.TempDir()
	cfg := mustDefaultConfig(t)
	cfg.DataDir = dir

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	assertAdmissionShadowSchema(t, s)
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	assertAdmissionShadowSchema(t, reopened)
}

func assertAdmissionShadowSchema(t *testing.T, s *Store) {
	t.Helper()
	wantTables := []string{
		"admission_shadow_proposals",
		"admission_shadow_reviews",
		"admission_shadow_runs",
	}
	rows, err := s.db.Query(`
		SELECT name FROM sqlite_master
		WHERE type = 'table' AND name LIKE 'admission_shadow_%'
		ORDER BY name`)
	if err != nil {
		t.Fatalf("list shadow tables: %v", err)
	}
	defer rows.Close()
	var gotTables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan shadow table: %v", err)
		}
		gotTables = append(gotTables, name)
	}
	if len(gotTables) != len(wantTables) {
		t.Fatalf("shadow tables = %v, want %v", gotTables, wantTables)
	}
	for i := range wantTables {
		if gotTables[i] != wantTables[i] {
			t.Fatalf("shadow tables = %v, want %v", gotTables, wantTables)
		}
	}

	for _, table := range wantTables {
		columns, err := s.db.Query(`PRAGMA table_info(` + table + `)`)
		if err != nil {
			t.Fatalf("table info %s: %v", table, err)
		}
		for columns.Next() {
			var cid, notNull, primaryKey int
			var name, typ string
			var defaultValue any
			if err := columns.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
				t.Fatalf("scan table info %s: %v", table, err)
			}
			switch name {
			case "evidence", "evidence_bundle", "prompt", "summary", "raw_evidence":
				t.Fatalf("%s must not contain raw evidence column %q", table, name)
			}
		}
		if err := columns.Close(); err != nil {
			t.Fatalf("close table info %s: %v", table, err)
		}
	}

	indexRows, err := s.db.Query(`
		SELECT name FROM sqlite_master
		WHERE type = 'index' AND tbl_name LIKE 'admission_shadow_%' AND sql IS NOT NULL`)
	if err != nil {
		t.Fatalf("list shadow indexes: %v", err)
	}
	defer indexRows.Close()
	var indexes []string
	for indexRows.Next() {
		var name string
		if err := indexRows.Scan(&name); err != nil {
			t.Fatalf("scan shadow index: %v", err)
		}
		indexes = append(indexes, name)
	}
	sort.Strings(indexes)
	if len(indexes) < 3 {
		t.Fatalf("shadow indexes = %v, want at least three query indexes", indexes)
	}

	var syncTriggerCount int
	if err := s.db.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'trigger'
		  AND tbl_name LIKE 'admission_shadow_%'
		  AND lower(sql) LIKE '%sync_mutations%'`).Scan(&syncTriggerCount); err != nil {
		t.Fatalf("query shadow sync triggers: %v", err)
	}
	if syncTriggerCount != 0 {
		t.Fatalf("shadow tables have %d sync mutation triggers, want 0", syncTriggerCount)
	}
}

func TestCreateAdmissionShadowRunPersistsRedactedImmutableSnapshot(t *testing.T) {
	s := newTestStore(t)
	beforeMutations := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM sync_mutations`)

	run, err := s.CreateAdmissionShadowRun(CreateAdmissionShadowRunParams{
		Project:              " ENGRAM ",
		SessionID:            "deleted-or-missing-session",
		Mode:                 "session",
		EvidenceVersion:      "v1",
		GeneratorVersion:     "v1",
		PolicyVersion:        "v1",
		DiagnosticCodes:      []string{"session_summary_fallback"},
		IncludedItems:        3,
		IncludedContentBytes: 144,
		Proposals: []AdmissionShadowProposalInput{
			{
				Type:                  "decision",
				Title:                 "Keep <private>title-secret</private> locally",
				Content:               "SQLite <private>outer <private>nested-secret</private> trailing-secret</private> is authoritative.",
				Scope:                 "project",
				Category:              "decision",
				Protected:             true,
				Recommendation:        "review",
				ProposalReasonCodes:   []string{"structured_section"},
				AssessmentReasonCodes: []string{"protected_proposal", "requires_review"},
				EvidenceRefs:          []string{"prompt:1", "summary:1"},
			},
			{
				Type:                  "discovery",
				Title:                 "Stable ordering",
				Content:               "Use ordinal ordering.",
				Scope:                 "project",
				Category:              "learning",
				Recommendation:        "admit",
				ProposalReasonCodes:   []string{"structured_section"},
				AssessmentReasonCodes: []string{"requires_review"},
				EvidenceRefs:          []string{"prompt:2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("create shadow run: %v", err)
	}
	if run.ID == "" || run.Project != "engram" || run.IncludedItems != 3 || run.IncludedContentBytes != 144 ||
		run.EvidenceVersion != "v1" || run.GeneratorVersion != "v1" || run.PolicyVersion != "v1" ||
		!reflect.DeepEqual(run.DiagnosticCodes, []string{"session_summary_fallback"}) {
		t.Fatalf("run metadata = %#v", run)
	}
	if len(run.Proposals) != 2 {
		t.Fatalf("run proposals = %d, want 2", len(run.Proposals))
	}
	if run.Proposals[0].Ordinal != 0 || run.Proposals[1].Ordinal != 1 {
		t.Fatalf("proposal ordinals = %d,%d", run.Proposals[0].Ordinal, run.Proposals[1].Ordinal)
	}
	encoded, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("marshal run: %v", err)
	}
	for _, secret := range []string{"title-secret", "nested-secret", "trailing-secret", "<private>", "</private>"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("persisted result contains private value %q: %s", secret, encoded)
		}
	}
	if !strings.Contains(run.Proposals[0].Content, "[REDACTED]") {
		t.Fatalf("redacted content = %q", run.Proposals[0].Content)
	}
	if got := run.Proposals[0].ProposalReasonCodes; !reflect.DeepEqual(got, []string{"structured_section"}) {
		t.Fatalf("proposal reasons = %#v", got)
	}
	if got := run.Proposals[0].AssessmentReasonCodes; !reflect.DeepEqual(got, []string{"protected_proposal", "requires_review"}) {
		t.Fatalf("assessment reasons = %#v", got)
	}
	if got := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM sync_mutations`); got != beforeMutations {
		t.Fatalf("sync mutations = %d, want unchanged %d", got, beforeMutations)
	}

	exported, err := s.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	exportJSON, err := json.Marshal(exported)
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}
	if strings.Contains(string(exportJSON), "Stable ordering") || strings.Contains(string(exportJSON), run.ID) {
		t.Fatalf("normal export included local shadow data: %s", exportJSON)
	}
}

func TestCreateAdmissionShadowRunRollsBackAtomically(t *testing.T) {
	s := newTestStore(t)

	_, err := s.CreateAdmissionShadowRun(CreateAdmissionShadowRunParams{
		Project:          "engram",
		Mode:             "session",
		EvidenceVersion:  "v1",
		GeneratorVersion: "v1",
		PolicyVersion:    "v1",
		Proposals: []AdmissionShadowProposalInput{
			validAdmissionShadowProposal("first"),
			{
				Type:           "decision",
				Title:          "invalid",
				Content:        "must roll back",
				Scope:          "project",
				Category:       "decision",
				Recommendation: "promote",
			},
		},
	})
	if err == nil {
		t.Fatal("create shadow run succeeded with invalid recommendation")
	}
	if got := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM admission_shadow_runs`); got != 0 {
		t.Fatalf("runs after rollback = %d, want 0", got)
	}
	if got := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM admission_shadow_proposals`); got != 0 {
		t.Fatalf("proposals after rollback = %d, want 0", got)
	}
}

func TestCreateAdmissionShadowRunRejectsUnsafeEvidenceReferences(t *testing.T) {
	s := newTestStore(t)
	unsafeReference := "prompt:raw evidence disguised as an identifier"
	proposal := validAdmissionShadowProposal("safe proposal")
	proposal.EvidenceRefs = []string{unsafeReference}

	_, err := s.CreateAdmissionShadowRun(CreateAdmissionShadowRunParams{
		Project:          "engram",
		Mode:             "session",
		EvidenceVersion:  "v1",
		GeneratorVersion: "v1",
		PolicyVersion:    "v1",
		Proposals:        []AdmissionShadowProposalInput{proposal},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid evidence reference") {
		t.Fatalf("error = %v, want invalid evidence reference", err)
	}
	if strings.Contains(err.Error(), unsafeReference) {
		t.Fatalf("error reflected unsafe evidence reference: %v", err)
	}
	if got := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM admission_shadow_runs`); got != 0 {
		t.Fatalf("runs after rejected reference = %d, want 0", got)
	}
}

func TestListAdmissionShadowProposalsDeterministicPendingAndProjectScoped(t *testing.T) {
	s := newTestStore(t)
	first := mustCreateAdmissionShadowRun(t, s, "engram", "first", "second")
	second := mustCreateAdmissionShadowRun(t, s, "engram", "third")
	_ = mustCreateAdmissionShadowRun(t, s, "other", "hidden")
	if _, err := s.db.Exec(`UPDATE admission_shadow_runs SET created_at = ? WHERE id = ?`, "2026-08-19 10:00:00", first.ID); err != nil {
		t.Fatalf("set first run time: %v", err)
	}
	if _, err := s.db.Exec(`UPDATE admission_shadow_runs SET created_at = ? WHERE id = ?`, "2026-08-19 10:00:01", second.ID); err != nil {
		t.Fatalf("set second run time: %v", err)
	}

	review, alreadyRecorded, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID: first.Proposals[1].ID,
		Verdict:    "reject",
		Note:       "Not durable.",
	})
	if err != nil {
		t.Fatalf("add review: %v", err)
	}
	if alreadyRecorded {
		t.Fatal("first review reported as already recorded")
	}

	got, err := s.ListAdmissionShadowProposals(" ENGRAM ", false)
	if err != nil {
		t.Fatalf("list shadow proposals: %v", err)
	}
	if titles := admissionShadowTitles(got); !reflect.DeepEqual(titles, []string{"first", "second", "third"}) {
		t.Fatalf("titles = %#v", titles)
	}
	if len(got[1].Reviews) != 1 || got[1].Reviews[0].ID != review.ID {
		t.Fatalf("review history = %#v", got[1].Reviews)
	}
	if got[0].RunID != first.ID || got[2].RunID != second.ID {
		t.Fatalf("run ordering = %q ... %q", got[0].RunID, got[2].RunID)
	}

	pending, err := s.ListAdmissionShadowProposals("engram", true)
	if err != nil {
		t.Fatalf("list pending proposals: %v", err)
	}
	if titles := admissionShadowTitles(pending); !reflect.DeepEqual(titles, []string{"first", "third"}) {
		t.Fatalf("pending titles = %#v", titles)
	}

	runs, err := s.ListAdmissionShadowRuns("engram")
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 2 || runs[0].ID != first.ID || runs[1].ID != second.ID {
		t.Fatalf("runs = %#v", runs)
	}
	other, err := s.ListAdmissionShadowProposals("other", false)
	if err != nil || !reflect.DeepEqual(admissionShadowTitles(other), []string{"hidden"}) {
		t.Fatalf("other project proposals = %#v, err = %v", other, err)
	}
}

func TestAddAdmissionShadowReviewIsAppendOnlyIdempotentAndRedacted(t *testing.T) {
	s := newTestStore(t)
	run := mustCreateAdmissionShadowRun(t, s, "engram", "proposal")
	proposalID := run.Proposals[0].ID
	longNote := "Review <private>outer <private>nested-note-secret</private> trailing-note-secret</private> " + strings.Repeat("x", MaxAdmissionShadowReviewNoteLength+20)

	first, alreadyRecorded, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID:  proposalID,
		Verdict:     "review",
		Note:        longNote,
		Unsupported: true,
		PrivacyLeak: true,
	})
	if err != nil {
		t.Fatalf("add first review: %v", err)
	}
	if alreadyRecorded {
		t.Fatal("first review reported as already recorded")
	}
	if len([]rune(first.Note)) > MaxAdmissionShadowReviewNoteLength || strings.Contains(first.Note, "nested-note-secret") || strings.Contains(first.Note, "trailing-note-secret") {
		t.Fatalf("bounded redacted note = %q", first.Note)
	}
	if first.Ordinal != 0 {
		t.Fatalf("first review ordinal = %d, want 0", first.Ordinal)
	}
	if !first.Unsupported || !first.PrivacyLeak {
		t.Fatalf("safety flags = unsupported:%t privacy_leak:%t", first.Unsupported, first.PrivacyLeak)
	}

	identical, alreadyRecorded, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID:  proposalID,
		Verdict:     " REVIEW ",
		Note:        "  " + longNote + "  ",
		Unsupported: true,
		PrivacyLeak: true,
	})
	if err != nil {
		t.Fatalf("repeat identical review: %v", err)
	}
	if !alreadyRecorded {
		t.Fatal("identical latest review was not reported as already recorded")
	}
	if identical.ID != first.ID {
		t.Fatalf("identical review id = %q, want %q", identical.ID, first.ID)
	}

	different, alreadyRecorded, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID:  proposalID,
		Verdict:     "review",
		Note:        longNote,
		Unsupported: false,
		PrivacyLeak: true,
	})
	if err != nil {
		t.Fatalf("append corrected review: %v", err)
	}
	if alreadyRecorded {
		t.Fatal("different review reported as already recorded")
	}
	if different.ID == first.ID {
		t.Fatal("different safety flags did not append a review")
	}
	if different.Ordinal != 1 {
		t.Fatalf("second review ordinal = %d, want 1", different.Ordinal)
	}
	returnToFirst, alreadyRecorded, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
		ProposalID:  proposalID,
		Verdict:     "review",
		Note:        longNote,
		Unsupported: true,
		PrivacyLeak: true,
	})
	if err != nil {
		t.Fatalf("return to earlier correction: %v", err)
	}
	if alreadyRecorded {
		t.Fatal("A to B to A final event reported as already recorded")
	}
	if returnToFirst.ID == first.ID || returnToFirst.ID == different.ID {
		t.Fatal("A to B to A must append the final A as a new audit event")
	}
	if returnToFirst.Ordinal != 2 {
		t.Fatalf("third review ordinal = %d, want 2", returnToFirst.Ordinal)
	}

	listed, err := s.ListAdmissionShadowProposals("engram", false)
	if err != nil {
		t.Fatalf("list reviews: %v", err)
	}
	if len(listed) != 1 || len(listed[0].Reviews) != 3 {
		t.Fatalf("review history = %#v", listed)
	}
	if _, _, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{ProposalID: "missing", Verdict: "admit"}); err == nil {
		t.Fatal("review of missing proposal succeeded")
	}
}

func TestDeleteProjectCascadesAdmissionShadowLocallyAndPreservesOthers(t *testing.T) {
	s := newTestStore(t)
	engramRun := mustCreateAdmissionShadowRun(t, s, "engram", "remove")
	otherRun := mustCreateAdmissionShadowRun(t, s, "other", "preserve")
	for _, proposalID := range []string{engramRun.Proposals[0].ID, otherRun.Proposals[0].ID} {
		if _, _, err := s.AddAdmissionShadowReview(AddAdmissionShadowReviewParams{
			ProposalID: proposalID,
			Verdict:    "admit",
			Note:       "reviewed",
		}); err != nil {
			t.Fatalf("add review: %v", err)
		}
	}

	exists, err := s.ProjectExists("engram")
	if err != nil || !exists {
		t.Fatalf("shadow-only project exists = %t, err=%v", exists, err)
	}
	result, err := s.DeleteProject("engram", false)
	if err != nil {
		t.Fatalf("delete project: %v", err)
	}
	if result.AdmissionShadowRunsDeleted != 1 {
		t.Fatalf("admission shadow runs deleted = %d, want 1", result.AdmissionShadowRunsDeleted)
	}
	if got := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM admission_shadow_runs WHERE project = 'engram'`); got != 0 {
		t.Fatalf("engram runs after delete = %d", got)
	}
	if got := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM admission_shadow_proposals WHERE run_id = ?`, engramRun.ID); got != 0 {
		t.Fatalf("engram proposals after delete = %d", got)
	}
	if got := admissionShadowScalarInt(t, s, `SELECT COUNT(*) FROM admission_shadow_reviews WHERE proposal_id = ?`, engramRun.Proposals[0].ID); got != 0 {
		t.Fatalf("engram reviews after delete = %d", got)
	}
	other, err := s.ListAdmissionShadowProposals("other", false)
	if err != nil {
		t.Fatalf("list preserved project: %v", err)
	}
	if len(other) != 1 || len(other[0].Reviews) != 1 || other[0].ID != otherRun.Proposals[0].ID {
		t.Fatalf("preserved project = %#v", other)
	}
}

func TestAdmissionShadowRunDoesNotDependOnSessionLifetime(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateSession("short-lived", "engram", "/tmp/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := s.CreateAdmissionShadowRun(CreateAdmissionShadowRunParams{
		Project:          "engram",
		SessionID:        "short-lived",
		Mode:             "session",
		EvidenceVersion:  "v1",
		GeneratorVersion: "v1",
		PolicyVersion:    "v1",
	})
	if err != nil {
		t.Fatalf("create shadow run: %v", err)
	}
	if err := s.DeleteSession("short-lived"); err != nil {
		t.Fatalf("delete source session: %v", err)
	}
	runs, err := s.ListAdmissionShadowRuns("engram")
	if err != nil {
		t.Fatalf("list shadow runs: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != run.ID || runs[0].SessionID != "short-lived" {
		t.Fatalf("shadow run after session cleanup = %#v", runs)
	}
}

func validAdmissionShadowProposal(title string) AdmissionShadowProposalInput {
	return AdmissionShadowProposalInput{
		Type:                  "decision",
		Title:                 title,
		Content:               title + " content",
		Scope:                 "project",
		Category:              "decision",
		Protected:             true,
		Recommendation:        "review",
		ProposalReasonCodes:   []string{"structured_section"},
		AssessmentReasonCodes: []string{"protected_proposal"},
		EvidenceRefs:          []string{"prompt:1"},
	}
}

func mustCreateAdmissionShadowRun(t *testing.T, s *Store, project string, titles ...string) *AdmissionShadowRun {
	t.Helper()
	proposals := make([]AdmissionShadowProposalInput, 0, len(titles))
	for _, title := range titles {
		proposals = append(proposals, validAdmissionShadowProposal(title))
	}
	run, err := s.CreateAdmissionShadowRun(CreateAdmissionShadowRunParams{
		Project:              project,
		SessionID:            "session-that-may-be-cleaned-up",
		Mode:                 "session",
		EvidenceVersion:      "v1",
		GeneratorVersion:     "v1",
		PolicyVersion:        "v1",
		IncludedItems:        len(proposals),
		IncludedContentBytes: 100,
		Proposals:            proposals,
	})
	if err != nil {
		t.Fatalf("create shadow run: %v", err)
	}
	return run
}

func admissionShadowTitles(proposals []AdmissionShadowProposal) []string {
	titles := make([]string, 0, len(proposals))
	for _, proposal := range proposals {
		titles = append(titles, proposal.Title)
	}
	return titles
}

func admissionShadowScalarInt(t *testing.T, s *Store, query string, args ...any) int {
	t.Helper()
	var value int
	if err := s.db.QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatalf("scalar query: %v", err)
	}
	return value
}
