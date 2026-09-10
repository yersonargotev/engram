package recallquery_test

import (
	"github.com/yersonargotev/engram/internal/recallquery"
	"testing"
)

func TestReportDistinguishesReformulationRecoveryAndRepeatedExposure(t *testing.T) {
	first, cumulative, err := recallquery.AnalyzeSearches([]recallquery.SearchEvidence{
		{Keys: []string{"history1", "invariant"}, CandidateBytes: 100, LatencyMillis: 2},
		{Keys: []string{"resolution", "invariant"}, CandidateBytes: 120, LatencyMillis: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Coverage != 0.5 || first.ReciprocalRank != 0.5 || first.Acceptable != 0 || first.SearchCalls != 1 {
		t.Fatalf("first: %+v", first)
	}
	if cumulative.Coverage != 1 || cumulative.ReciprocalRank != 0.5 || cumulative.Acceptable != 1 || cumulative.Exposures != 4 || cumulative.DuplicateExposures != 1 || cumulative.SearchCalls != 2 || cumulative.CandidateBytes != 220 {
		t.Fatalf("cumulative: %+v", cumulative)
	}
	if cumulative.DiscoveryLatencyMillis == nil || *cumulative.DiscoveryLatencyMillis != 2 {
		t.Fatalf("proxy includes unnecessary reformulation: %+v", cumulative)
	}
	if cumulative.AcceptableEvidence.CILower < 20 || cumulative.AcceptableEvidence.CILower > 21 || cumulative.AcceptableEvidence.CIUpper != 100 {
		t.Fatalf("Wilson: %+v", cumulative.AcceptableEvidence)
	}
}

func TestReportRejectsUnboundedReformulationAndCountsUnavailableEvidence(t *testing.T) {
	if _, _, err := recallquery.AnalyzeSearches(make([]recallquery.SearchEvidence, 3)); err == nil {
		t.Fatal("third search accepted")
	}
	if _, _, err := recallquery.AnalyzeSearches([]recallquery.SearchEvidence{{Keys: []string{"invariant"}, CandidateBytes: 4097}, {}}); err == nil {
		t.Fatal("oversize response accepted")
	}
}

func TestDispositionRequiresCompleteEvidenceAndSeparatesCorrectionFromRanking(t *testing.T) {
	groups := []recallquery.Group{}
	for _, variant := range []string{"baseline", "editorial", "supersession", "editorial_supersession"} {
		for _, class := range []string{"identifier", "concept", "task"} {
			groups = append(groups, recallquery.Group{Variant: variant, QueryClass: class, Attempted: 3, First: recallquery.Summary{Completed: 3, Coverage: 1, Acceptable: 3}, Cumulative: recallquery.Summary{Completed: 3, Coverage: 1, Acceptable: 3}})
		}
	}
	r := recallquery.Report{Groups: groups}
	if r.Assess() != "no_ranking_change_indicated" {
		t.Fatal(r.Assess())
	}
	for i := range r.Groups {
		if r.Groups[i].QueryClass == "task" {
			r.Groups[i].First.Acceptable = 0
			r.Groups[i].Cumulative.Acceptable = 0
		}
	}
	if r.Assess() != "ranking_investigation_requires_separate_approval" {
		t.Fatal(r.Assess())
	}
	r.Groups[11].First.Acceptable = 3
	if r.Assess() != "editorial_lifecycle_correction_indicated" {
		t.Fatal(r.Assess())
	}
	r.Groups[0].OperationalFailures = 1
	if r.Assess() != "evaluation_incomplete" {
		t.Fatal(r.Assess())
	}
}
