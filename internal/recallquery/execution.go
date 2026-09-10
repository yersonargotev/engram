package recallquery

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/store"
)

const fixtureProject = "synthetic-recall-query"

var variants = []string{"baseline", "editorial", "supersession", "editorial_supersession"}

// Execute opens held-out only after all calibration prerequisites pass. Every
// cell owns a disposable Store, so neither user state nor another cell is read.
func Execute(ctx context.Context, root string, includeHeldOut bool) (Report, error) {
	report := Report{StudyID: "concept-versus-identifier", Version: "2", ContractSHA256: FrozenContractSHA256, Disposition: "evaluation_incomplete", Groups: []Group{}, ContentSelections: 0, LabelOmission: "retrieval_only", TaskTimeToUsefulAvailable: false}
	c, err := Verify(root)
	if err != nil {
		return report, err
	}
	report.StudyID = c.StudyID
	report.Version = c.Version
	report.CoreSourceRevision = c.SourceRevision
	report.Protocol = c.Protocol
	calibration, err := loadCorpus(root, "calibration", c.CalibrationSHA256, c.UnitsPerCohort)
	if err != nil {
		return report, err
	}
	report.Groups = executeCohort(ctx, calibration, c)
	report.CalibrationPassed = groupsPassed(report.Groups)
	if !report.CalibrationPassed || !includeHeldOut {
		return report, nil
	}
	report.HeldOutOpened = true
	held, err := loadCorpus(root, "held-out", c.HeldOutSHA256, c.UnitsPerCohort)
	if err != nil {
		return report, err
	}
	for _, a := range calibration.Units {
		for _, b := range held.Units {
			if a.ID == b.ID || a.Issue == b.Issue || a.Concept == b.Concept {
				return report, fmt.Errorf("calibration/held-out overlap")
			}
		}
	}
	heldGroups := executeCohort(ctx, held, c)
	report.Groups = append(report.Groups, heldGroups...)
	if groupsPassed(heldGroups) {
		report.Disposition = (Report{Groups: heldGroups}).Assess()
	}
	return report, nil
}

func groupsPassed(groups []Group) bool {
	if len(groups) != 12 {
		return false
	}
	for _, g := range groups {
		if g.OperationalFailures != 0 || g.Attempted != 3 || g.Cumulative.Completed != 3 {
			return false
		}
	}
	return true
}

func executeCohort(ctx context.Context, corpus Corpus, c Contract) []Group {
	groups := []Group{}
	for _, variant := range variants {
		for _, class := range []string{"identifier", "concept", "task"} {
			g := Group{Cohort: corpus.Cohort, Variant: variant, QueryClass: class, FailureCodes: map[string]int{}}
			for _, u := range corpus.Units {
				g.Attempted++
				row, err := executeUnit(ctx, u, variant, class, c)
				g.SuccessfulCallExposures += row.exposures
				if err != nil {
					g.OperationalFailures++
					g.FailureCodes[row.failure]++
					continue
				}
				g.First.add(row.first)
				g.Cumulative.add(row.cumulative)
			}
			g.First.finish()
			g.Cumulative.finish()
			g.First.AcceptableEvidence.Unknown = g.OperationalFailures
			g.Cumulative.AcceptableEvidence.Unknown = g.OperationalFailures
			g.Labels = Labels{UnknownUtility: g.SuccessfulCallExposures, UnknownQuality: g.SuccessfulCallExposures, OmittedAssessments: g.SuccessfulCallExposures}
			groups = append(groups, g)
		}
	}
	return groups
}

type row struct {
	first, cumulative outcome
	exposures         int
	failure           string
}

func executeUnit(ctx context.Context, u Unit, variant, class string, c Contract) (result row, err error) {
	result.failure = "fixture_failure"
	dir, err := os.MkdirTemp("", "engram-recall-query-*")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(dir)
	s, err := store.New(store.FallbackConfig(dir))
	if err != nil {
		return result, err
	}
	defer s.Close()
	keys, err := seed(s, u, variant)
	if err != nil {
		return result, err
	}
	service := memoryops.New(s)
	query := u.Issue
	switch class {
	case "concept":
		query = u.Concept
	case "task":
		query = u.Task
	}
	calls := []SearchEvidence{}
	for _, q := range []string{query, u.Reformulation} {
		result.failure = "recall_failure"
		started := time.Now()
		found, callErr := service.RecallContext(ctx, memoryops.RecallInput{Query: q, Project: fixtureProject, Scope: "project", ProjectStrength: project.IdentityStrengthExplicit, Limit: c.CandidateLimit, MatchMode: "all", BinaryVersion: "source-evaluation", BinaryRevision: c.SourceRevision})
		latency := float64(time.Since(started).Nanoseconds()) / 1e6
		if callErr != nil {
			return result, callErr
		}
		if found.Warning != nil || len(found.Diagnostics) > 0 {
			return result, fmt.Errorf("Recall unavailable")
		}
		encoded, encodeErr := json.Marshal(found.Candidates)
		result.failure = "budget_or_eligibility_violation"
		if encodeErr != nil || len(found.Candidates) > 5 || len(encoded) > 4096 || found.DeliveredUTF8Bytes != len(encoded) || found.ResultCount != len(found.Candidates) || found.IncludeHistory || found.Provenance.ProtocolVersion != c.Protocol {
			return result, fmt.Errorf("Recall contract violation")
		}
		keysSeen := []string{}
		for _, candidate := range found.Candidates {
			key, ok := keys[candidate.ID]
			if !ok || candidate.Project != fixtureProject || candidate.Scope != "project" || key == "outside" || key == "deleted" || (strings.Contains(variant, "supersession") && key == "diagnosis") {
				return result, fmt.Errorf("Recall eligibility violation")
			}
			keysSeen = append(keysSeen, key)
		}
		result.exposures += len(found.Candidates)
		calls = append(calls, SearchEvidence{Keys: keysSeen, CandidateBytes: len(encoded), LatencyMillis: latency})
	}
	result.first, result.cumulative, err = analyzeSearches(calls)
	if err != nil {
		return result, err
	}
	result.failure = ""
	return result, nil
}

func seed(s *store.Store, u Unit, variant string) (map[int64]string, error) {
	resolution := u.Resolution
	if strings.Contains(variant, "editorial") {
		resolution = u.Editorial
	}
	memories := []struct {
		key, title, content, date, project string
		pin                                bool
	}{
		{"invariant", u.Concept + " invariant", u.Invariant, "2020-01-01 00:00:00", fixtureProject, false},
		{"history1", u.Concept + " delivery one", "Earlier delivery for " + u.Concept + " issue " + u.Issue + " passed its scoped checks. It does not establish present guidance.", "2021-01-01 00:00:00", fixtureProject, false},
		{"history2", u.Concept + " delivery two", "Earlier delivery for " + u.Concept + " issue " + u.Issue + " passed its scoped checks. It does not establish present guidance.", "2022-01-01 00:00:00", fixtureProject, false},
		{"history3", u.Concept + " delivery three", "Earlier delivery for " + u.Concept + " issue " + u.Issue + " passed its scoped checks. It does not establish present guidance.", "2023-01-01 00:00:00", fixtureProject, false},
		{"diagnosis", u.Concept + " diagnosis", u.Diagnosis, "2024-01-01 00:00:00", fixtureProject, false},
		{"resolution", "Issue " + u.Issue + " resolution", resolution, "2024-02-01 00:00:00", fixtureProject, false},
		{"distractor1", "Recent pinned garden plan", "Synthetic gardening calendar for unrelated flowers.", "2025-01-01 00:00:00", fixtureProject, true},
		{"distractor2", "Recent pinned cooking plan", "Synthetic cooking schedule for unrelated recipes.", "2025-02-01 00:00:00", fixtureProject, true},
		{"outside", u.Concept + " other project", u.Invariant, "2025-03-01 00:00:00", "synthetic-other-project", true},
		{"deleted", u.Concept + " removed", u.Invariant, "2025-04-01 00:00:00", fixtureProject, false},
	}
	data := store.ExportData{Version: "2", Sessions: []store.Session{{ID: "synthetic-session", Project: fixtureProject, StartedAt: "2020-01-01 00:00:00"}}}
	for _, m := range memories {
		p := m.project
		data.Observations = append(data.Observations, store.Observation{SyncID: "obs-synthetic-" + m.key, SessionID: "synthetic-session", Type: "decision", Title: m.title, Content: m.content, Project: &p, Scope: "project", CreatedAt: m.date, UpdatedAt: m.date})
	}
	if _, err := s.Import(&data); err != nil {
		return nil, err
	}
	keys := map[int64]string{}
	ids := map[string]int64{}
	for _, m := range memories {
		obs, err := s.GetObservationBySyncID("obs-synthetic-" + m.key)
		if err != nil {
			return nil, err
		}
		keys[obs.ID] = m.key
		ids[m.key] = obs.ID
		if m.pin {
			if err := s.PinObservation(obs.ID); err != nil {
				return nil, err
			}
		}
	}
	if err := s.DeleteObservation(ids["deleted"], false); err != nil {
		return nil, err
	}
	if strings.Contains(variant, "supersession") {
		if _, err := memoryops.New(s).Compare(memoryops.CompareInput{MemoryIDA: ids["resolution"], MemoryIDB: ids["diagnosis"], Relation: "supersedes", Confidence: 1, Reasoning: "Frozen synthetic resolution replaces the commit-scoped diagnosis", Model: "synthetic-evaluator"}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}
