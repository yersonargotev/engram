package memoryops

import (
	"encoding/json"
	"fmt"
	"github.com/yersonargotev/engram/internal/store"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/project"
)

func TestRecallPresentsRecordedEvidenceWithoutInferringApplicability(t *testing.T) {
	service := newTestService(t)
	saveObservation(t, service, "engram", "Cache invariant", "For release v1, cache keys include tenant identity.")
	result, err := service.Recall(RecallInput{Query: "Cache invariant", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit})
	if err != nil || result.Warning != nil || len(result.Candidates) != 1 {
		t.Fatalf("Recall = %#v, %v", result, err)
	}
	candidate := result.Candidates[0]
	if candidate.CreatedAt == "" || candidate.UpdatedAt == "" || candidate.ReviewState != "active" {
		t.Fatalf("missing recorded evidence: %#v", candidate)
	}
	content, err := service.RecallContent(RecallContentInput{RecallID: result.RecallID, ResultID: candidate.ResultID, Project: "engram", ProjectStrength: project.IdentityStrengthExplicit})
	if err != nil || content.Warning != nil || content.Memory.CreatedAt != candidate.CreatedAt || content.Memory.ReviewState != "active" {
		t.Fatalf("inspection evidence = %#v, %v", content, err)
	}
}

func TestRecallHistoryDiscoversSupersededContentWithBoundSelection(t *testing.T) {
	service := newTestService(t)
	old := saveObservation(t, service, "engram", "Orion cache diagnosis", "Verified at commit abc123: cache failures came from TTL.")
	replacement := saveObservation(t, service, "engram", "Cache correction", "Verified at commit def456: tenant isolation caused the failures.")
	if _, err := service.Compare(CompareInput{MemoryIDA: replacement.ID, MemoryIDB: old.ID, Relation: "supersedes", Confidence: 1, Reasoning: "Corrected diagnosis", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	input := RecallInput{Query: "Orion", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit}
	ordinary, err := service.Recall(input)
	if err != nil || ordinary.Warning != nil || ordinary.ResultCount != 0 {
		t.Fatalf("ordinary = %#v, %v", ordinary, err)
	}
	input.IncludeHistory = true
	history, err := service.Recall(input)
	if err != nil || history.Warning != nil || history.ResultCount != 1 || !history.IncludeHistory {
		t.Fatalf("history = %#v, %v", history, err)
	}
	selected := RecallContentInput{RecallID: history.RecallID, ResultID: history.Candidates[0].ResultID, Project: "engram", ProjectStrength: project.IdentityStrengthExplicit}
	content, err := service.RecallContent(selected)
	if err != nil || content.Warning != nil || content.Memory.ID != old.ID || !content.IncludeHistory {
		t.Fatalf("historical content = %#v, %v", content, err)
	}
	selected.Project = "other"
	denied, err := service.RecallContent(selected)
	if err != nil || denied.Warning == nil || denied.Memory.Content != "" {
		t.Fatalf("cross-project content = %#v, %v", denied, err)
	}
	selected.Project = "engram"
	if err := service.store.DeleteObservation(old.ID, false); err != nil {
		t.Fatal(err)
	}
	denied, err = service.RecallContent(selected)
	if err != nil || denied.Warning == nil || denied.Memory.Content != "" {
		t.Fatalf("deleted content = %#v, %v", denied, err)
	}
}

func TestRecallHistoryShowsDirectedSupersessionWithoutExposingUnavailableEndpoints(t *testing.T) {
	service := newTestService(t)
	old := saveObservation(t, service, "engram", "Orion historic diagnosis", "At abc123, TTL appeared responsible.")
	replacement := saveObservation(t, service, "engram", "Tenant isolation correction", "At def456, tenant isolation was confirmed.")
	if _, err := service.Compare(CompareInput{MemoryIDA: replacement.ID, MemoryIDB: old.ID, Relation: "supersedes", Confidence: 1, Reasoning: "Corrected", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	input := RecallInput{Query: "Orion", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit, IncludeHistory: true}
	result, err := service.Recall(input)
	if err != nil || result.Warning != nil || result.ResultCount != 1 {
		t.Fatalf("history = %#v, %v", result, err)
	}
	candidate := result.Candidates[0]
	if candidate.ReviewState != "active" || len(candidate.Supersessions) != 1 || candidate.Supersessions[0].Direction != "superseded_by" || candidate.Supersessions[0].MemoryID != replacement.ID {
		t.Fatalf("supersession = %#v", candidate)
	}
	selected := RecallContentInput{RecallID: result.RecallID, ResultID: candidate.ResultID, Project: "engram", ProjectStrength: project.IdentityStrengthExplicit}
	full, err := service.RecallContent(selected)
	if err != nil || full.Warning != nil || len(full.Memory.Supersessions) != 1 {
		t.Fatalf("inspection = %#v, %v", full, err)
	}
	replacementSearch, err := service.Recall(RecallInput{Query: candidate.Supersessions[0].Title, Project: "engram", ProjectStrength: project.IdentityStrengthExplicit})
	if err != nil || replacementSearch.ResultCount != 1 || replacementSearch.Candidates[0].ID != replacement.ID {
		t.Fatalf("replacement discovery = %#v, %v", replacementSearch, err)
	}
	if err := service.store.DeleteObservation(replacement.ID, false); err != nil {
		t.Fatal(err)
	}
	full, err = service.RecallContent(selected)
	if err != nil || full.Warning != nil || len(full.Memory.Supersessions) != 1 {
		t.Fatalf("deleted endpoint = %#v, %v", full, err)
	}
	relation := full.Memory.Supersessions[0]
	if relation.MemoryID != 0 || relation.Title != "" || relation.SyncID != "" || relation.EndpointAvailable {
		t.Fatalf("leaked endpoint = %#v", relation)
	}
}

func TestRecallHistoryTemporalEvidenceAndBudget(t *testing.T) {
	service := newTestService(t)
	seed := func(title, content, kind, date string) int64 {
		t.Helper()
		observation := saveObservation(t, service, "engram", title, content)
		if _, err := service.store.DB().Exec(`UPDATE observations SET type = ?, created_at = ?, updated_at = ? WHERE id = ?`, kind, date, date, observation.ID); err != nil {
			t.Fatal(err)
		}
		return observation.ID
	}
	invariant := seed("tenant tenant isolation", "Invariant: cache keys include tenant identity across supported releases.", "decision", "2020-01-01 00:00:00")
	diagnosis := seed("Orion diagnosis", "At commit abc123: tenant failures appeared to be TTL related.", "bugfix", "2021-01-01 00:00:00")
	replacement := seed("tenant correction", "At commit def456: missing tenant isolation caused the failure.", "bugfix", "2022-01-01 00:00:00")
	delivery := seed("Session summary: rollout", "Historical rollout completed; tenant was one checklist item.", "session_summary", "2024-01-01 00:00:00")
	renamed := seed("Session summary: Orion", "Orion was the historical name in this project's tenant design evidence; ownership remains engram.", "session_summary", "2023-01-01 00:00:00")
	unrelated := seed("Recent unrelated event", "Color palette release completed.", "manual", "2025-01-01 00:00:00")
	if _, err := service.Compare(CompareInput{MemoryIDA: replacement, MemoryIDB: diagnosis, Relation: "supersedes", Confidence: 1, Reasoning: "Corrected diagnosis", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	for _, history := range []bool{false, true} {
		result, err := service.Recall(RecallInput{Query: "tenant", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit, IncludeHistory: history})
		if err != nil || result.Warning != nil {
			t.Fatalf("recall = %#v, %v", result, err)
		}
		positions := map[int64]int{}
		for i, c := range result.Candidates {
			positions[c.ID] = i
		}
		for _, id := range []int64{invariant, replacement, delivery, renamed} {
			if _, ok := positions[id]; !ok {
				t.Fatalf("missing %d: %#v", id, result)
			}
		}
		if _, ok := positions[unrelated]; ok {
			t.Fatal("unrelated recent event returned")
		}
		if _, ok := positions[diagnosis]; ok != history {
			t.Fatalf("history eligibility = %v", result.ResultIDs)
		}
		if positions[invariant] >= positions[delivery] {
			t.Fatal("new weak delivery outranked older invariant")
		}
		if result.Candidates[positions[invariant]].CreatedAt != "2020-01-01 00:00:00" {
			t.Fatal("recorded date lost")
		}
		encoded, err := json.Marshal(result.Candidates)
		if err != nil || len(encoded) != result.DeliveredUTF8Bytes || len(encoded) > 4096 {
			t.Fatalf("actual bytes %d; reported %d", len(encoded), result.DeliveredUTF8Bytes)
		}
	}
	// Long authored claims, titles, and several relation endpoints share the same budget.
	for i := 0; i < 6; i++ {
		id := seed(fmt.Sprintf("boundhistory %d %s", i, strings.Repeat("界", 100)), "At commit abc123 only: "+strings.Repeat("界", 1000), "manual", "2021-01-01 00:00:00")
		for j := 0; j < 4; j++ {
			target := seed(fmt.Sprintf("Replacement %d %d %s", i, j, strings.Repeat("界", 100)), "Independent correction evidence.", "manual", "2022-01-01 00:00:00")
			if _, err := service.Compare(CompareInput{MemoryIDA: target, MemoryIDB: id, Relation: "supersedes", Confidence: 1, Reasoning: "Replaced", Model: "test"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	bounded, err := service.Recall(RecallInput{Query: "boundhistory", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit, IncludeHistory: true, Limit: 10})
	if err != nil || bounded.Warning != nil || bounded.ResultCount == 0 {
		t.Fatalf("bounded = %#v, %v", bounded, err)
	}
	encoded, _ := json.Marshal(bounded.Candidates)
	if len(encoded) > 4096 || len(encoded) != bounded.DeliveredUTF8Bytes {
		t.Fatalf("candidate bytes = %d", len(encoded))
	}
	if !strings.HasPrefix(bounded.Candidates[0].Summary, "At commit abc123 only:") {
		t.Fatal("leading authored qualification lost")
	}
	for _, c := range bounded.Candidates {
		if !utf8.ValidString(c.Summary) || len(c.Supersessions) != 3 || c.SupersessionsOmitted != 1 {
			t.Fatalf("bounded context = %#v", c)
		}
	}
}

func TestRecallHistoricalSelectionPreservesOrdinaryEligibilityAndContinuation(t *testing.T) {
	service := newTestService(t)
	old := saveObservation(t, service, "engram", "Segment history", strings.Repeat("a", RecallContentBudgetBytes-1)+"🧠tail")
	input := RecallInput{Query: "Segment history", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit}
	ordinary, err := service.Recall(input)
	if err != nil {
		t.Fatal(err)
	}
	replacement := saveObservation(t, service, "engram", "Correction", "Confirmed correction.")
	if _, err := service.Compare(CompareInput{MemoryIDA: replacement.ID, MemoryIDB: old.ID, Relation: "supersedes", Confidence: 1, Reasoning: "Replaced", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	selected := RecallContentInput{RecallID: ordinary.RecallID, ResultID: ordinary.Candidates[0].ResultID, Project: "engram", ProjectStrength: project.IdentityStrengthExplicit}
	denied, err := service.RecallContent(selected)
	if err != nil || denied.Warning == nil || denied.Memory.Content != "" {
		t.Fatalf("ordinary stale selection=%#v %v", denied, err)
	}
	input.IncludeHistory = true
	history, err := service.Recall(input)
	if err != nil || history.Warning != nil || history.ResultCount != 1 {
		t.Fatalf("history=%#v %v", history, err)
	}
	selected.RecallID = history.RecallID
	selected.ResultID = history.Candidates[0].ResultID
	full, err := service.RecallContent(selected)
	if err != nil || full.Warning != nil || !full.Truncated || full.ContinuationPosition == nil || full.DeliveredUTF8Bytes != RecallContentBudgetBytes-1 {
		t.Fatalf("segment=%#v %v", full, err)
	}
	selected.Position = *full.ContinuationPosition
	tail, err := service.RecallContent(selected)
	if err != nil || tail.Warning != nil || tail.Memory.Content != "🧠tail" || tail.Truncated {
		t.Fatalf("tail=%#v %v", tail, err)
	}
	selected.Position = 1
	denied, err = service.RecallContent(selected)
	if err != nil || denied.Warning == nil || denied.Memory.Content != "" {
		t.Fatal("unexposed continuation admitted")
	}
}

func TestRecallHistoryDoesNotBroadenReviewOrPersonalScope(t *testing.T) {
	service := newTestService(t)
	overdue := saveObservation(t, service, "engram", "Boundary overdue", "Review required.")
	if _, err := service.store.DB().Exec(`UPDATE observations SET review_after='2000-01-01 00:00:00' WHERE id=?`, overdue.ID); err != nil {
		t.Fatal(err)
	}
	personal := saveObservation(t, service, "engram", "Boundary personal", "Personal evidence.")
	scope := "personal"
	if _, err := service.store.UpdateObservation(personal.ID, store.UpdateObservationParams{Scope: &scope}); err != nil {
		t.Fatal(err)
	}
	saveObservation(t, service, "other", "Boundary other", "Other project.")
	result, err := service.Recall(RecallInput{Query: "Boundary", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit, IncludeHistory: true})
	if err != nil || result.Warning != nil || result.ResultCount != 0 {
		t.Fatalf("broadened history=%#v %v", result, err)
	}
}

func TestRecallHistoryRedactsCrossProjectSupersessionEndpoint(t *testing.T) {
	service := newTestService(t)
	old := saveObservation(t, service, "engram", "Hidden endpoint history", "Original attributable claim.")
	replacement := saveObservation(t, service, "engram", "Private correction title", "Private correction content.")
	if _, err := service.Compare(CompareInput{MemoryIDA: replacement.ID, MemoryIDB: old.ID, Relation: "supersedes", Confidence: 1, Reasoning: "Corrected", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	// Model a counterpart whose ownership changed after the relation was recorded.
	if _, err := service.store.DB().Exec(`UPDATE observations SET project='other' WHERE id=?`, replacement.ID); err != nil {
		t.Fatal(err)
	}
	result, err := service.Recall(RecallInput{Query: "Hidden endpoint", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit, IncludeHistory: true})
	if err != nil || result.Warning != nil || result.ResultCount != 1 {
		t.Fatalf("history=%#v %v", result, err)
	}
	candidate := result.Candidates[0]
	if len(candidate.Supersessions) != 1 {
		t.Fatalf("missing direction: %#v", candidate)
	}
	relation := candidate.Supersessions[0]
	if relation.EndpointAvailable || relation.MemoryID != 0 || relation.Title != "" || relation.SyncID != "" || relation.Direction != "superseded_by" {
		t.Fatalf("leaked endpoint=%#v", relation)
	}
	full, err := service.RecallContent(RecallContentInput{RecallID: result.RecallID, ResultID: candidate.ResultID, Project: "engram", ProjectStrength: project.IdentityStrengthExplicit})
	if err != nil || full.Warning != nil || len(full.Memory.Supersessions) != 1 || full.Memory.Supersessions[0].EndpointAvailable {
		t.Fatalf("content=%#v %v", full, err)
	}
}
