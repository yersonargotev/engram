package memoryops

import (
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/store"
)

func TestRecallFeedbackReportKeepsUnknownsAndComputesAggregateMetrics(t *testing.T) {
	service := newTestService(t)
	for index := 0; index < 4; index++ {
		saveObservation(t, service, "engram", "Report candidate "+string(rune('a'+index)), "aggregate feedback metric shared evidence "+string(rune('a'+index)))
	}
	service.newRecallID = func() (string, error) { return "recall-feedback-report", nil }
	service.recallElapsed = func(started time.Time) time.Duration { return 12 * time.Millisecond }
	reportTurn := &store.CheckpointIdentity{
		Host: "codex", SessionID: "session-feedback-report", RootTurnID: "turn-feedback-report",
	}
	recall, err := service.Recall(RecallInput{
		Query: "aggregate feedback metric shared evidence", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit, BinaryVersion: "3.4.0-test", BinaryRevision: "report-revision",
		TurnIdentity: reportTurn,
	})
	if err != nil || len(recall.Candidates) != 4 {
		t.Fatalf("Recall result = %#v, err=%v", recall, err)
	}
	content, err := service.RecallContent(RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		BinaryVersion: "3.4.0-test", BinaryRevision: "report-revision",
	})
	if err != nil || content.Warning != nil || content.DeliveredUTF8Bytes == 0 {
		t.Fatalf("Recall content result = %#v, err=%v", content, err)
	}
	created, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: reportTurn.Host, SessionID: reportTurn.SessionID, RootTurnID: reportTurn.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
		RecallFeedback: &RecallFeedbackInput{
			RecallID: recall.RecallID,
			Results: []RecallFeedbackItemInput{
				{ResultID: recall.Candidates[0].ResultID, Utility: RecallUtilityDecisive, Quality: RecallQualityCurrent, Source: RecallFeedbackSourceEvaluator},
				{ResultID: recall.Candidates[1].ResultID, Utility: RecallUtilityDuplicate, Quality: RecallQualityStale, Source: RecallFeedbackSourceEvaluator},
			},
		},
	})
	if err != nil || created.RecallFeedback == nil || created.RecallFeedback.Status != RecallFeedbackStatusRecorded {
		t.Fatalf("record report feedback: result=%#v err=%v", created, err)
	}

	service.newRecallID = func() (string, error) { return "recall-feedback-empty", nil }
	emptyTurn := &store.CheckpointIdentity{
		Host: "codex", SessionID: "session-feedback-empty", RootTurnID: "turn-feedback-empty",
	}
	empty, err := service.Recall(RecallInput{
		Query: "no matching memory exists for false empty review", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit, TurnIdentity: emptyTurn,
	})
	if err != nil || empty.ResultCount != 0 {
		t.Fatalf("empty Recall result = %#v, err=%v", empty, err)
	}
	falseEmpty := true
	emptyResult, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: emptyTurn.Host, SessionID: emptyTurn.SessionID, RootTurnID: emptyTurn.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
		RecallFeedback: &RecallFeedbackInput{
			RecallID:   empty.RecallID,
			FalseEmpty: &RecallFalseEmptyInput{Value: falseEmpty, Source: RecallFeedbackSourceEvaluator},
		},
	})
	if err != nil || emptyResult.RecallFeedback == nil ||
		emptyResult.RecallFeedback.EmptyReviewsRecorded != 1 {
		t.Fatalf("record false-empty review: result=%#v err=%v", emptyResult, err)
	}

	report, err := service.RecallFeedbackReport()
	if err != nil {
		t.Fatalf("aggregate Recall feedback report: %v", err)
	}
	if report.SchemaVersion != RecallFeedbackReportSchemaVersion || report.ExposedResults != 4 ||
		report.EmptyRuns != 1 || report.LabelCoverage.Numerator != 2 ||
		report.LabelCoverage.Denominator != 4 || report.LabelCoverage.Unknown != 2 ||
		!approximately(report.LabelCoverage.Rate, 0.5) || report.LabelCoverage.Confidence95.Upper <= report.LabelCoverage.Confidence95.Lower {
		t.Fatalf("report coverage = %#v; report=%#v", report.LabelCoverage, report)
	}
	evaluator := findRecallFeedbackSourceReport(t, report.Sources, RecallFeedbackSourceEvaluator)
	for name, metric := range map[string]RecallFeedbackRate{
		"utility": evaluator.Utility, "noise": evaluator.Noise,
		"harm": evaluator.Harm, "duplicate": evaluator.Duplicate,
	} {
		if metric.Numerator != 1 || metric.Denominator != 2 || metric.Unknown != 2 ||
			!approximately(metric.Rate, 0.5) || metric.Confidence95.Upper <= metric.Confidence95.Lower {
			t.Fatalf("%s metric = %#v", name, metric)
		}
	}
	if evaluator.FalseEmpty.Numerator != 1 || evaluator.FalseEmpty.Denominator != 1 ||
		evaluator.FalseEmpty.Unknown != 0 || evaluator.TimeToUseful.Denominator != 2 ||
		evaluator.TimeToUseful.Samples != 1 || evaluator.TimeToUseful.Unknown != 1 ||
		evaluator.TimeToUseful.P50Milliseconds != 12 {
		t.Fatalf("evaluator aggregate = %#v", evaluator)
	}
	search := findRecallOperationReport(t, report.Operations, RecallFeedbackOperationSearch)
	if search.Events != 2 || search.LatencySamples != 2 || search.UnknownLatency != 0 ||
		search.P50LatencyMilliseconds != 12 || search.P95LatencyMilliseconds != 12 ||
		search.VolumeSamples != 2 || search.UnknownVolume != 0 || search.TotalExposedResults != 4 ||
		search.TotalUTF8Bytes <= 0 {
		t.Fatalf("search operation report = %#v", search)
	}
	get := findRecallOperationReport(t, report.Operations, RecallFeedbackOperationGet)
	if get.Events != 1 || get.LatencySamples != 1 || get.UnknownLatency != 0 ||
		get.P50LatencyMilliseconds != 12 || get.P95LatencyMilliseconds != 12 ||
		get.VolumeSamples != 1 || get.UnknownVolume != 0 || get.TotalExposedResults != 1 ||
		get.TotalUTF8Bytes != int64(content.DeliveredUTF8Bytes) {
		t.Fatalf("get operation report = %#v", get)
	}
}

func approximately(got, want float64) bool {
	return math.Abs(got-want) < 0.000001
}

func findRecallFeedbackSourceReport(t *testing.T, reports []RecallFeedbackSourceReport, source string) RecallFeedbackSourceReport {
	t.Helper()
	for _, report := range reports {
		if report.Source == source {
			return report
		}
	}
	t.Fatalf("source %q missing from %#v", source, reports)
	return RecallFeedbackSourceReport{}
}

func findRecallOperationReport(t *testing.T, reports []RecallFeedbackOperationReport, operation string) RecallFeedbackOperationReport {
	t.Helper()
	for _, report := range reports {
		if report.Operation == operation {
			return report
		}
	}
	t.Fatalf("operation %q missing from %#v", operation, reports)
	return RecallFeedbackOperationReport{}
}

func TestRecallFeedbackSnapshotsUnknownExposuresAndPreservesSourceHistory(t *testing.T) {
	service := newTestService(t)
	first := saveObservation(t, service, "engram", "Feedback alpha", "shared recall feedback evidence alpha")
	second := saveObservation(t, service, "engram", "Feedback beta", "shared recall feedback evidence beta")
	service.newRecallID = func() (string, error) { return "recall-feedback-history", nil }
	turnIdentity := &store.CheckpointIdentity{
		Host: "codex", SessionID: "session-feedback-history", RootTurnID: "turn-feedback-history",
	}
	recall, err := service.Recall(RecallInput{
		Query: "shared recall feedback evidence", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit, TurnIdentity: turnIdentity,
	})
	if err != nil || len(recall.Candidates) != 2 {
		t.Fatalf("Recall result = %#v, err=%v", recall, err)
	}

	identity := CheckpointRecordInput{
		Host: turnIdentity.Host, SessionID: turnIdentity.SessionID, RootTurnID: turnIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
		RecallFeedback: &RecallFeedbackInput{
			RecallID: recall.RecallID,
			Results: []RecallFeedbackItemInput{{
				ResultID: recall.Candidates[0].ResultID, Utility: RecallUtilityOrienting,
				Quality: RecallQualityCurrent, Source: RecallFeedbackSourceAgentExplicit,
			}},
		},
	}
	created, err := service.RecordCheckpoint(identity)
	if err != nil || created.RecallFeedback == nil || created.RecallFeedback.Status != RecallFeedbackStatusRecorded {
		t.Fatalf("record initial feedback: result=%#v err=%v", created, err)
	}
	for table, want := range map[string]int{
		"recall_feedback_runs": 1, "recall_feedback_exposures": 2, "recall_feedback_labels": 1,
	} {
		var got int
		if err := service.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s rows = %d, want %d, err=%v", table, got, want, err)
		}
	}

	service.newRecallID = func() (string, error) { return "recall-feedback-history-repeat", nil }
	repeatedRecall, err := service.Recall(RecallInput{
		Query: "shared recall feedback evidence", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit, TurnIdentity: turnIdentity,
	})
	if err != nil || len(repeatedRecall.Candidates) != 2 || repeatedRecall.Candidates[0].ID != recall.Candidates[0].ID {
		t.Fatalf("repeated Recall result = %#v, err=%v", repeatedRecall, err)
	}
	repeatedInput := identity
	repeatedInput.RecallFeedback = &RecallFeedbackInput{
		RecallID: repeatedRecall.RecallID,
		Results: []RecallFeedbackItemInput{{
			ResultID: repeatedRecall.Candidates[0].ResultID, Utility: RecallUtilityOrienting,
			Quality: RecallQualityCurrent, Source: RecallFeedbackSourceAgentExplicit,
		}},
	}
	repeated, err := service.RecordCheckpoint(repeatedInput)
	if err != nil || repeated.RecallFeedback == nil ||
		repeated.RecallFeedback.Status != RecallFeedbackStatusAlreadyRecorded ||
		repeated.RecallFeedback.LabelsAlreadyRecorded != 1 {
		t.Fatalf("same turn/Memory/source replay through another run: result=%#v err=%v", repeated, err)
	}

	evaluator := identity
	evaluator.RecallFeedback = &RecallFeedbackInput{
		RecallID: recall.RecallID,
		Results: []RecallFeedbackItemInput{{
			ResultID: recall.Candidates[0].ResultID, Utility: RecallUtilityDecisive,
			Quality: RecallQualityCurrent, Source: RecallFeedbackSourceEvaluator,
		}},
	}
	appended, err := service.RecordCheckpoint(evaluator)
	if err != nil || appended.Idempotency != CheckpointIdempotencyAlreadyRecorded ||
		appended.RecallFeedback == nil || appended.RecallFeedback.Status != RecallFeedbackStatusRecorded {
		t.Fatalf("append evaluator feedback: result=%#v err=%v", appended, err)
	}

	conflicting := identity
	conflicting.RecallFeedback = &RecallFeedbackInput{
		RecallID: recall.RecallID,
		Results: []RecallFeedbackItemInput{{
			ResultID: recall.Candidates[0].ResultID, Utility: RecallUtilityUnused,
			Quality: RecallQualityStale, Source: RecallFeedbackSourceAgentExplicit,
		}},
	}
	conflict, err := service.RecordCheckpoint(conflicting)
	if err != nil || conflict.RecallFeedback == nil || conflict.RecallFeedback.Status != RecallFeedbackStatusFailed ||
		conflict.RecallFeedback.Error == nil || conflict.RecallFeedback.Error.Code != RecallFeedbackErrorCodeConflict {
		t.Fatalf("conflicting same-source feedback: result=%#v err=%v", conflict, err)
	}

	otherTurn := identity
	otherTurn.SessionID = "session-feedback-other"
	otherTurn.RootTurnID = "turn-feedback-other"
	otherTurn.RecallFeedback = evaluator.RecallFeedback
	claimed, err := service.RecordCheckpoint(otherTurn)
	if err != nil || claimed.Idempotency != CheckpointIdempotencyCreated || claimed.RecallFeedback == nil ||
		claimed.RecallFeedback.Status != RecallFeedbackStatusFailed || claimed.RecallFeedback.Error == nil ||
		claimed.RecallFeedback.Error.Code != RecallFeedbackErrorCodeTurnMismatch {
		t.Fatalf("Recall claimed by another turn: result=%#v err=%v", claimed, err)
	}
	report, err := service.RecallFeedbackReport()
	if err != nil {
		t.Fatalf("report repeated Recall exposure: %v", err)
	}
	if report.ExposedResults != 2 || report.LabelCoverage.Numerator != 1 ||
		report.LabelCoverage.Denominator != 2 || report.LabelCoverage.Unknown != 1 {
		t.Fatalf("distinct turn/Memory coverage = %#v; report=%#v", report.LabelCoverage, report)
	}
	agent := findRecallFeedbackSourceReport(t, report.Sources, RecallFeedbackSourceAgentExplicit)
	if agent.Utility.Numerator != 1 || agent.Utility.Denominator != 1 || agent.Utility.Unknown != 1 {
		t.Fatalf("distinct agent utility = %#v", agent.Utility)
	}
	if err := service.store.DeleteObservation(first.ID, true); err != nil {
		t.Fatalf("hard-delete labelled Recall Memory: %v", err)
	}
	if err := service.store.DeleteObservation(second.ID, true); err != nil {
		t.Fatalf("hard-delete unknown Recall Memory: %v", err)
	}
	afterDelete, err := service.RecallFeedbackReport()
	if err != nil {
		t.Fatalf("report after hard delete: %v", err)
	}
	if afterDelete.ExposedResults != 2 || afterDelete.LabelCoverage.Numerator != 1 ||
		afterDelete.LabelCoverage.Denominator != 2 || afterDelete.LabelCoverage.Unknown != 1 {
		t.Fatalf("hard delete changed historical exposure cohort: coverage=%#v report=%#v", afterDelete.LabelCoverage, afterDelete)
	}

	for _, table := range []string{"recall_feedback_runs", "recall_feedback_exposures", "recall_feedback_labels"} {
		rows, err := service.store.DB().Query("PRAGMA table_info(" + table + ")")
		if err != nil {
			t.Fatalf("inspect %s schema: %v", table, err)
		}
		var columns []string
		for rows.Next() {
			var cid, notNull, primaryKey int
			var name, typ string
			var defaultValue any
			if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
				t.Fatalf("scan %s schema: %v", table, err)
			}
			columns = append(columns, name)
		}
		_ = rows.Close()
		for _, forbidden := range []string{"host", "session_id", "root_turn_id", "memory_id", "memory_sync_id", "observation_id", "recall_id", "result_id", "query", "content"} {
			if slices.Contains(columns, forbidden) {
				t.Fatalf("%s schema leaked raw/content column %q: %v", table, forbidden, columns)
			}
		}
	}

	var storedKeys []string
	rows, err := service.store.DB().Query(`SELECT memory_key FROM recall_feedback_exposures ORDER BY result_rank`)
	if err != nil {
		t.Fatalf("load exposure keys: %v", err)
	}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatalf("scan exposure key: %v", err)
		}
		storedKeys = append(storedKeys, key)
	}
	_ = rows.Close()
	uniqueKeys := make(map[string]struct{}, len(storedKeys))
	for _, key := range storedKeys {
		uniqueKeys[key] = struct{}{}
	}
	if len(storedKeys) != 4 || len(uniqueKeys) != 2 ||
		slices.Contains(storedKeys, first.SyncID) || slices.Contains(storedKeys, second.SyncID) {
		t.Fatalf("salted exposure keys = %v", storedKeys)
	}
}

func TestRecallFeedbackTimeToUsefulIncludesReformulationInterval(t *testing.T) {
	service := newTestService(t)
	saveObservation(t, service, "engram", "Useful reformulation", "distinctive useful reformulation evidence")
	turnIdentity := &store.CheckpointIdentity{
		Host: "codex", SessionID: "session-reformulation", RootTurnID: "turn-reformulation",
	}
	started := time.Unix(100, 0)
	elapsed := 10 * time.Millisecond
	service.recallStartedAt = func() time.Time { return started }
	service.recallElapsed = func(time.Time) time.Duration { return elapsed }
	service.newRecallID = func() (string, error) { return "recall-reformulation-empty", nil }
	first, err := service.Recall(RecallInput{
		Query: "zzzz-no-memory-can-match-zzzz", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit, TurnIdentity: turnIdentity,
	})
	if err != nil || first.ResultCount != 0 {
		t.Fatalf("first Recall = %#v, err=%v", first, err)
	}

	started = time.Unix(105, 0)
	elapsed = 5 * time.Millisecond
	service.newRecallID = func() (string, error) { return "recall-reformulation-useful", nil }
	second, err := service.Recall(RecallInput{
		Query: "distinctive useful reformulation evidence", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit, TurnIdentity: turnIdentity,
	})
	if err != nil || second.ResultCount != 1 {
		t.Fatalf("second Recall = %#v, err=%v", second, err)
	}
	result, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: turnIdentity.Host, SessionID: turnIdentity.SessionID, RootTurnID: turnIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
		RecallFeedback: &RecallFeedbackInput{
			RecallID: second.RecallID,
			Results: []RecallFeedbackItemInput{{
				ResultID: second.Candidates[0].ResultID, Utility: RecallUtilityDecisive,
				Quality: RecallQualityCurrent, Source: RecallFeedbackSourceEvaluator,
			}},
		},
	})
	if err != nil || result.RecallFeedback == nil || result.RecallFeedback.Status != RecallFeedbackStatusRecorded {
		t.Fatalf("record useful reformulation feedback: result=%#v err=%v", result, err)
	}

	report, err := service.RecallFeedbackReport()
	if err != nil {
		t.Fatalf("report reformulation interval: %v", err)
	}
	evaluator := findRecallFeedbackSourceReport(t, report.Sources, RecallFeedbackSourceEvaluator)
	if evaluator.TimeToUseful.Denominator != 1 || evaluator.TimeToUseful.Samples != 1 ||
		evaluator.TimeToUseful.Unknown != 0 || evaluator.TimeToUseful.P50Milliseconds != 5005 ||
		evaluator.TimeToUseful.P95Milliseconds != 5005 {
		t.Fatalf("time to useful = %#v", evaluator.TimeToUseful)
	}
}

func TestRecallFeedbackPreservesUnknownExposureAfterHardDeleteWithoutFeedback(t *testing.T) {
	service := newTestService(t)
	memory := saveObservation(t, service, "engram", "Unknown exposure", "unlabelled exposure survives hard deletion")
	turnIdentity := &store.CheckpointIdentity{
		Host: "codex", SessionID: "session-unknown-delete", RootTurnID: "turn-unknown-delete",
	}
	service.newRecallID = func() (string, error) { return "recall-unknown-delete", nil }
	recall, err := service.Recall(RecallInput{
		Query: "unlabelled exposure survives hard deletion", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit, TurnIdentity: turnIdentity,
	})
	if err != nil || recall.ResultCount != 1 {
		t.Fatalf("bound unknown Recall = %#v, err=%v", recall, err)
	}
	if err := service.store.DeleteObservation(memory.ID, true); err != nil {
		t.Fatalf("hard-delete unlabelled exposed Memory: %v", err)
	}

	report, err := service.RecallFeedbackReport()
	if err != nil {
		t.Fatalf("report unlabelled exposure after hard delete: %v", err)
	}
	if report.ExposedResults != 1 || report.LabelCoverage.Numerator != 0 ||
		report.LabelCoverage.Denominator != 1 || report.LabelCoverage.Unknown != 1 {
		t.Fatalf("unlabelled hard-delete cohort = %#v; report=%#v", report.LabelCoverage, report)
	}
	var labels int
	if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM recall_feedback_labels`).Scan(&labels); err != nil {
		t.Fatalf("count explicit labels: %v", err)
	}
	if labels != 0 {
		t.Fatalf("bound Recall synthesized %d labels, want none", labels)
	}
}

func TestRecallFeedbackRejectsAnUnboundRecallWithoutChangingCheckpoint(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-feedback-unbound", nil }
	recall, err := service.Recall(RecallInput{
		Query: "no unbound feedback candidate", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil || recall.ResultCount != 0 {
		t.Fatalf("unbound Recall = %#v, err=%v", recall, err)
	}
	result, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: "codex", SessionID: "session-unbound-feedback", RootTurnID: "turn-unbound-feedback",
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
		RecallFeedback: &RecallFeedbackInput{
			RecallID: recall.RecallID,
			FalseEmpty: &RecallFalseEmptyInput{
				Value: true, Source: RecallFeedbackSourceAgentExplicit,
			},
		},
	})
	if err != nil || result.Idempotency != CheckpointIdempotencyCreated || result.RecallFeedback == nil ||
		result.RecallFeedback.Status != RecallFeedbackStatusFailed || result.RecallFeedback.Error == nil ||
		result.RecallFeedback.Error.Code != RecallFeedbackErrorCodeTurnMismatch {
		t.Fatalf("unbound feedback result = %#v, err=%v", result, err)
	}
	if _, err := service.CheckpointStatus(CheckpointStatusInput{
		Host: "codex", SessionID: "session-unbound-feedback", RootTurnID: "turn-unbound-feedback",
	}); err != nil {
		t.Fatalf("checkpoint missing after unbound feedback rejection: %v", err)
	}
}

func TestCheckpointRecordsOnlyExplicitFeedbackForExposedRecallResults(t *testing.T) {
	service := newTestService(t)
	memory := saveObservation(t, service, "engram", "Recall feedback decision", "Use the Core-owned checkpoint feedback boundary.")
	service.newRecallID = func() (string, error) { return "recall-feedback-exposed", nil }
	turnIdentity := &store.CheckpointIdentity{
		Host: "codex", SessionID: "session-feedback", RootTurnID: "turn-feedback",
	}
	recall, err := service.Recall(RecallInput{
		Query: "Core-owned checkpoint feedback", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit, BinaryVersion: "3.4.0-test", BinaryRevision: "feedback-revision",
		TurnIdentity: turnIdentity,
	})
	if err != nil || len(recall.Candidates) != 1 || recall.Candidates[0].ID != memory.ID {
		t.Fatalf("Recall result = %#v, err=%v", recall, err)
	}

	input := CheckpointRecordInput{
		Host: turnIdentity.Host, SessionID: turnIdentity.SessionID, RootTurnID: turnIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped,
		ReasonCode:  store.CheckpointSkipReasonNoDurableKnowledge,
		RecallFeedback: &RecallFeedbackInput{
			RecallID: recall.RecallID,
			Results: []RecallFeedbackItemInput{{
				ResultID: recall.Candidates[0].ResultID,
				Utility:  RecallUtilityDecisive,
				Quality:  RecallQualityCurrent,
				Source:   RecallFeedbackSourceAgentExplicit,
			}},
		},
	}
	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record checkpoint with Recall feedback: %v", err)
	}
	if created.Idempotency != CheckpointIdempotencyCreated || created.RecallFeedback == nil ||
		created.RecallFeedback.Status != RecallFeedbackStatusRecorded || created.RecallFeedback.LabelsRecorded != 1 {
		t.Fatalf("created result = %#v", created)
	}

	var turnKey, memoryKey, utility, quality, source string
	if err := service.store.DB().QueryRow(`
		SELECT turn_key, memory_key, utility, quality, label_source
		FROM recall_feedback_labels`).Scan(&turnKey, &memoryKey, &utility, &quality, &source); err != nil {
		t.Fatalf("load stored Recall feedback: %v", err)
	}
	for _, raw := range []string{input.Host, input.SessionID, input.RootTurnID, memory.SyncID} {
		if strings.Contains(turnKey, raw) || strings.Contains(memoryKey, raw) {
			t.Fatalf("salted feedback keys leaked raw identifier %q: turn=%q memory=%q", raw, turnKey, memoryKey)
		}
	}
	if len(turnKey) != 64 || len(memoryKey) != 64 || utility != RecallUtilityDecisive ||
		quality != RecallQualityCurrent || source != RecallFeedbackSourceAgentExplicit {
		t.Fatalf("stored feedback = turn %q memory %q utility %q quality %q source %q", turnKey, memoryKey, utility, quality, source)
	}

	replayed, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("replay checkpoint with Recall feedback: %v", err)
	}
	if replayed.Idempotency != CheckpointIdempotencyAlreadyRecorded || replayed.RecallFeedback == nil ||
		replayed.RecallFeedback.Status != RecallFeedbackStatusAlreadyRecorded {
		t.Fatalf("replayed result = %#v", replayed)
	}

	invalid := input
	invalid.SessionID = "session-feedback-invalid"
	invalid.RootTurnID = "turn-feedback-invalid"
	invalidTurnIdentity := store.CheckpointIdentity{
		Host: invalid.Host, SessionID: invalid.SessionID, RootTurnID: invalid.RootTurnID,
	}
	if err := service.store.RecordRecallRunContext(t.Context(), store.RecallRunRecord{
		RecallID: "recall-feedback-invalid-result", Project: "engram", Scope: "project",
		TurnIdentity: &invalidTurnIdentity,
	}); err != nil {
		t.Fatalf("record invalid-result Recall run: %v", err)
	}
	invalid.RecallFeedback = &RecallFeedbackInput{
		RecallID: "recall-feedback-invalid-result",
		Results: []RecallFeedbackItemInput{{
			ResultID: "result-never-exposed", Utility: RecallUtilityUnused,
			Quality: RecallQualityUnknown, Source: RecallFeedbackSourceEvaluator,
		}},
	}
	withInvalidFeedback, err := service.RecordCheckpoint(invalid)
	if err != nil {
		t.Fatalf("invalid optional feedback failed checkpoint: %v", err)
	}
	if withInvalidFeedback.Idempotency != CheckpointIdempotencyCreated || withInvalidFeedback.RecallFeedback == nil ||
		withInvalidFeedback.RecallFeedback.Status != RecallFeedbackStatusFailed ||
		withInvalidFeedback.RecallFeedback.Error == nil ||
		withInvalidFeedback.RecallFeedback.Error.Code != RecallFeedbackErrorCodeResultNotExposed {
		t.Fatalf("invalid feedback result = %#v", withInvalidFeedback)
	}
	if _, err := service.CheckpointStatus(CheckpointStatusInput{
		Host: invalid.Host, SessionID: invalid.SessionID, RootTurnID: invalid.RootTurnID,
	}); err != nil {
		t.Fatalf("checkpoint was not retained after optional feedback rejection: %v", err)
	}
}

func TestCheckpointSurvivesVisibleRecallFeedbackPersistenceFailure(t *testing.T) {
	service := newTestService(t)
	memory := saveObservation(t, service, "engram", "Feedback failure", "The checkpoint remains terminal when feedback persistence fails.")
	service.newRecallID = func() (string, error) { return "recall-feedback-failure", nil }
	turnIdentity := &store.CheckpointIdentity{
		Host: "codex", SessionID: "session-feedback-failure", RootTurnID: "turn-feedback-failure",
	}
	recall, err := service.Recall(RecallInput{
		Query: "feedback persistence fails", Project: "engram", Scope: "project",
		ProjectStrength: project.IdentityStrengthExplicit, TurnIdentity: turnIdentity,
	})
	if err != nil || len(recall.Candidates) != 1 || recall.Candidates[0].ID != memory.ID {
		t.Fatalf("Recall result = %#v, err=%v", recall, err)
	}
	if _, err := service.store.DB().Exec(`
		CREATE TRIGGER fail_recall_feedback
		BEFORE INSERT ON recall_feedback_labels
		BEGIN
			SELECT RAISE(ABORT, 'injected Recall feedback failure');
		END;`); err != nil {
		t.Fatalf("install feedback failure trigger: %v", err)
	}

	result, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: turnIdentity.Host, SessionID: turnIdentity.SessionID, RootTurnID: turnIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
		RecallFeedback: &RecallFeedbackInput{
			RecallID: recall.RecallID,
			Results: []RecallFeedbackItemInput{{
				ResultID: recall.Candidates[0].ResultID, Utility: RecallUtilityOrienting,
				Quality: RecallQualityCurrent, Source: RecallFeedbackSourceUserExplicit,
			}},
		},
	})
	if err != nil {
		t.Fatalf("feedback persistence failure failed checkpoint: %v", err)
	}
	if result.Idempotency != CheckpointIdempotencyCreated || result.RecallFeedback == nil ||
		result.RecallFeedback.Status != RecallFeedbackStatusFailed || result.RecallFeedback.Error == nil ||
		result.RecallFeedback.Error.Code != RecallFeedbackErrorCodeFailed {
		t.Fatalf("checkpoint result = %#v", result)
	}
	if _, err := service.CheckpointStatus(CheckpointStatusInput{
		Host: "codex", SessionID: "session-feedback-failure", RootTurnID: "turn-feedback-failure",
	}); err != nil {
		t.Fatalf("checkpoint missing after feedback persistence failure: %v", err)
	}
}
