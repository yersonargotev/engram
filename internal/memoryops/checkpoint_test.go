package memoryops

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestCheckpointRecordAndStatusShareIdempotentDomainResult(t *testing.T) {
	service := newTestService(t)
	input := CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-domain-123",
		RootTurnID:  "turn-domain-456",
		Disposition: store.CheckpointDispositionSkipped,
		ReasonCode:  store.CheckpointSkipReasonNoDurableKnowledge,
	}

	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record checkpoint: %v", err)
	}
	if created.Idempotency != CheckpointIdempotencyCreated || created.Checkpoint == nil {
		t.Fatalf("created result = %#v", created)
	}

	replayed, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("replay checkpoint: %v", err)
	}
	if replayed.Idempotency != CheckpointIdempotencyAlreadyRecorded {
		t.Fatalf("replay idempotency = %q", replayed.Idempotency)
	}
	if !reflect.DeepEqual(replayed.Checkpoint, created.Checkpoint) {
		t.Fatalf("replayed checkpoint = %#v, want %#v", replayed.Checkpoint, created.Checkpoint)
	}

	status, err := service.CheckpointStatus(CheckpointStatusInput{
		Host:       input.Host,
		SessionID:  input.SessionID,
		RootTurnID: input.RootTurnID,
	})
	if err != nil {
		t.Fatalf("checkpoint status: %v", err)
	}
	if !reflect.DeepEqual(status.Checkpoint, created.Checkpoint) {
		t.Fatalf("status checkpoint = %#v, want %#v", status.Checkpoint, created.Checkpoint)
	}
}

func TestVerifyCheckpointStopOwnsSingleRecoveryDecision(t *testing.T) {
	service := newTestService(t)
	identity := CheckpointVerificationInput{
		Host: "codex", SessionID: "session-stop-domain", RootTurnID: "turn-stop-domain",
	}

	missing, err := service.VerifyCheckpoint(identity)
	if err != nil {
		t.Fatalf("verify missing checkpoint: %v", err)
	}
	if missing != CheckpointVerificationContinuationRequired {
		t.Fatalf("missing outcome = %q, want %q", missing, CheckpointVerificationContinuationRequired)
	}

	identity.RecoveryActive = true
	stillMissing, err := service.VerifyCheckpoint(identity)
	if err != nil {
		t.Fatalf("verify missing checkpoint during recovery: %v", err)
	}
	if stillMissing != CheckpointVerificationRecoveryExhausted {
		t.Fatalf("recovery outcome = %q, want %q", stillMissing, CheckpointVerificationRecoveryExhausted)
	}

	if _, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: identity.Host, SessionID: identity.SessionID, RootTurnID: identity.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	}); err != nil {
		t.Fatalf("record terminal checkpoint: %v", err)
	}

	complete, err := service.VerifyCheckpoint(identity)
	if err != nil {
		t.Fatalf("verify terminal checkpoint: %v", err)
	}
	if complete != CheckpointVerificationComplete {
		t.Fatalf("terminal outcome = %q, want %q", complete, CheckpointVerificationComplete)
	}
}

func TestVerifyCheckpointAcceptsEveryTerminalDisposition(t *testing.T) {
	service := newTestService(t)
	memory := saveObservation(t, service, "engram", "Terminal saved checkpoint", "Durable acceptance evidence.")
	tests := []CheckpointRecordInput{
		{
			Host: "codex", SessionID: "session-terminal-skipped", RootTurnID: "turn-terminal-skipped",
			Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
		},
		{
			Host: "codex", SessionID: "session-terminal-saved", RootTurnID: "turn-terminal-saved",
			Disposition: store.CheckpointDispositionSaved, Project: "engram", MemoryIDs: []int64{memory.ID},
		},
		{
			Host: "codex", SessionID: "session-terminal-review", RootTurnID: "turn-terminal-review",
			Disposition: store.CheckpointDispositionNeedsReview, Project: "engram",
			Proposal: &CheckpointProposalInput{
				Title: "Terminal review checkpoint", Content: "Review before admitting this Memory.",
			},
		},
	}
	for _, record := range tests {
		t.Run(record.Disposition, func(t *testing.T) {
			if _, err := service.RecordCheckpoint(record); err != nil {
				t.Fatalf("record %s checkpoint: %v", record.Disposition, err)
			}
			outcome, err := service.VerifyCheckpoint(CheckpointVerificationInput{
				Host: record.Host, SessionID: record.SessionID, RootTurnID: record.RootTurnID,
			})
			if err != nil || outcome != CheckpointVerificationComplete {
				t.Fatalf("verify %s checkpoint outcome=%q err=%v", record.Disposition, outcome, err)
			}
		})
	}
}

func TestSavedCheckpointAttachesAnExistingMemory(t *testing.T) {
	service := newTestService(t)
	memory := saveObservation(t, service, "engram", "Saved checkpoint", "Attach this durable decision to the completed root turn.")
	input := CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-saved-existing",
		RootTurnID:  "turn-saved-existing",
		Disposition: store.CheckpointDispositionSaved,
		Project:     "engram",
		MemoryIDs:   []int64{memory.ID},
	}

	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record saved checkpoint: %v", err)
	}
	if created.Idempotency != CheckpointIdempotencyCreated {
		t.Fatalf("idempotency = %q, want %q", created.Idempotency, CheckpointIdempotencyCreated)
	}
	wantReferences := []store.CheckpointReference{{
		Kind:         store.CheckpointReferenceKindMemory,
		MemoryID:     memory.ID,
		MemorySyncID: memory.SyncID,
		Project:      "engram",
	}}
	if created.Checkpoint.Disposition != store.CheckpointDispositionSaved ||
		!reflect.DeepEqual(created.Checkpoint.References, wantReferences) {
		t.Fatalf("saved checkpoint = %#v, want references %#v", created.Checkpoint, wantReferences)
	}

	status, err := service.CheckpointStatus(CheckpointStatusInput{
		Host:       input.Host,
		SessionID:  input.SessionID,
		RootTurnID: input.RootTurnID,
	})
	if err != nil {
		t.Fatalf("saved checkpoint status: %v", err)
	}
	if !reflect.DeepEqual(status.Checkpoint, created.Checkpoint) {
		t.Fatalf("status checkpoint = %#v, want %#v", status.Checkpoint, created.Checkpoint)
	}
}

func TestCheckpointPreflightReusesExactDuplicatesBoundsCandidatesAndDoesNotPersist(t *testing.T) {
	service := newTestService(t)
	exact := saveObservation(t, service, "engram", "Terminal Memory checkpoint", "Reuse this exact durable decision.")
	for index, content := range []string{
		"Terminal Memory checkpoint architecture keeps Core authoritative.",
		"Terminal Memory checkpoint policy keeps proposals local.",
		"Terminal Memory checkpoint decision preserves exact replay.",
		"Terminal Memory checkpoint design keeps adapters thin.",
	} {
		result, err := service.Save(SaveInput{
			SessionID:        "session-preflight-candidate-" + string(rune('a'+index)),
			CWD:              "/work/engram",
			Project:          "engram",
			Type:             "architecture",
			Title:            "Terminal Memory checkpoint candidate " + string(rune('a'+index)),
			Content:          content,
			CandidateOptions: store.CandidateOptions{SkipInsert: true},
		})
		if err != nil || result.Observation == nil {
			t.Fatalf("seed semantic candidate %d: result=%#v err=%v", index, result, err)
		}
	}
	otherProject := saveObservation(t, service, "other", exact.Title, exact.Content)

	tables := []string{
		"sessions", "observations", "observations_fts", "memory_relations", "sync_mutations",
		"memory_proposals", "memory_checkpoints", "memory_checkpoint_references",
		"memory_checkpoint_proposal_references",
	}
	before := checkpointTableCounts(t, service, tables)
	exactBefore, err := service.Get(exact.ID)
	if err != nil {
		t.Fatalf("get exact duplicate before preflight: %v", err)
	}

	result, err := service.PreflightCheckpoint(CheckpointPreflightInput{
		Project: "ENGRAM",
		Memories: []CheckpointMemoryInput{
			{Type: exact.Type, Title: exact.Title, Content: exact.Content, Scope: exact.Scope},
			{Type: "architecture", Title: "Terminal Memory checkpoint architecture", Content: "Assess semantic candidates without writes."},
		},
	})
	if err != nil {
		t.Fatalf("preflight checkpoint: %v", err)
	}
	if result.Project != "engram" || result.CandidateLimit != 3 {
		t.Fatalf("preflight bounds = %#v", result)
	}
	if len(result.ExactDuplicates) != 1 || result.ExactDuplicates[0].InputIndex != 0 ||
		result.ExactDuplicates[0].Reference.MemoryID != exact.ID ||
		result.ExactDuplicates[0].Reference.MemoryID == otherProject.ID ||
		result.ExactDuplicates[0].Reference.Project != "engram" {
		t.Fatalf("exact duplicates = %#v, want Memory %d in engram", result.ExactDuplicates, exact.ID)
	}
	if len(result.Candidates) != 3 {
		t.Fatalf("semantic candidates = %#v, want exactly three", result.Candidates)
	}
	seen := map[int64]bool{}
	for _, candidate := range result.Candidates {
		if candidate.InputIndex != 1 || candidate.Reference.Project != "engram" ||
			candidate.Reference.MemoryID == exact.ID || candidate.Reference.MemoryID == otherProject.ID ||
			candidate.Title == "" || candidate.Content == "" || seen[candidate.Reference.MemoryID] {
			t.Fatalf("invalid semantic candidate = %#v", candidate)
		}
		seen[candidate.Reference.MemoryID] = true
	}

	after := checkpointTableCounts(t, service, tables)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("preflight changed persistent state: before=%v after=%v", before, after)
	}
	exactAfter, err := service.Get(exact.ID)
	if err != nil {
		t.Fatalf("get exact duplicate after preflight: %v", err)
	}
	if !reflect.DeepEqual(exactAfter, exactBefore) {
		t.Fatalf("preflight mutated exact duplicate: before=%#v after=%#v", exactBefore, exactAfter)
	}
}

func TestCheckpointPreflightDefaultsToProjectScopeAndKeepsProspectiveInputsRepresented(t *testing.T) {
	service := newTestService(t)
	for index := 0; index < 3; index++ {
		result, err := service.Save(SaveInput{
			SessionID: "session-preflight-alpha-" + string(rune('a'+index)), CWD: "/work/engram",
			Project: "engram", Type: "decision", Scope: "project",
			Title:            "alphaquartz candidate " + string(rune('a'+index)),
			Content:          "alphaquartz weak relation " + string(rune('a'+index)),
			CandidateOptions: store.CandidateOptions{SkipInsert: true},
		})
		if err != nil || result.Observation == nil {
			t.Fatalf("seed alpha candidate %d: result=%#v err=%v", index, result, err)
		}
	}
	projectCandidate, err := service.Save(SaveInput{
		SessionID: "session-preflight-beta-project", CWD: "/work/engram", Project: "engram",
		Type: "decision", Scope: "project", Title: "betacobalt material conflict",
		Content: "betacobalt architecture conflict", CandidateOptions: store.CandidateOptions{SkipInsert: true},
	})
	if err != nil || projectCandidate.Observation == nil {
		t.Fatalf("seed project candidate: result=%#v err=%v", projectCandidate, err)
	}
	personalCandidate, err := service.Save(SaveInput{
		SessionID: "session-preflight-beta-personal", CWD: "/work/engram", Project: "engram",
		Type: "decision", Scope: "personal", Title: "betacobalt private candidate",
		Content: "betacobalt personal-only content", CandidateOptions: store.CandidateOptions{SkipInsert: true},
	})
	if err != nil || personalCandidate.Observation == nil {
		t.Fatalf("seed personal candidate: result=%#v err=%v", personalCandidate, err)
	}

	result, err := service.PreflightCheckpoint(CheckpointPreflightInput{
		Project: "engram",
		Memories: []CheckpointMemoryInput{
			{Type: "decision", Title: "alphaquartz prospective", Content: "alphaquartz new outcome"},
			{Type: "decision", Title: "betacobalt prospective", Content: "betacobalt unresolved architecture"},
		},
	})
	if err != nil {
		t.Fatalf("preflight multiple inputs: %v", err)
	}
	representedSecondInput := false
	for _, candidate := range result.Candidates {
		if candidate.Reference.MemoryID == personalCandidate.Observation.ID || candidate.Scope != "project" {
			t.Fatalf("default project scope exposed personal candidate: %#v", candidate)
		}
		if candidate.InputIndex == 1 && candidate.Reference.MemoryID == projectCandidate.Observation.ID {
			representedSecondInput = true
		}
	}
	if !representedSecondInput {
		t.Fatalf("global candidate bound starved second prospective Memory: %#v", result.Candidates)
	}
}

func TestCheckpointPreflightAndCommitShareExactMemoryIdentity(t *testing.T) {
	tests := []struct {
		name          string
		seedTool      string
		seedTopic     string
		proposedTool  string
		proposedTopic string
	}{
		{name: "topic key", seedTopic: "architecture/old", proposedTopic: "architecture/new"},
		{name: "tool name", seedTool: "claude", proposedTool: "codex"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newTestService(t)
			seed, err := service.Save(SaveInput{
				SessionID: "session-preflight-seed", CWD: "/work/engram", Project: "engram",
				Type: "architecture", Title: "Identity-aware duplicate", Content: "The durable content is identical.",
				ToolName: test.seedTool, TopicKey: test.seedTopic,
				CandidateOptions: store.CandidateOptions{SkipInsert: true},
			})
			if err != nil || seed.Observation == nil {
				t.Fatalf("seed Memory: result=%#v err=%v", seed, err)
			}
			prospective := CheckpointMemoryInput{
				Type: "architecture", Title: "Identity-aware duplicate", Content: "The durable content is identical.",
				ToolName: test.proposedTool, TopicKey: test.proposedTopic,
			}
			preflight, err := service.PreflightCheckpoint(CheckpointPreflightInput{
				Project: "engram", Memories: []CheckpointMemoryInput{prospective},
			})
			if err != nil {
				t.Fatalf("preflight identity-aware Memory: %v", err)
			}
			if len(preflight.ExactDuplicates) != 0 {
				t.Fatalf("different durable identity was reused by preflight: %#v", preflight.ExactDuplicates)
			}

			result, err := service.RecordCheckpoint(CheckpointRecordInput{
				Host: "codex", SessionID: "session-preflight-commit", RootTurnID: "turn-" + strings.ReplaceAll(test.name, " ", "-"),
				Disposition: store.CheckpointDispositionSaved, Project: "engram", Memories: []CheckpointMemoryInput{prospective},
			})
			if err != nil {
				t.Fatalf("commit identity-aware Memory: %v", err)
			}
			if len(result.Checkpoint.References) != 1 || result.Checkpoint.References[0].MemoryID == seed.Observation.ID {
				t.Fatalf("commit reused Memory rejected by preflight: %#v", result.Checkpoint.References)
			}
			created, err := service.store.GetObservation(result.Checkpoint.References[0].MemoryID)
			if err != nil {
				t.Fatalf("load committed Memory: %v", err)
			}
			createdTool, createdTopic := "", ""
			if created.ToolName != nil {
				createdTool = *created.ToolName
			}
			if created.TopicKey != nil {
				createdTopic = *created.TopicKey
			}
			if createdTool != test.proposedTool || createdTopic != test.proposedTopic {
				t.Fatalf("committed durable identity = tool %v topic %v", created.ToolName, created.TopicKey)
			}
			seedAfter, err := service.store.GetObservation(seed.Observation.ID)
			if err != nil || seedAfter.DuplicateCount != seed.Observation.DuplicateCount {
				t.Fatalf("rejected duplicate mutated: before=%#v after=%#v err=%v", seed.Observation, seedAfter, err)
			}
		})
	}
}

func TestNeedsReviewCheckpointAtomicallyPreservesSettledMemoriesAndOneProposal(t *testing.T) {
	service := newTestService(t)
	existing := saveObservation(t, service, "engram", "Settled Memory", "This result is settled and durable.")
	input := CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-mixed-memory",
		RootTurnID:  "turn-mixed-memory",
		Disposition: store.CheckpointDispositionNeedsReview,
		Project:     "engram",
		MemoryIDs:   []int64{existing.ID},
		Memories: []CheckpointMemoryInput{{
			Type: "discovery", Title: "Settled discovery", Content: "This additional result is also settled.",
		}},
		Proposal: &CheckpointProposalInput{
			Title: "Unresolved architecture choice", Content: "This material conflict still needs review.",
		},
	}

	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record Mixed Memory checkpoint: %v", err)
	}
	if created.Idempotency != CheckpointIdempotencyCreated || created.Checkpoint == nil ||
		created.Checkpoint.Disposition != store.CheckpointDispositionNeedsReview ||
		len(created.Checkpoint.References) != 2 || created.Checkpoint.Proposal == nil {
		t.Fatalf("Mixed Memory checkpoint = %#v", created)
	}
	if created.Checkpoint.References[0].MemoryID != existing.ID ||
		created.Checkpoint.References[1].MemoryID == 0 ||
		created.Checkpoint.Proposal.Project != "engram" {
		t.Fatalf("Mixed Memory references = %#v proposal=%#v", created.Checkpoint.References, created.Checkpoint.Proposal)
	}

	var proposals, checkpoints, references, mutations int
	for table, target := range map[string]*int{
		"memory_proposals": &proposals, "memory_checkpoints": &checkpoints,
		"memory_checkpoint_references": &references, "sync_mutations": &mutations,
	} {
		if err := service.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(target); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
	}
	if proposals != 1 || checkpoints != 1 || references != 2 || mutations == 0 {
		t.Fatalf("Mixed Memory state proposals=%d checkpoints=%d references=%d mutations=%d", proposals, checkpoints, references, mutations)
	}

	replayed, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID,
		Disposition: store.CheckpointDispositionNeedsReview,
		ReasonCode:  "invalid-retry-reason",
		Project:     "other",
		MemoryIDs:   []int64{-1, -1},
		Proposal:    &CheckpointProposalInput{},
	})
	if err != nil {
		t.Fatalf("replay Mixed Memory checkpoint before payload validation: %v", err)
	}
	if replayed.Idempotency != CheckpointIdempotencyAlreadyRecorded ||
		!reflect.DeepEqual(replayed.Checkpoint, created.Checkpoint) {
		t.Fatalf("replayed Mixed Memory checkpoint = %#v, want %#v", replayed, created)
	}
}

func TestMixedMemoryCheckpointRollsBackEveryWriteOnProposalReferenceFailure(t *testing.T) {
	service := newTestService(t)
	if _, err := service.store.DB().Exec(`
		CREATE TRIGGER fail_mixed_proposal_reference
		BEFORE INSERT ON memory_checkpoint_proposal_references
		BEGIN
			SELECT RAISE(ABORT, 'injected Mixed Memory proposal reference failure');
		END;`); err != nil {
		t.Fatalf("install Mixed Memory failure trigger: %v", err)
	}

	_, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: "codex", SessionID: "session-mixed-rollback", RootTurnID: "turn-mixed-rollback",
		Disposition: store.CheckpointDispositionNeedsReview, Project: "engram", CWD: "/work/engram",
		Memories: []CheckpointMemoryInput{{
			Type: "decision", Title: "Settled rollback canary", Content: "This Memory must roll back with the proposal.",
		}},
		Proposal: &CheckpointProposalInput{
			Title: "Unresolved rollback canary", Content: "This proposal must roll back with the Memory.",
		},
	})
	if err == nil {
		t.Fatal("record Mixed Memory checkpoint succeeded despite injected proposal-reference failure")
	}
	for table, want := range map[string]int{
		"sessions": 0, "observations": 0, "observations_fts": 0, "sync_mutations": 0,
		"memory_proposals": 0, "memory_checkpoints": 0, "memory_checkpoint_references": 0,
		"memory_checkpoint_proposal_references": 0,
	} {
		var got int
		if queryErr := service.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); queryErr != nil {
			t.Fatalf("count %s: %v", table, queryErr)
		}
		if got != want {
			t.Fatalf("%s rows = %d, want %d after Mixed Memory rollback", table, got, want)
		}
	}
}

func TestCheckpointExactReplayPrecedesRetryPayloadValidation(t *testing.T) {
	service := newTestService(t)
	input := CheckpointRecordInput{
		Host: "codex", SessionID: "session-replay-first", RootTurnID: "turn-replay-first",
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	}
	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record original checkpoint: %v", err)
	}
	replayed, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped,
		ReasonCode:  "invalid-on-retry",
		Project:     "other",
		MemoryIDs:   []int64{-1},
		Proposal:    &CheckpointProposalInput{},
	})
	if err != nil {
		t.Fatalf("exact replay validated retry payload: %v", err)
	}
	if replayed.Idempotency != CheckpointIdempotencyAlreadyRecorded ||
		!reflect.DeepEqual(replayed.Checkpoint, created.Checkpoint) {
		t.Fatalf("exact replay = %#v, want original %#v", replayed, created)
	}
}

func checkpointTableCounts(t *testing.T, service *Service, tables []string) map[string]int {
	t.Helper()
	counts := make(map[string]int, len(tables))
	for _, table := range tables {
		var count int
		if err := service.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		counts[table] = count
	}
	return counts
}

func TestCheckpointErrorsHaveStableTransportCodes(t *testing.T) {
	service := newTestService(t)
	tests := []struct {
		name     string
		record   *CheckpointRecordInput
		status   *CheckpointStatusInput
		wantCode string
		wantErr  error
	}{
		{
			name: "unsupported disposition",
			record: &CheckpointRecordInput{
				Host: "codex", SessionID: "session", RootTurnID: "turn", Disposition: "deferred",
			},
			wantCode: CheckpointErrorCodeInvalidDisposition,
			wantErr:  ErrCheckpointInvalidDisposition,
		},
		{
			name: "missing identity",
			record: &CheckpointRecordInput{
				SessionID: "session", RootTurnID: "turn", Disposition: store.CheckpointDispositionSkipped,
				ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
			},
			wantCode: CheckpointErrorCodeInvalidIdentity,
			wantErr:  store.ErrCheckpointInvalidIdentity,
		},
		{
			name: "processing failure is not a semantic skip",
			record: &CheckpointRecordInput{
				Host: "codex", SessionID: "session", RootTurnID: "turn", Disposition: store.CheckpointDispositionSkipped,
				ReasonCode: "processing_failed",
			},
			wantCode: CheckpointErrorCodeInvalidReason,
			wantErr:  store.ErrCheckpointInvalidReason,
		},
		{
			name:     "status not found",
			status:   &CheckpointStatusInput{Host: "codex", SessionID: "missing-session", RootTurnID: "missing-turn"},
			wantCode: CheckpointErrorCodeNotFound,
			wantErr:  store.ErrCheckpointNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			if tt.record != nil {
				_, err = service.RecordCheckpoint(*tt.record)
			} else {
				_, err = service.CheckpointStatus(*tt.status)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if got := CheckpointErrorCode(err); got != tt.wantCode {
				t.Fatalf("error code = %q, want %q", got, tt.wantCode)
			}
		})
	}
}

func TestSavedCheckpointRejectsInvalidReferenceSetsWithoutRecording(t *testing.T) {
	service := newTestService(t)
	engramMemory := saveObservation(t, service, "engram", "Engram decision", "This Memory belongs to Engram.")
	otherMemory := saveObservation(t, service, "other", "Other decision", "This Memory belongs to another project.")
	tests := []struct {
		name      string
		memoryIDs []int64
		wantErr   error
		wantCode  string
	}{
		{name: "empty", wantErr: store.ErrCheckpointInvalidReferences, wantCode: CheckpointErrorCodeInvalidReferences},
		{name: "duplicate", memoryIDs: []int64{engramMemory.ID, engramMemory.ID}, wantErr: store.ErrCheckpointInvalidReferences, wantCode: CheckpointErrorCodeInvalidReferences},
		{name: "nonexistent", memoryIDs: []int64{engramMemory.ID + otherMemory.ID + 1000}, wantErr: store.ErrCheckpointMemoryNotFound, wantCode: CheckpointErrorCodeMemoryNotFound},
		{name: "cross project", memoryIDs: []int64{engramMemory.ID, otherMemory.ID}, wantErr: store.ErrCheckpointProjectMismatch, wantCode: CheckpointErrorCodeProjectMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity := CheckpointStatusInput{
				Host:       "codex",
				SessionID:  "session-invalid-" + strings.ReplaceAll(tt.name, " ", "-"),
				RootTurnID: "turn-invalid-" + strings.ReplaceAll(tt.name, " ", "-"),
			}
			_, err := service.RecordCheckpoint(CheckpointRecordInput{
				Host:        identity.Host,
				SessionID:   identity.SessionID,
				RootTurnID:  identity.RootTurnID,
				Disposition: store.CheckpointDispositionSaved,
				Project:     "engram",
				MemoryIDs:   tt.memoryIDs,
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if got := CheckpointErrorCode(err); got != tt.wantCode {
				t.Fatalf("error code = %q, want %q", got, tt.wantCode)
			}
			if _, statusErr := service.CheckpointStatus(identity); !errors.Is(statusErr, store.ErrCheckpointNotFound) {
				t.Fatalf("checkpoint changed after rejection: %v", statusErr)
			}
		})
	}
}

func TestSavedCheckpointRollsBackCreatedMemoriesAndCheckpointOnReferenceFailure(t *testing.T) {
	service := newTestService(t)
	if _, err := service.store.DB().Exec(`
		CREATE TRIGGER fail_checkpoint_reference
		BEFORE INSERT ON memory_checkpoint_references
		BEGIN
			SELECT RAISE(ABORT, 'injected checkpoint reference failure');
		END;`); err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}

	_, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-atomic-rollback",
		RootTurnID:  "turn-atomic-rollback",
		Disposition: store.CheckpointDispositionSaved,
		Project:     "engram",
		CWD:         "/work/engram",
		Memories: []CheckpointMemoryInput{
			{Type: "decision", Title: "Atomic canary one", Content: "The first created Memory must roll back."},
			{Type: "discovery", Title: "Atomic canary two", Content: "The second created Memory must roll back."},
		},
	})
	if err == nil {
		t.Fatal("record saved checkpoint succeeded despite injected reference failure")
	}

	for table, want := range map[string]int{
		"sessions":                     0,
		"observations":                 0,
		"memory_checkpoints":           0,
		"memory_checkpoint_references": 0,
		"sync_mutations":               0,
	} {
		var got int
		if queryErr := service.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); queryErr != nil {
			t.Fatalf("count %s: %v", table, queryErr)
		}
		if got != want {
			t.Fatalf("%s rows = %d, want %d after rollback", table, got, want)
		}
	}
}

func TestSavedCheckpointCreatesMultipleMemoriesReplaysAndReopens(t *testing.T) {
	cfg, err := store.DefaultConfig()
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	cfg.DataDir = t.TempDir()
	firstStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	service := New(firstStore)
	existing := saveObservation(t, service, "engram", "Existing Memory", "This Memory was saved earlier in the turn.")
	input := CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-saved-create",
		RootTurnID:  "turn-saved-create",
		Disposition: store.CheckpointDispositionSaved,
		Project:     "ENGRAM",
		CWD:         "/work/engram",
		MemoryIDs:   []int64{existing.ID},
		Memories: []CheckpointMemoryInput{
			{Type: "decision", Title: "Created decision", Content: "Create this decision inside finalization."},
			{Type: "discovery", Title: "Created discovery", Content: "Create this discovery inside finalization."},
		},
	}

	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record saved checkpoint: %v", err)
	}
	if len(created.Checkpoint.References) != 3 {
		t.Fatalf("references = %#v, want three", created.Checkpoint.References)
	}
	if created.Checkpoint.References[0].MemoryID != existing.ID {
		t.Fatalf("first reference = %#v, want existing Memory", created.Checkpoint.References[0])
	}
	for _, reference := range created.Checkpoint.References {
		if reference.Kind != store.CheckpointReferenceKindMemory || reference.Project != "engram" {
			t.Fatalf("reference = %#v", reference)
		}
		if _, getErr := service.Get(reference.MemoryID); getErr != nil {
			t.Fatalf("get referenced Memory %d: %v", reference.MemoryID, getErr)
		}
	}

	var memoriesBeforeReplay, mutationsBeforeReplay int
	if err := firstStore.DB().QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&memoriesBeforeReplay); err != nil {
		t.Fatalf("count Memories: %v", err)
	}
	if err := firstStore.DB().QueryRow(`SELECT COUNT(*) FROM sync_mutations`).Scan(&mutationsBeforeReplay); err != nil {
		t.Fatalf("count sync mutations: %v", err)
	}
	replayed, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID,
		Disposition: store.CheckpointDispositionSaved,
	})
	if err != nil {
		t.Fatalf("replay saved checkpoint: %v", err)
	}
	if replayed.Idempotency != CheckpointIdempotencyAlreadyRecorded || !reflect.DeepEqual(replayed.Checkpoint, created.Checkpoint) {
		t.Fatalf("replayed result = %#v, want original %#v", replayed, created)
	}
	var memoriesAfterReplay, mutationsAfterReplay int
	if err := firstStore.DB().QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&memoriesAfterReplay); err != nil {
		t.Fatalf("count Memories after replay: %v", err)
	}
	if err := firstStore.DB().QueryRow(`SELECT COUNT(*) FROM sync_mutations`).Scan(&mutationsAfterReplay); err != nil {
		t.Fatalf("count sync mutations after replay: %v", err)
	}
	if memoriesAfterReplay != memoriesBeforeReplay || mutationsAfterReplay != mutationsBeforeReplay {
		t.Fatalf("replay changed state: Memories %d->%d, mutations %d->%d", memoriesBeforeReplay, memoriesAfterReplay, mutationsBeforeReplay, mutationsAfterReplay)
	}

	if err := firstStore.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopenedStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	status, err := New(reopenedStore).CheckpointStatus(CheckpointStatusInput{
		Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID,
	})
	if err != nil {
		t.Fatalf("status after reopen: %v", err)
	}
	if !reflect.DeepEqual(status.Checkpoint, created.Checkpoint) {
		t.Fatalf("reopened checkpoint = %#v, want %#v", status.Checkpoint, created.Checkpoint)
	}
}

func TestSavedCheckpointRejectsIllegalTerminalTransitions(t *testing.T) {
	service := newTestService(t)
	memory := saveObservation(t, service, "engram", "Transition Memory", "Use this Memory to exercise terminal transitions.")

	skippedIdentity := CheckpointRecordInput{
		Host: "codex", SessionID: "session-skipped-first", RootTurnID: "turn-skipped-first",
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	}
	if _, err := service.RecordCheckpoint(skippedIdentity); err != nil {
		t.Fatalf("record skipped checkpoint: %v", err)
	}
	_, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: skippedIdentity.Host, SessionID: skippedIdentity.SessionID, RootTurnID: skippedIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionSaved, Project: "engram", MemoryIDs: []int64{memory.ID},
	})
	if !errors.Is(err, store.ErrCheckpointConflict) {
		t.Fatalf("saved after skipped error = %v, want conflict", err)
	}
	_, err = service.RecordCheckpoint(CheckpointRecordInput{
		Host: skippedIdentity.Host, SessionID: skippedIdentity.SessionID, RootTurnID: skippedIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionNeedsReview, Project: "engram", Proposal: validCheckpointProposal(),
	})
	if !errors.Is(err, store.ErrCheckpointConflict) {
		t.Fatalf("needs_review after skipped error = %v, want conflict", err)
	}

	savedIdentity := CheckpointRecordInput{
		Host: "codex", SessionID: "session-saved-first", RootTurnID: "turn-saved-first",
		Disposition: store.CheckpointDispositionSaved, Project: "engram", MemoryIDs: []int64{memory.ID},
	}
	if _, err := service.RecordCheckpoint(savedIdentity); err != nil {
		t.Fatalf("record saved checkpoint: %v", err)
	}
	_, err = service.RecordCheckpoint(CheckpointRecordInput{
		Host: savedIdentity.Host, SessionID: savedIdentity.SessionID, RootTurnID: savedIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	})
	if !errors.Is(err, store.ErrCheckpointConflict) {
		t.Fatalf("skipped after saved error = %v, want conflict", err)
	}
	_, err = service.RecordCheckpoint(CheckpointRecordInput{
		Host: savedIdentity.Host, SessionID: savedIdentity.SessionID, RootTurnID: savedIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionNeedsReview, Project: "engram", Proposal: validCheckpointProposal(),
	})
	if !errors.Is(err, store.ErrCheckpointConflict) {
		t.Fatalf("needs_review after saved error = %v, want conflict", err)
	}

	needsReviewIdentity := CheckpointRecordInput{
		Host: "codex", SessionID: "session-needs-review-first", RootTurnID: "turn-needs-review-first",
		Disposition: store.CheckpointDispositionNeedsReview, Project: "engram", Proposal: validCheckpointProposal(),
	}
	if _, err := service.RecordCheckpoint(needsReviewIdentity); err != nil {
		t.Fatalf("record needs-review checkpoint: %v", err)
	}
	for _, next := range []CheckpointRecordInput{
		{
			Host: needsReviewIdentity.Host, SessionID: needsReviewIdentity.SessionID, RootTurnID: needsReviewIdentity.RootTurnID,
			Disposition: store.CheckpointDispositionSaved, Project: "engram", MemoryIDs: []int64{memory.ID},
		},
		{
			Host: needsReviewIdentity.Host, SessionID: needsReviewIdentity.SessionID, RootTurnID: needsReviewIdentity.RootTurnID,
			Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
		},
	} {
		if _, err := service.RecordCheckpoint(next); !errors.Is(err, store.ErrCheckpointConflict) {
			t.Fatalf("transition after needs_review error = %v, want conflict", err)
		}
	}
}

func validCheckpointProposal() *CheckpointProposalInput {
	return &CheckpointProposalInput{
		Title: "Transition proposal", Content: "Use this proposal to exercise transitions.",
	}
}

func TestNeedsReviewCheckpointCreatesOneInspectableProposalWithoutPromotion(t *testing.T) {
	service := newTestService(t)
	input := CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-needs-review-create",
		RootTurnID:  "turn-needs-review-create",
		Disposition: store.CheckpointDispositionNeedsReview,
		Project:     "engram",
		Proposal: &CheckpointProposalInput{
			Title:   "Review checkpoint ownership",
			Content: "The checkpoint may own local audit evidence.",
		},
	}

	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record needs-review checkpoint: %v", err)
	}
	if created.Idempotency != CheckpointIdempotencyCreated || created.Checkpoint == nil ||
		created.Checkpoint.Disposition != store.CheckpointDispositionNeedsReview ||
		len(created.Checkpoint.References) != 0 || created.Checkpoint.Proposal == nil {
		t.Fatalf("created result = %#v", created)
	}
	proposal := created.Checkpoint.Proposal
	if proposal.ID == "" || proposal.Project != "engram" || proposal.Title != input.Proposal.Title ||
		proposal.Content != input.Proposal.Content || proposal.CreatedAt == "" {
		t.Fatalf("proposal snapshot = %#v", proposal)
	}

	for _, table := range []string{"observations", "sync_mutations"} {
		var count int
		if err := service.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s rows = %d, want 0", table, count)
		}
	}
}

func TestNeedsReviewCheckpointReplaysOriginalSnapshotWithoutPayload(t *testing.T) {
	service := newTestService(t)
	input := CheckpointRecordInput{
		Host: "codex", SessionID: "session-needs-review-existing", RootTurnID: "turn-needs-review-existing",
		Disposition: store.CheckpointDispositionNeedsReview, Project: "engram",
		Proposal: &CheckpointProposalInput{Title: "Original proposal", Content: "Replay must preserve this snapshot."},
	}

	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record needs-review checkpoint: %v", err)
	}
	if created.Checkpoint.Proposal == nil {
		t.Fatalf("created checkpoint = %#v", created.Checkpoint)
	}
	wantProposal := *created.Checkpoint.Proposal

	replayed, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID,
		Disposition: store.CheckpointDispositionNeedsReview,
	})
	if err != nil {
		t.Fatalf("replay needs-review checkpoint: %v", err)
	}
	if replayed.Idempotency != CheckpointIdempotencyAlreadyRecorded ||
		!reflect.DeepEqual(replayed.Checkpoint, created.Checkpoint) ||
		!reflect.DeepEqual(replayed.Checkpoint.Proposal, &wantProposal) {
		t.Fatalf("replayed result = %#v, want original %#v", replayed, created)
	}
}

func TestNeedsReviewCheckpointRejectsInvalidReferencesWithoutChangingState(t *testing.T) {
	service := newTestService(t)
	validInline := &CheckpointProposalInput{Title: "Inline proposal", Content: "Review this proposal."}
	tests := []struct {
		name     string
		project  string
		proposal *CheckpointProposalInput
		reason   string
	}{
		{name: "empty", project: "engram"},
		{name: "missing project", proposal: validInline},
		{name: "missing title", project: "engram", proposal: &CheckpointProposalInput{Content: "Missing title."}},
		{name: "missing content", project: "engram", proposal: &CheckpointProposalInput{Title: "Missing content"}},
		{name: "reason is forbidden", project: "engram", proposal: validInline, reason: store.CheckpointSkipReasonNoDurableKnowledge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity := CheckpointStatusInput{
				Host: "codex", SessionID: "session-invalid-" + strings.ReplaceAll(tt.name, " ", "-"),
				RootTurnID: "turn-invalid-" + strings.ReplaceAll(tt.name, " ", "-"),
			}
			_, err := service.RecordCheckpoint(CheckpointRecordInput{
				Host: identity.Host, SessionID: identity.SessionID, RootTurnID: identity.RootTurnID,
				Disposition: store.CheckpointDispositionNeedsReview, Project: tt.project,
				Proposal: tt.proposal, ReasonCode: tt.reason,
			})
			if !errors.Is(err, store.ErrCheckpointInvalidReferences) {
				t.Fatalf("error = %v, want ErrCheckpointInvalidReferences", err)
			}
			if got := CheckpointErrorCode(err); got != CheckpointErrorCodeInvalidReferences {
				t.Fatalf("error code = %q, want %q", got, CheckpointErrorCodeInvalidReferences)
			}
			if _, statusErr := service.CheckpointStatus(identity); !errors.Is(statusErr, store.ErrCheckpointNotFound) {
				t.Fatalf("checkpoint changed after rejection: %v", statusErr)
			}
		})
	}
	var proposals int
	if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM memory_proposals`).Scan(&proposals); err != nil || proposals != 0 {
		t.Fatalf("proposal rows after rejection = %d, err = %v", proposals, err)
	}
}

func TestCheckpointRejectsProposalFieldsOutsideNeedsReview(t *testing.T) {
	service := newTestService(t)
	proposal := validCheckpointProposal()
	for _, tt := range []struct {
		name  string
		input CheckpointRecordInput
	}{
		{
			name: "skipped inline proposal",
			input: CheckpointRecordInput{
				Disposition: store.CheckpointDispositionSkipped,
				ReasonCode:  store.CheckpointSkipReasonNoDurableKnowledge,
				Proposal:    proposal,
			},
		},
		{
			name: "saved inline proposal",
			input: CheckpointRecordInput{
				Disposition: store.CheckpointDispositionSaved,
				Project:     "engram",
				MemoryIDs:   []int64{1},
				Proposal:    proposal,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.input.Host = "codex"
			tt.input.SessionID = "session-" + strings.ReplaceAll(tt.name, " ", "-")
			tt.input.RootTurnID = "turn-" + strings.ReplaceAll(tt.name, " ", "-")
			_, err := service.RecordCheckpoint(tt.input)
			if !errors.Is(err, store.ErrCheckpointInvalidReferences) {
				t.Fatalf("error = %v, want ErrCheckpointInvalidReferences", err)
			}
			if got := CheckpointErrorCode(err); got != CheckpointErrorCodeInvalidReferences {
				t.Fatalf("error code = %q, want %q", got, CheckpointErrorCodeInvalidReferences)
			}
			if _, statusErr := service.CheckpointStatus(CheckpointStatusInput{
				Host: tt.input.Host, SessionID: tt.input.SessionID, RootTurnID: tt.input.RootTurnID,
			}); !errors.Is(statusErr, store.ErrCheckpointNotFound) {
				t.Fatalf("checkpoint changed after rejection: %v", statusErr)
			}
		})
	}
}

func TestNeedsReviewCheckpointRollsBackCreatedProposalAndCheckpointOnReferenceFailure(t *testing.T) {
	service := newTestService(t)
	if _, err := service.store.DB().Exec(`
		CREATE TRIGGER fail_checkpoint_proposal_reference
		BEFORE INSERT ON memory_checkpoint_proposal_references
		BEGIN
			SELECT RAISE(ABORT, 'injected checkpoint proposal reference failure');
		END;`); err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}

	_, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: "codex", SessionID: "session-proposal-rollback", RootTurnID: "turn-proposal-rollback",
		Disposition: store.CheckpointDispositionNeedsReview, Project: "engram",
		Proposal: &CheckpointProposalInput{
			Title: "Atomic proposal canary", Content: "This proposal must roll back.",
		},
	})
	if err == nil {
		t.Fatal("record needs-review checkpoint succeeded despite injected reference failure")
	}
	for table, want := range map[string]int{
		"memory_proposals":                      0,
		"memory_checkpoints":                    0,
		"memory_checkpoint_proposal_references": 0,
		"observations":                          0,
		"sync_mutations":                        0,
	} {
		var got int
		if queryErr := service.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); queryErr != nil {
			t.Fatalf("count %s: %v", table, queryErr)
		}
		if got != want {
			t.Fatalf("%s rows = %d, want %d after rollback", table, got, want)
		}
	}
}

func TestNeedsReviewCheckpointAndProposalPersistAcrossReopen(t *testing.T) {
	cfg, err := store.DefaultConfig()
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	cfg.DataDir = t.TempDir()
	firstStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	input := CheckpointRecordInput{
		Host: "codex", SessionID: "session-proposal-reopen", RootTurnID: "turn-proposal-reopen",
		Disposition: store.CheckpointDispositionNeedsReview, Project: "engram",
		Proposal: &CheckpointProposalInput{
			Title: "Reopen proposal", Content: "This proposal must survive reopen.",
		},
	}
	created, err := New(firstStore).RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record needs-review checkpoint: %v", err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopenedStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	status, err := New(reopenedStore).CheckpointStatus(CheckpointStatusInput{
		Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID,
	})
	if err != nil {
		t.Fatalf("status after reopen: %v", err)
	}
	if !reflect.DeepEqual(status.Checkpoint, created.Checkpoint) {
		t.Fatalf("reopened checkpoint = %#v, want %#v", status.Checkpoint, created.Checkpoint)
	}
	if status.Checkpoint.Proposal == nil || status.Checkpoint.Proposal.Title != input.Proposal.Title ||
		status.Checkpoint.Proposal.Content != input.Proposal.Content {
		t.Fatalf("reopened proposal = %#v", status.Checkpoint.Proposal)
	}
}
