package store

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestCheckpointSupersessionRetainsDirectedHistoryAndReplays(t *testing.T) {
	s := newTestStore(t)
	initial, _, err := s.RecordSavedCheckpoint(RecordSavedCheckpointParams{Identity: CheckpointIdentity{"codex", "supersession-session", "diagnosis"}, Project: "test", Memories: []AddObservationParams{{Title: "Issue diagnosis", Content: "The failure reproduces at commit abc."}}})
	if err != nil {
		t.Fatal(err)
	}
	oldID := initial.References[0].MemoryID
	old, version, err := s.CheckpointSupersessionTarget(oldID)
	if err != nil {
		t.Fatal(err)
	}
	index := 0
	params := RecordSavedCheckpointParams{Identity: CheckpointIdentity{"codex", "supersession-session", "resolution"}, Project: "test", Memories: []AddObservationParams{{Title: "Verified resolution", Content: "Commit def fixes the issue."}}, Supersessions: []CheckpointSupersession{{ReplacementInputIndex: &index, TargetMemoryID: oldID, TargetVersion: version, Reason: "Verified at def"}}}
	result, replay, err := s.RecordSavedCheckpoint(params)
	if err != nil || replay {
		t.Fatalf("record: %v replay %v", err, replay)
	}
	current, err := s.GetObservation(oldID)
	if err != nil || current.Content != old.Content {
		t.Fatalf("historical content changed: %#v %v", current, err)
	}
	relations, err := s.GetRelationsForObservations([]string{old.SyncID})
	if err != nil {
		t.Fatal(err)
	}
	incoming := relations[old.SyncID].AsTarget
	if len(incoming) != 1 || incoming[0].Relation != RelationSupersedes || incoming[0].SourceID != result.References[0].MemorySyncID {
		t.Fatalf("incoming: %#v", incoming)
	}
	params.Supersessions[0].TargetVersion = "stale retry payload"
	again, replay, err := s.RecordSavedCheckpoint(params)
	if err != nil || !replay || again.References[0] != result.References[0] {
		t.Fatalf("replay: %#v %v %v", again, replay, err)
	}
}

func TestCheckpointSupersessionRejectsReverseRelationWithoutMutation(t *testing.T) {
	s := newTestStore(t)
	oldID, newID, params := supersessionFixture(t, s)
	old, _ := s.GetObservation(oldID)
	replacement, _ := s.GetObservation(newID)
	if _, err := s.JudgeBySemantic(JudgeBySemanticParams{SourceID: old.SyncID, TargetID: replacement.SyncID, Relation: RelationRelated, Confidence: 1, Reasoning: "Historical association"}); err != nil {
		t.Fatal(err)
	}
	_, version, err := s.CheckpointSupersessionTarget(oldID)
	if err != nil {
		t.Fatal(err)
	}
	params.Supersessions[0].TargetVersion = version
	_, _, err = s.RecordSavedCheckpoint(params)
	if !errors.Is(err, ErrCheckpointInvalidSupersession) {
		t.Fatalf("reverse relation error %v", err)
	}
	if _, err := s.GetMemoryCheckpoint(params.Identity); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("checkpoint error %v", err)
	}
	relations, _ := s.GetRelationsForObservations([]string{old.SyncID})
	if len(relations[old.SyncID].AsSource) != 1 || relations[old.SyncID].AsSource[0].Relation != RelationRelated {
		t.Fatalf("changed relation %#v", relations)
	}
}

func supersessionFixture(t *testing.T, s *Store) (int64, int64, RecordSavedCheckpointParams) {
	t.Helper()
	initial, _, err := s.RecordSavedCheckpoint(RecordSavedCheckpointParams{Identity: CheckpointIdentity{"codex", "fixture-session", "initial"}, Project: "test", Memories: []AddObservationParams{{Title: "Synthetic cache diagnosis", Content: "The cache failure reproduces at commit abc."}, {Title: "Synthetic cache resolution", Content: "The cache failure was fixed at commit def."}}})
	if err != nil {
		t.Fatal(err)
	}
	oldID, newID := initial.References[0].MemoryID, initial.References[1].MemoryID
	_, version, err := s.CheckpointSupersessionTarget(oldID)
	if err != nil {
		t.Fatal(err)
	}
	return oldID, newID, RecordSavedCheckpointParams{Identity: CheckpointIdentity{"codex", "fixture-session", "replacement"}, Project: "test", MemoryIDs: []int64{newID}, Supersessions: []CheckpointSupersession{{ReplacementMemoryID: newID, TargetMemoryID: oldID, TargetVersion: version, Reason: "Verified fix at def"}}}
}

func TestCheckpointSupersessionRejectsInvalidAndStaleDeclarationsAtomically(t *testing.T) {
	tests := []struct {
		name   string
		change func(*testing.T, *Store, int64, int64, *RecordSavedCheckpointParams)
		want   error
	}{
		{"missing reason", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			p.Supersessions[0].Reason = " "
		}, ErrCheckpointInvalidSupersession},
		{"missing version", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			p.Supersessions[0].TargetVersion = ""
		}, ErrCheckpointInvalidSupersession},
		{"both selectors", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			i := 0
			p.Supersessions[0].ReplacementInputIndex = &i
		}, ErrCheckpointInvalidSupersession},
		{"out of range index", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			i := 1
			p.Supersessions[0].ReplacementInputIndex = &i
			p.Supersessions[0].ReplacementMemoryID = 0
		}, ErrCheckpointInvalidSupersession},
		{"uncommitted replacement", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			p.MemoryIDs = []int64{oldID}
		}, ErrCheckpointInvalidSupersession},
		{"self reference", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			p.MemoryIDs = []int64{oldID}
			p.Supersessions[0].ReplacementMemoryID = oldID
		}, ErrCheckpointInvalidSupersession},
		{"missing target", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			p.Supersessions[0].TargetMemoryID = 999999
		}, ErrCheckpointMemoryNotFound},
		{"duplicate target", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			p.Supersessions = append(p.Supersessions, p.Supersessions[0])
		}, ErrCheckpointInvalidSupersession},
		{"target content changed", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			content := "An amended diagnosis at abc"
			if _, err := s.UpdateObservation(oldID, UpdateObservationParams{Content: &content}); err != nil {
				t.Fatal(err)
			}
		}, ErrCheckpointSupersessionStale},
		{"target pin changed", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			if err := s.PinObservation(oldID); err != nil {
				t.Fatal(err)
			}
		}, ErrCheckpointSupersessionStale},
		{"target relation changed", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			old, _ := s.GetObservation(oldID)
			replacement, _ := s.GetObservation(newID)
			if _, err := s.JudgeBySemantic(JudgeBySemanticParams{SourceID: replacement.SyncID, TargetID: old.SyncID, Relation: RelationCompatible, Confidence: 1, Reasoning: "Independent scope"}); err != nil {
				t.Fatal(err)
			}
		}, ErrCheckpointSupersessionStale},
		{"target deleted", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			if err := s.DeleteObservation(oldID, false); err != nil {
				t.Fatal(err)
			}
		}, ErrCheckpointMemoryNotFound},
		{"replacement deleted", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			if err := s.DeleteObservation(newID, false); err != nil {
				t.Fatal(err)
			}
		}, ErrCheckpointMemoryNotFound},
		{"target other project", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			other := supersessionOtherProjectMemory(t, s)
			p.Supersessions[0].TargetMemoryID = other
			_, version, err := s.CheckpointSupersessionTarget(other)
			if err != nil {
				t.Fatal(err)
			}
			p.Supersessions[0].TargetVersion = version
		}, ErrCheckpointProjectMismatch},
		{"replacement other project", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			other := supersessionOtherProjectMemory(t, s)
			p.MemoryIDs = []int64{other}
			p.Supersessions[0].ReplacementMemoryID = other
		}, ErrCheckpointProjectMismatch},
		{"inline duplicate is target", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			old, _ := s.GetObservation(oldID)
			p.Memories = []AddObservationParams{{Title: old.Title, Content: old.Content}}
			p.MemoryIDs = nil
			i := 0
			p.Supersessions[0].ReplacementMemoryID = 0
			p.Supersessions[0].ReplacementInputIndex = &i
		}, ErrCheckpointInvalidSupersession},
		{"topic overwrite", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			topic := "cache-state"
			if _, err := s.UpdateObservation(oldID, UpdateObservationParams{TopicKey: &topic}); err != nil {
				t.Fatal(err)
			}
			_, v, err := s.CheckpointSupersessionTarget(oldID)
			if err != nil {
				t.Fatal(err)
			}
			p.Supersessions[0].TargetVersion = v
			p.Memories = []AddObservationParams{{Title: "New diagnosis", Content: "New verified content", TopicKey: topic}}
		}, ErrCheckpointInvalidSupersession},
		{"cyclic declarations", func(t *testing.T, s *Store, oldID, newID int64, p *RecordSavedCheckpointParams) {
			_, v, err := s.CheckpointSupersessionTarget(newID)
			if err != nil {
				t.Fatal(err)
			}
			p.MemoryIDs = append(p.MemoryIDs, oldID)
			p.Supersessions = append(p.Supersessions, CheckpointSupersession{ReplacementMemoryID: oldID, TargetMemoryID: newID, TargetVersion: v, Reason: "Reverse cycle"})
		}, ErrCheckpointInvalidSupersession},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			oldID, newID, p := supersessionFixture(t, s)
			tt.change(t, s, oldID, newID, &p)
			before, err := s.Export()
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = s.RecordSavedCheckpoint(p)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error %v want %v", err, tt.want)
			}
			after, err := s.Export()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("rejection changed persisted Memory or relation state")
			}
			if _, err := s.GetMemoryCheckpoint(p.Identity); !errors.Is(err, ErrCheckpointNotFound) {
				t.Fatalf("checkpoint after rejection %v", err)
			}
		})
	}
}

func supersessionOtherProjectMemory(t *testing.T, s *Store) int64 {
	t.Helper()
	checkpoint, _, err := s.RecordSavedCheckpoint(RecordSavedCheckpointParams{Identity: CheckpointIdentity{"codex", "other-session", "other-turn"}, Project: "other", Memories: []AddObservationParams{{Title: "Other project", Content: "Independent project knowledge"}}})
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint.References[0].MemoryID
}

func TestCheckpointSupersessionRollsBackEveryAtomicWriteBoundary(t *testing.T) {
	for _, disposition := range []string{CheckpointDispositionSaved, CheckpointDispositionNeedsReview} {
		t.Run(disposition, func(t *testing.T) {
			for _, boundary := range []string{"relation", "checkpoint", "reference", "session sync", "memory sync", "relation sync", "proposal reference", "commit"} {
				t.Run(boundary, func(t *testing.T) {
					if disposition == CheckpointDispositionSaved && boundary == "proposal reference" {
						t.Skip("saved has no proposal")
					}
					s := newTestStore(t)
					oldID, _, p := supersessionFixture(t, s)
					if err := s.EnrollProject("test"); err != nil {
						t.Fatal(err)
					}
					index := 0
					p.MemoryIDs = nil
					p.Memories = []AddObservationParams{{Title: "Atomic final resolution", Content: "Cache coherence is fixed by validating the generation."}}
					p.Supersessions[0].ReplacementMemoryID = 0
					p.Supersessions[0].ReplacementInputIndex = &index
					before, err := s.Export()
					if err != nil {
						t.Fatal(err)
					}
					beforeSync, err := s.ListPendingSyncMutations(DefaultSyncTargetKey, 1000)
					if err != nil {
						t.Fatal(err)
					}
					injected := errors.New("injected atomic supersession failure")
					fired := false
					exec := s.hooks.exec
					commit := s.hooks.commit
					s.hooks.exec = func(db execer, query string, args ...any) (sql.Result, error) {
						hit := boundary == "relation" && strings.Contains(query, "INSERT INTO memory_relations") || boundary == "checkpoint" && strings.Contains(query, "INSERT INTO memory_checkpoints (") || boundary == "reference" && strings.Contains(query, "INSERT INTO memory_checkpoint_references") || boundary == "proposal reference" && strings.Contains(query, "INSERT INTO memory_checkpoint_proposal_references")
						if strings.Contains(query, "INSERT INTO sync_mutations") && len(args) > 1 {
							entity, _ := args[1].(string)
							hit = hit || boundary == "session sync" && entity == SyncEntitySession || boundary == "memory sync" && entity == SyncEntityObservation || boundary == "relation sync" && entity == SyncEntityRelation
						}
						if hit {
							fired = true
							return nil, injected
						}
						return exec(db, query, args...)
					}
					if boundary == "commit" {
						s.hooks.commit = func(tx *sql.Tx) error { fired = true; return injected }
					}
					record := func() error {
						if disposition == CheckpointDispositionSaved {
							_, _, err := s.RecordSavedCheckpoint(p)
							return err
						}
						_, _, err := s.RecordNeedsReviewCheckpoint(RecordNeedsReviewCheckpointParams{Identity: p.Identity, Project: p.Project, MemoryIDs: p.MemoryIDs, Memories: p.Memories, Supersessions: p.Supersessions, Proposal: &MemoryProposalInput{Title: "Unresolved scope", Content: "Architecture applicability still needs review."}})
						return err
					}
					err = record()
					s.hooks.exec = exec
					s.hooks.commit = commit
					if !fired || !errors.Is(err, injected) {
						t.Fatalf("failure fired %v error %v", fired, err)
					}
					after, err := s.Export()
					if err != nil {
						t.Fatal(err)
					}
					afterSync, err := s.ListPendingSyncMutations(DefaultSyncTargetKey, 1000)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(beforeSync, afterSync) {
						t.Fatal("failed checkpoint left partial Memory/relation/sync state")
					}
					if _, err := s.GetMemoryCheckpoint(p.Identity); !errors.Is(err, ErrCheckpointNotFound) {
						t.Fatalf("failed checkpoint exists: %v", err)
					}
					var proposals int
					if err := s.DB().QueryRow(`SELECT COUNT(*) FROM memory_proposals`).Scan(&proposals); err != nil || proposals != 0 {
						t.Fatalf("orphan proposal count %d: %v", proposals, err)
					}
					if err := record(); err != nil {
						t.Fatalf("retry failed: %v", err)
					}
					target, err := s.GetObservation(oldID)
					if err != nil {
						t.Fatal(err)
					}
					relations, err := s.GetRelationsForObservations([]string{target.SyncID})
					if err != nil || len(relations[target.SyncID].AsTarget) != 1 {
						t.Fatalf("retry relations %#v %v", relations, err)
					}
				})
			}
		})
	}
}

func TestCheckpointSupersessionReferenceAndDuplicateReusePreserveRecallAndMixedProposal(t *testing.T) {
	for _, mode := range []string{"reference", "inline duplicate", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			s := newTestStore(t)
			oldID, newID, p := supersessionFixture(t, s)
			before, err := s.RecallCandidatesContext(context.Background(), "cache", SearchOptions{Project: "test", Limit: 10})
			if err != nil || len(before) != 2 {
				t.Fatalf("prose alone Recall %#v %v", before, err)
			}
			if mode == "inline duplicate" {
				replacement, _ := s.GetObservation(newID)
				p.MemoryIDs = nil
				p.Memories = []AddObservationParams{{Title: replacement.Title, Content: replacement.Content}}
				i := 0
				p.Supersessions[0].ReplacementMemoryID = 0
				p.Supersessions[0].ReplacementInputIndex = &i
			}
			var result *MemoryCheckpoint
			if mode == "mixed" {
				result, _, err = s.RecordNeedsReviewCheckpoint(RecordNeedsReviewCheckpointParams{Identity: p.Identity, Project: p.Project, MemoryIDs: p.MemoryIDs, Supersessions: p.Supersessions, Proposal: &MemoryProposalInput{Title: "Unresolved cache scope", Content: "Applicability to distributed cache remains uncertain."}})
			} else {
				result, _, err = s.RecordSavedCheckpoint(p)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(result.References) != 1 || result.References[0].MemoryID != newID {
				t.Fatalf("replacement not reused %#v", result)
			}
			after, err := s.RecallCandidatesContext(context.Background(), "cache", SearchOptions{Project: "test", Limit: 10})
			if err != nil || len(after) != 1 || after[0].ID != newID {
				t.Fatalf("explicit Recall %#v %v", after, err)
			}
			historical, err := s.GetObservation(oldID)
			if err != nil || historical.Content != "The cache failure reproduces at commit abc." {
				t.Fatalf("history %#v %v", historical, err)
			}
			if mode == "mixed" && (result.Proposal == nil || result.Disposition != CheckpointDispositionNeedsReview) {
				t.Fatalf("mixed result %#v", result)
			}
			all, err := s.AllObservations("test", "project", 10)
			if err != nil || len(all) != 2 {
				t.Fatalf("duplicate/proposal entered Memory %#v %v", all, err)
			}
			_, _, err = s.RecordSkippedCheckpoint(RecordSkippedCheckpointParams{Identity: p.Identity, ReasonCode: CheckpointSkipReasonNoDurableKnowledge})
			if !errors.Is(err, ErrCheckpointConflict) {
				t.Fatalf("changed disposition error %v", err)
			}
		})
	}
}

func TestCheckpointSupersessionResponseLossReopenReturnsOriginalWithoutReapplying(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	oldID, _, p := supersessionFixture(t, s)
	original, _, err := s.RecordSavedCheckpoint(p)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.Export()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p.Supersessions[0] = CheckpointSupersession{TargetMemoryID: 999999, TargetVersion: "invalid after response loss"}
	replayed, already, err := s.RecordSavedCheckpoint(p)
	if err != nil || !already || !reflect.DeepEqual(original, replayed) {
		t.Fatalf("response-loss replay %#v %v %v", replayed, already, err)
	}
	after, err := s.Export()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("response-loss replay changed Memory or relation state")
	}
	old, err := s.GetObservation(oldID)
	if err != nil {
		t.Fatal(err)
	}
	relations, err := s.GetRelationsForObservations([]string{old.SyncID})
	if err != nil || len(relations[old.SyncID].AsTarget) != 1 {
		t.Fatalf("replayed relation %#v %v", relations, err)
	}
}
