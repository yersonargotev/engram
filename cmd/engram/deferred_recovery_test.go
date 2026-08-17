package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestCmdConflictsDeferred_RecoverRejectsInvalidArgumentsBeforeOpeningStore(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing id", args: []string{"--recover"}},
		{name: "empty id", args: []string{"--recover", ""}},
		{name: "inspect", args: []string{"--recover", "rel-1", "--inspect", "rel-1"}},
		{name: "replay", args: []string{"--recover", "rel-1", "--replay"}},
		{name: "status", args: []string{"--recover", "rel-1", "--status", "dead"}},
		{name: "limit", args: []string{"--recover", "rel-1", "--limit", "1"}},
		{name: "unknown flag", args: []string{"--recover", "rel-1", "--unknown"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opened := false
			originalStoreNew := storeNew
			storeNew = func(store.Config) (*store.Store, error) {
				opened = true
				return nil, errors.New("store must not open")
			}
			t.Cleanup(func() { storeNew = originalStoreNew })

			args := append([]string{"engram", "conflicts", "deferred"}, tc.args...)
			args = append(args, "--json")
			withArgs(t, args...)
			stubExitWithPanic(t)
			_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConflicts(store.Config{}) })
			if recovered == nil {
				t.Fatal("expected invalid arguments to exit non-zero")
			}
			if opened {
				t.Fatal("store was opened before argument validation")
			}
			var envelope cliErrorEnvelope
			if err := json.Unmarshal([]byte(stderr), &envelope); err != nil {
				t.Fatalf("decode JSON error %q: %v", stderr, err)
			}
			if envelope.Code != "invalid_arguments" {
				t.Fatalf("code = %q, want invalid_arguments", envelope.Code)
			}
		})
	}
}

func seedRecoverableDeferredCLI(t *testing.T, status string) (store.Config, string) {
	t.Helper()
	cfg := testConfig(t)
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	if err := s.CreateSession("ses-recover-cli", "recover-cli", "/tmp/recover-cli"); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	ids := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		id, err := s.AddObservation(store.AddObservationParams{
			SessionID: "ses-recover-cli",
			Type:      "decision",
			Title:     fmt.Sprintf("Recovery observation %d", i),
			Content:   "recovery CLI fixture",
			Project:   "recover-cli",
			Scope:     "project",
		})
		if err != nil {
			t.Fatalf("AddObservation: %v", err)
		}
		var syncID string
		if err := s.DB().QueryRow(`SELECT sync_id FROM observations WHERE id = ?`, id).Scan(&syncID); err != nil {
			t.Fatalf("read observation sync id: %v", err)
		}
		ids = append(ids, syncID)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close fixture store: %v", err)
	}

	syncID := "rel-recover-cli"
	payload, err := json.Marshal(map[string]any{
		"sync_id":         syncID,
		"source_id":       ids[0],
		"target_id":       ids[1],
		"relation":        "related",
		"judgment_status": "judged",
		"project":         "recover-cli",
		"created_at":      "2026-08-16T10:00:00Z",
		"updated_at":      "2026-08-16T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal recovery payload: %v", err)
	}
	db := openTestDB(t, cfg)
	seedDeferredRowCLI(t, db, syncID, string(payload), 8, status)
	if _, err := db.Exec(`UPDATE sync_apply_deferred SET last_error = 'historical error' WHERE sync_id = ?`, syncID); err != nil {
		t.Fatalf("seed recovery history: %v", err)
	}
	return cfg, syncID
}

func TestCmdConflictsDeferred_RecoverHumanOutputDistinguishesRepeat(t *testing.T) {
	cfg, syncID := seedRecoverableDeferredCLI(t, "dead")

	withArgs(t, "engram", "conflicts", "deferred", "--recover", syncID)
	stdout, stderr := captureOutput(t, func() { cmdConflicts(cfg) })
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	if !strings.Contains(stdout, "Recovered") || !strings.Contains(stdout, syncID) || !strings.Contains(stdout, "locally") {
		t.Fatalf("unexpected recovered output: %q", stdout)
	}

	withArgs(t, "engram", "conflicts", "deferred", "--recover", syncID)
	stdout, stderr = captureOutput(t, func() { cmdConflicts(cfg) })
	if stderr != "" {
		t.Fatalf("unexpected repeat stderr: %q", stderr)
	}
	if !strings.Contains(stdout, "already recovered") || !strings.Contains(stdout, syncID) {
		t.Fatalf("unexpected repeat output: %q", stdout)
	}
}

func TestCmdConflictsDeferred_RecoverJSONResults(t *testing.T) {
	cfg, syncID := seedRecoverableDeferredCLI(t, "dead")

	for _, wantResult := range []string{"recovered", "already_recovered"} {
		withArgs(t, "engram", "conflicts", "deferred", "--recover", syncID, "--json")
		stdout, stderr := captureOutput(t, func() { cmdConflicts(cfg) })
		if stderr != "" {
			t.Fatalf("unexpected stderr: %q", stderr)
		}
		var result store.DeferredRecoveryResult
		if err := json.Unmarshal([]byte(stdout), &result); err != nil {
			t.Fatalf("decode recovery JSON %q: %v", stdout, err)
		}
		want := store.DeferredRecoveryResult{SyncID: syncID, Status: "applied", Result: wantResult}
		if result != want {
			t.Fatalf("result = %+v, want %+v", result, want)
		}
	}
}

func TestCmdConflictsDeferred_RecoverJSONErrors(t *testing.T) {
	tests := []struct {
		name        string
		prepare     func(t *testing.T) (store.Config, string)
		wantCode    string
		wantDetails map[string]any
	}{
		{
			name: "not found",
			prepare: func(t *testing.T) (store.Config, string) {
				cfg := testConfig(t)
				s, err := store.New(cfg)
				if err != nil {
					t.Fatalf("store.New: %v", err)
				}
				_ = s.Close()
				return cfg, "rel-missing"
			},
			wantCode: "deferred_not_found",
		},
		{
			name: "invalid state",
			prepare: func(t *testing.T) (store.Config, string) {
				return seedRecoverableDeferredCLI(t, "deferred")
			},
			wantCode:    "invalid_recovery_state",
			wantDetails: map[string]any{"status": "deferred"},
		},
		{
			name: "unsupported entity",
			prepare: func(t *testing.T) (store.Config, string) {
				cfg, syncID := seedRecoverableDeferredCLI(t, "dead")
				db := openTestDB(t, cfg)
				if _, err := db.Exec(`UPDATE sync_apply_deferred SET entity = 'observation' WHERE sync_id = ?`, syncID); err != nil {
					t.Fatalf("set unsupported entity: %v", err)
				}
				return cfg, syncID
			},
			wantCode: "unsupported_deferred_entity",
		},
		{
			name: "invalid payload",
			prepare: func(t *testing.T) (store.Config, string) {
				cfg, syncID := seedRecoverableDeferredCLI(t, "dead")
				db := openTestDB(t, cfg)
				if _, err := db.Exec(`UPDATE sync_apply_deferred SET payload = 'invalid' WHERE sync_id = ?`, syncID); err != nil {
					t.Fatalf("set invalid payload: %v", err)
				}
				return cfg, syncID
			},
			wantCode:    "deferred_recovery_failed",
			wantDetails: map[string]any{"reason": "invalid_payload"},
		},
		{
			name: "missing dependency",
			prepare: func(t *testing.T) (store.Config, string) {
				cfg, syncID := seedRecoverableDeferredCLI(t, "dead")
				db := openTestDB(t, cfg)
				var payload map[string]any
				var raw string
				if err := db.QueryRow(`SELECT payload FROM sync_apply_deferred WHERE sync_id = ?`, syncID).Scan(&raw); err != nil {
					t.Fatalf("read payload: %v", err)
				}
				if err := json.Unmarshal([]byte(raw), &payload); err != nil {
					t.Fatalf("decode payload: %v", err)
				}
				payload["target_id"] = "obs-missing-cli"
				updated, _ := json.Marshal(payload)
				if _, err := db.Exec(`UPDATE sync_apply_deferred SET payload = ? WHERE sync_id = ?`, string(updated), syncID); err != nil {
					t.Fatalf("set missing dependency: %v", err)
				}
				return cfg, syncID
			},
			wantCode:    "deferred_recovery_failed",
			wantDetails: map[string]any{"reason": "dependency_missing"},
		},
		{
			name: "apply failure",
			prepare: func(t *testing.T) (store.Config, string) {
				cfg, syncID := seedRecoverableDeferredCLI(t, "dead")
				db := openTestDB(t, cfg)
				if _, err := db.Exec(`
					CREATE TRIGGER reject_recovered_relation
					BEFORE INSERT ON memory_relations
					BEGIN SELECT RAISE(ABORT, 'injected apply failure'); END;
				`); err != nil {
					t.Fatalf("create rejection trigger: %v", err)
				}
				return cfg, syncID
			},
			wantCode:    "deferred_recovery_failed",
			wantDetails: map[string]any{"reason": "apply_failed"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, syncID := tc.prepare(t)
			withArgs(t, "engram", "conflicts", "deferred", "--recover", syncID, "--json")
			stubExitWithPanic(t)
			_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConflicts(cfg) })
			if recovered == nil {
				t.Fatal("expected recovery failure to exit non-zero")
			}
			var envelope cliErrorEnvelope
			if err := json.Unmarshal([]byte(stderr), &envelope); err != nil {
				t.Fatalf("decode JSON error %q: %v", stderr, err)
			}
			if envelope.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", envelope.Code, tc.wantCode)
			}
			for key, want := range tc.wantDetails {
				if got := envelope.Details[key]; got != want {
					t.Errorf("details[%q] = %v, want %v", key, got, want)
				}
			}

			withArgs(t, "engram", "conflicts", "deferred", "--recover", syncID)
			_, humanStderr, recovered := captureOutputAndRecover(t, func() { cmdConflicts(cfg) })
			if recovered == nil {
				t.Fatal("expected human recovery failure to exit non-zero")
			}
			if !strings.HasPrefix(humanStderr, "error: ") || strings.HasPrefix(humanStderr, "{") {
				t.Fatalf("unexpected human error: %q", humanStderr)
			}
		})
	}
}
