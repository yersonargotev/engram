package activationstudy

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsContractDrift(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	contractPath := filepath.Join(dir, "contract.json")
	hashPath := filepath.Join(dir, "contract.sha256")
	contract := validContractJSON()
	if err := os.WriteFile(contractPath, []byte(contract), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(contract))
	if err := os.WriteFile(hashPath, []byte(hex.EncodeToString(digest[:])+"  contract.json\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatalf("load frozen contract: %v", err)
	}
	if study.Hash != hex.EncodeToString(digest[:]) {
		t.Fatalf("hash = %q, want %q", study.Hash, hex.EncodeToString(digest[:]))
	}

	if err := os.WriteFile(contractPath, []byte(strings.Replace(contract, `"repetitions": 2`, `"repetitions": 3`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(contractPath, hashPath); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("Load() error = %v, want hash mismatch", err)
	}
}

func TestLoadRejectsIncompleteOrEnabledIntegrationContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		replace func(string) string
		want    string
	}{
		{
			name: "missing treatment",
			replace: func(raw string) string {
				return strings.Replace(raw, ",\n    {\"id\": \"neutral\", \"kind\": \"neutral\", \"memory_guidance\": \"absent\"}", "", 1)
			},
			want: "treatments",
		},
		{
			name: "missing prompt class",
			replace: func(raw string) string {
				return strings.Replace(raw, ",\n    {\"id\": \"explicit-preservation\", \"class\": \"explicit_preservation\", \"text\": \"Preserve the supplied durable preference.\"}", "", 1)
			},
			want: "prompt classes",
		},
		{
			name: "mixed session shapes",
			replace: func(raw string) string {
				return strings.Replace(raw, `"session_shapes": ["fresh"]`, `"session_shapes": ["fresh", "resumed"]`, 1)
			},
			want: "fresh session",
		},
		{
			name: "missing event",
			replace: func(raw string) string {
				return strings.Replace(raw, `, "integration_failure"`, "", 1)
			},
			want: "events",
		},
		{
			name: "prompt corpus drift",
			replace: func(raw string) string {
				return strings.Replace(raw, "Summarize the project memory model.", "Summarize a different model.", 1)
			},
			want: "prompt corpus hash mismatch",
		},
		{
			name: "plugin enabled",
			replace: func(raw string) string {
				return strings.Replace(raw, `"plugin_enabled": false`, `"plugin_enabled": true`, 1)
			},
			want: "integration",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := test.replace(validContractJSON())
			contractPath, hashPath := writeFrozenContract(t, raw)
			if _, err := Load(contractPath, hashPath); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func writeFrozenContract(t *testing.T, raw string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	contractPath := filepath.Join(dir, "contract.json")
	hashPath := filepath.Join(dir, "contract.sha256")
	if err := os.WriteFile(contractPath, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(raw))
	if err := os.WriteFile(hashPath, []byte(hex.EncodeToString(digest[:])+"  contract.json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return contractPath, hashPath
}

func validContractJSON() string {
	return `{
  "schema_version": "codex-activation-study-v1",
  "harness_version": "codex-activation-harness-v1",
  "fixture_builder_version": "codex-activation-fixtures-v1",
  "event_schema_version": "codex-activation-events-v1",
  "study_id": "repository-scoped-engram-activation",
  "study_version": "v1",
  "status": "frozen",
  "source_revision": "6527374bfe7c45f3e54940f7496c05b677f1706c",
  "codex": {
    "version": "0.151.0",
    "model": "gpt-5.6-luna",
    "reasoning_effort": "low",
    "go_version": "go1.27.0",
    "sandbox": "workspace-write",
    "approval_policy": "never",
    "available_skills": ["engram-memory-cli"],
    "shell_environment_inherit": "all",
    "per_run_timeout_seconds": 180,
    "plugin_enabled": false,
    "mcp_enabled": false,
    "prompt_hooks_enabled": false,
    "stop_verifier_enabled": false,
    "disabled_features": ["apps", "hooks", "multi_agent", "multi_agent_v2", "plugins"],
    "ephemeral": true,
    "ignore_user_config": true,
    "ignore_rules": true
  },
  "engram": {
    "source_revision": "6527374bfe7c45f3e54940f7496c05b677f1706c",
    "cli_mode": "real binary built from source_revision with isolated ENGRAM_DATA_DIR"
  },
  "user_skill": {
    "name": "engram-memory-cli",
    "revision": "local-2026-08-29",
    "tree_sha256": "1c569a28e174e587fb88e391a8ca5ab900753918a7c91bdf13b807ee2ea4f657"
  },
  "treatments": [
    {"id": "engram-normal", "kind": "engram", "memory_guidance": "normal"},
    {"id": "engram-ablated", "kind": "engram", "memory_guidance": "ablated"},
    {"id": "neutral", "kind": "neutral", "memory_guidance": "absent"}
  ],
  "prompts": [
    {"id": "project-question", "class": "project_question", "text": "Summarize the project memory model."},
    {"id": "implementation", "class": "implementation", "text": "Identify the smallest implementation change."},
    {"id": "diagnosis", "class": "diagnosis", "text": "Diagnose a missing project fact."},
    {"id": "routine", "class": "routine_non_durable", "text": "Report the current Git branch."},
    {"id": "explicit-recall", "class": "explicit_recall", "text": "Recall the stored evaluation color pair."},
    {"id": "explicit-preservation", "class": "explicit_preservation", "text": "Preserve the supplied durable preference."}
  ],
  "repetitions": 2,
  "prompt_corpus_sha256": "c0591ae6b6931d7f1db9957db8e1364fadb2ec0bfde431ac66dd71ecbb4b6dc1",
  "randomization_seed": "codex-activation-v1-order",
  "stopping_rule": "Run every planned cell once; retain integration failures and do not replace outcomes.",
  "session_shapes": ["fresh"],
  "events": [
    "skill_description_available", "user_skill_read", "project_memory_protocol_read",
    "project_memory_cli_read", "overlapping_memory_skills_read", "current_project_invoked",
    "task_brief_invoked", "targeted_search_invoked", "memory_write_attempted",
    "memory_write_succeeded", "memory_write_skipped", "checkpoint_attempted",
    "checkpoint_succeeded", "engram_not_invoked", "integration_failure",
    "useful_recall", "useful_preservation"
  ],
  "metrics": {
    "rate_interval": "wilson-95",
    "paired_difference_interval": "deterministic-bootstrap-95",
    "bootstrap_resamples": 10000,
    "bootstrap_seed": "codex-activation-v1-bootstrap"
  },
  "exclusions": ["pre-run verification failure"],
  "omissions": ["missing Codex event evidence is an omission, never semantic non-invocation"],
  "privacy": {
    "retain_raw_events": false,
    "retain_final_messages": false,
    "retain_local_identifiers": false,
    "allowed_shared_artifacts": ["contract", "synthetic_prompts", "fixture_identities", "bounded_events", "aggregate_metrics"]
  },
  "retention": {
    "raw_evidence": "memory-or-temporary-only; delete after classification",
    "bounded_events": "versioned with the study"
  }
}`
}
