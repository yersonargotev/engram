package recallstudy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const frozenSourceRevision = "105778d820029a2326043739fd676647e5c037f6"

func TestLoadFreezesRecallStudyContractAndRejectsDrift(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "evals", "recall-study", "v1")
	contractPath := filepath.Join(root, "contract.json")
	hashPath := filepath.Join(root, "contract.sha256")
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if study.Contract.StudyID != "codex-useful-recall" || study.Contract.Cohorts.RequiredTotal != 1551 {
		t.Fatalf("loaded contract = %+v", study.Contract)
	}

	raw, err := os.ReadFile(contractPath)
	if err != nil {
		t.Fatal(err)
	}
	drifted := strings.Replace(string(raw), `"name": "gpt-5.6-luna"`, `"name": "changed-model"`, 1)
	tamperedPath := filepath.Join(t.TempDir(), "contract.json")
	if err := os.WriteFile(tamperedPath, []byte(drifted), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(drifted))
	tamperedHash := filepath.Join(filepath.Dir(tamperedPath), "contract.sha256")
	if err := os.WriteFile(tamperedHash, []byte(hex.EncodeToString(digest[:])+"  contract.json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(tamperedPath, tamperedHash); err == nil || !strings.Contains(err.Error(), "trust anchor") {
		t.Fatalf("Load() error = %v, want trust anchor rejection", err)
	}
}

func TestLoadRejectsChangedFrozenRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Contract)
		want   string
	}{
		{"sample size", func(c *Contract) { c.Cohorts.RequiredPerTreatment = 516 }, "sample size"},
		{"study identity", func(c *Contract) { c.StudyID = "changed-study" }, "identity"},
		{"treatments", func(c *Contract) { c.Treatments[2].ID = "implicit-recall" }, "treatments"},
		{"no recall scope", func(c *Contract) { c.TaskClasses[0].SelfContained = false }, "self-contained"},
		{"source revision", func(c *Contract) { c.SourceRevision = "main" }, "source revision"},
		{"protocol revision", func(c *Contract) { c.Revisions.ProtocolContract.Version = "2" }, "revisions"},
		{"policy revision", func(c *Contract) { c.Revisions.Policy.Revision = "sha256:" + strings.Repeat("0", 64) }, "revisions"},
		{"metric revision", func(c *Contract) { c.Revisions.Metric.Revision = "sha256:" + strings.Repeat("0", 64) }, "revisions"},
		{"threshold drift", func(c *Contract) { c.Gates[0].Clauses[0].Threshold = -3 }, "gates"},
		{"label vocabulary", func(c *Contract) { c.EvaluationRubric.Utility[0] = "helpful" }, "rubric"},
		{"shared row output", func(c *Contract) { c.AllowedOutputs.Shared = append(c.AllowedOutputs.Shared, "row_level_runs") }, "outputs"},
		{"held-out access", func(c *Contract) { c.Runner.NoHeldOutAccessModes = []string{"verify"} }, "held-out"},
		{"rollout enabled", func(c *Contract) { c.Runner.AutomaticRolloutEnabled = true }, "rollout"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contract := validContract()
			test.mutate(&contract)
			if err := contract.validate(); err == nil || !strings.Contains(strings.ToLower(err.Error()), test.want) {
				t.Fatalf("Load() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestCommittedRecallStudyV1IsSelfConsistent(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "evals", "recall-study", "v1")
	study, err := Load(filepath.Join(root, "contract.json"), filepath.Join(root, "contract.sha256"))
	if err != nil {
		t.Fatalf("load committed contract: %v", err)
	}
	calibration, err := LoadManifest(filepath.Join(root, "calibration", "manifest.json"), filepath.Join(root, "calibration", "manifest.sha256"))
	if err != nil {
		t.Fatalf("load committed calibration manifest: %v", err)
	}
	heldOut, err := LoadManifest(filepath.Join(root, "held-out", "manifest.json"), filepath.Join(root, "held-out", "manifest.sha256"))
	if err != nil {
		t.Fatalf("load committed held-out manifest: %v", err)
	}
	if _, err := study.Verify(VerificationInput{
		Calibration: &calibration.Manifest, HeldOut: &heldOut.Manifest,
		Compatibility: compatibleEvidence(study),
		Consent: ConsentEvidence{StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
			CalibrationGranted: true, HeldOutGranted: true, ProofSHA256: strings.Repeat("c", 64)},
	}); err != nil {
		t.Fatalf("verify committed study: %v", err)
	}
}

func validContract() Contract {
	artifact := func(version string) ArtifactRevision {
		return ArtifactRevision{Version: version, Revision: frozenSourceRevision}
	}
	return Contract{
		SchemaVersion:  "recall-study-contract-v1",
		StudyID:        "codex-useful-recall",
		StudyVersion:   "v1",
		Status:         "frozen",
		SourceRevision: frozenSourceRevision,
		Cohorts: CohortsContract{
			RequiredPerTreatment: 517,
			RequiredTotal:        1551,
			Calibration: CohortContract{ID: "calibration-v1", FirstSamplingUnit: 1, SamplingUnits: 60, ManifestSHA256: strings.Repeat("a", 64),
				Namespace: "cal", SelectionSeed: "codex-useful-recall-v1-calibration-v1"},
			HeldOut: CohortContract{ID: "held-out-v1", FirstSamplingUnit: 61, SamplingUnits: 457, ManifestSHA256: strings.Repeat("b", 64),
				Namespace: "hold", SelectionSeed: "codex-useful-recall-v1-held-out-v1"},
		},
		SamplingUnit: "matched-task-block",
		TaskClasses: []TaskClass{
			{ID: "repository-question", SelfContained: true},
			{ID: "implementation", SelfContained: true},
			{ID: "diagnosis", SelfContained: true},
			{ID: "verification", SelfContained: true},
			{ID: "routine-non-durable", SelfContained: true},
		},
		Treatments: []Treatment{
			{ID: "broad-chronological", Recall: "broad-chronological-injection"},
			{ID: "targeted-recall", Recall: "cue-only-agent-initiated"},
			{ID: "no-recall", Recall: "cue-only-none"},
		},
		Randomization: RandomizationContract{Method: "sha256-seeded-block-order-v1", Seed: "codex-useful-recall-v1", PairingKey: "sampling_unit_id", Stratification: "task_class"},
		Model:         ModelContract{Provider: "openai", Name: "gpt-5.6-luna", ReasoningEffort: "low", CodexVersion: "0.152.0"},
		Repository:    RepositoryContract{URL: "https://github.com/yersonargotev/engram.git", Revision: frozenSourceRevision},
		TaskProtocol:  TaskProtocolContract{Version: "recall-task-protocol-v1", Execution: "fresh-ephemeral-checkout", FixedEnvironment: true, OperationalFailures: "separate-from-recall-quality"},
		EvaluationRubric: EvaluationRubricContract{
			Version:       "recall-labels-v1",
			Utility:       []string{"decisive", "orienting", "duplicate", "unused", "unknown"},
			Quality:       []string{"current", "stale", "contradictory", "unknown"},
			LabelSources:  []string{"agent_explicit", "user_explicit", "evaluator", "unknown"},
			MissingLabels: "unknown", Omissions: "preserved", Disagreements: "preserved", FalseEmptyReview: "explicit-or-unknown",
		},
		Consent: ConsentContract{Required: true, Scope: "project-task-and-content", Proof: "sha256-commitment", Missing: "reject-before-evidence"},
		AllowedOutputs: OutputContract{
			Private:         []string{"consented_task_inputs", "row_level_runs", "explicit_labels"},
			Shared:          []string{"contract", "cohort_manifests", "aggregate_report"},
			ForbiddenFields: []string{"prompt", "query", "memory_content", "assistant_text", "transcript_path", "raw_identifier", "repository_diff"},
		},
		Revisions: RevisionsContract{
			ManagedPack: artifact("3.3.0"), EngramBinary: artifact("3.0.0"), CodexPlugin: artifact("0.1.7"),
			ProtocolContract: artifact("1"), TelemetrySchema: artifact("recall-baseline-events-v1"),
			CaptureSchema: artifact("diagnostic-capture-v1"), Policy: ArtifactRevision{Version: "recall-policy-v1", Revision: frozenPolicyRevision},
			Metric: ArtifactRevision{Version: "recall-study-metrics-v1", Revision: frozenMetricRevision}, Source: artifact(frozenSourceRevision),
		},
		Power:     PowerContract{SchemaVersion: "recall-baseline-power-v1", Method: "two-sided-two-proportion-normal-bonferroni-v1", BaselineRate: .50, MinimumDetectableDifference: .10, FamilywiseAlpha: .05, Power: .80, Comparisons: 3, Treatments: 3, RequiredPerTreatment: 517, RequiredTotal: 1551},
		Intervals: IntervalContract{Rate: "wilson-95", PairedDifference: "deterministic-bootstrap-95", BootstrapResamples: 10000, BootstrapSeed: "codex-useful-recall-v1-bootstrap"},
		Gates:     frozenGates(),
		Runner: RunnerContract{PlanSchemaVersion: "recall-study-run-plan-v1", RowSchemaVersion: RowSetSchemaVersion,
			ExecutionStage: "issue-110", AcceptsTaskInputs: false, DefaultRecallEnabled: false, AutomaticRolloutEnabled: false,
			HeldOutMode: "issue-110-execution-only", NoHeldOutAccessModes: []string{"verify", "dry-run", "plan-calibration", "report"}},
	}
}

func frozenGates() []GateContract {
	clause := func(metric, statistic, comparator string, threshold float64) GateClause {
		return GateClause{Metric: metric, Statistic: statistic, Comparator: comparator, Threshold: threshold}
	}
	return []GateContract{
		{ID: "checkpoint-non-inferiority", Clauses: []GateClause{clause("checkpoint_rate_delta_pp", "ci_lower", "gte", -2)}},
		{ID: "stop-growth", Clauses: []GateClause{clause("stop_growth_pp", "ci_upper", "lt", 1)}},
		{ID: "injected-bytes", Clauses: []GateClause{clause("automatic_injected_bytes_reduction_percent", "ci_lower", "gte", 30)}},
		{ID: "startup-compact-latency", Clauses: []GateClause{clause("startup_compact_p95_reduction_percent", "ci_lower", "gte", 25)}},
		{ID: "recall-latency", Clauses: []GateClause{clause("recall_p95_ms", "point", "lt", 250)}},
		{ID: "utility", Clauses: []GateClause{clause("utility_relative_improvement_percent", "point", "gte", 10), clause("utility_relative_improvement_percent", "ci_lower", "gte", 0)}},
		{ID: "noise", Clauses: []GateClause{clause("noise_rate_percent", "ci_upper", "lt", 20), clause("noise_improvement_pp", "ci_lower", "gt", 0)}},
		{ID: "harm", Clauses: []GateClause{clause("harm_rate_percent", "ci_upper", "lte", 2), clause("harm_difference_pp", "ci_upper", "lte", 0)}},
		{ID: "false-empty", Clauses: []GateClause{clause("false_empty_rate_percent", "ci_upper", "lte", 5)}},
		{ID: "label-coverage", Clauses: []GateClause{clause("explicit_label_coverage_percent", "ci_lower", "gte", 80)}},
	}
}

func writeFrozenJSON(t *testing.T, name string, value any) (string, string) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	hashPath := filepath.Join(dir, name+".sha256")
	if err := os.WriteFile(hashPath, []byte(hex.EncodeToString(digest[:])+"  "+name+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, hashPath
}
