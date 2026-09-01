package recallstudy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

const (
	ContractSchemaVersion  = "recall-study-contract-v1"
	FrozenV1ContractSHA256 = "f312b9dcaedc859f6512771d5dc19d52bc7d5c84b2c519a10170541ccbf6e466"
	maxContractBytes       = 1 << 20
	frozenPolicyRevision   = "sha256:4756324cf6c42839d2fb25de3db01f301c21141926fe7d15c05a7515fabe8510"
	frozenMetricRevision   = "sha256:2730b78c4616b54df30c3108356de72441a3aaabc10e8624593fd76c7b5aef9b"
)

type Study struct {
	Contract Contract
	Hash     string
}

type Contract struct {
	SchemaVersion    string                   `json:"schema_version"`
	StudyID          string                   `json:"study_id"`
	StudyVersion     string                   `json:"study_version"`
	Status           string                   `json:"status"`
	SourceRevision   string                   `json:"source_revision"`
	Cohorts          CohortsContract          `json:"cohorts"`
	SamplingUnit     string                   `json:"sampling_unit"`
	TaskClasses      []TaskClass              `json:"task_classes"`
	Treatments       []Treatment              `json:"treatments"`
	Randomization    RandomizationContract    `json:"randomization"`
	Model            ModelContract            `json:"model"`
	Repository       RepositoryContract       `json:"repository"`
	TaskProtocol     TaskProtocolContract     `json:"task_protocol"`
	EvaluationRubric EvaluationRubricContract `json:"evaluation_rubric"`
	Consent          ConsentContract          `json:"consent"`
	AllowedOutputs   OutputContract           `json:"allowed_outputs"`
	Revisions        RevisionsContract        `json:"revisions"`
	Power            PowerContract            `json:"power"`
	Intervals        IntervalContract         `json:"intervals"`
	Gates            []GateContract           `json:"gates"`
	Runner           RunnerContract           `json:"runner"`
}

type CohortsContract struct {
	RequiredPerTreatment int            `json:"required_per_treatment"`
	RequiredTotal        int            `json:"required_total"`
	Calibration          CohortContract `json:"calibration"`
	HeldOut              CohortContract `json:"held_out"`
}

type CohortContract struct {
	ID                string `json:"id"`
	FirstSamplingUnit int    `json:"first_sampling_unit"`
	SamplingUnits     int    `json:"sampling_units"`
	ManifestSHA256    string `json:"manifest_sha256"`
	Namespace         string `json:"namespace"`
	SelectionSeed     string `json:"selection_seed"`
}

type TaskClass struct {
	ID            string `json:"id"`
	SelfContained bool   `json:"self_contained"`
}

type Treatment struct {
	ID     string `json:"id"`
	Recall string `json:"recall"`
}

type RandomizationContract struct {
	Method         string `json:"method"`
	Seed           string `json:"seed"`
	PairingKey     string `json:"pairing_key"`
	Stratification string `json:"stratification"`
}

type ModelContract struct {
	Provider        string `json:"provider"`
	Name            string `json:"name"`
	ReasoningEffort string `json:"reasoning_effort"`
	CodexVersion    string `json:"codex_version"`
}

type RepositoryContract struct {
	URL      string `json:"url"`
	Revision string `json:"revision"`
}

type TaskProtocolContract struct {
	Version             string `json:"version"`
	ArtifactSHA256      string `json:"artifact_sha256"`
	Execution           string `json:"execution"`
	FixedEnvironment    bool   `json:"fixed_environment"`
	OperationalFailures string `json:"operational_failures"`
}

type EvaluationRubricContract struct {
	Version          string   `json:"version"`
	Utility          []string `json:"utility"`
	Quality          []string `json:"quality"`
	LabelSources     []string `json:"label_sources"`
	MissingLabels    string   `json:"missing_labels"`
	Omissions        string   `json:"omissions"`
	Disagreements    string   `json:"disagreements"`
	FalseEmptyReview string   `json:"false_empty_review"`
}

type ConsentContract struct {
	Required bool   `json:"required"`
	Scope    string `json:"scope"`
	Proof    string `json:"proof"`
	Missing  string `json:"missing"`
}

type OutputContract struct {
	Private         []string `json:"private"`
	Shared          []string `json:"shared"`
	ForbiddenFields []string `json:"forbidden_fields"`
}

type ArtifactRevision struct {
	Version  string `json:"version"`
	Revision string `json:"revision"`
}

type RevisionsContract struct {
	ManagedPack      ArtifactRevision `json:"managed_pack"`
	EngramBinary     ArtifactRevision `json:"engram_binary"`
	CodexPlugin      ArtifactRevision `json:"codex_plugin"`
	ProtocolContract ArtifactRevision `json:"protocol_contract"`
	TelemetrySchema  ArtifactRevision `json:"telemetry_schema"`
	CaptureSchema    ArtifactRevision `json:"capture_schema"`
	Policy           ArtifactRevision `json:"policy"`
	Metric           ArtifactRevision `json:"metric"`
	Source           ArtifactRevision `json:"source"`
}

type PowerContract struct {
	SchemaVersion               string  `json:"schema_version"`
	Method                      string  `json:"method"`
	BaselineRate                float64 `json:"baseline_rate"`
	MinimumDetectableDifference float64 `json:"minimum_detectable_absolute_difference"`
	FamilywiseAlpha             float64 `json:"familywise_alpha"`
	Power                       float64 `json:"power"`
	Comparisons                 int     `json:"comparisons"`
	Treatments                  int     `json:"treatments"`
	RequiredPerTreatment        int     `json:"required_per_treatment"`
	RequiredTotal               int     `json:"required_total"`
}

type IntervalContract struct {
	Rate               string `json:"rate"`
	PairedDifference   string `json:"paired_difference"`
	BootstrapResamples int    `json:"bootstrap_resamples"`
	BootstrapSeed      string `json:"bootstrap_seed"`
}

type GateContract struct {
	ID      string       `json:"id"`
	Clauses []GateClause `json:"clauses"`
}

type GateClause struct {
	Metric     string  `json:"metric"`
	Statistic  string  `json:"statistic"`
	Comparator string  `json:"comparator"`
	Threshold  float64 `json:"threshold"`
}

type RunnerContract struct {
	PlanSchemaVersion       string   `json:"plan_schema_version"`
	RowSchemaVersion        string   `json:"row_schema_version"`
	ExecutionStage          string   `json:"execution_stage"`
	AcceptsTaskInputs       bool     `json:"accepts_task_inputs"`
	DefaultRecallEnabled    bool     `json:"default_recall_enabled"`
	AutomaticRolloutEnabled bool     `json:"automatic_rollout_enabled"`
	HeldOutMode             string   `json:"held_out_mode"`
	NoHeldOutAccessModes    []string `json:"no_held_out_access_modes"`
}

func Load(contractPath, hashPath string) (*Study, error) {
	return loadContract(contractPath, hashPath, true)
}

func loadContract(contractPath, hashPath string, enforceTrustAnchor bool) (*Study, error) {
	raw, err := readBoundedFile(contractPath, maxContractBytes)
	if err != nil {
		return nil, fmt.Errorf("read Recall study contract: %w", err)
	}
	hashRaw, err := readBoundedFile(hashPath, 4096)
	if err != nil {
		return nil, fmt.Errorf("read Recall study contract hash: %w", err)
	}
	want := strings.Fields(string(hashRaw))
	if len(want) == 0 || !validHexDigest(want[0], sha256.Size) {
		return nil, fmt.Errorf("Recall study contract hash sidecar is invalid")
	}
	digest := sha256.Sum256(raw)
	actual := hex.EncodeToString(digest[:])
	if actual != strings.ToLower(want[0]) {
		return nil, fmt.Errorf("Recall study contract hash mismatch: got %s, want %s", actual, want[0])
	}

	var contract Contract
	if err := decodeStrictJSON(raw, &contract); err != nil {
		return nil, fmt.Errorf("decode Recall study contract: %w", err)
	}
	if enforceTrustAnchor && actual != FrozenV1ContractSHA256 {
		return nil, fmt.Errorf("Recall study contract does not match the compiled frozen v1 trust anchor")
	}
	if err := contract.validate(); err != nil {
		return nil, err
	}
	if enforceTrustAnchor {
		root := filepath.Dir(contractPath)
		if err := verifyFrozenArtifact(root, "policy", contract.Revisions.Policy.Revision); err != nil {
			return nil, err
		}
		if err := verifyFrozenArtifact(root, "metrics", contract.Revisions.Metric.Revision); err != nil {
			return nil, err
		}
		if err := verifyFrozenArtifact(root, "task-protocol", "sha256:"+contract.TaskProtocol.ArtifactSHA256); err != nil {
			return nil, err
		}
	}
	return &Study{Contract: contract, Hash: actual}, nil
}

func verifyFrozenArtifact(root, name, revision string) error {
	want := strings.TrimPrefix(revision, "sha256:")
	if !validHexDigest(want, sha256.Size) {
		return fmt.Errorf("Recall study %s revision is not content-addressed", name)
	}
	raw, err := readBoundedFile(filepath.Join(root, name+".json"), maxContractBytes)
	if err != nil {
		return fmt.Errorf("read frozen Recall study %s: %w", name, err)
	}
	digest := sha256.Sum256(raw)
	actual := hex.EncodeToString(digest[:])
	if actual != want {
		return fmt.Errorf("frozen Recall study %s hash mismatch: got %s, want %s", name, actual, want)
	}
	sidecar, err := readBoundedFile(filepath.Join(root, name+".sha256"), 4096)
	if err != nil {
		return fmt.Errorf("read frozen Recall study %s sidecar: %w", name, err)
	}
	fields := strings.Fields(string(sidecar))
	if len(fields) == 0 || fields[0] != want {
		return fmt.Errorf("frozen Recall study %s sidecar does not match its contract revision", name)
	}
	return nil
}

func (contract Contract) cohort(id string) (CohortContract, bool) {
	switch id {
	case contract.Cohorts.Calibration.ID:
		return contract.Cohorts.Calibration, true
	case contract.Cohorts.HeldOut.ID:
		return contract.Cohorts.HeldOut, true
	default:
		return CohortContract{}, false
	}
}

func (contract Contract) validate() error {
	if contract.SchemaVersion != ContractSchemaVersion || contract.StudyID != "codex-useful-recall" || contract.StudyVersion != "v1" || contract.Status != "frozen" {
		return fmt.Errorf("Recall study identity must be frozen under %s", ContractSchemaVersion)
	}
	if !validHexDigest(contract.SourceRevision, 20) || contract.Repository.Revision != contract.SourceRevision || contract.Repository.URL != "https://github.com/yersonargotev/engram.git" {
		return fmt.Errorf("Recall study source revision must be one exact Engram commit")
	}
	if err := validateCohorts(contract.Cohorts, len(contract.Treatments)); err != nil {
		return err
	}
	if contract.SamplingUnit != "matched-task-block" || !validTaskClasses(contract.TaskClasses) {
		return fmt.Errorf("Recall study task classes must be unique, frozen, and self-contained for the no-Recall treatment")
	}
	wantTreatments := []Treatment{
		{ID: "broad-chronological", Recall: "broad-chronological-injection"},
		{ID: "targeted-recall", Recall: "cue-only-agent-initiated"},
		{ID: "no-recall", Recall: "cue-only-none"},
	}
	if !reflect.DeepEqual(contract.Treatments, wantTreatments) {
		return fmt.Errorf("Recall study treatments must be broad chronological, targeted Recall, and no Recall")
	}
	if contract.Randomization.Method != "sha256-seeded-block-order-v1" || strings.TrimSpace(contract.Randomization.Seed) == "" ||
		contract.Randomization.PairingKey != "sampling_unit_id" || contract.Randomization.Stratification != "task_class" {
		return fmt.Errorf("Recall study randomization and pairing contract is incomplete")
	}
	if contract.Model.Provider == "" || contract.Model.Name == "" || contract.Model.ReasoningEffort == "" || contract.Model.CodexVersion == "" {
		return fmt.Errorf("Recall study model contract is incomplete")
	}
	if contract.TaskProtocol.Version != "recall-task-protocol-v1" || contract.TaskProtocol.ArtifactSHA256 != "669c2261f43f946dac302605401694c827d255693b0ee3688bac7871c12f148c" ||
		contract.TaskProtocol.Execution != "fresh-ephemeral-checkout" ||
		!contract.TaskProtocol.FixedEnvironment || contract.TaskProtocol.OperationalFailures != "separate-from-recall-quality" {
		return fmt.Errorf("Recall study task protocol is incomplete")
	}
	if !validRubric(contract.EvaluationRubric) {
		return fmt.Errorf("Recall study rubric must preserve labels, unknowns, omissions, disagreements, and false-empty review")
	}
	if !contract.Consent.Required || contract.Consent.Scope != "project-task-and-content" || contract.Consent.Proof != "sha256-commitment" || contract.Consent.Missing != "reject-before-evidence" {
		return fmt.Errorf("Recall study consent contract is incomplete")
	}
	if !validOutputs(contract.AllowedOutputs) {
		return fmt.Errorf("Recall study outputs must keep row-level state private and shared output aggregate-only")
	}
	if !validRevisions(contract.Revisions, contract.SourceRevision) {
		return fmt.Errorf("Recall study revisions must freeze Pack, binary, plugin, Protocol, telemetry, Capture, policy, metric, and source")
	}
	if !reflect.DeepEqual(contract.Power, expectedPower()) || contract.Cohorts.RequiredPerTreatment != contract.Power.RequiredPerTreatment || contract.Cohorts.RequiredTotal != contract.Power.RequiredTotal {
		return fmt.Errorf("Recall study power analysis and sample size must match the frozen baseline")
	}
	if contract.Intervals.Rate != "wilson-95" || contract.Intervals.PairedDifference != "deterministic-bootstrap-95" ||
		contract.Intervals.BootstrapResamples != 10000 || strings.TrimSpace(contract.Intervals.BootstrapSeed) == "" {
		return fmt.Errorf("Recall study confidence-interval contract is incomplete")
	}
	if !reflect.DeepEqual(contract.Gates, expectedGates()) {
		return fmt.Errorf("Recall study gates must exactly encode the preregistered general-availability thresholds")
	}
	if contract.Runner.PlanSchemaVersion != "recall-study-run-plan-v1" || contract.Runner.RowSchemaVersion != RowSetSchemaVersion ||
		contract.Runner.ExecutionStage != "issue-110" || contract.Runner.AcceptsTaskInputs ||
		contract.Runner.DefaultRecallEnabled || contract.Runner.AutomaticRolloutEnabled {
		return fmt.Errorf("Recall study cannot enable default Recall or automatic rollout")
	}
	wantNoAccess := []string{"dry-run", "plan-calibration", "report", "verify"}
	actualNoAccess := append([]string(nil), contract.Runner.NoHeldOutAccessModes...)
	sort.Strings(actualNoAccess)
	if contract.Runner.HeldOutMode != "issue-110-execution-only" || !reflect.DeepEqual(actualNoAccess, wantNoAccess) {
		return fmt.Errorf("Recall study held-out execution must remain isolated to issue #110")
	}
	return nil
}

func validateCohorts(cohorts CohortsContract, treatmentCount int) error {
	calibrationEnd := cohorts.Calibration.FirstSamplingUnit + cohorts.Calibration.SamplingUnits
	if cohorts.RequiredPerTreatment != 517 || cohorts.RequiredTotal != 1551 || treatmentCount != 3 ||
		cohorts.RequiredTotal != cohorts.RequiredPerTreatment*treatmentCount ||
		cohorts.Calibration.ID != "calibration-v1" || cohorts.HeldOut.ID != "held-out-v1" ||
		cohorts.Calibration.FirstSamplingUnit != 1 || cohorts.Calibration.SamplingUnits < 1 ||
		cohorts.HeldOut.FirstSamplingUnit != calibrationEnd || cohorts.HeldOut.SamplingUnits < 1 ||
		cohorts.Calibration.SamplingUnits+cohorts.HeldOut.SamplingUnits != cohorts.RequiredPerTreatment ||
		cohorts.Calibration.Namespace != "cal" || cohorts.HeldOut.Namespace != "hold" ||
		cohorts.Calibration.SelectionSeed != "codex-useful-recall-v1-calibration-v1" ||
		cohorts.HeldOut.SelectionSeed != "codex-useful-recall-v1-held-out-v1" ||
		!validHexDigest(cohorts.Calibration.ManifestSHA256, sha256.Size) || !validHexDigest(cohorts.HeldOut.ManifestSHA256, sha256.Size) {
		return fmt.Errorf("Recall study sample size and non-overlapping cohort ranges are invalid")
	}
	return nil
}

func validTaskClasses(classes []TaskClass) bool {
	want := []string{"diagnosis", "implementation", "repository-question", "routine-non-durable", "verification"}
	if len(classes) != len(want) {
		return false
	}
	seen := make([]string, 0, len(classes))
	for _, class := range classes {
		if strings.TrimSpace(class.ID) == "" || !class.SelfContained {
			return false
		}
		seen = append(seen, class.ID)
	}
	sort.Strings(seen)
	return reflect.DeepEqual(seen, want)
}

func validRubric(rubric EvaluationRubricContract) bool {
	return rubric.Version == "recall-labels-v1" &&
		reflect.DeepEqual(rubric.Utility, []string{"decisive", "orienting", "duplicate", "unused", "unknown"}) &&
		reflect.DeepEqual(rubric.Quality, []string{"current", "stale", "contradictory", "unknown"}) &&
		reflect.DeepEqual(rubric.LabelSources, []string{"agent_explicit", "user_explicit", "evaluator", "unknown"}) &&
		rubric.MissingLabels == "unknown" && rubric.Omissions == "preserved" && rubric.Disagreements == "preserved" && rubric.FalseEmptyReview == "explicit-or-unknown"
}

func validOutputs(outputs OutputContract) bool {
	if !reflect.DeepEqual(outputs.Private, []string{"consented_task_inputs", "row_level_runs", "explicit_labels"}) ||
		!reflect.DeepEqual(outputs.Shared, []string{"contract", "cohort_manifests", "aggregate_report"}) {
		return false
	}
	wantForbidden := []string{"assistant_text", "memory_content", "prompt", "query", "raw_identifier", "repository_diff", "transcript_path"}
	actual := append([]string(nil), outputs.ForbiddenFields...)
	sort.Strings(actual)
	return reflect.DeepEqual(actual, wantForbidden)
}

func validRevisions(revisions RevisionsContract, source string) bool {
	want := map[string]string{
		"managed_pack": "3.3.0", "engram_binary": "3.0.0", "codex_plugin": "0.1.7", "protocol_contract": "1",
		"telemetry_schema": "recall-baseline-events-v1", "capture_schema": "diagnostic-capture-v1",
		"policy": "recall-policy-v1", "metric": "recall-study-metrics-v1", "source": source,
	}
	actual := map[string]ArtifactRevision{
		"managed_pack": revisions.ManagedPack, "engram_binary": revisions.EngramBinary, "codex_plugin": revisions.CodexPlugin,
		"protocol_contract": revisions.ProtocolContract, "telemetry_schema": revisions.TelemetrySchema, "capture_schema": revisions.CaptureSchema,
		"policy": revisions.Policy, "metric": revisions.Metric, "source": revisions.Source,
	}
	for name, version := range want {
		artifact := actual[name]
		wantRevision := source
		if name == "policy" {
			wantRevision = frozenPolicyRevision
		}
		if name == "metric" {
			wantRevision = frozenMetricRevision
		}
		if artifact.Version != version || artifact.Revision != wantRevision {
			return false
		}
	}
	return true
}

func expectedPower() PowerContract {
	return PowerContract{SchemaVersion: "recall-baseline-power-v1", Method: "two-sided-two-proportion-normal-bonferroni-v1", BaselineRate: .50,
		MinimumDetectableDifference: .10, FamilywiseAlpha: .05, Power: .80, Comparisons: 3, Treatments: 3,
		RequiredPerTreatment: 517, RequiredTotal: 1551}
}

func expectedGates() []GateContract {
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

func readBoundedFile(path string, maximum int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	limited := io.LimitReader(file, maximum+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum {
		return nil, fmt.Errorf("file exceeds %d bytes", maximum)
	}
	return raw, nil
}

func validHexDigest(value string, bytes int) bool {
	if len(value) != bytes*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
