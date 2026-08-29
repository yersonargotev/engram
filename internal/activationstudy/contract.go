// Package activationstudy owns frozen, privacy-bounded Codex activation studies.
package activationstudy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxContractBytes = 1 << 20

// Study is a validated contract bound to the exact bytes named by Hash.
type Study struct {
	Contract Contract
	Hash     string
}

// Contract freezes every variable that may affect one activation cohort.
type Contract struct {
	SchemaVersion         string            `json:"schema_version"`
	HarnessVersion        string            `json:"harness_version"`
	FixtureBuilderVersion string            `json:"fixture_builder_version"`
	EventSchemaVersion    string            `json:"event_schema_version"`
	StudyID               string            `json:"study_id"`
	StudyVersion          string            `json:"study_version"`
	Status                string            `json:"status"`
	SourceRevision        string            `json:"source_revision"`
	Codex                 CodexContract     `json:"codex"`
	Engram                EngramContract    `json:"engram"`
	UserSkill             SkillContract     `json:"user_skill"`
	Treatments            []Treatment       `json:"treatments"`
	Prompts               []Prompt          `json:"prompts"`
	Repetitions           int               `json:"repetitions"`
	PromptCorpusSHA256    string            `json:"prompt_corpus_sha256"`
	RandomizationSeed     string            `json:"randomization_seed"`
	StoppingRule          string            `json:"stopping_rule"`
	SessionShapes         []string          `json:"session_shapes"`
	Events                []string          `json:"events"`
	Metrics               MetricsContract   `json:"metrics"`
	Exclusions            []string          `json:"exclusions"`
	Omissions             []string          `json:"omissions"`
	Privacy               PrivacyContract   `json:"privacy"`
	Retention             RetentionContract `json:"retention"`
}

type CodexContract struct {
	Version                 string   `json:"version"`
	Model                   string   `json:"model"`
	ReasoningEffort         string   `json:"reasoning_effort"`
	GoVersion               string   `json:"go_version"`
	Sandbox                 string   `json:"sandbox"`
	ApprovalPolicy          string   `json:"approval_policy"`
	AvailableSkills         []string `json:"available_skills"`
	ShellEnvironmentInherit string   `json:"shell_environment_inherit"`
	PerRunTimeoutSeconds    int      `json:"per_run_timeout_seconds"`
	PluginEnabled           bool     `json:"plugin_enabled"`
	MCPEnabled              bool     `json:"mcp_enabled"`
	PromptHooksEnabled      bool     `json:"prompt_hooks_enabled"`
	StopVerifierEnabled     bool     `json:"stop_verifier_enabled"`
	DisabledFeatures        []string `json:"disabled_features"`
	Ephemeral               bool     `json:"ephemeral"`
	IgnoreUserConfig        bool     `json:"ignore_user_config"`
	IgnoreRules             bool     `json:"ignore_rules"`
}

type EngramContract struct {
	SourceRevision string `json:"source_revision"`
	CLIMode        string `json:"cli_mode"`
}

type SkillContract struct {
	Name       string `json:"name"`
	Revision   string `json:"revision"`
	TreeSHA256 string `json:"tree_sha256"`
}

type Treatment struct {
	ID             string `json:"id"`
	Kind           string `json:"kind"`
	MemoryGuidance string `json:"memory_guidance"`
}

type Prompt struct {
	ID    string `json:"id"`
	Class string `json:"class"`
	Text  string `json:"text"`
}

type MetricsContract struct {
	RateInterval             string `json:"rate_interval"`
	PairedDifferenceInterval string `json:"paired_difference_interval"`
	BootstrapResamples       int    `json:"bootstrap_resamples"`
	BootstrapSeed            string `json:"bootstrap_seed"`
}

type PrivacyContract struct {
	RetainRawEvents        bool     `json:"retain_raw_events"`
	RetainFinalMessages    bool     `json:"retain_final_messages"`
	RetainLocalIdentifiers bool     `json:"retain_local_identifiers"`
	AllowedSharedArtifacts []string `json:"allowed_shared_artifacts"`
}

type RetentionContract struct {
	RawEvidence   string `json:"raw_evidence"`
	BoundedEvents string `json:"bounded_events"`
}

// Load verifies the sidecar digest before decoding or trusting any contract field.
func Load(contractPath, hashPath string) (*Study, error) {
	raw, err := readBoundedFile(contractPath, maxContractBytes)
	if err != nil {
		return nil, fmt.Errorf("read activation study contract: %w", err)
	}
	hashRaw, err := readBoundedFile(hashPath, 4096)
	if err != nil {
		return nil, fmt.Errorf("read activation study contract hash: %w", err)
	}
	expected := strings.Fields(string(hashRaw))
	if len(expected) == 0 || len(expected[0]) != sha256.Size*2 {
		return nil, fmt.Errorf("invalid activation study contract hash sidecar")
	}
	if _, err := hex.DecodeString(expected[0]); err != nil {
		return nil, fmt.Errorf("invalid activation study contract hash sidecar: %w", err)
	}
	actualDigest := sha256.Sum256(raw)
	actual := hex.EncodeToString(actualDigest[:])
	if !strings.EqualFold(expected[0], actual) {
		return nil, fmt.Errorf("activation study contract hash mismatch: got %s, want %s", actual, strings.ToLower(expected[0]))
	}
	if len(expected) > 1 && filepath.Base(contractPath) != filepath.Base(expected[1]) {
		return nil, fmt.Errorf("activation study contract hash names %q, not %q", expected[1], filepath.Base(contractPath))
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var contract Contract
	if err := decoder.Decode(&contract); err != nil {
		return nil, fmt.Errorf("decode activation study contract: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("decode activation study contract: multiple JSON values are not allowed")
	}
	if err := contract.validate(); err != nil {
		return nil, err
	}
	return &Study{Contract: contract, Hash: actual}, nil
}

func readBoundedFile(path string, maximum int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum {
		return nil, fmt.Errorf("%s exceeds %d bytes", filepath.Base(path), maximum)
	}
	return raw, nil
}

func (contract Contract) validate() error {
	if contract.SchemaVersion != "codex-activation-study-v1" {
		return fmt.Errorf("unsupported activation study schema %q", contract.SchemaVersion)
	}
	if contract.HarnessVersion != "codex-activation-harness-v1" || contract.FixtureBuilderVersion != "codex-activation-fixtures-v1" || contract.EventSchemaVersion != "codex-activation-events-v1" {
		return fmt.Errorf("activation study harness, fixture, and event schema versions must be frozen at v1")
	}
	if contract.Status != "frozen" {
		return fmt.Errorf("activation study contract status must be frozen")
	}
	for name, value := range map[string]string{
		"study_id": contract.StudyID, "study_version": contract.StudyVersion,
		"source_revision": contract.SourceRevision, "codex.version": contract.Codex.Version,
		"codex.model": contract.Codex.Model, "codex.reasoning_effort": contract.Codex.ReasoningEffort,
		"codex.go_version": contract.Codex.GoVersion, "engram.source_revision": contract.Engram.SourceRevision,
		"codex.shell_environment_inherit": contract.Codex.ShellEnvironmentInherit,
		"engram.cli_mode":                 contract.Engram.CLIMode,
		"user_skill.name":                 contract.UserSkill.Name, "user_skill.revision": contract.UserSkill.Revision,
		"randomization_seed": contract.RandomizationSeed, "stopping_rule": contract.StoppingRule,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("activation study contract requires %s", name)
		}
	}
	if contract.SourceRevision != contract.Engram.SourceRevision {
		return fmt.Errorf("activation study source revisions must match")
	}
	if !validHexDigest(contract.SourceRevision, 20) {
		return fmt.Errorf("activation study source revision must be a full Git SHA-1")
	}
	if !validHexDigest(contract.UserSkill.TreeSHA256, sha256.Size) {
		return fmt.Errorf("activation study user skill tree_sha256 must be a SHA-256 digest")
	}
	if contract.Repetitions < 1 {
		return fmt.Errorf("activation study repetitions must be positive")
	}
	availableSkills := sortedUnique(contract.Codex.AvailableSkills)
	if len(availableSkills) != len(contract.Codex.AvailableSkills) || strings.Join(availableSkills, "\x00") != strings.Join(contract.Codex.AvailableSkills, "\x00") || !containsString(availableSkills, contract.UserSkill.Name) {
		return fmt.Errorf("activation study available_skills must be sorted, unique, and include the frozen user skill")
	}
	if contract.Codex.PluginEnabled || contract.Codex.MCPEnabled || contract.Codex.PromptHooksEnabled || contract.Codex.StopVerifierEnabled {
		return fmt.Errorf("activation study primary cohort requires plugin, MCP, prompt-hook, and Stop-verifier integration to be disabled")
	}
	if !contract.Codex.Ephemeral || !contract.Codex.IgnoreUserConfig || !contract.Codex.IgnoreRules ||
		contract.Codex.Sandbox != "workspace-write" || contract.Codex.ApprovalPolicy != "never" ||
		contract.Codex.ShellEnvironmentInherit != "all" || contract.Codex.PerRunTimeoutSeconds < 1 {
		return fmt.Errorf("activation study Codex isolation flags are incomplete")
	}
	if missing := missingStrings(contract.Codex.DisabledFeatures, []string{"apps", "hooks", "multi_agent", "multi_agent_v2", "plugins"}); len(missing) > 0 {
		return fmt.Errorf("activation study disabled_features is missing %s", strings.Join(missing, ", "))
	}
	if err := validateTreatments(contract.Treatments); err != nil {
		return err
	}
	if err := validatePrompts(contract.Prompts); err != nil {
		return err
	}
	if actual := PromptCorpusHash(contract.Prompts); actual != contract.PromptCorpusSHA256 {
		return fmt.Errorf("activation study prompt corpus hash mismatch: got %s, want %s", actual, contract.PromptCorpusSHA256)
	}
	if len(contract.SessionShapes) != 1 || contract.SessionShapes[0] != "fresh" {
		return fmt.Errorf("activation study v1 must measure only the fresh session shape")
	}
	requiredEvents := []string{
		"skill_description_available", "user_skill_read", "project_memory_protocol_read",
		"project_memory_cli_read", "overlapping_memory_skills_read", "current_project_invoked",
		"task_brief_invoked", "targeted_search_invoked", "memory_write_attempted",
		"memory_write_succeeded", "memory_write_skipped", "checkpoint_attempted",
		"checkpoint_succeeded", "engram_not_invoked", "integration_failure",
		"useful_recall", "useful_preservation",
	}
	if missing := missingStrings(contract.Events, requiredEvents); len(missing) > 0 || len(contract.Events) != len(requiredEvents) {
		return fmt.Errorf("activation study events must match the frozen event inventory; missing %s", strings.Join(missing, ", "))
	}
	if contract.Metrics.RateInterval != "wilson-95" || contract.Metrics.PairedDifferenceInterval != "deterministic-bootstrap-95" ||
		contract.Metrics.BootstrapResamples < 1000 || strings.TrimSpace(contract.Metrics.BootstrapSeed) == "" {
		return fmt.Errorf("activation study metrics contract is incomplete")
	}
	if contract.Privacy.RetainRawEvents || contract.Privacy.RetainFinalMessages || contract.Privacy.RetainLocalIdentifiers {
		return fmt.Errorf("activation study privacy contract cannot retain raw evidence or local identifiers")
	}
	if len(contract.Exclusions) == 0 || len(contract.Omissions) == 0 || strings.TrimSpace(contract.Retention.RawEvidence) == "" || strings.TrimSpace(contract.Retention.BoundedEvents) == "" {
		return fmt.Errorf("activation study exclusions, omissions, and retention must be frozen")
	}
	return nil
}

func validHexDigest(value string, bytes int) bool {
	if len(value) != bytes*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validateTreatments(treatments []Treatment) error {
	want := map[string]Treatment{
		"engram-normal":  {ID: "engram-normal", Kind: "engram", MemoryGuidance: "normal"},
		"engram-ablated": {ID: "engram-ablated", Kind: "engram", MemoryGuidance: "ablated"},
		"neutral":        {ID: "neutral", Kind: "neutral", MemoryGuidance: "absent"},
	}
	if len(treatments) != len(want) {
		return fmt.Errorf("activation study treatments must contain normal Engram, ablated Engram, and neutral")
	}
	seen := make(map[string]bool, len(treatments))
	for _, treatment := range treatments {
		expected, ok := want[treatment.ID]
		if !ok || treatment != expected || seen[treatment.ID] {
			return fmt.Errorf("activation study treatments contain an invalid or duplicate treatment %q", treatment.ID)
		}
		seen[treatment.ID] = true
	}
	return nil
}

func validatePrompts(prompts []Prompt) error {
	requiredClasses := []string{
		"project_question", "implementation", "diagnosis", "routine_non_durable",
		"explicit_recall", "explicit_preservation",
	}
	if len(prompts) != len(requiredClasses) {
		return fmt.Errorf("activation study prompt classes must contain exactly the six frozen classes")
	}
	classSeen := make(map[string]bool, len(prompts))
	idSeen := make(map[string]bool, len(prompts))
	for _, prompt := range prompts {
		if strings.TrimSpace(prompt.ID) == "" || strings.TrimSpace(prompt.Text) == "" || idSeen[prompt.ID] {
			return fmt.Errorf("activation study prompts require unique IDs and non-empty text")
		}
		idSeen[prompt.ID] = true
		classSeen[prompt.Class] = true
	}
	if missing := missingStrings(mapKeys(classSeen), requiredClasses); len(missing) > 0 || len(classSeen) != len(requiredClasses) {
		return fmt.Errorf("activation study prompt classes are incomplete; missing %s", strings.Join(missing, ", "))
	}
	return nil
}

func PromptCorpusHash(prompts []Prompt) string {
	hash := sha256.New()
	for _, prompt := range prompts {
		fmt.Fprintf(hash, "%s\x00%s\x00%s\n", prompt.ID, prompt.Class, prompt.Text)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func missingStrings(actual, required []string) []string {
	seen := make(map[string]bool, len(actual))
	for _, value := range actual {
		seen[value] = true
	}
	var missing []string
	for _, value := range required {
		if !seen[value] {
			missing = append(missing, value)
		}
	}
	return missing
}

func mapKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for value := range values {
		keys = append(keys, value)
	}
	return keys
}
