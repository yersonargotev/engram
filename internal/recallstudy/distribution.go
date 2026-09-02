package recallstudy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

const (
	DistributionOutcomeSchemaVersion   = "recall-distribution-outcome-v1"
	DistributionVerificationSchema     = "recall-distribution-verification-v1"
	DistributionActionPreserve         = "preserve_verified_tuple"
	DistributionPostInstallNotVerified = "not_verified"
	FrozenV1DistributionSHA256         = "cb7b20ec58a3a1fab2d5e95db090252d8eea3e3a96986137e6dba2639c30f5fd"
	maxDistributionOutcomeBytes        = 1 << 20
)

var requiredDistributionArtifacts = map[string]string{
	"managed_pack_manifest": "pack.json",
	"managed_pack_skill":    "skills/engram-memory-cli/SKILL.md",
	"protocol_fixture":      "assets/protocol-contract-v1.json",
	"codex_plugin_manifest": "plugin/codex/.codex-plugin/plugin.json",
}

type FrozenPublication struct {
	Publication Publication
	SHA256      string
}

type FrozenDistributionOutcome struct {
	Outcome DistributionOutcome
	SHA256  string
}

type DistributionOutcome struct {
	SchemaVersion                      string                 `json:"schema_version"`
	StudyID                            string                 `json:"study_id"`
	StudyVersion                       string                 `json:"study_version"`
	ContractSHA256                     string                 `json:"contract_sha256"`
	PublicationSHA256                  string                 `json:"publication_sha256"`
	Disposition                        string                 `json:"disposition"`
	Action                             string                 `json:"action"`
	RolloutEnabled                     bool                   `json:"rollout_enabled"`
	ReleaseRequired                    bool                   `json:"release_required"`
	LegacyContractionAllowed           bool                   `json:"legacy_contraction_allowed"`
	LegacyPromptArchiveAction          string                 `json:"legacy_prompt_archive_action"`
	LocalSchemasParticipatingByDefault bool                   `json:"local_schemas_participating_by_default"`
	SelectedCompatibility              CompatibilityEvidence  `json:"selected_compatibility"`
	SourceArtifacts                    []DistributionArtifact `json:"source_artifacts"`
}

type DistributionArtifact struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type DistributionVerification struct {
	SchemaVersion                      string                               `json:"schema_version"`
	StudyID                            string                               `json:"study_id"`
	StudyVersion                       string                               `json:"study_version"`
	ContractSHA256                     string                               `json:"contract_sha256"`
	PublicationSHA256                  string                               `json:"publication_sha256"`
	DistributionSHA256                 string                               `json:"distribution_sha256"`
	SourceRevision                     string                               `json:"source_revision"`
	SourceRevisionVerified             bool                                 `json:"source_revision_verified"`
	SourceArtifactsVerified            bool                                 `json:"source_artifacts_verified"`
	PostInstallReadiness               string                               `json:"post_install_readiness"`
	PostInstallVerificationCommand     string                               `json:"post_install_verification_command"`
	Disposition                        string                               `json:"disposition"`
	Action                             string                               `json:"action"`
	RolloutEnabled                     bool                                 `json:"rollout_enabled"`
	ReleaseRequired                    bool                                 `json:"release_required"`
	LegacyContractionAllowed           bool                                 `json:"legacy_contraction_allowed"`
	LegacyPromptArchiveAction          string                               `json:"legacy_prompt_archive_action"`
	LocalSchemasParticipatingByDefault bool                                 `json:"local_schemas_participating_by_default"`
	Compatibility                      protocolcontract.CompatibilityReport `json:"compatibility"`
	SourceArtifacts                    []DistributionArtifact               `json:"source_artifacts"`
}

func LoadPublication(path string) (FrozenPublication, error) {
	raw, err := readBoundedFile(path, maxDistributionOutcomeBytes)
	if err != nil {
		return FrozenPublication{}, fmt.Errorf("read Recall study publication: %w", err)
	}
	var publication Publication
	if err := decodeStrictJSON(raw, &publication); err != nil {
		return FrozenPublication{}, fmt.Errorf("decode Recall study publication: %w", err)
	}
	return FrozenPublication{Publication: publication, SHA256: digestHex(raw)}, nil
}

func LoadDistributionOutcome(path, hashPath string) (FrozenDistributionOutcome, error) {
	raw, err := readBoundedFile(path, maxDistributionOutcomeBytes)
	if err != nil {
		return FrozenDistributionOutcome{}, fmt.Errorf("read Recall distribution outcome: %w", err)
	}
	wantRaw, err := readBoundedFile(hashPath, 4096)
	if err != nil {
		return FrozenDistributionOutcome{}, fmt.Errorf("read Recall distribution outcome hash: %w", err)
	}
	want := strings.Fields(string(wantRaw))
	if len(want) == 0 || !validHexDigest(want[0], sha256.Size) {
		return FrozenDistributionOutcome{}, fmt.Errorf("Recall distribution outcome hash sidecar is invalid")
	}
	actual := digestHex(raw)
	if actual != strings.ToLower(want[0]) {
		return FrozenDistributionOutcome{}, fmt.Errorf("Recall distribution outcome hash mismatch: got %s, want %s", actual, want[0])
	}
	var outcome DistributionOutcome
	if err := decodeStrictJSON(raw, &outcome); err != nil {
		return FrozenDistributionOutcome{}, fmt.Errorf("decode Recall distribution outcome: %w", err)
	}
	if actual != FrozenV1DistributionSHA256 {
		return FrozenDistributionOutcome{}, fmt.Errorf("Recall distribution outcome does not match the compiled frozen v1 trust anchor")
	}
	return FrozenDistributionOutcome{Outcome: outcome, SHA256: actual}, nil
}

func (study *Study) VerifyDistributionOutcome(publication FrozenPublication, frozen FrozenDistributionOutcome, sourceRoot string) (DistributionVerification, error) {
	if study == nil {
		return DistributionVerification{}, fmt.Errorf("Recall distribution verification requires a frozen study")
	}
	published := publication.Publication
	outcome := frozen.Outcome
	if published.SchemaVersion != PublicationSchemaVersion || published.StudyID != study.Contract.StudyID ||
		published.StudyVersion != study.Contract.StudyVersion || published.ContractSHA256 != study.Hash ||
		published.CalibrationManifestSHA256 != study.Contract.Cohorts.Calibration.ManifestSHA256 ||
		published.HeldOutManifestSHA256 != study.Contract.Cohorts.HeldOut.ManifestSHA256 || published.RolloutEnabled {
		return DistributionVerification{}, fmt.Errorf("Recall distribution publication identity or rollout state changed")
	}
	if outcome.SchemaVersion != DistributionOutcomeSchemaVersion || outcome.StudyID != published.StudyID ||
		outcome.StudyVersion != published.StudyVersion || outcome.ContractSHA256 != published.ContractSHA256 ||
		outcome.PublicationSHA256 != publication.SHA256 || outcome.Disposition != published.Disposition {
		return DistributionVerification{}, fmt.Errorf("Recall distribution outcome does not bind the frozen publication")
	}
	if outcome.Disposition != DispositionContinueCanary || outcome.Action != DistributionActionPreserve ||
		outcome.RolloutEnabled || outcome.ReleaseRequired || outcome.LegacyContractionAllowed ||
		outcome.LegacyPromptArchiveAction != "preserve" || outcome.LocalSchemasParticipatingByDefault {
		return DistributionVerification{}, fmt.Errorf("continue-canary must preserve the verified tuple without rollout, release, contraction, or local schema participation")
	}
	if !validCompatibilityEvidence(outcome.SelectedCompatibility, study.Contract) ||
		outcome.SelectedCompatibility.Compatibility.Status != protocolcontract.CompatibilityReady ||
		outcome.SelectedCompatibility.Compatibility.ReasonCode != protocolcontract.ReasonLegacyCompatible ||
		!outcome.SelectedCompatibility.Compatibility.Legacy {
		return DistributionVerification{}, fmt.Errorf("Recall distribution selected Compatibility tuple is unsupported or changed")
	}
	if err := verifyDistributionArtifacts(sourceRoot, outcome.SourceArtifacts); err != nil {
		return DistributionVerification{}, err
	}
	if err := verifyDistributionRevisionArtifacts(sourceRoot, study.Contract.SourceRevision, outcome.SourceArtifacts); err != nil {
		return DistributionVerification{}, err
	}

	return DistributionVerification{
		SchemaVersion: DistributionVerificationSchema, StudyID: outcome.StudyID, StudyVersion: outcome.StudyVersion,
		ContractSHA256: outcome.ContractSHA256, PublicationSHA256: outcome.PublicationSHA256, DistributionSHA256: frozen.SHA256,
		SourceRevision: study.Contract.SourceRevision, SourceRevisionVerified: true, SourceArtifactsVerified: true,
		PostInstallReadiness: DistributionPostInstallNotVerified, PostInstallVerificationCommand: "engram setup status codex --json",
		Disposition: outcome.Disposition, Action: outcome.Action, RolloutEnabled: outcome.RolloutEnabled,
		ReleaseRequired: outcome.ReleaseRequired, LegacyContractionAllowed: outcome.LegacyContractionAllowed,
		LegacyPromptArchiveAction:          outcome.LegacyPromptArchiveAction,
		LocalSchemasParticipatingByDefault: outcome.LocalSchemasParticipatingByDefault,
		Compatibility:                      outcome.SelectedCompatibility.Compatibility, SourceArtifacts: append([]DistributionArtifact(nil), outcome.SourceArtifacts...),
	}, nil
}

func verifyDistributionRevisionArtifacts(sourceRoot, revision string, artifacts []DistributionArtifact) error {
	if strings.TrimSpace(sourceRoot) == "" || !validHexDigest(revision, 20) {
		return fmt.Errorf("Recall distribution source revision identity changed")
	}
	if err := exec.Command("git", "-C", sourceRoot, "cat-file", "-e", revision+"^{commit}").Run(); err != nil {
		return fmt.Errorf("verify Recall distribution source revision %s: %w", revision, err)
	}
	for _, artifact := range artifacts {
		raw, err := exec.Command("git", "-C", sourceRoot, "show", revision+":"+artifact.Path).Output()
		if err != nil {
			return fmt.Errorf("read Recall distribution source artifact %s at revision %s: %w", artifact.Path, revision, err)
		}
		if len(raw) > maxDistributionOutcomeBytes {
			return fmt.Errorf("Recall distribution source artifact %s at revision %s exceeds the verification limit", artifact.Path, revision)
		}
		if digestHex(raw) != strings.ToLower(artifact.SHA256) {
			return fmt.Errorf("Recall distribution source artifact %s does not match revision %s", artifact.Path, revision)
		}
	}
	return nil
}

func verifyDistributionArtifacts(sourceRoot string, artifacts []DistributionArtifact) error {
	if strings.TrimSpace(sourceRoot) == "" || len(artifacts) != len(requiredDistributionArtifacts) {
		return fmt.Errorf("Recall distribution requires one exact source artifact set")
	}
	seen := make(map[string]bool, len(artifacts))
	for _, artifact := range artifacts {
		wantPath, ok := requiredDistributionArtifacts[artifact.Name]
		if !ok || artifact.Path != wantPath || seen[artifact.Name] || !validHexDigest(artifact.SHA256, sha256.Size) {
			return fmt.Errorf("Recall distribution source artifact identity changed")
		}
		seen[artifact.Name] = true
		path := filepath.Join(sourceRoot, filepath.FromSlash(artifact.Path))
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("inspect Recall distribution source artifact %s: %w", artifact.Path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("Recall distribution source artifact %s is not a regular file", artifact.Path)
		}
		raw, err := readBoundedFile(path, maxDistributionOutcomeBytes)
		if err != nil {
			return fmt.Errorf("read Recall distribution source artifact %s: %w", artifact.Path, err)
		}
		if digestHex(raw) != strings.ToLower(artifact.SHA256) {
			return fmt.Errorf("Recall distribution source artifact %s hash mismatch", artifact.Path)
		}
	}
	return nil
}

func digestHex(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
