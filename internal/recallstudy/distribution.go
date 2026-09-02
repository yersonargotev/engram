package recallstudy

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

const (
	DistributionOutcomeSchemaVersion   = "recall-distribution-outcome-v1"
	DistributionVerificationSchema     = "recall-distribution-verification-v1"
	DistributionActionPreserve         = "preserve_verified_tuple"
	DistributionPostInstallNotVerified = "not_verified"
	DistributionRevisionProofSchema    = "git-object-membership-proof-v1"
	DistributionSourceSnapshotDir      = "source-snapshot"
	FrozenV1DistributionSHA256         = "44970ac08d15030cb0df16b5e909e9db75cec94465980f82751f238c588a7417"
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
	SchemaVersion                      string                    `json:"schema_version"`
	StudyID                            string                    `json:"study_id"`
	StudyVersion                       string                    `json:"study_version"`
	ContractSHA256                     string                    `json:"contract_sha256"`
	PublicationSHA256                  string                    `json:"publication_sha256"`
	Disposition                        string                    `json:"disposition"`
	Action                             string                    `json:"action"`
	RolloutEnabled                     bool                      `json:"rollout_enabled"`
	ReleaseRequired                    bool                      `json:"release_required"`
	LegacyContractionAllowed           bool                      `json:"legacy_contraction_allowed"`
	LegacyPromptArchiveAction          string                    `json:"legacy_prompt_archive_action"`
	LocalSchemasParticipatingByDefault bool                      `json:"local_schemas_participating_by_default"`
	SelectedCompatibility              CompatibilityEvidence     `json:"selected_compatibility"`
	SourceRevisionProof                DistributionRevisionProof `json:"source_revision_proof"`
	SourceArtifacts                    []DistributionArtifact    `json:"source_artifacts"`
}

type DistributionRevisionProof struct {
	SchemaVersion string                          `json:"schema_version"`
	ObjectFormat  string                          `json:"object_format"`
	CommitBase64  string                          `json:"commit_base64"`
	Trees         []DistributionRevisionProofTree `json:"trees"`
}

type DistributionRevisionProofTree struct {
	ObjectID   string `json:"object_id"`
	BodyBase64 string `json:"body_base64"`
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
	snapshotRoot := distributionSourceSnapshotRoot(sourceRoot, outcome.StudyVersion)
	if err := verifyDistributionArtifacts(snapshotRoot, outcome.SourceArtifacts); err != nil {
		return DistributionVerification{}, err
	}
	if err := verifyDistributionRevisionProof(snapshotRoot, study.Contract.SourceRevision, outcome.SourceArtifacts, outcome.SourceRevisionProof); err != nil {
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

func distributionSourceSnapshotRoot(sourceRoot, studyVersion string) string {
	return filepath.Join(sourceRoot, "evals", "recall-study", studyVersion, DistributionSourceSnapshotDir)
}

type gitTreeEntry struct {
	Mode, ObjectID string
}

func verifyDistributionRevisionProof(sourceRoot, revision string, artifacts []DistributionArtifact, proof DistributionRevisionProof) error {
	if strings.TrimSpace(sourceRoot) == "" || !validHexDigest(revision, 20) ||
		proof.SchemaVersion != DistributionRevisionProofSchema || proof.ObjectFormat != "sha1" {
		return fmt.Errorf("Recall distribution source revision proof identity changed")
	}
	commit, err := base64.StdEncoding.DecodeString(proof.CommitBase64)
	if err != nil || gitObjectID("commit", commit) != strings.ToLower(revision) {
		return fmt.Errorf("Recall distribution source revision proof does not match commit %s", revision)
	}
	rootTree, err := gitCommitTree(commit)
	if err != nil {
		return err
	}
	trees := make(map[string][]byte, len(proof.Trees))
	for _, treeProof := range proof.Trees {
		body, decodeErr := base64.StdEncoding.DecodeString(treeProof.BodyBase64)
		objectID := strings.ToLower(treeProof.ObjectID)
		if decodeErr != nil || !validHexDigest(objectID, 20) || gitObjectID("tree", body) != objectID {
			return fmt.Errorf("Recall distribution source revision tree proof is invalid")
		}
		if _, duplicate := trees[objectID]; duplicate {
			return fmt.Errorf("Recall distribution source revision tree proof is duplicated")
		}
		trees[objectID] = body
	}

	usedTrees := make(map[string]bool, len(trees))
	for _, artifact := range artifacts {
		objectID := rootTree
		segments := strings.Split(artifact.Path, "/")
		for index, segment := range segments {
			body, ok := trees[objectID]
			if !ok {
				return fmt.Errorf("Recall distribution source revision proof omits tree for %s", artifact.Path)
			}
			usedTrees[objectID] = true
			entries, parseErr := parseGitTree(body)
			if parseErr != nil {
				return parseErr
			}
			entry, ok := entries[segment]
			if !ok {
				return fmt.Errorf("Recall distribution source revision proof omits %s", artifact.Path)
			}
			if index < len(segments)-1 {
				if entry.Mode != "40000" {
					return fmt.Errorf("Recall distribution source revision proof path %s is not a tree", artifact.Path)
				}
				objectID = entry.ObjectID
				continue
			}
			if entry.Mode != "100644" && entry.Mode != "100755" {
				return fmt.Errorf("Recall distribution source revision proof path %s is not a regular file", artifact.Path)
			}
			raw, readErr := readBoundedFile(filepath.Join(sourceRoot, filepath.FromSlash(artifact.Path)), maxDistributionOutcomeBytes)
			if readErr != nil {
				return fmt.Errorf("read Recall distribution source artifact %s for revision proof: %w", artifact.Path, readErr)
			}
			if gitObjectID("blob", raw) != entry.ObjectID {
				return fmt.Errorf("Recall distribution source artifact %s does not match revision %s", artifact.Path, revision)
			}
		}
	}
	if len(usedTrees) != len(trees) {
		return fmt.Errorf("Recall distribution source revision proof contains an unrelated tree")
	}
	return nil
}

func gitCommitTree(commit []byte) (string, error) {
	lineEnd := bytes.IndexByte(commit, '\n')
	if lineEnd < 0 || !bytes.HasPrefix(commit[:lineEnd], []byte("tree ")) {
		return "", fmt.Errorf("Recall distribution source revision proof commit omits its root tree")
	}
	objectID := string(commit[len("tree "):lineEnd])
	if !validHexDigest(objectID, 20) {
		return "", fmt.Errorf("Recall distribution source revision proof commit root tree is invalid")
	}
	return strings.ToLower(objectID), nil
}

func parseGitTree(body []byte) (map[string]gitTreeEntry, error) {
	entries := make(map[string]gitTreeEntry)
	for offset := 0; offset < len(body); {
		space := bytes.IndexByte(body[offset:], ' ')
		if space <= 0 {
			return nil, fmt.Errorf("Recall distribution source revision tree proof is malformed")
		}
		space += offset
		null := bytes.IndexByte(body[space+1:], 0)
		if null <= 0 {
			return nil, fmt.Errorf("Recall distribution source revision tree proof is malformed")
		}
		null += space + 1
		objectEnd := null + 1 + sha1.Size
		if objectEnd > len(body) {
			return nil, fmt.Errorf("Recall distribution source revision tree proof is malformed")
		}
		mode := string(body[offset:space])
		name := string(body[space+1 : null])
		if name == "" || strings.Contains(name, "/") || !validGitTreeMode(mode) {
			return nil, fmt.Errorf("Recall distribution source revision tree proof entry is invalid")
		}
		if _, duplicate := entries[name]; duplicate {
			return nil, fmt.Errorf("Recall distribution source revision tree proof entry is duplicated")
		}
		entries[name] = gitTreeEntry{Mode: mode, ObjectID: hex.EncodeToString(body[null+1 : objectEnd])}
		offset = objectEnd
	}
	return entries, nil
}

func validGitTreeMode(mode string) bool {
	switch mode {
	case "40000", "100644", "100755", "120000", "160000":
		return true
	default:
		return false
	}
}

func gitObjectID(kind string, body []byte) string {
	hash := sha1.New() // Git commit and tree identity is defined by SHA-1 for this repository.
	_, _ = fmt.Fprintf(hash, "%s %d%c", kind, len(body), byte(0))
	_, _ = hash.Write(body)
	return hex.EncodeToString(hash.Sum(nil))
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
