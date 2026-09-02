package recallstudy

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestFrozenV1ContinueCanaryDistributionPinsTupleWithoutContracting(t *testing.T) {
	root, _, study, publication, outcome := frozenDistributionFixture(t)
	currentPack, err := os.ReadFile(filepath.Join(root, "pack.json"))
	if err != nil {
		t.Fatal(err)
	}
	if digestForTest(currentPack) == outcome.Outcome.SourceArtifacts[0].SHA256 {
		t.Fatal("test requires the current Managed Pack to differ from the frozen v1 source snapshot")
	}
	verification, err := study.VerifyDistributionOutcome(publication, outcome, root)
	if err != nil {
		t.Fatal(err)
	}
	if !verification.SourceRevisionVerified || !verification.SourceArtifactsVerified ||
		verification.PostInstallReadiness != DistributionPostInstallNotVerified ||
		verification.PostInstallVerificationCommand != "engram setup status codex --json" ||
		verification.Disposition != DispositionContinueCanary ||
		verification.Action != DistributionActionPreserve || verification.ReleaseRequired ||
		verification.RolloutEnabled || verification.LegacyContractionAllowed ||
		verification.LocalSchemasParticipatingByDefault {
		t.Fatalf("continue-canary distribution verification = %#v", verification)
	}
	if verification.Compatibility.Status != "ready" || verification.Compatibility.ReasonCode != "legacy_compatible" ||
		len(verification.Compatibility.Axes) != 4 || verification.Compatibility.Axes[0].Version != "3.3.0" ||
		verification.Compatibility.Axes[1].Version != "3.0.0" || verification.Compatibility.Axes[2].Version != "0.1.7" ||
		verification.Compatibility.Axes[3].Version != "1" {
		t.Fatalf("pinned Compatibility tuple = %#v", verification.Compatibility)
	}
}

func TestDistributionLoadersRejectUntrustedOrMalformedInputs(t *testing.T) {
	t.Run("publication read", func(t *testing.T) {
		if _, err := LoadPublication(filepath.Join(t.TempDir(), "missing.json")); err == nil || !strings.Contains(err.Error(), "read Recall study publication") {
			t.Fatalf("LoadPublication error = %v", err)
		}
	})
	t.Run("publication JSON", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "publication.json")
		if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPublication(path); err == nil || !strings.Contains(err.Error(), "decode Recall study publication") {
			t.Fatalf("LoadPublication error = %v", err)
		}
	})

	tests := []struct {
		name, body, sidecar, want string
	}{
		{name: "missing outcome", sidecar: strings.Repeat("0", 64), want: "read Recall distribution outcome"},
		{name: "missing sidecar", body: `{}`, want: "read Recall distribution outcome hash"},
		{name: "invalid sidecar", body: `{}`, sidecar: "not-a-digest", want: "hash sidecar is invalid"},
		{name: "hash mismatch", body: `{}`, sidecar: strings.Repeat("0", 64), want: "hash mismatch"},
		{name: "malformed JSON", body: `{`, sidecar: digestForTest([]byte(`{`)), want: "decode Recall distribution outcome"},
		{name: "trust anchor", body: `{}`, sidecar: digestForTest([]byte(`{}`)), want: "compiled frozen v1 trust anchor"},
	}
	for _, test := range tests {
		t.Run("distribution "+test.name, func(t *testing.T) {
			dir := t.TempDir()
			outcome := filepath.Join(dir, "distribution.json")
			sidecar := filepath.Join(dir, "distribution.sha256")
			if test.name != "missing outcome" {
				if err := os.WriteFile(outcome, []byte(test.body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if test.name != "missing sidecar" {
				if err := os.WriteFile(sidecar, []byte(test.sidecar+"  distribution.json\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := LoadDistributionOutcome(outcome, sidecar); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadDistributionOutcome error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestVerifyDistributionOutcomeRejectsReinterpretationAndArtifactDrift(t *testing.T) {
	root, _, study, publication, frozen := frozenDistributionFixture(t)
	tests := []struct {
		name   string
		mutate func(*DistributionOutcome)
	}{
		{name: "outcome schema", mutate: func(outcome *DistributionOutcome) { outcome.SchemaVersion = "changed" }},
		{name: "outcome study", mutate: func(outcome *DistributionOutcome) { outcome.StudyID = "changed" }},
		{name: "outcome version", mutate: func(outcome *DistributionOutcome) { outcome.StudyVersion = "changed" }},
		{name: "outcome contract", mutate: func(outcome *DistributionOutcome) { outcome.ContractSHA256 = strings.Repeat("0", 64) }},
		{name: "outcome publication", mutate: func(outcome *DistributionOutcome) { outcome.PublicationSHA256 = strings.Repeat("0", 64) }},
		{name: "disposition", mutate: func(outcome *DistributionOutcome) { outcome.Disposition = "general_availability" }},
		{name: "action", mutate: func(outcome *DistributionOutcome) { outcome.Action = "release" }},
		{name: "rollout", mutate: func(outcome *DistributionOutcome) { outcome.RolloutEnabled = true }},
		{name: "release", mutate: func(outcome *DistributionOutcome) { outcome.ReleaseRequired = true }},
		{name: "legacy contraction", mutate: func(outcome *DistributionOutcome) { outcome.LegacyContractionAllowed = true }},
		{name: "legacy archive", mutate: func(outcome *DistributionOutcome) { outcome.LegacyPromptArchiveAction = "delete" }},
		{name: "local schema participation", mutate: func(outcome *DistributionOutcome) { outcome.LocalSchemasParticipatingByDefault = true }},
		{name: "Compatibility revisions", mutate: func(outcome *DistributionOutcome) {
			outcome.SelectedCompatibility.Revisions.ManagedPack.Version = "changed"
		}},
		{name: "Compatibility status", mutate: func(outcome *DistributionOutcome) {
			outcome.SelectedCompatibility.Compatibility.Status = "incompatible"
		}},
		{name: "Compatibility reason", mutate: func(outcome *DistributionOutcome) { outcome.SelectedCompatibility.Compatibility.ReasonCode = "changed" }},
		{name: "Compatibility legacy", mutate: func(outcome *DistributionOutcome) { outcome.SelectedCompatibility.Compatibility.Legacy = false }},
		{name: "artifact drift", mutate: func(outcome *DistributionOutcome) { outcome.SourceArtifacts[0].SHA256 = strings.Repeat("0", 64) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := frozen
			changed.Outcome = cloneDistributionOutcome(t, frozen.Outcome)
			test.mutate(&changed.Outcome)
			if _, err := study.VerifyDistributionOutcome(publication, changed, root); err == nil {
				t.Fatal("mutated continue-canary distribution verified")
			}
		})
	}
}

func TestVerifyDistributionOutcomeRejectsPublicationDriftAndMissingStudy(t *testing.T) {
	root, _, study, publication, frozen := frozenDistributionFixture(t)
	if _, err := (*Study)(nil).VerifyDistributionOutcome(publication, frozen, root); err == nil {
		t.Fatal("nil study verified")
	}
	tests := []struct {
		name   string
		mutate func(*Publication)
	}{
		{name: "schema", mutate: func(publication *Publication) { publication.SchemaVersion = "changed" }},
		{name: "study", mutate: func(publication *Publication) { publication.StudyID = "changed" }},
		{name: "version", mutate: func(publication *Publication) { publication.StudyVersion = "changed" }},
		{name: "contract", mutate: func(publication *Publication) { publication.ContractSHA256 = strings.Repeat("0", 64) }},
		{name: "calibration", mutate: func(publication *Publication) { publication.CalibrationManifestSHA256 = strings.Repeat("0", 64) }},
		{name: "held out", mutate: func(publication *Publication) { publication.HeldOutManifestSHA256 = strings.Repeat("0", 64) }},
		{name: "rollout", mutate: func(publication *Publication) { publication.RolloutEnabled = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := publication
			test.mutate(&changed.Publication)
			if _, err := study.VerifyDistributionOutcome(changed, frozen, root); err == nil {
				t.Fatal("changed publication verified")
			}
		})
	}
	standaloneRoot := t.TempDir()
	standaloneSnapshotRoot := distributionSourceSnapshotRoot(standaloneRoot, frozen.Outcome.StudyVersion)
	copyDistributionArtifacts(t, distributionSourceSnapshotRoot(root, frozen.Outcome.StudyVersion), standaloneSnapshotRoot, frozen.Outcome.SourceArtifacts)
	verification, err := study.VerifyDistributionOutcome(publication, frozen, standaloneRoot)
	if err != nil || !verification.SourceRevisionVerified {
		t.Fatalf("self-contained source revision proof failed outside a full Git clone: verification=%#v err=%v", verification, err)
	}
}

func TestVerifyDistributionOutcomeRejectsFrozenSnapshotDrift(t *testing.T) {
	root, _, study, publication, frozen := frozenDistributionFixture(t)
	temporaryRoot := t.TempDir()
	snapshotRoot := distributionSourceSnapshotRoot(temporaryRoot, frozen.Outcome.StudyVersion)
	copyDistributionArtifacts(t, distributionSourceSnapshotRoot(root, frozen.Outcome.StudyVersion), snapshotRoot, frozen.Outcome.SourceArtifacts)

	packPath := filepath.Join(snapshotRoot, "pack.json")
	if err := os.WriteFile(packPath, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := study.VerifyDistributionOutcome(publication, frozen, temporaryRoot); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("frozen snapshot drift error = %v", err)
	}
}

func TestVerifyDistributionArtifactsRejectsIncompleteOrInvalidIdentity(t *testing.T) {
	root, _, _, _, frozen := frozenDistributionFixture(t)
	snapshotRoot := distributionSourceSnapshotRoot(root, frozen.Outcome.StudyVersion)
	artifacts := frozen.Outcome.SourceArtifacts
	tests := []struct {
		name   string
		root   string
		mutate func([]DistributionArtifact) []DistributionArtifact
	}{
		{name: "blank root", root: " "},
		{name: "missing", root: snapshotRoot, mutate: func(items []DistributionArtifact) []DistributionArtifact { return items[:len(items)-1] }},
		{name: "unknown name", root: snapshotRoot, mutate: mutateArtifact(0, func(item *DistributionArtifact) { item.Name = "unknown" })},
		{name: "wrong path", root: snapshotRoot, mutate: mutateArtifact(0, func(item *DistributionArtifact) { item.Path = "README.md" })},
		{name: "duplicate", root: snapshotRoot, mutate: mutateArtifact(1, func(item *DistributionArtifact) { *item = artifacts[0] })},
		{name: "invalid digest", root: snapshotRoot, mutate: mutateArtifact(0, func(item *DistributionArtifact) { item.SHA256 = "invalid" })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			items := append([]DistributionArtifact(nil), artifacts...)
			if test.mutate != nil {
				items = test.mutate(items)
			}
			if err := verifyDistributionArtifacts(test.root, items); err == nil {
				t.Fatal("invalid artifact identity verified")
			}
		})
	}

	temporaryRoot := t.TempDir()
	copyDistributionArtifacts(t, snapshotRoot, temporaryRoot, artifacts)
	changed := append([]DistributionArtifact(nil), artifacts...)
	changed[0].SHA256 = strings.Repeat("0", 64)
	if err := verifyDistributionArtifacts(temporaryRoot, changed); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("artifact drift error = %v", err)
	}
	oversized := filepath.Join(temporaryRoot, filepath.FromSlash(artifacts[0].Path))
	if err := os.WriteFile(oversized, make([]byte, maxDistributionOutcomeBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyDistributionArtifacts(temporaryRoot, artifacts); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized artifact error = %v", err)
	}
}

func TestVerifyDistributionRevisionProofWorksWithoutGitAndRejectsDrift(t *testing.T) {
	root, _, study, _, frozen := frozenDistributionFixture(t)
	artifacts := frozen.Outcome.SourceArtifacts
	proof := frozen.Outcome.SourceRevisionProof
	standaloneRoot := t.TempDir()
	copyDistributionArtifacts(t, distributionSourceSnapshotRoot(root, frozen.Outcome.StudyVersion), standaloneRoot, artifacts)
	if err := verifyDistributionRevisionProof(standaloneRoot, study.Contract.SourceRevision, artifacts, proof); err != nil {
		t.Fatalf("self-contained revision proof = %v", err)
	}

	tests := []struct {
		name     string
		root     string
		revision string
		mutate   func(*DistributionRevisionProof)
	}{
		{name: "blank root", root: " ", revision: study.Contract.SourceRevision},
		{name: "malformed revision", root: standaloneRoot, revision: "invalid"},
		{name: "schema", root: standaloneRoot, revision: study.Contract.SourceRevision, mutate: func(proof *DistributionRevisionProof) { proof.SchemaVersion = "changed" }},
		{name: "object format", root: standaloneRoot, revision: study.Contract.SourceRevision, mutate: func(proof *DistributionRevisionProof) { proof.ObjectFormat = "sha256" }},
		{name: "commit base64", root: standaloneRoot, revision: study.Contract.SourceRevision, mutate: func(proof *DistributionRevisionProof) { proof.CommitBase64 = "!" }},
		{name: "commit identity", root: standaloneRoot, revision: strings.Repeat("0", 40)},
		{name: "tree base64", root: standaloneRoot, revision: study.Contract.SourceRevision, mutate: func(proof *DistributionRevisionProof) { proof.Trees[0].BodyBase64 = "!" }},
		{name: "tree identity", root: standaloneRoot, revision: study.Contract.SourceRevision, mutate: func(proof *DistributionRevisionProof) { proof.Trees[0].ObjectID = strings.Repeat("0", 40) }},
		{name: "duplicate tree", root: standaloneRoot, revision: study.Contract.SourceRevision, mutate: func(proof *DistributionRevisionProof) { proof.Trees = append(proof.Trees, proof.Trees[0]) }},
		{name: "missing tree", root: standaloneRoot, revision: study.Contract.SourceRevision, mutate: func(proof *DistributionRevisionProof) { proof.Trees = proof.Trees[1:] }},
		{name: "unrelated tree", root: standaloneRoot, revision: study.Contract.SourceRevision, mutate: func(proof *DistributionRevisionProof) {
			proof.Trees = append(proof.Trees, DistributionRevisionProofTree{ObjectID: gitObjectID("tree", nil), BodyBase64: ""})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := cloneRevisionProof(t, proof)
			if test.mutate != nil {
				test.mutate(&changed)
			}
			if err := verifyDistributionRevisionProof(test.root, test.revision, artifacts, changed); err == nil {
				t.Fatal("invalid source revision proof verified")
			}
		})
	}

	packPath := filepath.Join(standaloneRoot, "pack.json")
	if err := os.WriteFile(packPath, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyDistributionRevisionProof(standaloneRoot, study.Contract.SourceRevision, artifacts, proof); err == nil || !strings.Contains(err.Error(), "does not match revision") {
		t.Fatalf("revision artifact drift error = %v", err)
	}
}

func TestGitRevisionProofParsersRejectMalformedObjects(t *testing.T) {
	if _, err := gitCommitTree([]byte("parent 0000000000000000000000000000000000000000\n")); err == nil {
		t.Fatal("commit without root tree parsed")
	}
	if _, err := gitCommitTree([]byte("tree invalid\n")); err == nil {
		t.Fatal("commit with invalid root tree parsed")
	}
	for _, body := range [][]byte{[]byte("bad"), append([]byte("100644 file\x00"), make([]byte, 19)...), append([]byte("999999 file\x00"), make([]byte, 20)...)} {
		if _, err := parseGitTree(body); err == nil {
			t.Fatalf("malformed tree parsed: %q", body)
		}
	}
}

func TestVerifyDistributionRevisionProofRejectsSemanticTreeDrift(t *testing.T) {
	fileBody := []byte("verified source artifact\n")
	blobID := gitObjectID("blob", fileBody)
	tests := []struct {
		name         string
		artifactPath string
		rootEntries  []syntheticGitTreeEntry
		want         string
	}{
		{name: "intermediate blob", artifactPath: "dir/file", rootEntries: []syntheticGitTreeEntry{{Mode: "100644", Name: "dir", ObjectID: blobID}}, want: "is not a tree"},
		{name: "terminal symlink", artifactPath: "file", rootEntries: []syntheticGitTreeEntry{{Mode: "120000", Name: "file", ObjectID: blobID}}, want: "is not a regular file"},
		{name: "terminal gitlink", artifactPath: "file", rootEntries: []syntheticGitTreeEntry{{Mode: "160000", Name: "file", ObjectID: strings.Repeat("1", 40)}}, want: "is not a regular file"},
		{name: "absent path", artifactPath: "file", rootEntries: []syntheticGitTreeEntry{{Mode: "100644", Name: "other", ObjectID: blobID}}, want: "omits file"},
		{name: "duplicate name", artifactPath: "file", rootEntries: []syntheticGitTreeEntry{{Mode: "100644", Name: "file", ObjectID: blobID}, {Mode: "100644", Name: "file", ObjectID: blobID}}, want: "entry is duplicated"},
		{name: "missing working file", artifactPath: "file", rootEntries: []syntheticGitTreeEntry{{Mode: "100644", Name: "file", ObjectID: blobID}}, want: "read Recall distribution source artifact"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			revision, proof := syntheticRevisionProof(t, test.rootEntries)
			artifacts := []DistributionArtifact{{Name: "synthetic", Path: test.artifactPath, SHA256: digestForTest(fileBody)}}
			if err := verifyDistributionRevisionProof(root, revision, artifacts, proof); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("semantic tree drift error = %v, want %q", err, test.want)
			}
		})
	}

	t.Run("regular file", func(t *testing.T) {
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, "file"), fileBody, 0o600); err != nil {
			t.Fatal(err)
		}
		revision, proof := syntheticRevisionProof(t, []syntheticGitTreeEntry{{Mode: "100644", Name: "file", ObjectID: blobID}})
		artifacts := []DistributionArtifact{{Name: "synthetic", Path: "file", SHA256: digestForTest(fileBody)}}
		if err := verifyDistributionRevisionProof(root, revision, artifacts, proof); err != nil {
			t.Fatalf("synthetic regular file proof = %v", err)
		}
	})
}

type syntheticGitTreeEntry struct {
	Mode, Name, ObjectID string
}

func syntheticRevisionProof(t *testing.T, entries []syntheticGitTreeEntry) (string, DistributionRevisionProof) {
	t.Helper()
	var treeBody []byte
	for _, entry := range entries {
		objectID, err := hex.DecodeString(entry.ObjectID)
		if err != nil || len(objectID) != 20 {
			t.Fatalf("synthetic object ID %q: %v", entry.ObjectID, err)
		}
		treeBody = append(treeBody, []byte(entry.Mode+" "+entry.Name)...)
		treeBody = append(treeBody, 0)
		treeBody = append(treeBody, objectID...)
	}
	treeID := gitObjectID("tree", treeBody)
	commitBody := []byte("tree " + treeID + "\n\nsynthetic revision proof\n")
	return gitObjectID("commit", commitBody), DistributionRevisionProof{
		SchemaVersion: DistributionRevisionProofSchema,
		ObjectFormat:  "sha1",
		CommitBase64:  base64.StdEncoding.EncodeToString(commitBody),
		Trees: []DistributionRevisionProofTree{{
			ObjectID: treeID, BodyBase64: base64.StdEncoding.EncodeToString(treeBody),
		}},
	}
}

func TestVerifyDistributionOutcomeRejectsSymlinkedSourceArtifactWithoutChangingIt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating an unprivileged symlink is not portable on Windows")
	}
	root, _, _, _, frozen := frozenDistributionFixture(t)
	snapshotRoot := distributionSourceSnapshotRoot(root, frozen.Outcome.StudyVersion)
	temporaryRoot := t.TempDir()
	copyDistributionArtifacts(t, snapshotRoot, temporaryRoot, frozen.Outcome.SourceArtifacts)
	pluginPath := filepath.Join(temporaryRoot, "plugin", "codex", ".codex-plugin", "plugin.json")
	if err := os.Remove(pluginPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(snapshotRoot, "plugin", "codex", ".codex-plugin", "plugin.json"), pluginPath); err != nil {
		t.Fatal(err)
	}
	if err := verifyDistributionArtifacts(temporaryRoot, frozen.Outcome.SourceArtifacts); err == nil {
		t.Fatal("symlinked source artifact verified")
	}
	info, err := os.Lstat(pluginPath)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("verification changed symlinked state: info=%v err=%v", info, err)
	}
}

func frozenDistributionFixture(t *testing.T) (string, string, *Study, FrozenPublication, FrozenDistributionOutcome) {
	t.Helper()
	root := filepath.Join("..", "..")
	studyRoot := filepath.Join(root, "evals", "recall-study", "v1")
	study, err := Load(filepath.Join(studyRoot, "contract.json"), filepath.Join(studyRoot, "contract.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	publication, err := LoadPublication(filepath.Join(studyRoot, "publication.json"))
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := LoadDistributionOutcome(filepath.Join(studyRoot, "distribution.json"), filepath.Join(studyRoot, "distribution.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	return root, studyRoot, study, publication, frozen
}

func copyDistributionArtifacts(t *testing.T, sourceRoot, destinationRoot string, artifacts []DistributionArtifact) {
	t.Helper()
	for _, artifact := range artifacts {
		source := filepath.Join(sourceRoot, filepath.FromSlash(artifact.Path))
		destination := filepath.Join(destinationRoot, filepath.FromSlash(artifact.Path))
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(destination, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func mutateArtifact(index int, mutate func(*DistributionArtifact)) func([]DistributionArtifact) []DistributionArtifact {
	return func(items []DistributionArtifact) []DistributionArtifact {
		mutate(&items[index])
		return items
	}
}

func digestForTest(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func cloneDistributionOutcome(t *testing.T, outcome DistributionOutcome) DistributionOutcome {
	t.Helper()
	raw, err := json.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	var cloned DistributionOutcome
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func cloneRevisionProof(t *testing.T, proof DistributionRevisionProof) DistributionRevisionProof {
	t.Helper()
	raw, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	var cloned DistributionRevisionProof
	if err := json.Unmarshal(raw, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}
