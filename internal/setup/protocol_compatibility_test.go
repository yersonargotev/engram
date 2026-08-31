package setup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

func TestProtocolFixtureMatchesSetupCompatibilityCoordinates(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "assets", "protocol-contract-v1.json"))
	if err != nil {
		t.Fatalf("read Protocol fixture: %v", err)
	}
	fixture, err := protocolcontract.ParseFixture(raw)
	if err != nil {
		t.Fatalf("parse Protocol fixture: %v", err)
	}
	if fixture.Distributions.ManagedPack.Version != currentManagedPackVersion ||
		fixture.Distributions.CodexPlugin.Version != currentCodexPluginVersion ||
		fixture.Legacy.ManagedPack.Version != legacyManagedPackVersion ||
		fixture.Legacy.ManagedPack.SHA256 != legacyManagedPackSHA256 ||
		fixture.Legacy.EngramBinary.Version != legacyEngramBinaryVersion ||
		fixture.Legacy.CodexPlugin.Version != legacyCodexPluginVersion ||
		fixture.Legacy.CodexPlugin.SHA256 != legacyCodexPluginManifestSHA256 ||
		fixture.Distributions.EngramBinary.LegacyCompatible != currentBinaryLegacyCompatible ||
		!fixture.Distributions.ManagedPack.LegacyCompatible || !fixture.Distributions.CodexPlugin.LegacyCompatible {
		t.Fatalf("setup compatibility coordinates drifted from fixture: %#v", fixture)
	}

	assertFileSHA256(t, filepath.Join("..", "..", "pack.json"), currentManagedPackManifestSHA256)
	assertFileSHA256(t, filepath.Join("..", "..", "skills", "engram-memory-cli", "SKILL.md"), currentManagedPackSkillSHA256)
	assertFileSHA256(t, filepath.Join("..", "..", "assets", "protocol-contract-v1.json"), currentManagedPackFixtureSHA256)
	assertFileSHA256(t, filepath.Join("..", "..", "plugin", "codex", ".codex-plugin", "plugin.json"), currentCodexPluginManifestSHA256)
	assertFileSHA256(t, filepath.Join("testdata", "managed-pack-3.2.0.json"), previousManagedPackManifestSHA256)
	assertFileSHA256(t, filepath.Join("testdata", "protocol-contract-v1-pack-3.2.0.json"), previousManagedPackFixtureSHA256)
	assertFileSHA256(t, filepath.Join("testdata", "managed-pack-3.1.2.json"), legacyManagedPackManifestSHA256)
}

func TestProtocolCompatibilityAcceptsVerifiedPreviousTupleDuringExpand(t *testing.T) {
	resetSetupSeams(t)
	bundleRoot := t.TempDir()
	manifestPath := filepath.Join(bundleRoot, "packs", "engram", "pack.json")
	previousManifest, err := os.ReadFile(filepath.Join("testdata", "managed-pack-3.2.0.json"))
	if err != nil {
		t.Fatalf("read previous Pack manifest: %v", err)
	}
	writeStatusTestFile(t, manifestPath, string(previousManifest))
	previousFixture, err := os.ReadFile(filepath.Join("testdata", "protocol-contract-v1-pack-3.2.0.json"))
	if err != nil {
		t.Fatalf("read previous Protocol fixture: %v", err)
	}
	writeStatusTestFile(t, filepath.Join(bundleRoot, "assets", "protocol-contract-v1.json"), string(previousFixture))
	skillPath := filepath.Join(bundleRoot, "skills", "engram-memory-cli", "SKILL.md")
	packCheck := codexStatusCheck("skill", CodexCheckReady, "engram_skill_discovered", "ready",
		codexEvidence("source", "standalone"),
		codexEvidence("path", skillPath),
		codexEvidence("sha256", previousManagedPackSkillSHA256),
		codexEvidence("version", previousManagedPackVersion),
	)
	plugin := codexPluginInspection{
		Check:    codexStatusCheck("plugin", CodexCheckReady, "plugin_ready", "ready"),
		Revision: testReleaseCommit,
		Capabilities: installedCodexPlugin{
			Version:        previousCodexPluginVersion,
			ManifestSHA256: previousCodexPluginManifestSHA256,
		},
	}

	report := inspectCodexProtocolCompatibility("3.0.1", testReleaseCommit, "/opt/engram/bin/engram", []CodexIntegrationCheck{packCheck}, plugin)
	if report.Status != protocolcontract.CompatibilityReady || report.ReasonCode != protocolcontract.ReasonLegacyCompatible || !report.Legacy {
		t.Fatalf("previous compatibility = %#v", report)
	}
	if report.Intersection == nil || *report.Intersection != (protocolcontract.VersionRange{Minimum: 1, Maximum: 1}) {
		t.Fatalf("previous intersection = %#v", report.Intersection)
	}
}

func TestPreviousCodexPluginCoordinateRequiresExactManifest(t *testing.T) {
	plugin := codexPluginInspection{
		Revision: testReleaseCommit,
		Capabilities: installedCodexPlugin{
			Version:        previousCodexPluginVersion,
			ManifestSHA256: strings.Repeat("0", 64),
		},
	}

	declaration := inspectCodexPluginProtocolDeclaration(plugin)
	if declaration.Supported != nil || declaration.Legacy {
		t.Fatalf("untrusted previous plugin asserted Protocol compatibility: %#v", declaration)
	}
}

func TestProtocolCompatibilityAcceptsVerifiedLegacyTupleDuringExpand(t *testing.T) {
	resetSetupSeams(t)
	bundleRoot := t.TempDir()
	manifestPath := filepath.Join(bundleRoot, "packs", "engram", "pack.json")
	legacyManifest, err := os.ReadFile(filepath.Join("testdata", "managed-pack-3.1.2.json"))
	if err != nil {
		t.Fatalf("read legacy Pack manifest: %v", err)
	}
	writeStatusTestFile(t, manifestPath, string(legacyManifest))
	skillPath := filepath.Join(bundleRoot, "skills", "engram-memory-cli", "SKILL.md")
	packCheck := codexStatusCheck("skill", CodexCheckReady, "engram_skill_discovered", "ready",
		codexEvidence("source", "standalone"),
		codexEvidence("path", skillPath),
		codexEvidence("sha256", legacyManagedPackSHA256),
	)
	plugin := codexPluginInspection{
		Check:    codexStatusCheck("plugin", CodexCheckReady, "plugin_ready", "ready"),
		Revision: testReleaseCommit,
		Capabilities: installedCodexPlugin{
			Version:        legacyCodexPluginVersion,
			ManifestSHA256: legacyCodexPluginManifestSHA256,
		},
	}

	report := inspectCodexProtocolCompatibility(legacyEngramBinaryVersion, testReleaseCommit, "/opt/engram/bin/engram", []CodexIntegrationCheck{packCheck}, plugin)
	if report.Status != protocolcontract.CompatibilityReady || report.ReasonCode != protocolcontract.ReasonLegacyCompatible || !report.Legacy {
		t.Fatalf("legacy compatibility = %#v", report)
	}
	if report.Intersection == nil || *report.Intersection != (protocolcontract.VersionRange{Minimum: 1, Maximum: 1}) {
		t.Fatalf("legacy intersection = %#v", report.Intersection)
	}
}

func TestManagedPackProtocolInspectionRejectsMalformedAndAmbiguousMetadata(t *testing.T) {
	resetSetupSeams(t)
	bundleRoot := t.TempDir()
	manifestPath := filepath.Join(bundleRoot, "packs", "engram", "pack.json")
	currentManifest, err := os.ReadFile(filepath.Join("..", "..", "pack.json"))
	if err != nil {
		t.Fatalf("read current Pack manifest: %v", err)
	}
	writeStatusTestFile(t, manifestPath, string(currentManifest))
	skillPath := filepath.Join(bundleRoot, "skills", "engram-memory-cli", "SKILL.md")
	check := codexStatusCheck("skill", CodexCheckReady, "engram_skill_discovered", "ready",
		codexEvidence("source", "standalone"),
		codexEvidence("path", skillPath),
		codexEvidence("sha256", currentManagedPackSkillSHA256),
		codexEvidence("version", currentManagedPackVersion),
	)

	malformed := inspectManagedPackProtocolDeclaration([]CodexIntegrationCheck{check})
	report := protocolcontract.Evaluate(malformed, validProtocolDeclaration("3.0.1"), validProtocolDeclaration(currentCodexPluginVersion))
	if report.ReasonCode != "managed_pack_protocol_range_malformed" {
		t.Fatalf("missing fixture report = %#v", report)
	}

	ambiguous := inspectManagedPackProtocolDeclaration([]CodexIntegrationCheck{check, check})
	report = protocolcontract.Evaluate(ambiguous, validProtocolDeclaration("3.0.1"), validProtocolDeclaration(currentCodexPluginVersion))
	if report.ReasonCode != "managed_pack_unprovenanced" {
		t.Fatalf("ambiguous pack report = %#v", report)
	}
}

func TestManagedPackProtocolInspectionRejectsSelfConsistentCustomizedBundle(t *testing.T) {
	resetSetupSeams(t)
	bundleRoot := t.TempDir()
	manifestPath := filepath.Join(bundleRoot, "packs", "engram", "pack.json")
	currentManifest, err := os.ReadFile(filepath.Join("..", "..", "pack.json"))
	if err != nil {
		t.Fatalf("read current Pack manifest: %v", err)
	}
	writeStatusTestFile(t, manifestPath, string(currentManifest))

	skillPath := filepath.Join(bundleRoot, "skills", "engram-memory-cli", "SKILL.md")
	customSkill := []byte("---\nname: engram-memory-cli\nversion: " + currentManagedPackVersion + "\n---\nCustomized but internally consistent.\n")
	writeStatusTestFile(t, skillPath, string(customSkill))
	customSkillDigest := sha256.Sum256(customSkill)
	customSkillSHA256 := hex.EncodeToString(customSkillDigest[:])

	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "assets", "protocol-contract-v1.json"))
	if err != nil {
		t.Fatalf("read current Protocol fixture: %v", err)
	}
	var customizedFixture map[string]any
	if err := json.Unmarshal(fixtureRaw, &customizedFixture); err != nil {
		t.Fatalf("decode current Protocol fixture: %v", err)
	}
	distributions := customizedFixture["distributions"].(map[string]any)
	managedPack := distributions["managed_pack"].(map[string]any)
	managedPack["skill_sha256"] = customSkillSHA256
	customFixtureRaw, err := json.MarshalIndent(customizedFixture, "", "  ")
	if err != nil {
		t.Fatalf("encode customized Protocol fixture: %v", err)
	}
	writeStatusTestFile(t, filepath.Join(bundleRoot, "assets", "protocol-contract-v1.json"), string(customFixtureRaw)+"\n")

	check := codexStatusCheck("skill", CodexCheckReady, "engram_skill_discovered", "ready",
		codexEvidence("source", "standalone"),
		codexEvidence("path", skillPath),
		codexEvidence("sha256", customSkillSHA256),
		codexEvidence("version", currentManagedPackVersion),
	)
	declaration := inspectManagedPackProtocolDeclaration([]CodexIntegrationCheck{check})
	if declaration.Provenance != "" {
		t.Fatalf("customized Pack asserted provenance: %#v", declaration)
	}
	report := protocolcontract.Evaluate(declaration, validProtocolDeclaration("3.0.1"), validProtocolDeclaration(currentCodexPluginVersion))
	if report.Status != protocolcontract.CompatibilityIncompatible || report.ReasonCode != "managed_pack_unprovenanced" {
		t.Fatalf("customized Pack compatibility = %#v", report)
	}
}

func TestProtocolCompatibilityRejectsPluginRangeWithoutIntersection(t *testing.T) {
	pack := validProtocolDeclaration(currentManagedPackVersion)
	plugin := codexPluginInspection{
		Check:    codexStatusCheck("plugin", CodexCheckReady, "plugin_ready", "ready"),
		Revision: testReleaseCommit,
		Capabilities: installedCodexPlugin{
			Version:        currentCodexPluginVersion,
			ManifestSHA256: "verified-manifest",
			Protocol:       &protocolcontract.VersionRange{Minimum: 2, Maximum: 2},
		},
	}
	report := protocolcontract.Evaluate(pack, validProtocolDeclaration("3.0.1"), inspectCodexPluginProtocolDeclaration(plugin))
	if report.Status != protocolcontract.CompatibilityIncompatible || report.ReasonCode != protocolcontract.ReasonNoProtocolIntersection {
		t.Fatalf("non-overlap report = %#v", report)
	}
}

func TestBinaryProtocolDeclarationFailsClosedOnMalformedSuppliedRevision(t *testing.T) {
	declaration := inspectEngramBinaryProtocolDeclaration("3.0.1", "dev", "/opt/engram/bin/engram")
	report := protocolcontract.Evaluate(validProtocolDeclaration(currentManagedPackVersion), declaration, validProtocolDeclaration(currentCodexPluginVersion))
	if report.Status != protocolcontract.CompatibilityIncompatible || report.ReasonCode != "engram_binary_unprovenanced" {
		t.Fatalf("malformed binary revision report = %#v", report)
	}

	declaration = inspectEngramBinaryProtocolDeclaration("3.0.1", testReleaseCommit, "/opt/engram/bin/engram")
	if !strings.Contains(declaration.Provenance, testReleaseCommit) || !strings.Contains(declaration.Provenance, "/opt/engram/bin/engram") {
		t.Fatalf("attributable binary declaration = %#v", declaration)
	}
}

func validProtocolDeclaration(version string) protocolcontract.Declaration {
	return protocolcontract.Declaration{
		Version:    version,
		Provenance: "verified:" + version,
		Supported:  &protocolcontract.VersionRange{Minimum: 1, Maximum: 1},
	}
}

func assertFileSHA256(t *testing.T, path, want string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	digest := sha256.Sum256(raw)
	if got := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("sha256(%s) = %s, want %s", path, got, want)
	}
}
