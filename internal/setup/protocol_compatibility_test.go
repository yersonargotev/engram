package setup

import (
	"fmt"
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
}

func TestProtocolCompatibilityAcceptsVerifiedLegacyTupleDuringExpand(t *testing.T) {
	resetSetupSeams(t)
	bundleRoot := t.TempDir()
	manifestPath := filepath.Join(bundleRoot, "packs", "engram", "pack.json")
	writeStatusTestFile(t, manifestPath, fmt.Sprintf(`{"schema_version":1,"id":"engram","version":%q,"resources":[]}`, legacyManagedPackVersion))
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
	writeStatusTestFile(t, manifestPath, fmt.Sprintf(`{
  "schema_version": 1,
  "id": "engram",
  "version": %q,
  "resources": [
    {"kind":"skill","id":"engram-memory-cli","source":"skills/engram-memory-cli"},
    {"kind":"asset","id":"protocol-contract-v1","source":"assets/protocol-contract-v1.json"}
  ]
}`, currentManagedPackVersion))
	skillPath := filepath.Join(bundleRoot, "skills", "engram-memory-cli", "SKILL.md")
	check := codexStatusCheck("skill", CodexCheckReady, "engram_skill_discovered", "ready",
		codexEvidence("source", "standalone"),
		codexEvidence("path", skillPath),
		codexEvidence("sha256", "current-skill"),
		codexEvidence("version", currentManagedPackVersion),
	)

	malformed := inspectManagedPackProtocolDeclaration([]CodexIntegrationCheck{check})
	report := protocolcontract.Evaluate(malformed, validProtocolDeclaration("3.0.1"), validProtocolDeclaration("0.1.6"))
	if report.ReasonCode != "managed_pack_protocol_range_malformed" {
		t.Fatalf("missing fixture report = %#v", report)
	}

	ambiguous := inspectManagedPackProtocolDeclaration([]CodexIntegrationCheck{check, check})
	report = protocolcontract.Evaluate(ambiguous, validProtocolDeclaration("3.0.1"), validProtocolDeclaration("0.1.6"))
	if report.ReasonCode != "managed_pack_unprovenanced" {
		t.Fatalf("ambiguous pack report = %#v", report)
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
	report := protocolcontract.Evaluate(validProtocolDeclaration("3.2.0"), declaration, validProtocolDeclaration("0.1.6"))
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
