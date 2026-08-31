package setup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

const (
	currentManagedPackVersion        = "3.2.0"
	currentManagedPackManifestSHA256 = "eea49916d97af47f8cf61ab7d40c7b11b1858c65d740dd663ee756766e064a6c"
	currentManagedPackSkillSHA256    = "817248be55234f3c7dbb31f4890886e251c1e430cc61d35dd60ef5215e8ec6a6"
	currentManagedPackFixtureSHA256  = "ee15977473bff7af95b92c68da61eb2bc6e9aafeb277a29421ecb48d166981e7"
	currentCodexPluginVersion        = "0.1.6"
	legacyManagedPackVersion         = "3.1.2"
	legacyManagedPackManifestSHA256  = "ce85707c61e7f1c59dddfb09cb8fdc4f460e62d48d9396dca9297da4552a2f09"
	legacyManagedPackSHA256          = "2b570eda04db214ed496d14c09d287f5b1c0750dc2f0ff1928b8b314e257e20c"
	legacyEngramBinaryVersion        = "3.0.0"
	legacyCodexPluginVersion         = "0.1.5"
	legacyCodexPluginManifestSHA256  = "b3ee231a8ba88ae54b018365af392d1976b1e8a02c2ea8c60d9e4464f824c4ed"
	currentBinaryLegacyCompatible    = true
)

type trustedManagedPackCoordinate struct {
	manifestSHA256 string
	skillSHA256    string
	fixtureSHA256  string
	legacy         bool
}

func trustedManagedPack(version string) (trustedManagedPackCoordinate, bool) {
	switch version {
	case currentManagedPackVersion:
		return trustedManagedPackCoordinate{
			manifestSHA256: currentManagedPackManifestSHA256,
			skillSHA256:    currentManagedPackSkillSHA256,
			fixtureSHA256:  currentManagedPackFixtureSHA256,
		}, true
	case legacyManagedPackVersion:
		return trustedManagedPackCoordinate{
			manifestSHA256: legacyManagedPackManifestSHA256,
			skillSHA256:    legacyManagedPackSHA256,
			legacy:         true,
		}, true
	default:
		return trustedManagedPackCoordinate{}, false
	}
}

func inspectCodexProtocolCompatibility(runningVersion, runningRevision, runningPath string, skillChecks []CodexIntegrationCheck, plugin codexPluginInspection) protocolcontract.CompatibilityReport {
	pack := inspectManagedPackProtocolDeclaration(skillChecks)
	binary := inspectEngramBinaryProtocolDeclaration(runningVersion, runningRevision, runningPath)
	return protocolcontract.Evaluate(pack, binary, inspectCodexPluginProtocolDeclaration(plugin))
}

func inspectEngramBinaryProtocolDeclaration(runningVersion, runningRevision, runningPath string) protocolcontract.Declaration {
	binary := protocolcontract.Declaration{
		Version: strings.TrimSpace(runningVersion),
		Legacy:  currentBinaryLegacyCompatible,
	}
	binaryRange := protocolcontract.BinarySupportedRange()
	binary.Supported = &binaryRange
	if strings.TrimSpace(runningPath) != "" {
		binary.Provenance = "executable:" + filepath.Clean(runningPath)
		if strings.TrimSpace(runningRevision) != "" {
			normalizedRevision, err := normalizeGitCommit(strings.TrimSpace(runningRevision))
			if err != nil {
				binary.Provenance = ""
			} else {
				binary.Provenance = fmt.Sprintf("repository:https://github.com/yersonargotev/engram.git#revision:%s;executable:%s", normalizedRevision, filepath.Clean(runningPath))
			}
		}
	}
	return binary
}

func inspectManagedPackProtocolDeclaration(skillChecks []CodexIntegrationCheck) protocolcontract.Declaration {
	var candidates []protocolcontract.Declaration
	for _, check := range skillChecks {
		if codexStatusEvidenceValue(check, "source") != "standalone" {
			continue
		}
		path := codexStatusEvidenceValue(check, "path")
		if filepath.Base(path) != "SKILL.md" || filepath.Base(filepath.Dir(path)) != "engram-memory-cli" || filepath.Base(filepath.Dir(filepath.Dir(path))) != "skills" {
			continue
		}
		bundleRoot := filepath.Dir(filepath.Dir(filepath.Dir(path)))
		manifestPath := filepath.Join(bundleRoot, "packs", "engram", "pack.json")
		manifestRaw, err := readFileFn(manifestPath)
		if err != nil {
			continue
		}
		var manifest struct {
			SchemaVersion int    `json:"schema_version"`
			ID            string `json:"id"`
			Version       string `json:"version"`
			Resources     []struct {
				Kind   string `json:"kind"`
				ID     string `json:"id"`
				Source string `json:"source"`
			} `json:"resources"`
		}
		if json.Unmarshal(manifestRaw, &manifest) != nil || manifest.SchemaVersion != 1 || manifest.ID != "engram" || strings.TrimSpace(manifest.Version) == "" {
			candidates = append(candidates, protocolcontract.Declaration{Version: "invalid"})
			continue
		}

		manifestDigest := sha256.Sum256(manifestRaw)
		manifestSHA256 := hex.EncodeToString(manifestDigest[:])
		trusted, trustedVersion := trustedManagedPack(manifest.Version)
		declaration := protocolcontract.Declaration{
			Version: manifest.Version,
		}
		skillHash := codexStatusEvidenceValue(check, "sha256")
		if !trustedVersion || manifestSHA256 != trusted.manifestSHA256 || skillHash != trusted.skillSHA256 {
			candidates = append(candidates, declaration)
			continue
		}
		declaration.Provenance = fmt.Sprintf(
			"distribution_authority:https://github.com/yersonargotev/engram;manifest:%s#sha256:%s",
			filepath.Clean(manifestPath), manifestSHA256,
		)
		if trusted.legacy {
			declaration.Supported = protocolV1Range()
			declaration.Legacy = true
			candidates = append(candidates, declaration)
			continue
		}

		assetSource := ""
		for _, resource := range manifest.Resources {
			switch {
			case resource.Kind == "skill" && resource.ID == "engram-memory-cli" && resource.Source != "skills/engram-memory-cli":
				declaration.Provenance = ""
			case resource.Kind == "asset" && resource.ID == "protocol-contract-v1":
				assetSource = filepath.FromSlash(resource.Source)
			}
		}
		if codexStatusEvidenceValue(check, "version") != manifest.Version || assetSource != filepath.FromSlash("assets/protocol-contract-v1.json") {
			declaration.Provenance = ""
			candidates = append(candidates, declaration)
			continue
		}
		fixtureRaw, err := readFileFn(filepath.Join(bundleRoot, assetSource))
		if err != nil {
			candidates = append(candidates, declaration)
			continue
		}
		fixtureDigest := sha256.Sum256(fixtureRaw)
		if hex.EncodeToString(fixtureDigest[:]) != trusted.fixtureSHA256 {
			declaration.Provenance = ""
			candidates = append(candidates, declaration)
			continue
		}
		fixture, err := protocolcontract.ParseFixture(fixtureRaw)
		if err != nil || fixture.Distributions.ManagedPack.Version != manifest.Version {
			declaration.Provenance = ""
			candidates = append(candidates, declaration)
			continue
		}
		if skillHash != fixture.Distributions.ManagedPack.SkillSHA256 || skillHash != trusted.skillSHA256 {
			declaration.Provenance = ""
			candidates = append(candidates, declaration)
			continue
		}
		rangeValue := fixture.Distributions.ManagedPack.Supported
		declaration.Supported = &rangeValue
		declaration.Legacy = fixture.Distributions.ManagedPack.LegacyCompatible
		candidates = append(candidates, declaration)
	}

	switch len(candidates) {
	case 0:
		return protocolcontract.Declaration{}
	case 1:
		return candidates[0]
	default:
		return protocolcontract.Declaration{Version: "ambiguous"}
	}
}

func inspectCodexPluginProtocolDeclaration(plugin codexPluginInspection) protocolcontract.Declaration {
	if strings.TrimSpace(plugin.Capabilities.Version) == "" {
		return protocolcontract.Declaration{}
	}
	declaration := protocolcontract.Declaration{
		Version: plugin.Capabilities.Version,
		Provenance: fmt.Sprintf("repository:https://github.com/yersonargotev/engram.git#revision:%s;manifest_sha256:%s",
			plugin.Revision, plugin.Capabilities.ManifestSHA256),
		Supported: plugin.Capabilities.Protocol,
		Legacy:    plugin.Capabilities.ProtocolLegacy,
	}
	if declaration.Version == legacyCodexPluginVersion &&
		plugin.Capabilities.ManifestSHA256 == legacyCodexPluginManifestSHA256 {
		declaration.Supported = protocolV1Range()
		declaration.Legacy = true
	}
	return declaration
}

func protocolV1Range() *protocolcontract.VersionRange {
	return &protocolcontract.VersionRange{Minimum: protocolcontract.Version, Maximum: protocolcontract.Version}
}
