// Package protocolcontract owns the host-neutral, machine-verifiable Protocol
// contract shared by Engram distributions and adapters.
package protocolcontract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
)

const (
	// Version is the monotonic Protocol contract version implemented by Core.
	Version = 1

	CheckpointPersistence        = "local_only"
	RecallDefault                = "agent_initiated"
	CaptureDefault               = "disabled"
	RecallInitialCandidateLimit  = 5
	RecallFollowupCandidateLimit = 10
	RecallCandidateUTF8Budget    = 4 * 1024
	RecallContentUTF8Limit       = 16 * 1024
	RecallContinuationMode       = "explicit_position"

	BinaryMinimumProtocolVersion = 1
	BinaryMaximumProtocolVersion = 1
)

var (
	identityFields         = []string{"host", "session_id", "root_turn_id"}
	checkpointDispositions = []string{"saved", "needs_review", "skipped"}
	minimumTools           = []string{"mem_current_project", "mem_search", "mem_get_observation", "mem_checkpoint", "mem_checkpoint_status"}
)

func IdentityFields() []string         { return slices.Clone(identityFields) }
func CheckpointDispositions() []string { return slices.Clone(checkpointDispositions) }
func MinimumTools() []string           { return slices.Clone(minimumTools) }

// VersionRange is an inclusive range of Protocol contract versions.
type VersionRange struct {
	Minimum int `json:"minimum"`
	Maximum int `json:"maximum"`
}

// BinarySupportedRange is the Protocol range declared by this Engram binary.
func BinarySupportedRange() VersionRange {
	return VersionRange{Minimum: BinaryMinimumProtocolVersion, Maximum: BinaryMaximumProtocolVersion}
}

// Declaration is one distributable's attributable Protocol support claim.
// Legacy marks an exact, verified expand-path declaration that predates
// explicit range metadata.
type Declaration struct {
	Version    string        `json:"version"`
	Provenance string        `json:"provenance"`
	Supported  *VersionRange `json:"supported_protocol,omitempty"`
	Legacy     bool          `json:"legacy,omitempty"`
}

type CompatibilityStatus string

const (
	CompatibilityReady        CompatibilityStatus = "ready"
	CompatibilityIncompatible CompatibilityStatus = "incompatible"
)

const (
	AxisManagedPack      = "managed_pack"
	AxisEngramBinary     = "engram_binary"
	AxisCodexPlugin      = "codex_plugin"
	AxisProtocolContract = "protocol_contract"

	ReasonProtocolCompatible     = "protocol_compatible"
	ReasonLegacyCompatible       = "legacy_compatible"
	ReasonNoProtocolIntersection = "no_protocol_intersection"
)

// CompatibilityAxis is one independently versioned side of the compatibility
// tuple. Protocol contract is a Core axis; the other three are distributions.
type CompatibilityAxis struct {
	Name       string              `json:"name"`
	Version    string              `json:"version"`
	Provenance string              `json:"provenance"`
	Supported  *VersionRange       `json:"supported_protocol,omitempty"`
	Status     CompatibilityStatus `json:"status"`
	ReasonCode string              `json:"reason_code"`
	Legacy     bool                `json:"legacy,omitempty"`
}

// CompatibilityReport is the deterministic result for one exact tuple.
type CompatibilityReport struct {
	SchemaVersion string              `json:"schema_version"`
	Status        CompatibilityStatus `json:"status"`
	ReasonCode    string              `json:"reason_code"`
	Reason        string              `json:"reason"`
	Axes          []CompatibilityAxis `json:"axes"`
	Intersection  *VersionRange       `json:"protocol_intersection,omitempty"`
	Legacy        bool                `json:"legacy,omitempty"`
}

// Evaluate validates all four axes and computes their Protocol range
// intersection. Distribution versions are deliberately not compared for
// equality: compatibility is established by attributable range overlap.
func Evaluate(managedPack, binary, plugin Declaration) CompatibilityReport {
	inputs := []struct {
		name        string
		declaration Declaration
	}{
		{name: AxisManagedPack, declaration: managedPack},
		{name: AxisEngramBinary, declaration: binary},
		{name: AxisCodexPlugin, declaration: plugin},
	}

	axes := make([]CompatibilityAxis, 0, 4)
	legacy := false
	for _, input := range inputs {
		axis := evaluateDeclaration(input.name, input.declaration)
		axes = append(axes, axis)
		legacy = legacy || input.declaration.Legacy
	}
	contractRange := &VersionRange{Minimum: Version, Maximum: Version}
	axes = append(axes, CompatibilityAxis{
		Name:       AxisProtocolContract,
		Version:    fmt.Sprintf("%d", Version),
		Provenance: "engram-core",
		Supported:  contractRange,
		Status:     CompatibilityReady,
		ReasonCode: "protocol_contract_ready",
	})

	for _, axis := range axes[:len(inputs)] {
		if axis.Status != CompatibilityReady {
			return incompatibleReport(axes, axis.ReasonCode, reasonFor(axis.ReasonCode))
		}
	}

	intersection := *contractRange
	for _, axis := range axes[:len(inputs)] {
		var ok bool
		intersection, ok = intersect(intersection, *axis.Supported)
		if !ok {
			return incompatibleReport(axes, ReasonNoProtocolIntersection, "The four Protocol ranges do not intersect.")
		}
	}

	reasonCode := ReasonProtocolCompatible
	reason := "All four attributable Protocol ranges intersect."
	if legacy {
		reasonCode = ReasonLegacyCompatible
		reason = "The tuple is compatible through a verified legacy expand path."
	}
	return CompatibilityReport{
		SchemaVersion: "protocol-compatibility-v1",
		Status:        CompatibilityReady,
		ReasonCode:    reasonCode,
		Reason:        reason,
		Axes:          axes,
		Intersection:  &intersection,
		Legacy:        legacy,
	}
}

func evaluateDeclaration(name string, declaration Declaration) CompatibilityAxis {
	axis := CompatibilityAxis{
		Name:       name,
		Version:    declaration.Version,
		Provenance: declaration.Provenance,
		Supported:  declaration.Supported,
		Legacy:     declaration.Legacy,
		Status:     CompatibilityReady,
		ReasonCode: name + "_ready",
	}
	switch {
	case declaration.Version == "":
		axis.Status = CompatibilityIncompatible
		axis.ReasonCode = name + "_missing"
	case declaration.Provenance == "":
		axis.Status = CompatibilityIncompatible
		axis.ReasonCode = name + "_unprovenanced"
	case declaration.Supported == nil || declaration.Supported.Minimum < 1 || declaration.Supported.Maximum < declaration.Supported.Minimum:
		axis.Status = CompatibilityIncompatible
		axis.ReasonCode = name + "_protocol_range_malformed"
	}
	return axis
}

func intersect(left, right VersionRange) (VersionRange, bool) {
	result := VersionRange{
		Minimum: max(left.Minimum, right.Minimum),
		Maximum: min(left.Maximum, right.Maximum),
	}
	return result, result.Minimum <= result.Maximum
}

func incompatibleReport(axes []CompatibilityAxis, reasonCode, reason string) CompatibilityReport {
	return CompatibilityReport{
		SchemaVersion: "protocol-compatibility-v1",
		Status:        CompatibilityIncompatible,
		ReasonCode:    reasonCode,
		Reason:        reason,
		Axes:          axes,
	}
}

func reasonFor(reasonCode string) string {
	switch {
	case hasSuffix(reasonCode, "_missing"):
		return "A required distribution declaration is missing."
	case hasSuffix(reasonCode, "_unprovenanced"):
		return "A distribution declaration lacks attributable provenance."
	default:
		return "A distribution declares a malformed Protocol range."
	}
}

func hasSuffix(value, suffix string) bool {
	return strings.HasSuffix(value, suffix)
}

// Fixture is the one versioned parity contract distributed by the Managed
// Pack and validated by Core, MCP, plugin, and setup projections.
type Fixture struct {
	SchemaVersion string               `json:"schema_version"`
	Protocol      FixtureProtocol      `json:"protocol"`
	Distributions FixtureDistributions `json:"distributions"`
	Legacy        FixtureLegacy        `json:"legacy_compatibility"`
}

type FixtureProtocol struct {
	Version                   int                  `json:"version"`
	IdentityFields            []string             `json:"identity_fields"`
	CheckpointDispositions    []string             `json:"checkpoint_dispositions"`
	CheckpointPersistence     string               `json:"checkpoint_persistence"`
	MinimumTools              []string             `json:"minimum_tools"`
	RecallDefault             string               `json:"recall_default"`
	RecallLimits              *FixtureRecallLimits `json:"recall_limits,omitempty"`
	CaptureDefault            string               `json:"capture_default"`
	RequiredVocabulary        []string             `json:"required_vocabulary"`
	CueMarkers                []string             `json:"cue_markers"`
	MCPInitializationGuidance []string             `json:"mcp_initialization_guidance"`
	CheckpointDescriptions    map[string]string    `json:"checkpoint_descriptions"`
}

type FixtureRecallLimits struct {
	InitialCandidates  int    `json:"initial_candidates"`
	FollowupCandidates int    `json:"followup_candidates"`
	CandidateUTF8Bytes int    `json:"candidate_utf8_bytes"`
	ContentUTF8Bytes   int    `json:"content_utf8_bytes"`
	Continuation       string `json:"continuation"`
}

type FixtureDistributions struct {
	ManagedPack  FixtureDistribution `json:"managed_pack"`
	EngramBinary FixtureDistribution `json:"engram_binary"`
	CodexPlugin  FixtureDistribution `json:"codex_plugin"`
}

type FixtureDistribution struct {
	Version          string       `json:"version,omitempty"`
	SkillSHA256      string       `json:"skill_sha256,omitempty"`
	LegacyCompatible bool         `json:"legacy_compatible,omitempty"`
	Supported        VersionRange `json:"supported_protocol"`
}

type FixtureLegacy struct {
	ManagedPack  FixtureLegacyDistribution `json:"managed_pack"`
	EngramBinary FixtureLegacyDistribution `json:"engram_binary"`
	CodexPlugin  FixtureLegacyDistribution `json:"codex_plugin"`
}

type FixtureLegacyDistribution struct {
	Version string `json:"version"`
	SHA256  string `json:"sha256,omitempty"`
}

// ParseFixture strictly decodes and validates the shared v1 parity fixture.
func ParseFixture(raw []byte) (Fixture, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var fixture Fixture
	if err := decoder.Decode(&fixture); err != nil {
		return Fixture{}, fmt.Errorf("decode Protocol fixture: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Fixture{}, err
	}
	if fixture.SchemaVersion != "engram-protocol-contract-v1" || fixture.Protocol.Version != Version {
		return Fixture{}, fmt.Errorf("Protocol fixture identity is not v%d", Version)
	}
	if !slices.Equal(fixture.Protocol.IdentityFields, identityFields) ||
		!slices.Equal(fixture.Protocol.CheckpointDispositions, checkpointDispositions) ||
		!slices.Equal(fixture.Protocol.MinimumTools, minimumTools) ||
		fixture.Protocol.CheckpointPersistence != CheckpointPersistence ||
		fixture.Protocol.RecallDefault != RecallDefault || fixture.Protocol.CaptureDefault != CaptureDefault {
		return Fixture{}, fmt.Errorf("Protocol fixture semantics differ from Core")
	}
	if fixture.Protocol.RecallLimits != nil && *fixture.Protocol.RecallLimits != (FixtureRecallLimits{
		InitialCandidates: RecallInitialCandidateLimit, FollowupCandidates: RecallFollowupCandidateLimit,
		CandidateUTF8Bytes: RecallCandidateUTF8Budget, ContentUTF8Bytes: RecallContentUTF8Limit,
		Continuation: RecallContinuationMode,
	}) {
		return Fixture{}, fmt.Errorf("Protocol fixture semantics differ from Core")
	}
	if len(fixture.Protocol.RequiredVocabulary) == 0 || len(fixture.Protocol.CueMarkers) != 2 ||
		len(fixture.Protocol.MCPInitializationGuidance) == 0 ||
		strings.TrimSpace(fixture.Protocol.CheckpointDescriptions["mem_checkpoint"]) == "" ||
		strings.TrimSpace(fixture.Protocol.CheckpointDescriptions["mem_checkpoint_status"]) == "" {
		return Fixture{}, fmt.Errorf("Protocol fixture parity vocabulary is incomplete")
	}
	for name, declaration := range map[string]FixtureDistribution{
		AxisManagedPack:  fixture.Distributions.ManagedPack,
		AxisEngramBinary: fixture.Distributions.EngramBinary,
		AxisCodexPlugin:  fixture.Distributions.CodexPlugin,
	} {
		if declaration.Supported.Minimum < 1 || declaration.Supported.Maximum < declaration.Supported.Minimum {
			return Fixture{}, fmt.Errorf("%s Protocol range is malformed", name)
		}
	}
	if fixture.Distributions.ManagedPack.Version == "" || len(fixture.Distributions.ManagedPack.SkillSHA256) != 64 ||
		fixture.Distributions.CodexPlugin.Version == "" ||
		fixture.Legacy.ManagedPack.Version == "" || len(fixture.Legacy.ManagedPack.SHA256) != 64 ||
		fixture.Legacy.EngramBinary.Version == "" || fixture.Legacy.CodexPlugin.Version == "" ||
		len(fixture.Legacy.CodexPlugin.SHA256) != 64 {
		return Fixture{}, fmt.Errorf("Protocol fixture distribution metadata is incomplete")
	}
	return fixture, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("Protocol fixture contains multiple JSON values")
		}
		return fmt.Errorf("decode Protocol fixture trailer: %w", err)
	}
	return nil
}
