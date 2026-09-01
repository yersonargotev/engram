package protocolcontract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestVersionedFixtureMatchesCoreAndDistributionMetadata(t *testing.T) {
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "assets", "protocol-contract-v1.json"))
	if err != nil {
		t.Fatalf("read Protocol fixture: %v", err)
	}
	fixture, err := ParseFixture(raw)
	if err != nil {
		t.Fatalf("parse Protocol fixture: %v", err)
	}

	if fixture.SchemaVersion != "engram-protocol-contract-v1" || fixture.Protocol.Version != Version {
		t.Fatalf("fixture identity = %#v", fixture)
	}
	if !reflect.DeepEqual(fixture.Protocol.IdentityFields, IdentityFields()) ||
		!reflect.DeepEqual(fixture.Protocol.CheckpointDispositions, CheckpointDispositions()) ||
		!reflect.DeepEqual(fixture.Protocol.MinimumTools, MinimumTools()) {
		t.Fatalf("fixture Protocol semantics drifted from Core: %#v", fixture.Protocol)
	}
	if fixture.Protocol.RecallLimits == nil || *fixture.Protocol.RecallLimits != (FixtureRecallLimits{
		InitialCandidates: RecallInitialCandidateLimit, FollowupCandidates: RecallFollowupCandidateLimit,
		CandidateUTF8Bytes: RecallCandidateUTF8Budget, ContentUTF8Bytes: RecallContentUTF8Limit,
		Continuation: RecallContinuationMode,
	}) {
		t.Fatalf("fixture Recall limits drifted from Core: %#v", fixture.Protocol.RecallLimits)
	}
	if fixture.Distributions.EngramBinary.Supported != BinarySupportedRange() {
		t.Fatalf("fixture binary range = %#v, Core = %#v", fixture.Distributions.EngramBinary.Supported, BinarySupportedRange())
	}

	packRaw, err := os.ReadFile(filepath.Join(root, "pack.json"))
	if err != nil {
		t.Fatalf("read Managed Pack metadata: %v", err)
	}
	pack := decodeJSONMap(t, packRaw)
	if pack["version"] != fixture.Distributions.ManagedPack.Version {
		t.Fatalf("Managed Pack version = %#v, fixture = %q", pack["version"], fixture.Distributions.ManagedPack.Version)
	}
	if !packHasProtocolFixture(pack, "assets/protocol-contract-v1.json") {
		t.Fatalf("Managed Pack does not distribute the Protocol fixture: %#v", pack["resources"])
	}

	pluginRaw, err := os.ReadFile(filepath.Join(root, "plugin", "codex", ".codex-plugin", "plugin.json"))
	if err != nil {
		t.Fatalf("read Codex plugin metadata: %v", err)
	}
	plugin := decodeJSONMap(t, pluginRaw)
	if plugin["version"] != fixture.Distributions.CodexPlugin.Version {
		t.Fatalf("Codex plugin version = %#v, fixture = %q", plugin["version"], fixture.Distributions.CodexPlugin.Version)
	}
	if !reflect.DeepEqual(plugin["engramProtocol"], map[string]any{"minimum": float64(1), "maximum": float64(1), "legacyCompatible": true}) {
		t.Fatalf("Codex plugin Protocol metadata = %#v", plugin["engramProtocol"])
	}
}

func TestParseFixtureRejectsUnknownOrSemanticallyDifferentContracts(t *testing.T) {
	tests := [][]byte{
		[]byte(`{"schema_version":"engram-protocol-contract-v1","unexpected":true}`),
		[]byte(`{"schema_version":"engram-protocol-contract-v1","protocol":{"version":2}}`),
	}
	for _, raw := range tests {
		if _, err := ParseFixture(raw); err == nil {
			t.Fatalf("ParseFixture(%s) succeeded", raw)
		}
	}
}

func decodeJSONMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	return value
}

func packHasProtocolFixture(pack map[string]any, source string) bool {
	resources, _ := pack["resources"].([]any)
	for _, item := range resources {
		resource, _ := item.(map[string]any)
		if resource["kind"] == "asset" && resource["id"] == "protocol-contract-v1" && resource["source"] == source {
			return true
		}
	}
	return false
}
