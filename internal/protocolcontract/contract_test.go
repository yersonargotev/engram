package protocolcontract

import (
	"reflect"
	"testing"
)

func TestContractV1PublishesStableMachineSemantics(t *testing.T) {
	wantTools := []string{
		"mem_current_project",
		"mem_search",
		"mem_get_observation",
		"mem_checkpoint",
		"mem_checkpoint_status",
	}
	if Version != 1 {
		t.Fatalf("Version = %d, want 1", Version)
	}
	if BinarySupportedRange() != (VersionRange{Minimum: 1, Maximum: 1}) {
		t.Fatalf("BinarySupportedRange = %#v", BinarySupportedRange())
	}
	if !reflect.DeepEqual(IdentityFields(), []string{"host", "session_id", "root_turn_id"}) {
		t.Fatalf("IdentityFields = %#v", IdentityFields())
	}
	if !reflect.DeepEqual(CheckpointDispositions(), []string{"saved", "needs_review", "skipped"}) {
		t.Fatalf("CheckpointDispositions = %#v", CheckpointDispositions())
	}
	if CheckpointPersistence != "local_only" || RecallDefault != "agent_initiated" || CaptureDefault != "disabled" {
		t.Fatalf("defaults = persistence=%q recall=%q capture=%q", CheckpointPersistence, RecallDefault, CaptureDefault)
	}
	if !reflect.DeepEqual(MinimumTools(), wantTools) {
		t.Fatalf("MinimumTools = %#v", MinimumTools())
	}
}

func TestEvaluateIntersectsRangesWithoutRequiringVersionEquality(t *testing.T) {
	report := Evaluate(
		Declaration{Version: "3.2.0", Provenance: "pack-manifest:abc", Supported: rangePtr(1, 3)},
		Declaration{Version: "4.0.0", Provenance: "binary:/opt/engram", Supported: rangePtr(1, 2)},
		Declaration{Version: "0.1.6", Provenance: "plugin-revision:def", Supported: rangePtr(1, 4)},
	)

	if report.Status != CompatibilityReady || report.ReasonCode != ReasonProtocolCompatible {
		t.Fatalf("report = %#v", report)
	}
	if report.Intersection == nil || *report.Intersection != (VersionRange{Minimum: 1, Maximum: 1}) {
		t.Fatalf("intersection = %#v", report.Intersection)
	}
	if len(report.Axes) != 4 || report.Axes[0].Name != AxisManagedPack || report.Axes[3].Name != AxisProtocolContract {
		t.Fatalf("axes = %#v", report.Axes)
	}
}

func TestEvaluateReportsLegacyCompatibilityWhenOneVerifiedAxisUsesTheExpandPath(t *testing.T) {
	report := Evaluate(
		Declaration{Version: "3.1.2", Provenance: "legacy-pack:abc", Supported: rangePtr(1, 1), Legacy: true},
		Declaration{Version: "3.0.0", Provenance: "binary:/opt/engram", Supported: rangePtr(1, 1)},
		Declaration{Version: "0.1.6", Provenance: "plugin-revision:def", Supported: rangePtr(1, 1)},
	)

	if report.Status != CompatibilityReady || report.ReasonCode != ReasonLegacyCompatible || !report.Legacy {
		t.Fatalf("report = %#v", report)
	}
}

func TestEvaluateFailsClosedWithStableReasons(t *testing.T) {
	tests := []struct {
		name   string
		pack   Declaration
		binary Declaration
		plugin Declaration
		reason string
	}{
		{
			name:   "missing declaration",
			binary: validDeclaration("3.0.0"),
			plugin: validDeclaration("0.1.6"),
			reason: "managed_pack_missing",
		},
		{
			name:   "unprovenanced declaration",
			pack:   Declaration{Version: "3.2.0", Supported: rangePtr(1, 1)},
			binary: validDeclaration("3.0.0"),
			plugin: validDeclaration("0.1.6"),
			reason: "managed_pack_unprovenanced",
		},
		{
			name:   "malformed declaration",
			pack:   Declaration{Version: "3.2.0", Provenance: "pack:abc", Supported: rangePtr(2, 1)},
			binary: validDeclaration("3.0.0"),
			plugin: validDeclaration("0.1.6"),
			reason: "managed_pack_protocol_range_malformed",
		},
		{
			name:   "non-overlapping ranges",
			pack:   Declaration{Version: "3.2.0", Provenance: "pack:abc", Supported: rangePtr(2, 2)},
			binary: Declaration{Version: "3.0.0", Provenance: "binary:abc", Supported: rangePtr(1, 1)},
			plugin: validDeclaration("0.1.6"),
			reason: ReasonNoProtocolIntersection,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := Evaluate(tt.pack, tt.binary, tt.plugin)
			if report.Status != CompatibilityIncompatible || report.ReasonCode != tt.reason || report.Intersection != nil {
				t.Fatalf("report = %#v, want reason %q", report, tt.reason)
			}
		})
	}
}

func rangePtr(minimum, maximum int) *VersionRange {
	return &VersionRange{Minimum: minimum, Maximum: maximum}
}

func validDeclaration(version string) Declaration {
	return Declaration{Version: version, Provenance: "verified:" + version, Supported: rangePtr(1, 1)}
}
