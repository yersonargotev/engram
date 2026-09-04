package mcp

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

func TestProtocolFixtureValidatesMCPAndCanonicalCueProjections(t *testing.T) {
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "assets", "protocol-contract-v1.json"))
	if err != nil {
		t.Fatalf("read Protocol fixture: %v", err)
	}
	fixture, err := protocolcontract.ParseFixture(raw)
	if err != nil {
		t.Fatalf("parse Protocol fixture: %v", err)
	}

	expectedAgentTools := make(map[string]bool, len(fixture.Protocol.MinimumTools))
	for _, tool := range fixture.Protocol.MinimumTools {
		expectedAgentTools[tool] = true
		if !ProfileAgent[tool] {
			t.Errorf("minimum Protocol tool %q is absent from the agent profile", tool)
		}
	}
	if !maps.Equal(ProfileAgent, expectedAgentTools) {
		t.Errorf("agent profile = %#v, want exactly Protocol tools %#v", ProfileAgent, expectedAgentTools)
	}
	for _, guidance := range fixture.Protocol.MCPInitializationGuidance {
		if !strings.Contains(serverInstructions, guidance) {
			t.Errorf("MCP initialization guidance is missing %q", guidance)
		}
	}

	tools := NewServerWithTools(nil, map[string]bool{
		"mem_checkpoint":        true,
		"mem_checkpoint_status": true,
	}).ListTools()
	for name, description := range fixture.Protocol.CheckpointDescriptions {
		tool := tools[name]
		if tool == nil || tool.Tool.Description != description {
			t.Errorf("%s description = %#v, want %q", name, tool, description)
		}
	}

	skillRaw, err := os.ReadFile(filepath.Join(root, "skills", "engram-memory", "SKILL.md"))
	if err != nil {
		t.Fatalf("read canonical memory skill: %v", err)
	}
	guidanceMarkers := make(map[string]bool, len(fixture.Protocol.MCPInitializationGuidance))
	for _, guidance := range fixture.Protocol.MCPInitializationGuidance {
		guidanceMarkers[guidance] = true
	}
	for _, marker := range []string{
		"Terminal Memory commit",
		"saved",
		"needs_review",
		"skipped(no_durable_knowledge)",
		"Current user intent, maintained source, and runtime evidence override Memory.",
	} {
		if !guidanceMarkers[marker] {
			t.Errorf("Protocol fixture is missing contractual initialization marker %q", marker)
		}
		if !strings.Contains(serverInstructions, marker) {
			t.Errorf("MCP initialization guidance is missing contractual marker %q", marker)
		}
		if !strings.Contains(string(skillRaw), marker) {
			t.Errorf("canonical skill is missing contractual initialization marker %q", marker)
		}
	}
	for _, marker := range fixture.Protocol.CueMarkers {
		if strings.Count(string(skillRaw), marker) != 1 {
			t.Errorf("canonical skill must contain cue marker %q exactly once", marker)
		}
	}

	contextRaw, err := os.ReadFile(filepath.Join(root, "CONTEXT.md"))
	if err != nil {
		t.Fatalf("read domain vocabulary: %v", err)
	}
	for _, term := range fixture.Protocol.RequiredVocabulary {
		if !strings.Contains(string(contextRaw), "**"+term+"**") {
			t.Errorf("CONTEXT.md does not define required Protocol term %q", term)
		}
	}
}
