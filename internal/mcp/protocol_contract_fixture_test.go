package mcp

import (
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

	for _, tool := range protocolcontract.MinimumTools() {
		if !ProfileAgent[tool] {
			t.Errorf("minimum Protocol tool %q is absent from the agent profile", tool)
		}
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

	skillRaw, err := os.ReadFile(filepath.Join(root, "plugin", "codex", "skills", "memory", "SKILL.md"))
	if err != nil {
		t.Fatalf("read canonical memory skill: %v", err)
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
