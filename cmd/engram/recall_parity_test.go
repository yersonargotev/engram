package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	mcppkg "github.com/mark3labs/mcp-go/mcp"
	engrammcp "github.com/yersonargotev/engram/internal/mcp"
	"github.com/yersonargotev/engram/internal/store"
)

func TestRecallCLIAndMCPReturnEquivalentCandidateSemantics(t *testing.T) {
	cfg := testConfig(t)
	s, err := store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateSession("recall-parity", "engram", t.TempDir()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		_, err := s.AddObservation(store.AddObservationParams{
			SessionID: "recall-parity",
			Type:      "decision",
			Title:     fmt.Sprintf("Recall parity decision %d", i),
			Content:   fmt.Sprintf("bounded parity candidate content %d", i),
			Project:   "engram",
			Scope:     "project",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	withArgs(t, "engram", "search", "bounded parity", "--project", "engram", "--json")
	stdout, stderr := captureOutput(t, func() { cmdSearch(cfg) })
	if stderr != "" {
		t.Fatalf("CLI stderr=%q", stderr)
	}
	cli := decodeCLIJSON(t, stdout)

	s, err = store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	result, err := engrammcp.SearchToolHandler(s, engrammcp.MCPConfig{
		BinaryVersion:  version,
		BinaryRevision: commit,
	})(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
		"query":   "bounded parity",
		"project": "engram",
	}}})
	if err != nil || result.IsError {
		t.Fatalf("MCP Recall err=%v result=%v", err, result)
	}
	text, ok := mcppkg.AsTextContent(result.Content[0])
	if !ok {
		t.Fatalf("MCP Recall content=%T", result.Content[0])
	}
	var mcpEnvelope map[string]any
	if err := json.Unmarshal([]byte(text.Text), &mcpEnvelope); err != nil {
		t.Fatalf("decode MCP Recall: %v\n%s", err, text.Text)
	}

	for _, field := range []string{"results", "result_ids", "result_count", "delivered_utf8_bytes", "provenance"} {
		if !reflect.DeepEqual(cli[field], mcpEnvelope[field]) {
			t.Fatalf("Recall parity field %q differs: CLI=%v MCP=%v", field, cli[field], mcpEnvelope[field])
		}
	}
	if cli["result_count"] != float64(5) {
		t.Fatalf("default Recall must remain bounded to five candidates: %v", cli)
	}
}
