package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	mcppkg "github.com/mark3labs/mcp-go/mcp"
	engrammcp "github.com/yersonargotev/engram/internal/mcp"
	"github.com/yersonargotev/engram/internal/store"
)

var recallParityFields = []string{
	"results", "result_ids", "result_count", "delivered_utf8_bytes", "provenance", "warning", "diagnostics",
}

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

func TestRecallCLIAndMCPReturnEquivalentWeakAuthoritySemantics(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg := testConfig(t)

	withArgs(t, "engram", "search", "authority parity", "--json")
	stdout, stderr := captureOutput(t, func() { cmdSearch(cfg) })
	if stderr != "" {
		t.Fatalf("CLI stderr=%q", stderr)
	}
	cli := decodeCLIJSON(t, stdout)

	s, err := store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	mcpEnvelope := recallParityMCPEnvelope(t, s, map[string]any{"query": "authority parity"})
	assertRecallNormalizedParity(t, cli, mcpEnvelope)
	warning := cli["warning"].(map[string]any)
	if warning["code"] != "recall_project_authority_required" {
		t.Fatalf("warning=%v", warning)
	}
}

func TestRecallCLIAndMCPReturnEquivalentFailOpenSemantics(t *testing.T) {
	cfg := testConfig(t)
	originalStoreNew := storeNew
	storeNew = func(store.Config) (*store.Store, error) { return nil, errors.New("store open failed") }
	t.Cleanup(func() { storeNew = originalStoreNew })

	withArgs(t, "engram", "search", "failure parity", "--project", "engram", "--json")
	stdout, stderr := captureOutput(t, func() { cmdSearch(cfg) })
	if stderr != "" {
		t.Fatalf("CLI stderr=%q", stderr)
	}
	cli := decodeCLIJSON(t, stdout)
	mcpEnvelope := recallParityMCPEnvelope(t, nil, map[string]any{"query": "failure parity", "project": "engram"})
	assertRecallNormalizedParity(t, cli, mcpEnvelope)

	diagnostic := cli["diagnostics"].([]any)[0].(map[string]any)
	if diagnostic["code"] != "recall_store_failure" || diagnostic["operation"] != "recall_candidates" {
		t.Fatalf("diagnostic=%v", diagnostic)
	}
}

func recallParityMCPEnvelope(t *testing.T, s *store.Store, args map[string]any) map[string]any {
	t.Helper()
	result, err := engrammcp.SearchToolHandler(s, engrammcp.MCPConfig{
		BinaryVersion: version, BinaryRevision: commit,
	})(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: args}})
	if err != nil || result.IsError {
		t.Fatalf("MCP Recall err=%v result=%v", err, result)
	}
	text, ok := mcppkg.AsTextContent(result.Content[0])
	if !ok {
		t.Fatalf("MCP Recall content=%T", result.Content[0])
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(text.Text), &envelope); err != nil {
		t.Fatalf("decode MCP Recall: %v\n%s", err, text.Text)
	}
	return envelope
}

func assertRecallNormalizedParity(t *testing.T, cli, mcpEnvelope map[string]any) {
	t.Helper()
	for _, field := range recallParityFields {
		cliValue, mcpValue := cli[field], mcpEnvelope[field]
		if field == "diagnostics" {
			cliDiagnostics := cliValue.([]any)
			mcpDiagnostics := mcpValue.([]any)
			if len(cliDiagnostics) != len(mcpDiagnostics) {
				t.Fatalf("Recall parity field diagnostics differs: CLI=%v MCP=%v", cliValue, mcpValue)
			}
			for i := range cliDiagnostics {
				cliDiagnostic := cliDiagnostics[i].(map[string]any)
				mcpDiagnostic := mcpDiagnostics[i].(map[string]any)
				for _, key := range []string{"code", "operation"} {
					if !reflect.DeepEqual(cliDiagnostic[key], mcpDiagnostic[key]) {
						t.Fatalf("Recall diagnostic parity %q differs: CLI=%v MCP=%v", key, cliDiagnostic, mcpDiagnostic)
					}
				}
			}
			continue
		}
		if !reflect.DeepEqual(cliValue, mcpValue) {
			t.Fatalf("Recall parity field %q differs: CLI=%v MCP=%v", field, cliValue, mcpValue)
		}
	}
}
