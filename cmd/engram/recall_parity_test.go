package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	mcppkg "github.com/mark3labs/mcp-go/mcp"
	engrammcp "github.com/yersonargotev/engram/internal/mcp"
	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

var recallParityFields = []string{
	"results", "result_ids", "result_count", "delivered_utf8_bytes", "provenance", "warning", "diagnostics",
}

func TestRecallContentCLIAndMCPExposeEquivalentPositionedSegments(t *testing.T) {
	cfg := testConfig(t)
	content := strings.Repeat("c", memoryops.RecallContentBudgetBytes) + "🧠continued"
	mustSeedObservation(t, cfg, "recall-content-parity", "engram", "decision", "Recall content parity", content, "project")

	withArgs(t, "engram", "search", "Recall content parity", "--project", "engram", "--json")
	stdout, stderr := captureOutput(t, func() { cmdSearch(cfg) })
	if stderr != "" {
		t.Fatalf("CLI search stderr=%q", stderr)
	}
	search := decodeCLIJSON(t, stdout)
	cliRecallID := search["recall_id"].(string)
	cliResultID := search["opaque_result_ids"].([]any)[0].(string)

	withArgs(t, "engram", "get", "--recall-id", cliRecallID, "--result-id", cliResultID, "--project", "engram", "--json")
	stdout, stderr = captureOutput(t, func() { cmdGet(cfg) })
	if stderr != "" {
		t.Fatalf("CLI get stderr=%q", stderr)
	}
	cli := decodeCLIJSON(t, stdout)

	s, err := store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	mcpSearch := recallParityMCPEnvelope(t, s, map[string]any{"query": "Recall content parity", "project": "engram"})
	mcpRecallID := mcpSearch["recall_id"].(string)
	mcpResultID := mcpSearch["opaque_result_ids"].([]any)[0].(string)
	result, err := engrammcp.GetObservationToolHandler(s, engrammcp.MCPConfig{
		BinaryVersion: version, BinaryRevision: commit,
	})(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
		"recall_id": mcpRecallID,
		"result_id": mcpResultID,
		"project":   "engram",
	}}})
	if err != nil || result.IsError {
		t.Fatalf("MCP get err=%v result=%v", err, result)
	}
	text, ok := mcppkg.AsTextContent(result.Content[0])
	if !ok {
		t.Fatalf("MCP get content=%T", result.Content[0])
	}
	var mcpEnvelope map[string]any
	if err := json.Unmarshal([]byte(text.Text), &mcpEnvelope); err != nil {
		t.Fatalf("decode MCP get: %v\n%s", err, text.Text)
	}

	for _, field := range []string{"memory", "position", "original_bytes", "delivered_utf8_bytes", "limit_bytes", "truncated", "continuation_position", "provenance"} {
		if !reflect.DeepEqual(cli[field], mcpEnvelope[field]) {
			t.Fatalf("Recall content parity field %q differs: CLI=%v MCP=%v", field, cli[field], mcpEnvelope[field])
		}
	}
	if cli["delivered_utf8_bytes"] != float64(memoryops.RecallContentBudgetBytes) || cli["continuation_position"] != float64(memoryops.RecallContentBudgetBytes) {
		t.Fatalf("CLI segment metadata=%v", cli)
	}

	position := int(cli["continuation_position"].(float64))
	withArgs(t, "engram", "get", "--recall-id", cliRecallID, "--result-id", cliResultID, "--position", strconv.Itoa(position), "--project", "engram", "--json")
	stdout, stderr = captureOutput(t, func() { cmdGet(cfg) })
	if stderr != "" {
		t.Fatalf("CLI continuation stderr=%q", stderr)
	}
	cliContinuation := decodeCLIJSON(t, stdout)
	mcpContinuationResult, err := engrammcp.GetObservationToolHandler(s, engrammcp.MCPConfig{
		BinaryVersion: version, BinaryRevision: commit,
	})(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
		"recall_id": mcpRecallID,
		"result_id": mcpResultID,
		"position":  float64(position),
		"project":   "engram",
	}}})
	if err != nil || mcpContinuationResult.IsError {
		t.Fatalf("MCP continuation err=%v result=%v", err, mcpContinuationResult)
	}
	mcpContinuationText, _ := mcppkg.AsTextContent(mcpContinuationResult.Content[0])
	var mcpContinuation map[string]any
	if err := json.Unmarshal([]byte(mcpContinuationText.Text), &mcpContinuation); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"memory", "position", "original_bytes", "delivered_utf8_bytes", "limit_bytes", "truncated", "provenance"} {
		if !reflect.DeepEqual(cliContinuation[field], mcpContinuation[field]) {
			t.Fatalf("Recall continuation parity field %q differs: CLI=%v MCP=%v", field, cliContinuation[field], mcpContinuation[field])
		}
	}
	if cliContinuation["truncated"] != false || cliContinuation["memory"].(map[string]any)["content"] != "🧠continued" {
		t.Fatalf("CLI continuation=%v", cliContinuation)
	}
}

func TestRecallContentCLIAndMCPReturnEquivalentStoreFailureWarnings(t *testing.T) {
	cfg := testConfig(t)
	originalStoreNew := storeNew
	storeNew = func(store.Config) (*store.Store, error) { return nil, errors.New("forced content store failure") }
	t.Cleanup(func() { storeNew = originalStoreNew })

	withArgs(t, "engram", "get", "--recall-id", "recall-unavailable", "--result-id", "result-unavailable", "--project", "engram", "--json")
	stdout, stderr := captureOutput(t, func() { cmdGet(cfg) })
	if stderr != "" {
		t.Fatalf("CLI get stderr=%q", stderr)
	}
	cli := decodeCLIJSON(t, stdout)

	result, err := engrammcp.GetObservationToolHandler(nil, engrammcp.MCPConfig{})(context.Background(), mcppkg.CallToolRequest{
		Params: mcppkg.CallToolParams{Arguments: map[string]any{
			"recall_id": "recall-unavailable",
			"result_id": "result-unavailable",
			"project":   "engram",
		}},
	})
	if err != nil || result.IsError {
		t.Fatalf("MCP get err=%v result=%v", err, result)
	}
	text, _ := mcppkg.AsTextContent(result.Content[0])
	var mcpEnvelope map[string]any
	if err := json.Unmarshal([]byte(text.Text), &mcpEnvelope); err != nil {
		t.Fatal(err)
	}

	for _, envelope := range []map[string]any{cli, mcpEnvelope} {
		warning := envelope["warning"].(map[string]any)
		diagnostics := envelope["diagnostics"].([]any)
		memory := envelope["memory"].(map[string]any)
		if warning["code"] != "recall_unavailable" || diagnostics[0].(map[string]any)["code"] != "recall_store_failure" || memory["content"] != "" {
			t.Fatalf("fail-open envelope=%v", envelope)
		}
	}
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

	if !reflect.DeepEqual(recallCandidatesWithoutOpaqueIDs(cli["results"]), recallCandidatesWithoutOpaqueIDs(mcpEnvelope["results"])) {
		t.Fatalf("Recall candidate semantics differ: CLI=%v MCP=%v", cli["results"], mcpEnvelope["results"])
	}
	for _, envelope := range []map[string]any{cli, mcpEnvelope} {
		ids := envelope["result_ids"].([]any)
		if len(ids) != 5 {
			t.Fatalf("legacy result identities=%v", ids)
		}
		opaqueIDs := envelope["opaque_result_ids"].([]any)
		if len(opaqueIDs) != 5 {
			t.Fatalf("opaque result identities=%v", opaqueIDs)
		}
		for _, id := range opaqueIDs {
			if value, _ := id.(string); !strings.HasPrefix(value, "result-") {
				t.Fatalf("result identity=%v", id)
			}
		}
	}
	for _, field := range []string{"result_count", "delivered_utf8_bytes", "provenance"} {
		if !reflect.DeepEqual(cli[field], mcpEnvelope[field]) {
			t.Fatalf("Recall parity field %q differs: CLI=%v MCP=%v", field, cli[field], mcpEnvelope[field])
		}
	}
	if cli["result_count"] != float64(5) {
		t.Fatalf("default Recall must remain bounded to five candidates: %v", cli)
	}
}

func recallCandidatesWithoutOpaqueIDs(value any) []map[string]any {
	items, _ := value.([]any)
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		candidate, _ := item.(map[string]any)
		copy := make(map[string]any, len(candidate)-1)
		for key, field := range candidate {
			if key != "result_id" {
				copy[key] = field
			}
		}
		result = append(result, copy)
	}
	return result
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
