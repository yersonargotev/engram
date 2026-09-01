package mcp

import (
	"context"
	"strings"
	"testing"
	"time"

	mcppkg "github.com/mark3labs/mcp-go/mcp"
	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestMemSavePromptDefaultsOffAndReturnsContentFreeMetadata(t *testing.T) {
	s := newMCPTestStore(t)
	const sentinel = "MCP-PROMPT-MUST-NOT-LEAK-102"
	res, err := handleSavePrompt(s, MCPConfig{DefaultProject: "engram"}, NewSessionActivity(time.Minute))(
		context.Background(),
		mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
			"session_id": "mcp-capture-off",
			"content":    sentinel,
		}}},
	)
	if err != nil || res.IsError {
		t.Fatalf("mem_save_prompt failed: err=%v result=%s", err, callResultText(t, res))
	}
	body := callResultJSON(t, res)
	if body["captured"] != false || body["reason_code"] != memoryops.CaptureReasonConsentDisabled {
		t.Fatalf("response = %#v, want disabled capture metadata", body)
	}
	if strings.Contains(callResultText(t, res), sentinel) {
		t.Fatalf("MCP result leaked prompt content: %s", callResultText(t, res))
	}
	assertMCPPromptCaptureBoundaryCounts(t, s, 0, 0)
}

func TestMemSavePromptCapturesOnlyWithExplicitConsent(t *testing.T) {
	s := newMCPTestStore(t)
	if _, err := memoryops.New(s).EnableCapture(memoryops.CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
	}); err != nil {
		t.Fatalf("enable capture: %v", err)
	}

	const sentinel = "MCP-CONSENTED-PROMPT-MUST-NOT-LEAK-102"
	res, err := handleSavePrompt(s, MCPConfig{DefaultProject: "engram"}, NewSessionActivity(time.Minute))(
		context.Background(),
		mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
			"session_id": "mcp-capture-on",
			"content":    sentinel,
		}}},
	)
	if err != nil || res.IsError {
		t.Fatalf("mem_save_prompt failed: err=%v result=%s", err, callResultText(t, res))
	}
	body := callResultJSON(t, res)
	if body["captured"] != true || body["reason_code"] != memoryops.CaptureReasonCaptured || body["expires_at"] == nil {
		t.Fatalf("response = %#v, want captured metadata", body)
	}
	if strings.Contains(callResultText(t, res), sentinel) {
		t.Fatalf("MCP result leaked prompt content: %s", callResultText(t, res))
	}
	assertMCPPromptCaptureBoundaryCounts(t, s, 1, 0)
}

func TestMemSaveCapturePromptDefaultsFalseEvenWithConsent(t *testing.T) {
	s := newMCPTestStore(t)
	if _, err := memoryops.New(s).EnableCapture(memoryops.CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
	}); err != nil {
		t.Fatalf("enable capture: %v", err)
	}
	activity := NewSessionActivity(time.Minute)
	sessionID := defaultSessionID("engram")
	activity.RecordPrompt(sessionID, "engram", "PROMPT-AVAILABLE-BUT-NOT-REQUESTED")
	h := handleSave(s, MCPConfig{DefaultProject: "engram"}, activity)

	baseArgs := map[string]any{
		"title":   "Capture default is off",
		"content": "**What**: verified explicit prompt capture\n**Why**: consent is independent",
		"type":    "decision",
	}
	res, err := h(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: baseArgs}})
	if err != nil || res.IsError {
		t.Fatalf("mem_save without capture_prompt failed: err=%v result=%s", err, callResultText(t, res))
	}
	assertMCPPromptCaptureBoundaryCounts(t, s, 0, 0)

	baseArgs["capture_prompt"] = true
	baseArgs["title"] = "Explicit capture request"
	res, err = h(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: baseArgs}})
	if err != nil || res.IsError {
		t.Fatalf("mem_save with capture_prompt failed: err=%v result=%s", err, callResultText(t, res))
	}
	assertMCPPromptCaptureBoundaryCounts(t, s, 1, 0)
}

func assertMCPPromptCaptureBoundaryCounts(t *testing.T, s *store.Store, wantDiagnostic, wantLegacy int) {
	t.Helper()
	var diagnostic, legacy int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM diagnostic_captures`).Scan(&diagnostic); err != nil {
		t.Fatalf("count Diagnostic captures: %v", err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM user_prompts`).Scan(&legacy); err != nil {
		t.Fatalf("count Legacy prompts: %v", err)
	}
	if diagnostic != wantDiagnostic || legacy != wantLegacy {
		t.Fatalf("capture boundary counts Diagnostic=%d Legacy=%d, want %d/%d", diagnostic, legacy, wantDiagnostic, wantLegacy)
	}
}
