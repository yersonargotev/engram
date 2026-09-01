package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCaptureStatusJSONDefaultsDisabledAndContentFree(t *testing.T) {
	cfg := testConfig(t)
	withArgs(t, "engram", "capture", "status", "--project", "Engram", "--type", store.CaptureContentTypePrompt, "--json")

	stdout, stderr := captureOutput(t, func() { cmdCapture(cfg) })
	if stderr != "" {
		t.Fatalf("capture status stderr=%q", stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	if payload["project"] != "engram" || payload["content_type"] != store.CaptureContentTypePrompt {
		t.Fatalf("capture status scope=%v", payload)
	}
	if payload["enabled"] != false || payload["scope"] != "none" {
		t.Fatalf("capture status consent=%v", payload)
	}
	if payload["retention_days"] != float64(store.DefaultDiagnosticRetentionDays) || payload["stored_count"] != float64(0) {
		t.Fatalf("capture status retention/count=%v", payload)
	}
	if strings.Contains(stdout, "content") && !strings.Contains(stdout, `"content_type"`) {
		t.Fatalf("capture status exposed content: %s", stdout)
	}
}

func TestCaptureEnableCreatesExplicitProjectConsent(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	withArgs(t, "engram", "capture", "enable", "--project", "Engram", "--type", store.CaptureContentTypePrompt, "--retention-days", "12", "--json")

	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCapture(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("capture enable panic=%v stderr=%q", recovered, stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	if payload["enabled"] != true || payload["scope"] != "project" || payload["retention_days"] != float64(12) {
		t.Fatalf("capture enable=%v", payload)
	}

	withArgs(t, "engram", "capture", "status", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--json")
	statusOutput, statusErr := captureOutput(t, func() { cmdCapture(cfg) })
	if statusErr != "" {
		t.Fatalf("capture status stderr=%q", statusErr)
	}
	status := decodeCLIJSON(t, statusOutput)
	if status["enabled"] != true || status["scope"] != "project" || status["retention_days"] != float64(12) {
		t.Fatalf("capture status=%v", status)
	}
}

func TestCaptureEnableCreatesExpiringSessionConsent(t *testing.T) {
	cfg := testConfig(t)
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	withArgs(t,
		"engram", "capture", "enable",
		"--project", "engram", "--type", store.CaptureContentTypePrompt,
		"--session-id", "opaque-session", "--expires-at", expiresAt.Format(time.RFC3339),
		"--json",
	)

	stdout, stderr := captureOutput(t, func() { cmdCapture(cfg) })
	if stderr != "" {
		t.Fatalf("capture enable stderr=%q", stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	if payload["enabled"] != true || payload["scope"] != "session" || payload["session_id"] != "opaque-session" {
		t.Fatalf("capture session enable=%v", payload)
	}
	if payload["expires_at"] != expiresAt.Format(time.RFC3339) {
		t.Fatalf("expires_at=%v, want %s", payload["expires_at"], expiresAt.Format(time.RFC3339))
	}
}

func TestCaptureDisableRevokesConsentWithoutPurgingContent(t *testing.T) {
	cfg := testConfig(t)
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	service := memoryops.New(s)
	if _, err := service.EnableCapture(memoryops.CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
	}); err != nil {
		t.Fatalf("EnableCapture() error = %v", err)
	}
	result, err := service.Capture(memoryops.CaptureInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
		SessionID: "opaque-session", Content: "private diagnostic sentinel",
	})
	if err != nil || !result.Captured {
		t.Fatalf("Capture() = %#v, err=%v", result, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	withArgs(t, "engram", "capture", "disable", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--json")
	stdout, stderr := captureOutput(t, func() { cmdCapture(cfg) })
	if stderr != "" {
		t.Fatalf("capture disable stderr=%q", stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	if payload["enabled"] != false || payload["stored_count"] != float64(1) {
		t.Fatalf("capture disable=%v", payload)
	}
	if strings.Contains(stdout, "private diagnostic sentinel") {
		t.Fatalf("capture disable exposed content: %s", stdout)
	}
}

func TestCapturePurgeRequiresSeparateConfirmationAndKeepsConsent(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	service := memoryops.New(s)
	if _, err := service.EnableCapture(memoryops.CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
	}); err != nil {
		t.Fatalf("EnableCapture() error = %v", err)
	}
	result, err := service.Capture(memoryops.CaptureInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
		SessionID: "opaque-session", Content: "purge-only private sentinel",
	})
	if err != nil || !result.Captured {
		t.Fatalf("Capture() = %#v, err=%v", result, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	withArgs(t, "engram", "capture", "purge", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCapture(cfg) })
	if stdout != "" || recovered == nil {
		t.Fatalf("unconfirmed purge stdout=%q panic=%v", stdout, recovered)
	}
	errorPayload := decodeCLIJSON(t, stderr)
	if errorPayload["code"] != "confirmation_required" || strings.Contains(stderr, "purge-only private sentinel") {
		t.Fatalf("unconfirmed purge error=%v", errorPayload)
	}

	withArgs(t, "engram", "capture", "purge", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--yes", "--json")
	stdout, stderr, recovered = captureOutputAndRecover(t, func() { cmdCapture(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("confirmed purge panic=%v stderr=%q", recovered, stderr)
	}
	purged := decodeCLIJSON(t, stdout)
	if purged["deleted"] != float64(1) || strings.Contains(stdout, "purge-only private sentinel") {
		t.Fatalf("confirmed purge=%v", purged)
	}

	withArgs(t, "engram", "capture", "status", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--json")
	statusOutput, statusErr := captureOutput(t, func() { cmdCapture(cfg) })
	if statusErr != "" {
		t.Fatalf("capture status stderr=%q", statusErr)
	}
	status := decodeCLIJSON(t, statusOutput)
	if status["enabled"] != true || status["stored_count"] != float64(0) {
		t.Fatalf("post-purge status=%v", status)
	}
}

func TestCaptureEnableRejectsInvalidConsentArguments(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing project", args: []string{"enable", "--type", store.CaptureContentTypePrompt}},
		{name: "missing type", args: []string{"enable", "--project", "engram"}},
		{name: "zero retention", args: []string{"enable", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--retention-days", "0"}},
		{name: "retention above maximum", args: []string{"enable", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--retention-days", "31"}},
		{name: "session without expiry", args: []string{"enable", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--session-id", "session"}},
		{name: "expiry without session", args: []string{"enable", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--expires-at", now.Add(time.Hour).Format(time.RFC3339)}},
		{name: "past expiry", args: []string{"enable", "--project", "engram", "--type", store.CaptureContentTypePrompt, "--session-id", "session", "--expires-at", now.Add(-time.Hour).Format(time.RFC3339)}},
		{name: "unsupported type", args: []string{"enable", "--project", "engram", "--type", "transcript"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubExitWithPanic(t)
			cfg := testConfig(t)
			args := append([]string{"engram", "capture"}, tt.args...)
			args = append(args, "--json")
			withArgs(t, args...)

			stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCapture(cfg) })
			if stdout != "" || recovered == nil {
				t.Fatalf("stdout=%q panic=%v", stdout, recovered)
			}
			payload := decodeCLIJSON(t, stderr)
			if payload["code"] != "invalid_arguments" {
				t.Fatalf("error=%v", payload)
			}
		})
	}
}

func TestCaptureStatusDefaultsToDetectedProject(t *testing.T) {
	cfg := testConfig(t)
	projectDir := t.TempDir()
	withCwd(t, projectDir)
	withArgs(t, "engram", "capture", "status", "--json")

	stdout, stderr := captureOutput(t, func() { cmdCapture(cfg) })
	if stderr != "" {
		t.Fatalf("capture status stderr=%q", stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	wantProject, _ := store.NormalizeProject(filepath.Base(projectDir))
	if payload["project"] != wantProject || payload["content_type"] != store.CaptureContentTypePrompt {
		t.Fatalf("capture status=%v, want detected project %q", payload, wantProject)
	}
}

func TestMainDispatchesCaptureStatus(t *testing.T) {
	stubRuntimeHooks(t)
	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)
	withArgs(t, "engram", "capture", "status", "--project", "engram", "--json")

	stdout, stderr := captureOutput(t, main)
	if stderr != "" {
		t.Fatalf("capture status stderr=%q", stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	if payload["enabled"] != false || payload["project"] != "engram" {
		t.Fatalf("capture status=%v", payload)
	}
}

func TestCaptureHelpIsContentFree(t *testing.T) {
	stubExitWithPanic(t)
	withArgs(t, "engram", "capture", "--help")

	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCapture(store.Config{}) })
	if recovered != nil || stderr != "" {
		t.Fatalf("capture help panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "usage: engram capture") || strings.Contains(stdout, "captured content") {
		t.Fatalf("capture help=%q", stdout)
	}
}

func TestCapturePurgeRequiresYesWhenInputIsNotInteractive(t *testing.T) {
	stubExitWithPanic(t)
	originalInteractive := captureInputInteractive
	captureInputInteractive = func() bool { return false }
	t.Cleanup(func() { captureInputInteractive = originalInteractive })
	cfg := testConfig(t)
	withArgs(t, "engram", "capture", "purge", "--project", "engram", "--type", store.CaptureContentTypePrompt)

	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCapture(cfg) })
	if stdout != "" || recovered == nil || !strings.Contains(stderr, "requires --yes in non-interactive mode") {
		t.Fatalf("capture purge stdout=%q stderr=%q panic=%v", stdout, stderr, recovered)
	}
}
