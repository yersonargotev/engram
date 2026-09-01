//go:build !windows

package plugin_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/recallbaseline"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCodexCoreLifecycleHooksRecordOnlyContentFreeOptInMetrics(t *testing.T) {
	root := repoRoot(t)
	pluginRoot := filepath.Join(root, "plugin", "codex")
	manifest := readCodexHooksManifest(t, filepath.Join(pluginRoot, "hooks", "hooks.json"))
	binDir := buildCodexActivationCLI(t, root)
	dataDir := t.TempDir()
	env := append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"ENGRAM_DATA_DIR="+dataDir,
		"ENGRAM_PROJECT=engram",
		"ENGRAM_CODEX_RECALL_CANARY=targeted-recall",
		"ENGRAM_RECALL_BASELINE=1",
	)

	sessionOutput := runCodexBaselineManifest(t, matchingSessionStartCommand(t, manifest, "startup"),
		`{"cwd":"/tmp/repo","session_id":"session-secret","source":"startup"}`, pluginRoot, env)
	promptOutput := runCodexBaselineManifest(t, singleCodexHookCommand(t, manifest, "UserPromptSubmit"),
		`{"cwd":"/tmp/repo","session_id":"session-secret","turn_id":"turn-secret","prompt":"PROMPT-CONTENT-MUST-NOT-LEAK"}`, pluginRoot, env)
	if !json.Valid([]byte(sessionOutput)) || !json.Valid([]byte(promptOutput)) {
		t.Fatalf("hook output is not valid JSON: session=%q prompt=%q", sessionOutput, promptOutput)
	}
	var sessionResponse codexHookResponse
	if err := json.Unmarshal([]byte(sessionOutput), &sessionResponse); err != nil {
		t.Fatalf("decode SessionStart output: %v", err)
	}

	s, err := store.New(store.FallbackConfig(dataDir))
	if err != nil {
		t.Fatalf("open prompt consent store: %v", err)
	}
	if err := s.UpsertCaptureConsent(store.CaptureConsent{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
		RetentionDays: store.DefaultDiagnosticRetentionDays, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("enable prompt capture: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close prompt consent store: %v", err)
	}
	consentedOutput := runCodexBaselineManifest(t, singleCodexHookCommand(t, manifest, "UserPromptSubmit"),
		`{"cwd":"/tmp/repo","session_id":"session-secret","turn_id":"turn-consented","prompt":"CONSENTED-PROMPT-STAYS-DIAGNOSTIC"}`, pluginRoot, env)
	if strings.Contains(consentedOutput, "CONSENTED-PROMPT-STAYS-DIAGNOSTIC") {
		t.Fatalf("consented prompt leaked into hook output: %s", consentedOutput)
	}

	report := waitCodexLifecycleBaseline(t, dataDir, 2)
	if report.Lifecycle.Capture.Events != 2 || report.Lifecycle.Capture.Disabled != 1 || report.Lifecycle.Capture.Enabled != 1 {
		t.Fatalf("prompt Capture metrics = %+v, want one disabled and one enabled event", report.Lifecycle.Capture)
	}
	if len(report.Operations) != 1 {
		t.Fatalf("lifecycle operations = %+v", report.Operations)
	}
	operation := report.Operations[0]
	wantBytes := int64(len(sessionResponse.HookSpecificOutput.AdditionalContext))
	if operation.Surface != recallbaseline.SurfaceLifecycle || operation.Operation != "session_start" ||
		operation.Events != 1 || operation.LatencySamples != 1 || operation.ByteSamples != 1 || operation.TotalUTF8Bytes != wantBytes {
		t.Fatalf("SessionStart metrics = %+v, want injected bytes %d", operation, wantBytes)
	}
	raw, err := os.ReadFile(recallbaseline.DatabasePath(dataDir))
	if err != nil {
		t.Fatalf("read content-free baseline: %v", err)
	}
	for _, forbidden := range []string{"PROMPT-CONTENT-MUST-NOT-LEAK", "CONSENTED-PROMPT-STAYS-DIAGNOSTIC", "/tmp/repo", "session-secret", "turn-secret", "turn-consented"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("baseline database leaked %q", forbidden)
		}
	}

	disabledDir := t.TempDir()
	disabledEnv := append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"ENGRAM_DATA_DIR="+disabledDir,
		"ENGRAM_PROJECT=engram",
		"ENGRAM_RECALL_BASELINE=",
	)
	runCodexBaselineManifest(t, singleCodexHookCommand(t, manifest, "UserPromptSubmit"),
		`{"session_id":"missing","turn_id":"turn","prompt":"disabled baseline"}`, pluginRoot, disabledEnv)
	if _, err := os.Stat(recallbaseline.DatabasePath(disabledDir)); !os.IsNotExist(err) {
		t.Fatalf("disabled hook created baseline state: %v", err)
	}
}

func waitCodexLifecycleBaseline(t *testing.T, dataDir string, captureEvents int64) recallbaseline.Report {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		ledger, err := recallbaseline.Open(recallbaseline.Config{DataDir: dataDir})
		if err != nil {
			t.Fatalf("open lifecycle baseline: %v", err)
		}
		report, reportErr := ledger.Report(protocolcontract.CompatibilityReport{})
		closeErr := ledger.Close()
		if reportErr != nil || closeErr != nil {
			t.Fatalf("report lifecycle baseline: report=%v close=%v", reportErr, closeErr)
		}
		if report.Lifecycle.Capture.Events >= captureEvents && len(report.Operations) > 0 {
			return report
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for lifecycle baseline: %+v", report)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runCodexBaselineManifest(t *testing.T, command, input, pluginRoot string, env []string) string {
	t.Helper()
	run := exec.Command(codexTestBash(t), "-c", command)
	run.Env = append(env, "PLUGIN_ROOT="+pluginRoot)
	run.Stdin = strings.NewReader(input)
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("run lifecycle manifest command: %v\n%s", err, output)
	}
	return string(output)
}
