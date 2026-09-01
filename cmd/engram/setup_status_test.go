package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/codexlifecycle"
	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/setup"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCmdSetupStatusCodexJSONDoesNotInstall(t *testing.T) {
	stubRuntimeHooks(t)
	stubExitWithPanic(t)

	setupInstallAgent = func(string, setup.InstallOptions) (*setup.Result, error) {
		t.Fatal("setup status must not execute installation")
		return nil, nil
	}
	setupInspectCodexStatus = func(runningVersion, runningRevision, cwd string) (setup.CodexIntegrationStatus, error) {
		if runningVersion != version {
			t.Fatalf("running version = %q, want %q", runningVersion, version)
		}
		if runningRevision != commit {
			t.Fatalf("running revision = %q, want %q", runningRevision, commit)
		}
		if strings.TrimSpace(cwd) == "" {
			t.Fatal("working directory must be forwarded to skill discovery")
		}
		compatibility := protocolcontract.Evaluate(
			protocolcontract.Declaration{Version: "3.2.0", Provenance: "pack:abc", Supported: &protocolcontract.VersionRange{Minimum: 1, Maximum: 1}},
			protocolcontract.Declaration{Version: runningVersion, Provenance: "binary:/usr/local/bin/engram", Supported: &protocolcontract.VersionRange{Minimum: 1, Maximum: 1}},
			protocolcontract.Declaration{Version: "0.1.6", Provenance: "plugin:def", Supported: &protocolcontract.VersionRange{Minimum: 1, Maximum: 1}},
		)
		return setup.CodexIntegrationStatus{
			SchemaVersion: setup.CodexIntegrationStatusSchemaVersion,
			Mode:          setup.CodexModeManualSkillCLI,
			Compatibility: compatibility,
			LifecycleCanary: setup.CodexLifecycleCanaryStatus{
				Enabled: true, Valid: true, Treatment: codexlifecycle.TreatmentCueOnlyTargetedRecall,
				SelectionSource: "environment", EnvironmentVariable: codexlifecycle.EnvTreatment,
				ActivationCue: setup.CodexCheckReady, InjectionLimitUTF8Bytes: codexlifecycle.MaxInjectedUTF8Bytes,
				Metrics:    setup.CodexLifecycleMetricsStatus{State: setup.CodexLifecycleMetricsNotObserved, Source: "recall_baseline_session_start", ReasonCode: "lifecycle_metrics_not_observed"},
				ReasonCode: "canary_targeted_recall",
			},
			PromptCapture: setup.CodexPromptCaptureStatus{
				Capability:     setup.CodexCheckReady,
				Identity:       setup.CodexCheckMissing,
				DefaultConsent: setup.CodexCaptureConsentDisabled,
				CurrentConsent: setup.CodexCaptureConsentDisabled,
				Project:        "engram",
				ContentType:    store.CaptureContentTypePrompt,
				Scope:          setup.CodexCaptureScopeNone,
				RetentionDays:  store.DefaultDiagnosticRetentionDays,
				ReasonCode:     "capture_consent_disabled",
			},
			SubagentCapture: setup.CodexSubagentCaptureStatus{
				Capability:    setup.CodexCheckReady,
				Hook:          setup.CodexCheckMissing,
				DefaultState:  setup.CodexSubagentCaptureDefaultDisabled,
				State:         setup.CodexSubagentCaptureDefaultDisabled,
				Project:       "engram",
				ContentType:   store.CaptureContentTypeSubagentOutput,
				Scope:         setup.CodexCaptureScopeNone,
				RetentionDays: store.DefaultDiagnosticRetentionDays,
				ReasonCode:    "subagent_capture_default_disabled",
			},
			Checks: []setup.CodexIntegrationCheck{
				{
					Capability: "engram_cli",
					Status:     setup.CodexCheckReady,
					ReasonCode: "engram_cli_available",
					Reason:     "Engram CLI is available.",
					Evidence: []setup.CodexIntegrationEvidence{
						{Name: "path", Value: "/usr/local/bin/engram"},
						{Name: "version", Value: "2.2.1"},
					},
				},
			},
		}, nil
	}

	withArgs(t, "engram", "setup", "status", "codex", "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdSetup(store.Config{}) })
	if recovered != nil || stderr != "" {
		t.Fatalf("setup status JSON failed: panic=%v stderr=%q", recovered, stderr)
	}

	var got setup.CodexIntegrationStatus
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("decode setup status JSON: %v\n%s", err, stdout)
	}
	if got.SchemaVersion != setup.CodexIntegrationStatusSchemaVersion || got.Mode != setup.CodexModeManualSkillCLI {
		t.Fatalf("setup status JSON = %#v", got)
	}
	if len(got.Checks) != 1 || got.Checks[0].Capability != "engram_cli" {
		t.Fatalf("setup status checks = %#v", got.Checks)
	}
	if got.Compatibility.Status != protocolcontract.CompatibilityReady || len(got.Compatibility.Axes) != 4 {
		t.Fatalf("setup status compatibility = %#v", got.Compatibility)
	}
	if !got.LifecycleCanary.Enabled || got.LifecycleCanary.Treatment != codexlifecycle.TreatmentCueOnlyTargetedRecall ||
		got.LifecycleCanary.ActivationCue != setup.CodexCheckReady || got.LifecycleCanary.Metrics.State != setup.CodexLifecycleMetricsNotObserved {
		t.Fatalf("setup lifecycle canary status = %#v", got.LifecycleCanary)
	}
	if got.PromptCapture.Capability != setup.CodexCheckReady ||
		got.PromptCapture.DefaultConsent != setup.CodexCaptureConsentDisabled ||
		got.PromptCapture.CurrentConsent != setup.CodexCaptureConsentDisabled ||
		got.PromptCapture.Project != "engram" {
		t.Fatalf("setup prompt capture status = %#v", got.PromptCapture)
	}
	if got.SubagentCapture.Capability != setup.CodexCheckReady ||
		got.SubagentCapture.Hook != setup.CodexCheckMissing ||
		got.SubagentCapture.State != setup.CodexSubagentCaptureDefaultDisabled ||
		got.SubagentCapture.Project != "engram" {
		t.Fatalf("setup subagent capture status = %#v", got.SubagentCapture)
	}
}

func TestSetupStatusCodexRunsBeforeUpdateChecksAndStoreResolution(t *testing.T) {
	stubRuntimeHooks(t)
	stubExitWithPanic(t)

	setupInspectCodexStatus = func(string, string, string) (setup.CodexIntegrationStatus, error) {
		return setup.CodexIntegrationStatus{
			SchemaVersion: setup.CodexIntegrationStatusSchemaVersion,
			Agent:         "codex",
			Mode:          setup.CodexModeUnknown,
			Checks:        []setup.CodexIntegrationCheck{},
		}, nil
	}

	args := []string{"setup", "status", "codex"}
	if shouldCheckForUpdates(args) {
		t.Fatal("setup status must not run the network update check")
	}

	withArgs(t, "engram", "setup", "status", "codex")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() {
		if !handleConfigFreeCommand(args) {
			t.Fatal("setup status must finish before store configuration and migrations")
		}
	})
	if recovered != nil || stderr != "" {
		t.Fatalf("config-free setup status failed: panic=%v stderr=%q", recovered, stderr)
	}
	if stdout != "Codex integration mode: unknown\n" {
		t.Fatalf("human status output = %q", stdout)
	}
}

func TestPrintCodexIntegrationStatusReportsTheFourVersionAxes(t *testing.T) {
	stubRuntimeHooks(t)
	report := protocolcontract.Evaluate(
		protocolcontract.Declaration{Version: "3.2.0", Provenance: "pack:abc", Supported: &protocolcontract.VersionRange{Minimum: 1, Maximum: 1}},
		protocolcontract.Declaration{Version: "3.0.1", Provenance: "binary:/opt/engram", Supported: &protocolcontract.VersionRange{Minimum: 1, Maximum: 1}},
		protocolcontract.Declaration{Version: "0.1.6", Provenance: "plugin:def", Supported: &protocolcontract.VersionRange{Minimum: 1, Maximum: 1}},
	)
	stdout, stderr, recovered := captureOutputAndRecover(t, func() {
		printCodexIntegrationStatus(setup.CodexIntegrationStatus{
			Mode:          setup.CodexModeCheckpointReady,
			Compatibility: report,
			LifecycleCanary: setup.CodexLifecycleCanaryStatus{
				Enabled: true, Valid: true, Treatment: codexlifecycle.TreatmentCueOnlyTargetedRecallExactSession,
				SelectionSource: "environment", EnvironmentVariable: codexlifecycle.EnvTreatment,
				ActivationCue: setup.CodexCheckReady, InjectionLimitUTF8Bytes: codexlifecycle.MaxInjectedUTF8Bytes,
				Metrics: setup.CodexLifecycleMetricsStatus{
					State: setup.CodexLifecycleMetricsObserved, Events: 3, P50LatencyMillis: 8.5, P95LatencyMillis: 12,
					TotalInjectedUTF8Bytes: 630, AverageInjectedUTF8Bytes: 210,
				},
			},
			PromptCapture:   setup.CodexPromptCaptureStatus{CurrentConsent: setup.CodexCaptureConsentDisabled},
			SubagentCapture: setup.CodexSubagentCaptureStatus{State: setup.CodexSubagentCaptureDefaultDisabled},
			Checks: []setup.CodexIntegrationCheck{{
				Capability: "prompt_capture",
				Status:     setup.CodexCheckReady,
				ReasonCode: "prompt_capture_available",
				Reason:     "Local Diagnostic prompt capture is available and disabled by default.",
				Evidence: []setup.CodexIntegrationEvidence{
					{Name: "identity", Value: "ready"},
					{Name: "default_consent", Value: "disabled"},
					{Name: "current_consent", Value: "disabled"},
				},
			}},
		})
	})
	if recovered != nil || stderr != "" {
		t.Fatalf("print status failed: panic=%v stderr=%q", recovered, stderr)
	}
	for _, want := range []string{
		"Protocol compatibility: ready (protocol_compatible)",
		"Managed Pack: 3.2.0; Protocol 1..1; pack:abc",
		"Engram binary: 3.0.1; Protocol 1..1; binary:/opt/engram",
		"Codex plugin: 0.1.6; Protocol 1..1; plugin:def",
		"Protocol contract: 1; Protocol 1..1; engram-core",
		"Protocol intersection: 1..1",
		"Codex lifecycle treatment: cue_only_targeted_recall_exact_session (canary enabled: true; source: environment)",
		"Activation cue: ready; injection limit: 4096 UTF-8 bytes",
		"Capture state: prompt=disabled; subagent=default_disabled",
		"Lifecycle metrics: observed; events=3; p50=8.5ms; p95=12ms; total injected=630 UTF-8 bytes; average=210",
		"prompt_capture: ready",
		"identity: ready",
		"default_consent: disabled",
		"current_consent: disabled",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("human status output does not contain %q:\n%s", want, stdout)
		}
	}
}

func TestCmdSetupStatusCodexUnknownArgumentHonorsJSONModeAnywhere(t *testing.T) {
	stubRuntimeHooks(t)
	stubExitWithPanic(t)
	setupInspectCodexStatus = func(string, string, string) (setup.CodexIntegrationStatus, error) {
		t.Fatal("invalid status arguments must fail before inspection")
		return setup.CodexIntegrationStatus{}, nil
	}

	withArgs(t, "engram", "setup", "status", "codex", "--typo", "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdSetup(store.Config{}) })
	if stdout != "" {
		t.Fatalf("stdout = %q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("exit = %#v", recovered)
	}
	if got := decodeCLIJSON(t, stderr)["code"]; got != "invalid_argument" {
		t.Fatalf("error code = %#v, want invalid_argument", got)
	}
}

func TestCmdSetupHelpDocumentsReadOnlyCodexStatus(t *testing.T) {
	stubRuntimeHooks(t)
	stubExitWithPanic(t)
	setupInspectCodexStatus = func(string, string, string) (setup.CodexIntegrationStatus, error) {
		t.Fatal("setup help must not inspect the profile")
		return setup.CodexIntegrationStatus{}, nil
	}

	withArgs(t, "engram", "setup", "--help")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdSetup(store.Config{}) })
	if recovered != nil || stderr != "" {
		t.Fatalf("setup help failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "engram setup status codex [--json]") || !strings.Contains(stdout, "read-only") {
		t.Fatalf("setup help does not document Codex status:\n%s", stdout)
	}
}
