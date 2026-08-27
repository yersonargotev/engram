package main

import (
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/setup"
)

func TestCmdSetupCodexPassesReleaseIdentityAndReportsIncompleteChecks(t *testing.T) {
	stubRuntimeHooks(t)
	stubExitWithPanic(t)
	cfg := testConfig(t)

	oldVersion, oldCommit := version, commit
	version = "2.2.1"
	commit = "7417bbcc5da4d0946f5746eed5c1b63ed9f0ca6c"
	t.Cleanup(func() {
		version = oldVersion
		commit = oldCommit
	})

	var gotOptions setup.InstallOptions
	setupInstallAgent = func(agent string, options setup.InstallOptions) (*setup.Result, error) {
		gotOptions = options
		return &setup.Result{
			Agent:       agent,
			Destination: "/tmp/codex",
			Files:       3,
			Checks: []setup.CapabilityCheck{
				{Capability: "plugin", Status: setup.CheckReady, Detail: "release verified"},
				{Capability: "mcp", Status: setup.CheckReady, Detail: "stable executable verified"},
				{Capability: "activation-cue", Status: setup.CheckMissing, Detail: "canonical cue missing"},
				{Capability: "verifier", Status: setup.CheckMissing, Detail: "Stop verifier missing"},
			},
		}, nil
	}

	withArgs(t, "engram", "setup", "codex")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdSetup(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("cmdSetup panic=%v stderr=%q", recovered, stderr)
	}
	if gotOptions.Version != version || gotOptions.Commit != commit || gotOptions.Development {
		t.Fatalf("install options = %+v, want stable build identity", gotOptions)
	}
	if strings.Contains(stdout, "✓ Installed codex") {
		t.Fatalf("incomplete setup claimed installation success:\n%s", stdout)
	}
	for _, expected := range []string{"Codex setup incomplete", "plugin: ready", "mcp: ready", "activation-cue: missing", "verifier: missing"} {
		if !strings.Contains(stdout, expected) {
			t.Fatalf("setup output missing %q:\n%s", expected, stdout)
		}
	}
}

func TestCmdSetupCodexDevelopmentFlagIsExplicit(t *testing.T) {
	stubRuntimeHooks(t)
	stubExitWithPanic(t)
	cfg := testConfig(t)

	var gotOptions setup.InstallOptions
	setupInstallAgent = func(agent string, options setup.InstallOptions) (*setup.Result, error) {
		gotOptions = options
		return &setup.Result{Agent: agent, Destination: "/tmp/codex", Complete: true}, nil
	}

	withArgs(t, "engram", "setup", "codex", "--development")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdSetup(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("cmdSetup panic=%v stderr=%q", recovered, stderr)
	}
	if !gotOptions.Development {
		t.Fatalf("install options = %+v, want explicit development mode", gotOptions)
	}
}
