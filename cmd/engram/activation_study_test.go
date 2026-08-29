package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestActivationStudyHelpIsConfigFree(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "real-store-must-not-exist")
	t.Setenv("ENGRAM_DATA_DIR", dataDir)
	withArgs(t, "engram", "activation-study", "--help")

	stdout, stderr, recovered := captureOutputAndRecover(t, func() {
		if !handleConfigFreeCommand(os.Args[1:]) {
			t.Fatal("activation-study was not handled as a config-free command")
		}
	})
	if recovered != nil || stderr != "" {
		t.Fatalf("help recovered=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "engram activation-study verify|run|analyze") {
		t.Fatalf("help output = %q", stdout)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("config-free help created the real store: %v", err)
	}
	if shouldCheckForUpdates(os.Args[1:]) {
		t.Fatal("activation-study should never perform update checks")
	}
}

func TestActivationStudyUnknownFlagUsesJSONError(t *testing.T) {
	stubExitWithPanic(t)
	withArgs(t, "engram", "activation-study", "analyze", "--unknown", "--json")

	_, stderr, recovered := captureOutputAndRecover(t, cmdActivationStudy)
	code, ok := recovered.(exitCode)
	if !ok || int(code) != 1 {
		t.Fatalf("recovered = %v, want exit 1", recovered)
	}
	if !strings.Contains(stderr, `"code":"unknown_flag"`) || !strings.Contains(stderr, "--unknown") {
		t.Fatalf("stderr = %q", stderr)
	}
}
