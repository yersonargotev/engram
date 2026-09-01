package plugin_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func codexTestBash(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "windows" {
		path, err := exec.LookPath("bash")
		if err != nil {
			t.Fatalf("find bash: %v", err)
		}
		return path
	}

	gitPath, err := exec.LookPath("git.exe")
	if err != nil {
		t.Fatalf("find Git for Windows: %v", err)
	}
	bashPath := filepath.Clean(filepath.Join(filepath.Dir(gitPath), "..", "bin", "bash.exe"))
	if _, err := os.Stat(bashPath); err != nil {
		t.Fatalf("find Git Bash at %s: %v", bashPath, err)
	}
	return bashPath
}

func requireCodexUnixTools(t *testing.T, bashPath string) {
	t.Helper()
	check := exec.Command(bashPath, "-lc", "command -v jq >/dev/null && command -v curl >/dev/null")
	if output, err := check.CombinedOutput(); err != nil {
		t.Fatalf("Unix lifecycle runtime tests require jq and curl: %v: %s", err, output)
	}
}
