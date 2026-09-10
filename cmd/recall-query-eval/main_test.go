package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestVerifierCommandNeedsNoHeldOutAndRejectsTampering(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "recall-query-eval")
	if raw, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, raw)
	}
	root := t.TempDir()
	for _, name := range []string{"contract.json", "calibration.json", "source.json"} {
		raw, err := os.ReadFile(filepath.Join("../../evals/recall-query/v2", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := exec.Command(binary, "-root", root, "-verify").Output()
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Verified      bool `json:"verified"`
		HeldOutOpened bool `json:"held_out_opened"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || !result.Verified || result.HeldOutOpened {
		t.Fatalf("verify output: %s %v", raw, err)
	}
	if err := os.WriteFile(filepath.Join(root, "calibration.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command(binary, "-root", root, "-verify").Run(); err == nil {
		t.Fatal("tampered corpus accepted")
	}
}
