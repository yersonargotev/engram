package recallquery_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yersonargotev/engram/internal/recallquery"
)

func TestVerifyRejectsChangedFrozenInputsWithoutOpeningHeldOut(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"contract.json", "calibration.json"} {
		raw, err := os.ReadFile(filepath.Join("../../evals/recall-query/v1", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := recallquery.Verify(root); err != nil {
		t.Fatalf("verification must not need held-out: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(root, "calibration.json"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(" ")
	_ = f.Close()
	if _, err := recallquery.Verify(root); err == nil {
		t.Fatal("changed fixture accepted")
	}
}
