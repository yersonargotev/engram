package recallquery_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yersonargotev/engram/internal/recallquery"
)

func TestVerifyRejectsChangedFrozenInputsWithoutOpeningHeldOut(t *testing.T) {
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

func TestVerifyRejectsChangedEvaluatorRules(t *testing.T) {
	root := "../../evals/recall-query/v2"
	sourceRoot := t.TempDir()
	for _, name := range []string{"contract.go", "execution.go", "report.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(sourceRoot, "internal/recallquery", name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../../cmd/recall-query-eval/main.go")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(sourceRoot, "cmd/recall-query-eval/main.go")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := recallquery.VerifySource(root, sourceRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "internal/recallquery/execution.go"), []byte("changed fixture generation"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := recallquery.VerifySource(root, sourceRoot); err == nil {
		t.Fatal("changed effective fixture code accepted without new version")
	}
}
