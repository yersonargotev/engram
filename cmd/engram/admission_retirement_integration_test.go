package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAdmissionRetirementKeepsActiveGuidanceFreeOfRetiredTerms(t *testing.T) {
	repositoryRoot := filepath.Clean(filepath.Join("..", ".."))
	activeGuidance := []string{
		filepath.Join(repositoryRoot, "README.md"),
		filepath.Join(repositoryRoot, "DOCS.md"),
		filepath.Join(repositoryRoot, "CONTEXT.md"),
		filepath.Join(repositoryRoot, "CONTRIBUTING.md"),
	}

	docsRoot := filepath.Join(repositoryRoot, "docs")
	err := filepath.WalkDir(docsRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path == filepath.Join(docsRoot, "adr") || path == filepath.Join(docsRoot, "research") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.EqualFold(filepath.Ext(path), ".md") {
			activeGuidance = append(activeGuidance, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("enumerate active guidance: %v", err)
	}

	for _, path := range activeGuidance {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read active guidance %s: %v", path, err)
		}
		lower := strings.ToLower(string(content))
		for _, retiredTerm := range []string{
			"admission",
			"promotion candidate",
			"automatic promotion",
			"promotion path",
			"promotion runs",
		} {
			if strings.Contains(lower, retiredTerm) {
				t.Errorf("active guidance %s still contains retired product term %q", path, retiredTerm)
			}
		}
	}
}

func TestAdmissionRetirementDeclaresV3BreakingReleaseContract(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "CHANGELOG.md"))
	if err != nil {
		t.Fatalf("read CHANGELOG.md: %v", err)
	}

	changelog := strings.ToLower(string(content))
	for _, required := range []string{
		"engram v3.0.0 breaking boundary",
		"no deprecation window",
		"no compatibility layer",
		"adr-0008",
	} {
		if !strings.Contains(changelog, required) {
			t.Errorf("CHANGELOG.md is missing the v3 release contract token %q", required)
		}
	}
}

func TestAdmissionRetirementDeclaresFrozenEvaluationsHistorical(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "evals", "README.md"))
	if err != nil {
		t.Fatalf("read evals/README.md: %v", err)
	}

	index := strings.Join(strings.Fields(strings.ToLower(string(content))), " ")
	for _, required := range []string{
		"frozen historical evidence",
		"not active product guidance",
		"must remain unchanged",
	} {
		if !strings.Contains(index, required) {
			t.Errorf("evals/README.md is missing the historical-evidence token %q", required)
		}
	}
}
