// Package recallquery evaluates bounded query discovery without changing Recall policy.
package recallquery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Contract struct {
	EvaluatorSHA256   string            `json:"evaluator_sha256"`
	StudyID           string            `json:"study_id"`
	Version           string            `json:"version"`
	SourceRevision    string            `json:"source_revision"`
	Protocol          int               `json:"protocol"`
	CalibrationSHA256 string            `json:"calibration_sha256"`
	HeldOutSHA256     string            `json:"held_out_sha256"`
	UnitsPerCohort    int               `json:"units_per_cohort"`
	CandidateLimit    int               `json:"candidate_limit"`
	CandidateBytes    int               `json:"candidate_bytes"`
	Reformulations    int               `json:"reformulations"`
	Policy            map[string]string `json:"policy"`
}

type Corpus struct {
	Cohort string `json:"cohort"`
	Units  []Unit `json:"units"`
}

type Unit struct {
	ID            string `json:"id"`
	Issue         string `json:"issue"`
	Concept       string `json:"concept"`
	Task          string `json:"task"`
	Reformulation string `json:"reformulation"`
	Invariant     string `json:"invariant"`
	Diagnosis     string `json:"diagnosis"`
	Resolution    string `json:"resolution"`
	Editorial     string `json:"editorial"`
	Rationale     string `json:"rationale"`
}

// Verify checks frozen prerequisites without opening held-out inputs.
func Verify(root string) (Contract, error) {
	var c Contract
	if err := readCommitted(filepath.Join(root, "contract.json"), FrozenContractSHA256, &c); err != nil {
		return c, err
	}
	sourceRoot, err := repositoryRoot()
	if err != nil {
		return c, err
	}
	if err := VerifySource(root, sourceRoot); err != nil {
		return c, err
	}
	_, err = loadCorpus(root, "calibration", c.CalibrationSHA256, c.UnitsPerCohort)
	return c, err
}

func readCommitted(path, digest string, out any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if !stat.Mode().IsRegular() || stat.Size() > 1<<20 {
		return fmt.Errorf("invalid bounded artifact: %s", path)
	}
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("frozen artifact commitment mismatch: %s", filepath.Base(path))
	}
	return json.Unmarshal(raw, out)
}

func loadCorpus(root, cohort, digest string, count int) (Corpus, error) {
	var c Corpus
	if err := readCommitted(filepath.Join(root, cohort+".json"), digest, &c); err != nil {
		return c, err
	}
	if c.Cohort != cohort || len(c.Units) != count {
		return c, fmt.Errorf("invalid cohort identity/count")
	}
	seen := map[string]bool{}
	for _, u := range c.Units {
		for _, v := range []string{u.ID, u.Issue, u.Concept, u.Task, u.Reformulation, u.Invariant, u.Diagnosis, u.Resolution, u.Editorial, u.Rationale} {
			if v == "" {
				return c, fmt.Errorf("missing unit evidence")
			}
		}
		for _, v := range []string{"id:" + u.ID, "issue:" + u.Issue, "concept:" + u.Concept} {
			if seen[v] {
				return c, fmt.Errorf("overlapping information needs")
			}
			seen[v] = true
		}
	}
	return c, nil
}

// VerifySource binds fixture generation, execution and metric code to the study.
// anchor.go contains only the compiled trust root and is intentionally separate.
func VerifySource(root, sourceRoot string) error {
	var c Contract
	if err := readCommitted(filepath.Join(root, "contract.json"), FrozenContractSHA256, &c); err != nil {
		return err
	}
	var sources map[string]string
	if err := readCommitted(filepath.Join(root, "source.json"), c.EvaluatorSHA256, &sources); err != nil {
		return err
	}
	discovered := 0
	for _, dir := range []string{"internal/recallquery", "cmd/recall-query-eval"} {
		files, err := filepath.Glob(filepath.Join(sourceRoot, dir, "*.go"))
		if err != nil {
			return err
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") || filepath.Base(file) == "anchor.go" {
				continue
			}
			discovered++
		}
	}
	if discovered != len(sources) {
		return fmt.Errorf("frozen evaluator source membership mismatch; new version required")
	}
	for name, digest := range sources {
		raw, err := os.ReadFile(filepath.Join(sourceRoot, filepath.FromSlash(name)))
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != digest {
			return fmt.Errorf("frozen evaluator source mismatch: %s; new version required", name)
		}
	}
	return nil
}

func repositoryRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("run from evaluator source checkout")
		}
		current = parent
	}
}
