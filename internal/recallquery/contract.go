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
)

const FrozenContractSHA256 = "688c7500cb040b900c75cc831d7584ae97e4ef97936da131b8eea850d76e4ff6"

type Contract struct {
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
	_, err := loadCorpus(root, "calibration", c.CalibrationSHA256, c.UnitsPerCohort)
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
