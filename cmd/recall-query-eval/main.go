// recall-query-eval is an offline developer tool, not a production Recall adapter.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/yersonargotev/engram/internal/recallquery"
)

type publication struct {
	recallquery.Report
	EvaluatorRevision string `json:"evaluator_revision"`
	ExecutableSHA256  string `json:"executable_sha256"`
	GoVersion         string `json:"go_version"`
	Platform          string `json:"platform"`
	WorkingTreeDirty  bool   `json:"working_tree_dirty"`
	Model             string `json:"model"`
	HostPlugin        string `json:"host_plugin"`
	ManagedPack       string `json:"managed_pack"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	root := flag.String("root", "evals/recall-query/v2", "frozen study directory")
	verify := flag.Bool("verify", false, "verify prerequisites without opening held-out")
	held := flag.Bool("held-out", false, "execute held-out only after successful calibration")
	flag.Parse()
	if flag.NArg() != 0 || (*verify && *held) {
		return fmt.Errorf("unexpected arguments or incompatible modes")
	}
	c, err := recallquery.Verify(*root)
	if err != nil {
		return err
	}
	if *verify {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"verified": true, "contract_sha256": recallquery.FrozenContractSHA256, "held_out_opened": false})
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return fmt.Errorf("build provenance unavailable; use go build")
	}
	revision := ""
	dirty := false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			dirty = setting.Value == "true"
		}
	}
	if revision == "" || dirty {
		return fmt.Errorf("build evaluator with go build from a clean committed tree")
	}
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(head)) != revision {
		return fmt.Errorf("run from the evaluator source checkout at its compiled revision")
	}
	status, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil || len(status) != 0 {
		return fmt.Errorf("evaluation requires a clean source checkout")
	}
	if err := exec.Command("git", "diff", "--quiet", c.SourceRevision, "HEAD", "--", "internal/store", "internal/memoryops", "internal/project", "internal/protocolcontract", "go.mod", "go.sum").Run(); err != nil {
		return fmt.Errorf("Core source does not match the frozen revision")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(executable)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	report, err := recallquery.Execute(context.Background(), *root, *held)
	output := publication{Report: report, EvaluatorRevision: revision, ExecutableSHA256: hex.EncodeToString(sum[:]), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, Model: "not_participating", HostPlugin: "not_participating", ManagedPack: "not_participating"}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if writeErr := encoder.Encode(output); writeErr != nil {
		return writeErr
	}
	if err != nil {
		return err
	}
	if !report.CalibrationPassed || (*held && report.Disposition == "evaluation_incomplete") {
		return fmt.Errorf("evaluation prerequisites failed; see aggregate evidence")
	}
	return nil
}
