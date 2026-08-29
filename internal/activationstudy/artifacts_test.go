package activationstudy

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEventSetRoundTripAndMarkdownReportAreDeterministic(t *testing.T) {
	t.Parallel()

	contractPath, hashPath := writeFrozenContract(t, validContractJSON())
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	eventSet := EventSet{
		SchemaVersion: "codex-activation-event-set-v1", StudyID: study.Contract.StudyID,
		StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash,
		Verification: validVerification(study), Records: recordsForPlan(study.Plan()),
	}
	path := filepath.Join(t.TempDir(), "events.json")
	if err := WriteJSON(path, eventSet); err != nil {
		t.Fatal(err)
	}
	loaded, err := ReadEventSet(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Records) != len(eventSet.Records) || loaded.ContractSHA256 != eventSet.ContractSHA256 {
		t.Fatalf("loaded event set = %#v", loaded)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o022 != 0 {
		t.Fatalf("events file is group/world writable: %v", info.Mode().Perm())
	}

	report, err := study.Analyze(loaded)
	if err != nil {
		t.Fatal(err)
	}
	first := RenderMarkdown(report)
	second := RenderMarkdown(report)
	if !bytes.Equal([]byte(first), []byte(second)) {
		t.Fatal("RenderMarkdown() is not deterministic")
	}
	for _, section := range []string{"# Codex Activation Study", "## Study questions", "## Rates", "## Paired differences", study.Hash} {
		if !strings.Contains(first, section) {
			t.Fatalf("Markdown report is missing %q", section)
		}
	}
	markdownPath := filepath.Join(t.TempDir(), "nested", "report.md")
	if err := WriteMarkdown(markdownPath, report); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(markdownPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(written) != first {
		t.Fatal("WriteMarkdown() did not preserve deterministic output")
	}
}

func TestWritePrivateJSONRestrictsRowLevelEvidenceToTheOwner(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.json")
	if err := WritePrivateJSON(path, map[string]bool{"bounded": true}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("private event mode = %o, want 600", info.Mode().Perm())
	}
}
