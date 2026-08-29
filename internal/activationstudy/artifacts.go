package activationstudy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const maxEventSetBytes = 32 << 20

func ReadEventSet(path string) (EventSet, error) {
	raw, err := readBoundedFile(path, maxEventSetBytes)
	if err != nil {
		return EventSet{}, fmt.Errorf("read activation event set: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var events EventSet
	if err := decoder.Decode(&events); err != nil {
		return EventSet{}, fmt.Errorf("decode activation event set: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return EventSet{}, fmt.Errorf("decode activation event set: multiple JSON values are not allowed")
	}
	return events, nil
}

// WriteJSON atomically writes one stable, indented shared artifact.
func WriteJSON(path string, value any) error {
	return writeJSON(path, value, 0o644)
}

// WritePrivateJSON atomically writes one owner-readable row-level artifact.
func WritePrivateJSON(path string, value any) error {
	return writeJSON(path, value, 0o600)
}

func writeJSON(path string, value any, mode os.FileMode) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("activation artifact output path is required")
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomicArtifact(path, append(encoded, '\n'), mode)
}

// WriteMarkdown atomically writes one deterministic aggregate report.
func WriteMarkdown(path string, report Report) error {
	return writeAtomicArtifact(path, []byte(RenderMarkdown(report)), 0o644)
}

func writeAtomicArtifact(path string, encoded []byte, mode os.FileMode) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("activation artifact output path is required")
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".activation-study-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	removeTemporary = false
	return nil
}

func RenderMarkdown(report Report) string {
	var output strings.Builder
	fmt.Fprintf(&output, "# Codex Activation Study\n\n")
	fmt.Fprintf(&output, "- Study: `%s/%s`\n", report.StudyID, report.StudyVersion)
	fmt.Fprintf(&output, "- Contract SHA-256: `%s`\n", report.ContractSHA256)
	fmt.Fprintf(&output, "- Planned/retained cells: %d/%d\n", report.SampleSize.Planned, report.SampleSize.Retained)
	fmt.Fprintf(&output, "- Integration failures: %d\n\n", report.SampleSize.IntegrationFailures)

	output.WriteString("## Study questions\n\n")
	fmt.Fprintf(&output, "1. **Repository guidance:** %s\n", report.Questions.RepositoryGuidance)
	fmt.Fprintf(&output, "2. **Overlapping skills:** %s\n", report.Questions.OverlappingSkills)
	fmt.Fprintf(&output, "3. **CLI after selection:** %s\n", report.Questions.CLIFollowsSelection)
	fmt.Fprintf(&output, "4. **Useful outcomes:** %s\n\n", report.Questions.UsefulOutcomes)

	rates := append([]RateMetric(nil), report.Rates...)
	sort.Slice(rates, func(i, j int) bool { return rateKey(rates[i]) < rateKey(rates[j]) })
	output.WriteString("## Rates\n\n")
	output.WriteString("| Scope | Value | Treatment | Event | N | Count | Omitted | Rate | Wilson 95% |\n")
	output.WriteString("|---|---|---|---|---:|---:|---:|---:|---:|\n")
	for _, metric := range rates {
		fmt.Fprintf(&output, "| %s | %s | %s | %s | %d | %d | %d | %.4f | %.4f–%.4f |\n",
			markdownCell(metric.Scope), markdownCell(metric.Value), markdownCell(metric.Treatment), markdownCell(metric.Event),
			metric.N, metric.Count, metric.Omitted, metric.Rate, metric.Lower, metric.Upper)
	}

	differences := append([]PairedDifference(nil), report.PairedDifferences...)
	sort.Slice(differences, func(i, j int) bool { return differenceKey(differences[i]) < differenceKey(differences[j]) })
	output.WriteString("\n## Paired differences\n\n")
	output.WriteString("| Scope | Value | First | Second | Event | N | Omitted | Difference | Bootstrap 95% |\n")
	output.WriteString("|---|---|---|---|---|---:|---:|---:|---:|\n")
	for _, metric := range differences {
		fmt.Fprintf(&output, "| %s | %s | %s | %s | %s | %d | %d | %.4f | %.4f–%.4f |\n",
			markdownCell(metric.Scope), markdownCell(metric.Value), markdownCell(metric.FirstTreatment), markdownCell(metric.SecondTreatment), markdownCell(metric.Event),
			metric.N, metric.Omitted, metric.Difference, metric.Lower, metric.Upper)
	}

	writeCodeCounts(&output, "Omissions", report.Omissions)
	writeCodeCounts(&output, "Protocol deviations", report.ProtocolDeviations)
	return output.String()
}

func writeCodeCounts(output *strings.Builder, title string, counts []CodeCount) {
	fmt.Fprintf(output, "\n## %s\n\n", title)
	if len(counts) == 0 {
		output.WriteString("None.\n")
		return
	}
	ordered := append([]CodeCount(nil), counts...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Code < ordered[j].Code })
	for _, count := range ordered {
		fmt.Fprintf(output, "- `%s`: %d\n", count.Code, count.Count)
	}
}

func markdownCell(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}
