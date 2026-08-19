package memoryops

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

type admissionV3Thresholds struct {
	Version                 string  `json:"version"`
	MinimumGenerationRecall float64 `json:"minimum_generation_recall"`
	MinimumAdmitPrecision   float64 `json:"minimum_admit_precision"`
	MaximumProtectedRejects int     `json:"maximum_protected_false_rejects"`
	MaximumUnsupported      int     `json:"maximum_unsupported_proposals"`
	MaximumPrivacyLeaks     int     `json:"maximum_privacy_leaks"`
}

type admissionV3Corpus struct {
	Version   string                `json:"version"`
	Partition string                `json:"partition"`
	Scenarios []admissionV3Scenario `json:"scenarios"`
}

type admissionV3Scenario struct {
	ID               string                   `json:"id"`
	Evidence         EvidenceBundle           `json:"evidence"`
	ExistingContents []string                 `json:"existing_contents"`
	Expected         []admissionV3Expectation `json:"expected"`
	ForbiddenText    []string                 `json:"forbidden_text"`
}

type admissionV3Expectation struct {
	Content        string                  `json:"content"`
	Category       ProposalCategory        `json:"category"`
	Recommendation AdmissionRecommendation `json:"recommendation"`
}

type admissionV3CorpusMetrics struct {
	ExpectedFacts         int
	GeneratedFacts        int
	MatchedFacts          int
	Admitted              int
	CorrectlyAdmitted     int
	ProtectedFalseRejects int
	UnsupportedProposals  int
	PrivacyLeaks          int
}

func TestAdmissionV3CalibrationCorpus(t *testing.T) {
	corpus := readAdmissionV3Corpus(t, "testdata/admission/v3/calibration.json")
	if corpus.Partition != "calibration" {
		t.Fatalf("partition = %q, want calibration", corpus.Partition)
	}
	metrics := executeAdmissionV3Corpus(t, corpus)
	if metrics.MatchedFacts != metrics.ExpectedFacts || metrics.UnsupportedProposals != 0 || metrics.PrivacyLeaks != 0 {
		t.Fatalf("calibration metrics = %#v", metrics)
	}
}

func TestAdmissionV3HeldOutCorpusMeetsPredeclaredThresholds(t *testing.T) {
	var thresholds admissionV3Thresholds
	decodeAdmissionV3JSON(t, "testdata/admission/v3/thresholds.json", &thresholds)
	if thresholds.Version != "v3" {
		t.Fatalf("threshold version = %q, want v3", thresholds.Version)
	}
	corpus := readAdmissionV3Corpus(t, "testdata/admission/v3/held_out.json")
	if corpus.Partition != "held_out" {
		t.Fatalf("partition = %q, want held_out", corpus.Partition)
	}
	metrics := executeAdmissionV3Corpus(t, corpus)

	recall := ratio(metrics.MatchedFacts, metrics.ExpectedFacts)
	precision := ratio(metrics.CorrectlyAdmitted, metrics.Admitted)
	if recall < thresholds.MinimumGenerationRecall {
		t.Errorf("generation recall = %.3f, want >= %.3f", recall, thresholds.MinimumGenerationRecall)
	}
	if precision < thresholds.MinimumAdmitPrecision {
		t.Errorf("admit precision = %.3f, want >= %.3f", precision, thresholds.MinimumAdmitPrecision)
	}
	if metrics.ProtectedFalseRejects > thresholds.MaximumProtectedRejects {
		t.Errorf("protected false rejects = %d, want <= %d", metrics.ProtectedFalseRejects, thresholds.MaximumProtectedRejects)
	}
	if metrics.UnsupportedProposals > thresholds.MaximumUnsupported {
		t.Errorf("unsupported proposals = %d, want <= %d", metrics.UnsupportedProposals, thresholds.MaximumUnsupported)
	}
	if metrics.PrivacyLeaks > thresholds.MaximumPrivacyLeaks {
		t.Errorf("privacy leaks = %d, want <= %d", metrics.PrivacyLeaks, thresholds.MaximumPrivacyLeaks)
	}
}

func executeAdmissionV3Corpus(t *testing.T, corpus admissionV3Corpus) admissionV3CorpusMetrics {
	t.Helper()
	if corpus.Version != "v3" || len(corpus.Scenarios) == 0 {
		t.Fatalf("invalid corpus header: %#v", corpus)
	}
	metrics := admissionV3CorpusMetrics{}
	for _, scenario := range corpus.Scenarios {
		t.Run(scenario.ID, func(t *testing.T) {
			service := newTestService(t)
			for index, content := range scenario.ExistingContents {
				saveObservation(t, service, "engram", scenario.ID+string(rune('a'+index)), content)
			}
			proposals, err := generateMemoryProposals(scenario.Evidence)
			if err != nil {
				t.Fatalf("generate proposals: %v", err)
			}
			metrics.ExpectedFacts += len(scenario.Expected)
			metrics.GeneratedFacts += len(proposals)
			matched := make(map[int]bool, len(scenario.Expected))
			for _, proposal := range proposals {
				assessment, err := service.assessMemoryProposal("engram", proposal)
				if err != nil {
					t.Fatalf("assess proposal: %v", err)
				}
				expectedIndex := -1
				for index, expected := range scenario.Expected {
					if expected.Content == proposal.Content && expected.Category == proposal.Category && expected.Recommendation == assessment.Recommendation {
						expectedIndex = index
						break
					}
				}
				if expectedIndex >= 0 {
					if !matched[expectedIndex] {
						metrics.MatchedFacts++
						matched[expectedIndex] = true
					}
				} else {
					metrics.UnsupportedProposals++
				}
				if assessment.Recommendation == AdmissionAdmit {
					metrics.Admitted++
					if expectedIndex >= 0 {
						metrics.CorrectlyAdmitted++
					}
				}
				if proposal.Protected && assessment.Recommendation == AdmissionReject {
					metrics.ProtectedFalseRejects++
				}
				for _, forbidden := range scenario.ForbiddenText {
					if strings.Contains(proposal.Title, forbidden) || strings.Contains(proposal.Content, forbidden) {
						metrics.PrivacyLeaks++
					}
				}
			}
			if len(matched) != len(scenario.Expected) {
				t.Errorf("matched expectations = %d, want %d; proposals=%#v", len(matched), len(scenario.Expected), proposals)
			}
		})
	}
	return metrics
}

func readAdmissionV3Corpus(t *testing.T, path string) admissionV3Corpus {
	t.Helper()
	var corpus admissionV3Corpus
	decodeAdmissionV3JSON(t, path, &corpus)
	return corpus
}

func decodeAdmissionV3JSON(t *testing.T, path string, target any) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("%s contains trailing JSON: %v", path, err)
	}
}

func ratio(numerator, denominator int) float64 {
	if denominator == 0 {
		return 1
	}
	return float64(numerator) / float64(denominator)
}
