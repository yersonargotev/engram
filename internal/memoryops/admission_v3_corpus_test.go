package memoryops

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const admissionV3MetricsVersion = "admission-v3-metrics-v1"

type admissionV3Thresholds struct {
	MinimumGenerationRecall float64 `json:"minimum_generation_recall"`
	MinimumAdmitPrecision   float64 `json:"minimum_admit_precision"`
	MaximumProtectedRejects int     `json:"maximum_protected_false_rejects"`
	MaximumUnsupported      int     `json:"maximum_unsupported_proposals"`
	MaximumPrivacyLeaks     int     `json:"maximum_privacy_leaks"`
}

type admissionCorpusManifest struct {
	SchemaVersion string                    `json:"schema_version"`
	CorpusVersion string                    `json:"corpus_version"`
	Partition     string                    `json:"partition"`
	CorpusFile    string                    `json:"corpus_file"`
	CorpusSHA256  string                    `json:"corpus_sha256"`
	ScenarioIDs   []string                  `json:"scenario_ids"`
	LabelSchema   string                    `json:"label_schema"`
	Provenance    admissionCorpusProvenance `json:"provenance"`
	Freeze        *admissionCorpusFreeze    `json:"freeze,omitempty"`
	Thresholds    *admissionV3Thresholds    `json:"thresholds,omitempty"`
}

type admissionCorpusProvenance struct {
	Kind          string `json:"kind"`
	Source        string `json:"source"`
	ConsentStatus string `json:"consent_status"`
}

type admissionCorpusFreeze struct {
	ImplementationCommit string `json:"implementation_commit"`
	EvidenceVersion      string `json:"evidence_version"`
	GeneratorVersion     string `json:"generator_version"`
	PolicyVersion        string `json:"policy_version"`
	MetricsVersion       string `json:"metrics_version"`
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
	corpus, manifest := readAdmissionV3Corpus(t, "testdata/admission/v3/calibration.manifest.json")
	if corpus.Partition != "calibration" {
		t.Fatalf("partition = %q, want calibration", corpus.Partition)
	}
	if manifest.Freeze != nil || manifest.Thresholds != nil {
		t.Fatalf("calibration manifest must not declare held-out freeze or thresholds: %#v", manifest)
	}
	metrics := executeAdmissionV3Corpus(t, corpus)
	if metrics.MatchedFacts != metrics.ExpectedFacts || metrics.UnsupportedProposals != 0 || metrics.PrivacyLeaks != 0 {
		t.Fatalf("calibration metrics = %#v", metrics)
	}
}

func TestAdmissionV3ObservedRegressionCorpus(t *testing.T) {
	corpus, manifest := readAdmissionV3Corpus(t, "testdata/admission/v3/observed_regression.manifest.json")
	if corpus.Partition != "observed_regression" {
		t.Fatalf("partition = %q, want observed_regression", corpus.Partition)
	}
	if manifest.Freeze != nil || manifest.Thresholds != nil {
		t.Fatalf("observed regression manifest must not claim a held-out freeze: %#v", manifest)
	}
	metrics := executeAdmissionV3Corpus(t, corpus)
	if metrics.MatchedFacts != metrics.ExpectedFacts || metrics.UnsupportedProposals != 0 || metrics.PrivacyLeaks != 0 {
		t.Fatalf("observed regression metrics = %#v", metrics)
	}
}

func executeAdmissionV3Corpus(t *testing.T, corpus admissionV3Corpus) admissionV3CorpusMetrics {
	t.Helper()
	if strings.TrimSpace(corpus.Version) == "" || len(corpus.Scenarios) == 0 {
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

func readAdmissionV3Corpus(t *testing.T, manifestPath string) (admissionV3Corpus, admissionCorpusManifest) {
	t.Helper()
	var manifest admissionCorpusManifest
	decodeAdmissionV3JSON(t, manifestPath, &manifest)
	if manifest.SchemaVersion != "admission-corpus-manifest-v1" || manifest.CorpusVersion == "" || manifest.Partition == "" ||
		manifest.CorpusFile == "" || manifest.CorpusSHA256 == "" || len(manifest.ScenarioIDs) == 0 || manifest.LabelSchema == "" {
		t.Fatalf("invalid corpus manifest header: %#v", manifest)
	}
	if manifest.Provenance.Kind != "synthetic" || strings.TrimSpace(manifest.Provenance.Source) == "" || manifest.Provenance.ConsentStatus != "not_applicable" {
		t.Fatalf("invalid synthetic corpus provenance: %#v", manifest.Provenance)
	}
	if filepath.Base(manifest.CorpusFile) != manifest.CorpusFile {
		t.Fatalf("corpus_file must be a local file name: %q", manifest.CorpusFile)
	}
	corpusPath := filepath.Join(filepath.Dir(manifestPath), manifest.CorpusFile)
	encoded, err := os.ReadFile(corpusPath)
	if err != nil {
		t.Fatalf("read %s: %v", corpusPath, err)
	}
	if actual := fmt.Sprintf("%x", sha256.Sum256(encoded)); actual != manifest.CorpusSHA256 {
		t.Fatalf("corpus hash = %q, want frozen %q", actual, manifest.CorpusSHA256)
	}
	var corpus admissionV3Corpus
	decodeAdmissionV3JSONBytes(t, corpusPath, encoded, &corpus)
	if corpus.Version != manifest.CorpusVersion || corpus.Partition != manifest.Partition {
		t.Fatalf("corpus header %#v does not match manifest %#v", corpus, manifest)
	}
	ids := make([]string, 0, len(corpus.Scenarios))
	seen := make(map[string]struct{}, len(corpus.Scenarios))
	for _, scenario := range corpus.Scenarios {
		if scenario.ID == "" {
			t.Fatal("corpus scenario id must not be empty")
		}
		if _, duplicate := seen[scenario.ID]; duplicate {
			t.Fatalf("duplicate corpus scenario id %q", scenario.ID)
		}
		seen[scenario.ID] = struct{}{}
		ids = append(ids, scenario.ID)
	}
	if !reflect.DeepEqual(ids, manifest.ScenarioIDs) {
		t.Fatalf("corpus scenario ids = %#v, want frozen %#v", ids, manifest.ScenarioIDs)
	}
	return corpus, manifest
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

func decodeAdmissionV3JSONBytes(t *testing.T, path string, encoded []byte, target any) {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
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
