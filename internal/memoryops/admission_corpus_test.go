package memoryops

import (
	"encoding/json"
	"io"
	"os"
	"reflect"
	"testing"
)

type generationCorpus struct {
	Version   string               `json:"version"`
	Scenarios []generationScenario `json:"scenarios"`
}

type generationScenario struct {
	ID       string           `json:"id"`
	Evidence EvidenceBundle   `json:"evidence"`
	Expected []MemoryProposal `json:"expected"`
}

type admissionCorpus struct {
	Version   string              `json:"version"`
	Scenarios []admissionScenario `json:"scenarios"`
}

type admissionScenario struct {
	ID               string              `json:"id"`
	ExistingContents []string            `json:"existing_contents"`
	Proposal         MemoryProposal      `json:"proposal"`
	Expected         AdmissionAssessment `json:"expected"`
}

func TestAdmissionGenerationCorpus(t *testing.T) {
	var corpus generationCorpus
	decodeCorpus(t, "testdata/admission/v1/generation.json", &corpus)
	if corpus.Version != EvidenceBundleVersion {
		t.Fatalf("corpus version = %q, want %q", corpus.Version, EvidenceBundleVersion)
	}
	if len(corpus.Scenarios) != 12 {
		t.Fatalf("generation scenarios = %d, want 12", len(corpus.Scenarios))
	}

	for _, scenario := range corpus.Scenarios {
		t.Run(scenario.ID, func(t *testing.T) {
			for repetition := 0; repetition < 3; repetition++ {
				proposals, err := generateMemoryProposals(scenario.Evidence)
				if err != nil {
					t.Fatalf("generate repetition %d: %v", repetition+1, err)
				}
				if !reflect.DeepEqual(proposals, scenario.Expected) {
					t.Fatalf("generation mismatch repetition %d:\n got: %#v\nwant: %#v", repetition+1, proposals, scenario.Expected)
				}
			}
		})
	}
}

func TestAdmissionAssessmentCorpus(t *testing.T) {
	var corpus admissionCorpus
	decodeCorpus(t, "testdata/admission/v1/admission.json", &corpus)
	if corpus.Version != EvidenceBundleVersion {
		t.Fatalf("corpus version = %q, want %q", corpus.Version, EvidenceBundleVersion)
	}
	if len(corpus.Scenarios) != 12 {
		t.Fatalf("admission scenarios = %d, want 12", len(corpus.Scenarios))
	}

	protectedCategories := map[ProposalCategory]bool{}
	for _, scenario := range corpus.Scenarios {
		t.Run(scenario.ID, func(t *testing.T) {
			service := newTestService(t)
			for index, content := range scenario.ExistingContents {
				saveObservation(t, service, "engram", scenario.ID+string(rune('a'+index)), content)
			}
			for repetition := 0; repetition < 3; repetition++ {
				assessment, err := service.assessMemoryProposal("engram", scenario.Proposal)
				if err != nil {
					t.Fatalf("assess repetition %d: %v", repetition+1, err)
				}
				if !reflect.DeepEqual(assessment, scenario.Expected) {
					t.Fatalf("assessment mismatch repetition %d:\n got: %#v\nwant: %#v", repetition+1, assessment, scenario.Expected)
				}
				if scenario.Proposal.Protected && assessment.Recommendation == AdmissionReject {
					t.Fatal("protected proposal was rejected")
				}
			}
			if scenario.Proposal.Protected {
				protectedCategories[scenario.Proposal.Category] = true
			}
		})
	}

	for _, category := range []ProposalCategory{ProposalExplicitRequest, ProposalDecision, ProposalRootCause, ProposalInvariant, ProposalConstraint, ProposalPreference} {
		if !protectedCategories[category] {
			t.Errorf("protected category %q is absent from admission corpus", category)
		}
	}
}

func decodeCorpus(t *testing.T, path string, target any) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open corpus %s: %v", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode corpus %s: %v", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("corpus %s contains trailing JSON: %v", path, err)
	}
}
