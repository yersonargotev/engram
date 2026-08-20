//go:build admission_heldout

package memoryops

import (
	"encoding/hex"
	"testing"
)

func TestAdmissionV4HeldOutCorpusMeetsPredeclaredThresholds(t *testing.T) {
	corpus, manifest := readAdmissionV3Corpus(t, "testdata/admission/v4/held_out.manifest.json")
	if corpus.Partition != "held_out" {
		t.Fatalf("partition = %q, want held_out", corpus.Partition)
	}
	if manifest.Freeze == nil || manifest.Thresholds == nil {
		t.Fatalf("held-out manifest requires freeze and thresholds: %#v", manifest)
	}
	freeze := manifest.Freeze
	if commit, err := hex.DecodeString(freeze.ImplementationCommit); err != nil || len(commit) != 20 {
		t.Fatalf("implementation_commit must be a full Git SHA-1: %q", freeze.ImplementationCommit)
	}
	if freeze.EvidenceVersion != EvidenceBundleVersion || freeze.GeneratorVersion != AdmissionGeneratorVersion ||
		freeze.PolicyVersion != AdmissionPolicyVersion || freeze.MetricsVersion != admissionV3MetricsVersion {
		t.Fatalf("runtime versions changed after held-out freeze: %#v", freeze)
	}
	thresholds := manifest.Thresholds
	if thresholds.MinimumGenerationRecall < 0 || thresholds.MinimumGenerationRecall > 1 ||
		thresholds.MinimumAdmitPrecision < 0 || thresholds.MinimumAdmitPrecision > 1 ||
		thresholds.MaximumProtectedRejects < 0 || thresholds.MaximumUnsupported < 0 || thresholds.MaximumPrivacyLeaks < 0 {
		t.Fatalf("invalid held-out thresholds: %#v", thresholds)
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
