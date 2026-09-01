package recallstudy

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestVerifyAndPlanFreezeNonOverlappingPairedCohorts(t *testing.T) {
	t.Parallel()

	study, calibration, heldOut := verifiedStudy(t)
	report, err := study.Verify(VerificationInput{
		Calibration:   calibration,
		HeldOut:       heldOut,
		Compatibility: CompatibilityEvidence{Revisions: study.Contract.Revisions, Ready: true},
		Consent: ConsentEvidence{StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
			CalibrationGranted: true, HeldOutGranted: true, ProofSHA256: strings.Repeat("c", 64)},
	})
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !report.Ready || report.HeldOutInputsAccessed || report.PlannedRuns != 1551 {
		t.Fatalf("verification report = %+v", report)
	}

	calibrationPlan, err := study.Plan(calibration)
	if err != nil {
		t.Fatal(err)
	}
	heldOutPlan, err := study.Plan(heldOut)
	if err != nil {
		t.Fatal(err)
	}
	if len(calibrationPlan) != 180 || len(heldOutPlan) != 1371 {
		t.Fatalf("plan sizes = %d calibration, %d held-out", len(calibrationPlan), len(heldOutPlan))
	}
	if !reflect.DeepEqual(calibrationPlan, mustPlan(t, study, calibration)) {
		t.Fatal("Plan() changed for the same frozen manifest and seed")
	}

	pairs := make(map[string]map[string]bool)
	for _, run := range append(calibrationPlan, heldOutPlan...) {
		if strings.ContainsAny(run.RunID, `/\\`) || strings.ContainsAny(run.SamplingUnitID, `/\\`) {
			t.Fatalf("protocol identity contains a path separator: %+v", run)
		}
		if pairs[run.SamplingUnitID] == nil {
			pairs[run.SamplingUnitID] = make(map[string]bool)
		}
		pairs[run.SamplingUnitID][run.Treatment] = true
	}
	if len(pairs) != 517 {
		t.Fatalf("paired sampling units = %d, want 517", len(pairs))
	}
	for id, treatments := range pairs {
		if len(treatments) != 3 || !treatments["broad-chronological"] || !treatments["targeted-recall"] || !treatments["no-recall"] {
			t.Fatalf("pair %q treatments = %#v", id, treatments)
		}
	}
}

func TestVerifyRejectsOverlapMissingConsentCompatibilityAndSampleDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Study, *Manifest, *Manifest, *VerificationInput)
		want   string
	}{
		{"cohort overlap", func(_ *Study, _ *Manifest, h *Manifest, _ *VerificationInput) { h.FirstSamplingUnit = 60 }, "overlap"},
		{"insufficient sample", func(_ *Study, c *Manifest, _ *Manifest, _ *VerificationInput) { c.SamplingUnits-- }, "sample"},
		{"missing consent", func(_ *Study, _ *Manifest, _ *Manifest, input *VerificationInput) {
			input.Consent.HeldOutGranted = false
		}, "consent"},
		{"unsupported tuple", func(_ *Study, _ *Manifest, _ *Manifest, input *VerificationInput) {
			input.Compatibility.Revisions.CodexPlugin.Version = "0.1.6"
		}, "compatibility"},
		{"not ready", func(_ *Study, _ *Manifest, _ *Manifest, input *VerificationInput) { input.Compatibility.Ready = false }, "compatibility"},
		{"manifest identity", func(_ *Study, _ *Manifest, h *Manifest, _ *VerificationInput) { h.StudyVersion = "v2" }, "manifest"},
		{"manifest selection", func(_ *Study, _ *Manifest, h *Manifest, _ *VerificationInput) { h.Namespace = "changed" }, "manifest"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			study, calibration, heldOut := verifiedStudy(t)
			input := VerificationInput{
				Calibration: calibration, HeldOut: heldOut,
				Compatibility: CompatibilityEvidence{Revisions: study.Contract.Revisions, Ready: true},
				Consent: ConsentEvidence{StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
					CalibrationGranted: true, HeldOutGranted: true, ProofSHA256: strings.Repeat("c", 64)},
			}
			test.mutate(study, calibration, heldOut, &input)
			if _, err := study.Verify(input); err == nil || !strings.Contains(strings.ToLower(err.Error()), test.want) {
				t.Fatalf("Verify() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLoadManifestRejectsHashDrift(t *testing.T) {
	t.Parallel()

	manifest := validManifest(validContract(), "calibration-v1", "cal", 1, 60)
	path, hashPath := writeFrozenJSON(t, "manifest.json", manifest)
	loaded, err := LoadManifest(path, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Hash == "" || loaded.Manifest.CohortID != "calibration-v1" {
		t.Fatalf("loaded manifest = %+v", loaded)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	drifted := strings.Replace(string(raw), manifest.SelectionSeed, "changed-after-freeze", 1)
	if err := os.WriteFile(path, []byte(drifted), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(path, hashPath); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("LoadManifest() error = %v, want hash mismatch", err)
	}
}

func verifiedStudy(t *testing.T) (*Study, *Manifest, *Manifest) {
	t.Helper()
	contract := validContract()
	calibrationValue := validManifest(contract, contract.Cohorts.Calibration.ID, "cal", contract.Cohorts.Calibration.FirstSamplingUnit, contract.Cohorts.Calibration.SamplingUnits)
	heldOutValue := validManifest(contract, contract.Cohorts.HeldOut.ID, "hold", contract.Cohorts.HeldOut.FirstSamplingUnit, contract.Cohorts.HeldOut.SamplingUnits)
	calibration := loadManifestValue(t, calibrationValue)
	heldOut := loadManifestValue(t, heldOutValue)
	contract.Cohorts.Calibration.ManifestSHA256 = calibration.Hash
	contract.Cohorts.HeldOut.ManifestSHA256 = heldOut.Hash
	contractPath, hashPath := writeFrozenJSON(t, "contract.json", contract)
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	return study, &calibration.Manifest, &heldOut.Manifest
}

func loadManifestValue(t *testing.T, manifest Manifest) *FrozenManifest {
	t.Helper()
	path, hashPath := writeFrozenJSON(t, "manifest.json", manifest)
	loaded, err := LoadManifest(path, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func validManifest(contract Contract, cohortID, namespace string, first, count int) Manifest {
	classes := make([]string, 0, len(contract.TaskClasses))
	for _, class := range contract.TaskClasses {
		classes = append(classes, class.ID)
	}
	return Manifest{
		SchemaVersion: "recall-study-manifest-v1", StudyID: contract.StudyID, StudyVersion: contract.StudyVersion,
		CohortID: cohortID, Status: "frozen", Namespace: namespace, FirstSamplingUnit: first,
		SamplingUnits: count, TaskClassCycle: classes, SelectionSeed: fmt.Sprintf("%s-%s", contract.Randomization.Seed, cohortID),
		ConsentRequirement: "explicit-before-evidence", InputCommitment: "sha256-per-consented-task",
	}
}

func mustPlan(t *testing.T, study *Study, manifest *Manifest) []PlannedRun {
	t.Helper()
	plan, err := study.Plan(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
