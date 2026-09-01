package recallstudy

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

func TestVerifyAndPlanFreezeNonOverlappingPairedCohorts(t *testing.T) {
	t.Parallel()

	study, calibration, heldOut := verifiedStudy(t)
	report, err := study.Verify(VerificationInput{
		Calibration:   calibration,
		HeldOut:       heldOut,
		Compatibility: compatibleEvidence(study),
		Consent:       consentEvidence(study, calibration, heldOut),
	})
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !report.Ready || report.HeldOutInputsAccessed || report.PlannedRuns != 1551 {
		t.Fatalf("verification report = %+v", report)
	}
	commitments := make(map[string]bool, 517)
	for _, manifest := range []*Manifest{calibration, heldOut} {
		for _, task := range manifest.Tasks {
			if commitments[task.InputSHA256] {
				t.Fatalf("cross-cohort task commitment reused: %s", task.InputSHA256)
			}
			commitments[task.InputSHA256] = true
		}
	}
	if len(commitments) != 517 {
		t.Fatalf("frozen task commitments = %d, want 517", len(commitments))
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
	changedSeed := *study
	changedSeed.Contract.Randomization.Seed = "different-frozen-seed"
	changedPlan := mustPlan(t, &changedSeed, calibration)
	if reflect.DeepEqual(calibrationPlan, changedPlan) {
		t.Fatal("Plan() ignored the frozen randomization seed")
	}
	for index := 0; index < len(calibrationPlan); index += len(study.Contract.Treatments) {
		block := calibrationPlan[index : index+len(study.Contract.Treatments)]
		for _, run := range block[1:] {
			if run.SamplingUnitID != block[0].SamplingUnitID || run.TaskClass != block[0].TaskClass {
				t.Fatalf("paired block split by randomization: %+v", block)
			}
		}
	}
	for index, class := range calibration.TaskClassCycle {
		if calibrationPlan[index*len(study.Contract.Treatments)].TaskClass != class {
			t.Fatalf("plan did not preserve task-class stratification at block %d", index)
		}
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
		{"unbound consent", func(_ *Study, _ *Manifest, _ *Manifest, input *VerificationInput) {
			input.Consent.ProofSHA256 = strings.Repeat("c", 64)
		}, "consent"},
		{"unsupported tuple", func(_ *Study, _ *Manifest, _ *Manifest, input *VerificationInput) {
			input.Compatibility.Revisions.CodexPlugin.Version = "0.1.6"
		}, "compatibility"},
		{"not ready", func(_ *Study, _ *Manifest, _ *Manifest, input *VerificationInput) {
			input.Compatibility.Compatibility.Status = protocolcontract.CompatibilityIncompatible
		}, "compatibility"},
		{"unattributable tuple", func(_ *Study, _ *Manifest, _ *Manifest, input *VerificationInput) {
			input.Compatibility.Compatibility.Axes[0].Provenance = "claimed-ready-without-frozen-source"
		}, "compatibility"},
		{"manifest identity", func(_ *Study, _ *Manifest, h *Manifest, _ *VerificationInput) { h.StudyVersion = "v2" }, "manifest"},
		{"manifest selection", func(_ *Study, _ *Manifest, h *Manifest, _ *VerificationInput) { h.Namespace = "changed" }, "manifest"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			study, calibration, heldOut := verifiedStudy(t)
			input := VerificationInput{
				Calibration: calibration, HeldOut: heldOut,
				Compatibility: compatibleEvidence(study),
				Consent:       consentEvidence(study, calibration, heldOut),
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

func TestVerifyRejectsManifestDriftWithRecomputedSidecar(t *testing.T) {
	t.Parallel()

	study, calibration, heldOut := verifiedStudy(t)
	drifted := *heldOut
	drifted.SelectionSeed = "recomputed-held-out-seed"
	loaded := loadManifestValue(t, drifted)
	_, err := study.Verify(VerificationInput{
		Calibration: calibration, HeldOut: &loaded.Manifest, Compatibility: compatibleEvidence(study),
		Consent: consentEvidence(study, calibration, &loaded.Manifest),
	})
	if err == nil || !strings.Contains(err.Error(), "manifest identity") {
		t.Fatalf("Verify() error = %v, want frozen manifest rejection", err)
	}
}

func TestVerifyTaskInputBindsFrozenMembershipAndFixtureSelection(t *testing.T) {
	t.Parallel()

	study, calibration, _ := verifiedStudy(t)
	member := calibration.Tasks[0]
	input := frozenTaskInput(study.Contract, calibration, member.SamplingUnitID, member.TaskClass)
	if err := study.VerifyTaskInput(calibration, input); err != nil {
		t.Fatalf("VerifyTaskInput() error = %v", err)
	}
	mutations := []struct {
		name   string
		mutate func(*TaskInput)
	}{
		{"fixture path", func(input *TaskInput) { input.FixturePath += ".substituted" }},
		{"fixture content", func(input *TaskInput) { input.FixtureUTF8 += " substituted" }},
		{"instruction", func(input *TaskInput) { input.InstructionUTF8 += " substituted" }},
		{"verifier identity", func(input *TaskInput) { input.VerifierID += "-substituted" }},
		{"verifier definition", func(input *TaskInput) { input.VerifierUTF8 += " substituted" }},
		{"expected result", func(input *TaskInput) { input.ExpectedResultUTF8 += " substituted" }},
	}
	for _, test := range mutations {
		changed := input
		test.mutate(&changed)
		if err := study.VerifyTaskInput(calibration, changed); err == nil || !strings.Contains(err.Error(), "commitment changed") {
			t.Fatalf("changed %s VerifyTaskInput() error = %v", test.name, err)
		}
	}
	input.SourceRevision = strings.Repeat("0", 40)
	if err := study.VerifyTaskInput(calibration, input); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("changed source VerifyTaskInput() error = %v", err)
	}
	input = frozenTaskInput(study.Contract, calibration, "hold-0061", input.TaskClass)
	if err := study.VerifyTaskInput(calibration, input); err == nil || !strings.Contains(err.Error(), "not a frozen cohort member") {
		t.Fatalf("cross-cohort VerifyTaskInput() error = %v", err)
	}
}

func TestFrozenTaskCommitmentsMatchIndependentReferenceVectors(t *testing.T) {
	t.Parallel()

	study, calibration, _ := verifiedStudy(t)
	common := TaskInput{StudyID: "codex-useful-recall", StudyVersion: "v1", CohortID: "calibration-v1",
		SourceRevision:     "105778d820029a2326043739fd676647e5c037f6",
		TaskProtocolSHA256: "f579656556750b9f2de67d27c1736a728d17878bffa67e8b362baef55885e345"}
	vectors := []struct {
		input TaskInput
		want  TaskCommitment
	}{
		{
			input: TaskInput{SamplingUnitID: "cal-0001", TaskClass: "repository-question", FixtureSeed: "81b9f9d1fdec31ea0904cc76bee306719550eedc2116f4de2ed1b9a0f3162fb9",
				FixturePath: "docs/recall-fact.json", FixtureUTF8: "{\"fact_key\":\"c55c7115b808c728\",\"fact_value\":\"8aa93d44caa533c1\"}\n",
				InstructionUTF8: "Read docs/recall-fact.json and return the fact_value for fact_key c55c7115b808c728 as {\"answer\":\"VALUE\"}.\n",
				VerifierID:      "exact-json-answer-v1", VerifierUTF8: "parse one JSON object; require exactly the string field answer; compare it byte-for-byte with the expected answer",
				ExpectedResultUTF8: "{\"answer\":\"8aa93d44caa533c1\"}\n"},
			want: TaskCommitment{SamplingUnitID: "cal-0001", TaskClass: "repository-question",
				FixtureSHA256: "a246b40ddb0e2ef8e376387157927f2f2bb55ca9160bdef262a52df5fb896e6d", InstructionSHA256: "c17ee0491ccb9e5126283043d306e8acbbd8ae10d40be355ad039a0c8fd980a1",
				VerifierSHA256: "92de57d43db7fe742ca4657a4fde646fca9566474ec969d10ad0cb44a751a2ce", ExpectedSHA256: "6ab3254a9fb71aa9a72e829ed51fb3f12ef5778c1695e53068aaf36066b8f7b3",
				InputSHA256: "cdbf6cc750d34d821c773b96768896faa8ba1b5d399dc72020664dc22458653b"},
		},
		{
			input: TaskInput{SamplingUnitID: "cal-0002", TaskClass: "implementation", FixtureSeed: "894dcc1725380eda897ae939ca3ead8c606196e079ddb16cd20fd732b907b123",
				FixturePath: "config/recall-setting.json", FixtureUTF8: "{\"current\":\"47dbff419d0c57aa\",\"required\":\"75c5ee24e935c39c\"}\n",
				InstructionUTF8: "Update config/recall-setting.json so current equals required; preserve exactly those two keys and emit no other file changes.\n",
				VerifierID:      "exact-file-json-v1", VerifierUTF8: "parse the named fixture as JSON; require exactly current and required; require both values equal the frozen expected value; reject any other changed path",
				ExpectedResultUTF8: "{\"current\":\"75c5ee24e935c39c\",\"required\":\"75c5ee24e935c39c\"}\n"},
			want: TaskCommitment{SamplingUnitID: "cal-0002", TaskClass: "implementation",
				FixtureSHA256: "e8e0e278499ac067aec9e31fab055d9f12e1668cc7a7cb04b8320efddd931f24", InstructionSHA256: "6f91a6567cfe357ab72f77f87ba934559080614a5e622c98b849ba9b8f6cbf98",
				VerifierSHA256: "30a28afabbb948d1cef8207ae0d3a90644c3ba8ab7b77070da48cfee336d9941", ExpectedSHA256: "5efdb66ff23fb83bb4f375012f0e56b1f0d09cad09e9b3caf745c7e93f2da0e2",
				InputSHA256: "8b2192ccc41aed34b55100a11d660623bad32b8136df03218582652f939301bb"},
		},
		{
			input: TaskInput{SamplingUnitID: "cal-0003", TaskClass: "diagnosis", FixtureSeed: "5ae46ce2b24b711ec135eed08f0b98c63a957ffe8e344e76e5aafd833481a147",
				FixturePath: "diagnostics/recall-case.json", FixtureUTF8: "{\"expected\":\"e2449491ae70b15d\",\"observed\":\"165aae0aca25b286\",\"root_cause_code\":\"2d0e44fdeac23078\"}\n",
				InstructionUTF8: "Diagnose diagnostics/recall-case.json and return its root cause as {\"root_cause_code\":\"CODE\"}; do not modify files.\n",
				VerifierID:      "exact-json-root-cause-v1", VerifierUTF8: "require no file changes; parse one JSON object; require exactly root_cause_code; compare it byte-for-byte with the frozen code",
				ExpectedResultUTF8: "{\"root_cause_code\":\"2d0e44fdeac23078\"}\n"},
			want: TaskCommitment{SamplingUnitID: "cal-0003", TaskClass: "diagnosis",
				FixtureSHA256: "49db6bda988f68fdcf2ea2e21ade8fd40644e44e462b5c9a9fdc13067793d3aa", InstructionSHA256: "96240e111066ee08bb3dcf465d2962af9d4976c1da3d32fb2fdba367562e270f",
				VerifierSHA256: "c6d8c6f6cc73500419b80b74c17fecb8b3efe2438c7bb66a3240fe184e7f0f68", ExpectedSHA256: "21e4db3862f57e8b581f709c1703eb185fa6709b2a6039ce7e3c2a3fe59aae43",
				InputSHA256: "ddaa0e4aa218e59ab8e39e6028b7ee71c293fe09213063b242bc2cba96c73efe"},
		},
		{
			input: TaskInput{SamplingUnitID: "cal-0004", TaskClass: "verification", FixtureSeed: "aced5a89ea3d5acf3c3637e053d4d4f2bda33f450bdf3f47a7fbab01ba2f1d3c",
				FixturePath: "verification/recall-check.json", FixtureUTF8: "{\"expected_sha256\":\"e8f70d7fadbc8725\",\"actual_sha256\":\"e8f70d7fadbc8725\"}\n",
				InstructionUTF8: "Verify verification/recall-check.json and return {\"matches\":true} only when the two digests match; do not modify files.\n",
				VerifierID:      "exact-json-verdict-v1", VerifierUTF8: "require no file changes; parse one JSON object; require exactly the boolean field matches; compare with the equality of the frozen fixture digests",
				ExpectedResultUTF8: "{\"matches\":true}\n"},
			want: TaskCommitment{SamplingUnitID: "cal-0004", TaskClass: "verification",
				FixtureSHA256: "642af5b33405191a3002bd2801db58ed8b8dc61681dca627d7ff4fb310fe9180", InstructionSHA256: "e9e3ed7438c5675967d2fcad82531b7a02ad908345b764e4c2dfc6e66215332c",
				VerifierSHA256: "737439635137c7e689fe65bba6788592ecf311f05f22d6769a18228317882d34", ExpectedSHA256: "833c5870785f518aa17b90be7e7b994aca1ff517574c2413c771708b4815e018",
				InputSHA256: "61cbbf47208710a8a4bf639233da04b8dc9c62bdcf4d442eb5d71187e7fe8534"},
		},
		{
			input: TaskInput{SamplingUnitID: "cal-0005", TaskClass: "routine-non-durable", FixtureSeed: "8621eeb26ff619bd4f610d1b64aa817b41518cbc04971886ac4f7057bd620749",
				FixturePath: "maintenance/recall-items.json", FixtureUTF8: "{\"items\":[\"4d1baf45cd503bb0\",\"32a57ae84a329874\",\"e7a41f134ac34339\"]}\n",
				InstructionUTF8: "Sort maintenance/recall-items.json items in ascending byte order; preserve exactly one items key and emit no other file changes.\n",
				VerifierID:      "exact-sorted-json-v1", VerifierUTF8: "parse the named fixture as JSON; require exactly items; require the frozen three strings in ascending byte order; reject any other changed path",
				ExpectedResultUTF8: "{\"items\":[\"32a57ae84a329874\",\"4d1baf45cd503bb0\",\"e7a41f134ac34339\"]}\n"},
			want: TaskCommitment{SamplingUnitID: "cal-0005", TaskClass: "routine-non-durable",
				FixtureSHA256: "ee2bf12a35fb1675a4b477b2ff3da7d482561f15c053d038afd3836377ee1bad", InstructionSHA256: "aad9ea96aa5e9dbd1c83886d0c92296943d8caf41ed4099dcbb2c5aff2d477f6",
				VerifierSHA256: "92522eb1257c5784d8c2a57a8e4784c97da4bc3079138f9cc56f67641b5e7bbc", ExpectedSHA256: "40d49f219b8e79b9919d57033dda8f514831cd72930795fcf340d9ca601213ad",
				InputSHA256: "a6963d2db461ce695ebf002a8e05766f7700a0baa2b31650bee239796e650b88"},
		},
	}
	for index, vector := range vectors {
		vector.input.StudyID = common.StudyID
		vector.input.StudyVersion = common.StudyVersion
		vector.input.CohortID = common.CohortID
		vector.input.SourceRevision = common.SourceRevision
		vector.input.TaskProtocolSHA256 = common.TaskProtocolSHA256
		if err := study.VerifyTaskInput(calibration, vector.input); err != nil {
			t.Fatalf("vector %d VerifyTaskInput() error = %v", index, err)
		}
		if calibration.Tasks[index] != vector.want {
			t.Fatalf("vector %d commitment = %+v, want %+v", index, calibration.Tasks[index], vector.want)
		}
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
	study, err := loadContract(contractPath, hashPath, false)
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
	manifest := Manifest{
		SchemaVersion: "recall-study-manifest-v1", StudyID: contract.StudyID, StudyVersion: contract.StudyVersion,
		CohortID: cohortID, Status: "frozen", Namespace: namespace, FirstSamplingUnit: first,
		SamplingUnits: count, TaskClassCycle: classes, SelectionSeed: fmt.Sprintf("%s-%s", contract.Randomization.Seed, cohortID),
		ConsentRequirement: "explicit-before-evidence", InputCommitment: "sha256-per-consented-task",
	}
	for offset := 0; offset < count; offset++ {
		unitID := fmt.Sprintf("%s-%04d", namespace, first+offset)
		class := classes[offset%len(classes)]
		input := frozenTaskInput(contract, &manifest, unitID, class)
		manifest.Tasks = append(manifest.Tasks, taskCommitmentFromInput(contract, &manifest, input))
	}
	return manifest
}

func mustPlan(t *testing.T, study *Study, manifest *Manifest) []PlannedRun {
	t.Helper()
	plan, err := study.Plan(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func compatibleEvidence(study *Study) CompatibilityEvidence {
	rangeV1 := &protocolcontract.VersionRange{Minimum: 1, Maximum: 1}
	provenance := "repository:https://github.com/yersonargotev/engram.git#revision:" + study.Contract.SourceRevision
	return CompatibilityEvidence{
		Revisions: study.Contract.Revisions,
		Compatibility: protocolcontract.Evaluate(
			protocolcontract.Declaration{Version: study.Contract.Revisions.ManagedPack.Version, Provenance: provenance, Supported: rangeV1},
			protocolcontract.Declaration{Version: study.Contract.Revisions.EngramBinary.Version, Provenance: provenance, Supported: rangeV1},
			protocolcontract.Declaration{Version: study.Contract.Revisions.CodexPlugin.Version, Provenance: provenance, Supported: rangeV1},
		),
	}
}

func consentEvidence(study *Study, calibration, heldOut *Manifest) ConsentEvidence {
	return ConsentEvidence{StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		CalibrationGranted: true, HeldOutGranted: true, ProofSHA256: study.ConsentCommitment(calibration, heldOut)}
}
