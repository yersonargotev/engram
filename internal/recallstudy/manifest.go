package recallstudy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

const (
	ManifestSchemaVersion     = "recall-study-manifest-v1"
	VerificationSchemaVersion = "recall-study-verification-v1"
	maxManifestBytes          = 1 << 20
)

type Manifest struct {
	SchemaVersion      string           `json:"schema_version"`
	StudyID            string           `json:"study_id"`
	StudyVersion       string           `json:"study_version"`
	CohortID           string           `json:"cohort_id"`
	Status             string           `json:"status"`
	Namespace          string           `json:"namespace"`
	FirstSamplingUnit  int              `json:"first_sampling_unit"`
	SamplingUnits      int              `json:"sampling_units"`
	TaskClassCycle     []string         `json:"task_class_cycle"`
	SelectionSeed      string           `json:"selection_seed"`
	ConsentRequirement string           `json:"consent_requirement"`
	InputCommitment    string           `json:"input_commitment"`
	Tasks              []TaskCommitment `json:"tasks"`
	Hash               string           `json:"-"`
}

type TaskCommitment struct {
	SamplingUnitID    string `json:"sampling_unit_id"`
	TaskClass         string `json:"task_class"`
	FixtureSHA256     string `json:"fixture_sha256"`
	InstructionSHA256 string `json:"instruction_sha256"`
	VerifierSHA256    string `json:"verifier_sha256"`
	ExpectedSHA256    string `json:"expected_result_sha256"`
	InputSHA256       string `json:"input_sha256"`
}

type TaskInput struct {
	StudyID            string `json:"study_id"`
	StudyVersion       string `json:"study_version"`
	CohortID           string `json:"cohort_id"`
	SamplingUnitID     string `json:"sampling_unit_id"`
	TaskClass          string `json:"task_class"`
	SourceRevision     string `json:"source_revision"`
	TaskProtocolSHA256 string `json:"task_protocol_sha256"`
	FixtureSeed        string `json:"fixture_seed"`
	FixturePath        string `json:"fixture_path"`
	FixtureUTF8        string `json:"fixture_utf8"`
	InstructionUTF8    string `json:"instruction_utf8"`
	VerifierID         string `json:"verifier_id"`
	VerifierUTF8       string `json:"verifier_utf8"`
	ExpectedResultUTF8 string `json:"expected_result_utf8"`
}

type FrozenManifest struct {
	Manifest Manifest
	Hash     string
}

type CompatibilityEvidence struct {
	Revisions     RevisionsContract                    `json:"revisions"`
	Compatibility protocolcontract.CompatibilityReport `json:"compatibility"`
}

type ConsentEvidence struct {
	StudyID            string `json:"study_id"`
	StudyVersion       string `json:"study_version"`
	CalibrationGranted bool   `json:"calibration_granted"`
	HeldOutGranted     bool   `json:"held_out_granted"`
	ProofSHA256        string `json:"proof_sha256"`
}

type VerificationInput struct {
	Calibration   *Manifest
	HeldOut       *Manifest
	Compatibility CompatibilityEvidence
	Consent       ConsentEvidence
}

type VerificationReport struct {
	SchemaVersion         string `json:"schema_version"`
	StudyID               string `json:"study_id"`
	StudyVersion          string `json:"study_version"`
	ContractSHA256        string `json:"contract_sha256"`
	CalibrationSHA256     string `json:"calibration_manifest_sha256"`
	HeldOutSHA256         string `json:"held_out_manifest_sha256"`
	SamplingUnits         int    `json:"sampling_units"`
	PlannedRuns           int    `json:"planned_runs"`
	Ready                 bool   `json:"ready"`
	HeldOutInputsAccessed bool   `json:"held_out_inputs_accessed"`
}

func ReadCompatibilityEvidence(path string) (CompatibilityEvidence, error) {
	var evidence CompatibilityEvidence
	if err := readStrictJSON(path, maxManifestBytes, &evidence); err != nil {
		return CompatibilityEvidence{}, fmt.Errorf("read Recall study Compatibility evidence: %w", err)
	}
	return evidence, nil
}

func ReadConsentEvidence(path string) (ConsentEvidence, error) {
	var evidence ConsentEvidence
	if err := readStrictJSON(path, maxManifestBytes, &evidence); err != nil {
		return ConsentEvidence{}, fmt.Errorf("read Recall study consent evidence: %w", err)
	}
	return evidence, nil
}

func LoadManifest(manifestPath, hashPath string) (*FrozenManifest, error) {
	raw, err := readBoundedFile(manifestPath, maxManifestBytes)
	if err != nil {
		return nil, fmt.Errorf("read Recall study manifest: %w", err)
	}
	hashRaw, err := readBoundedFile(hashPath, 4096)
	if err != nil {
		return nil, fmt.Errorf("read Recall study manifest hash: %w", err)
	}
	want := strings.Fields(string(hashRaw))
	if len(want) == 0 || !validHexDigest(want[0], sha256.Size) {
		return nil, fmt.Errorf("Recall study manifest hash sidecar is invalid")
	}
	digest := sha256.Sum256(raw)
	actual := hex.EncodeToString(digest[:])
	if actual != strings.ToLower(want[0]) {
		return nil, fmt.Errorf("Recall study manifest hash mismatch: got %s, want %s", actual, want[0])
	}

	var manifest Manifest
	if err := decodeStrictJSON(raw, &manifest); err != nil {
		return nil, fmt.Errorf("decode Recall study manifest: %w", err)
	}
	if err := manifest.validate(); err != nil {
		return nil, err
	}
	manifest.Hash = actual
	return &FrozenManifest{Manifest: manifest, Hash: actual}, nil
}

func (manifest Manifest) validate() error {
	if manifest.SchemaVersion != ManifestSchemaVersion || manifest.StudyID == "" || manifest.StudyVersion == "" ||
		manifest.Status != "frozen" || manifest.CohortID == "" || manifest.Namespace == "" {
		return fmt.Errorf("Recall study manifest identity must be frozen")
	}
	if strings.ContainsAny(manifest.Namespace, `/\\`) || manifest.FirstSamplingUnit < 1 || manifest.SamplingUnits < 1 {
		return fmt.Errorf("Recall study manifest sampling range is invalid")
	}
	if len(manifest.TaskClassCycle) == 0 || strings.TrimSpace(manifest.SelectionSeed) == "" {
		return fmt.Errorf("Recall study manifest task-class selection is incomplete")
	}
	if manifest.ConsentRequirement != "explicit-before-evidence" || manifest.InputCommitment != "sha256-per-consented-task" {
		return fmt.Errorf("Recall study manifest consent and input commitment are incomplete")
	}
	if len(manifest.Tasks) != manifest.SamplingUnits {
		return fmt.Errorf("Recall study manifest task membership is incomplete")
	}
	seenUnits := make(map[string]bool, len(manifest.Tasks))
	seenInputs := make(map[string]bool, len(manifest.Tasks))
	for offset, task := range manifest.Tasks {
		wantUnit := fmt.Sprintf("%s-%04d", manifest.Namespace, manifest.FirstSamplingUnit+offset)
		wantClass := manifest.TaskClassCycle[offset%len(manifest.TaskClassCycle)]
		if task.SamplingUnitID != wantUnit || task.TaskClass != wantClass ||
			!validHexDigest(task.FixtureSHA256, sha256.Size) || !validHexDigest(task.InstructionSHA256, sha256.Size) ||
			!validHexDigest(task.VerifierSHA256, sha256.Size) || !validHexDigest(task.ExpectedSHA256, sha256.Size) ||
			!validHexDigest(task.InputSHA256, sha256.Size) ||
			seenUnits[task.SamplingUnitID] || seenInputs[task.InputSHA256] {
			return fmt.Errorf("Recall study manifest task membership is invalid or duplicated")
		}
		seenUnits[task.SamplingUnitID] = true
		seenInputs[task.InputSHA256] = true
	}
	return nil
}

func (study *Study) Verify(input VerificationInput) (VerificationReport, error) {
	if study == nil || input.Calibration == nil || input.HeldOut == nil {
		return VerificationReport{}, fmt.Errorf("Recall study verification requires both frozen manifests")
	}
	contract := study.Contract
	calibrationEnd := input.Calibration.FirstSamplingUnit + input.Calibration.SamplingUnits
	heldOutEnd := input.HeldOut.FirstSamplingUnit + input.HeldOut.SamplingUnits
	if input.Calibration.FirstSamplingUnit < heldOutEnd && input.HeldOut.FirstSamplingUnit < calibrationEnd {
		return VerificationReport{}, fmt.Errorf("Recall study calibration and held-out manifests overlap")
	}
	if input.Calibration.SamplingUnits+input.HeldOut.SamplingUnits != contract.Cohorts.RequiredPerTreatment {
		return VerificationReport{}, fmt.Errorf("Recall study sample is insufficient or drifted")
	}
	if err := study.verifyManifest(input.Calibration, contract.Cohorts.Calibration); err != nil {
		return VerificationReport{}, err
	}
	if err := study.verifyManifest(input.HeldOut, contract.Cohorts.HeldOut); err != nil {
		return VerificationReport{}, err
	}
	seenTasks := make(map[string]bool, len(input.Calibration.Tasks))
	for _, task := range input.Calibration.Tasks {
		seenTasks[task.InputSHA256] = true
	}
	for _, task := range input.HeldOut.Tasks {
		if seenTasks[task.InputSHA256] {
			return VerificationReport{}, fmt.Errorf("Recall study calibration and held-out task commitments overlap")
		}
	}
	if !validCompatibilityEvidence(input.Compatibility, contract) {
		return VerificationReport{}, fmt.Errorf("Recall study Compatibility tuple is unsupported or changed")
	}
	consent := input.Consent
	if consent.StudyID != contract.StudyID || consent.StudyVersion != contract.StudyVersion ||
		!consent.CalibrationGranted || !consent.HeldOutGranted || consent.ProofSHA256 != study.ConsentCommitment(input.Calibration, input.HeldOut) {
		return VerificationReport{}, fmt.Errorf("Recall study consent is missing or does not match the frozen study")
	}
	return VerificationReport{
		SchemaVersion: VerificationSchemaVersion, StudyID: contract.StudyID, StudyVersion: contract.StudyVersion,
		ContractSHA256: study.Hash, CalibrationSHA256: input.Calibration.Hash, HeldOutSHA256: input.HeldOut.Hash,
		SamplingUnits: contract.Cohorts.RequiredPerTreatment, PlannedRuns: contract.Cohorts.RequiredTotal,
		Ready: true, HeldOutInputsAccessed: false,
	}, nil
}

// ConsentCommitment binds one explicit grant to the frozen contract and both
// immutable task memberships without reading any task input.
func (study *Study) ConsentCommitment(calibration, heldOut *Manifest) string {
	if study == nil || calibration == nil || heldOut == nil {
		return ""
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		"recall-study-consent-v1", study.Hash, calibration.Hash, heldOut.Hash,
	}, "\x00")))
	return hex.EncodeToString(digest[:])
}

func validCompatibilityEvidence(evidence CompatibilityEvidence, contract Contract) bool {
	if !reflect.DeepEqual(evidence.Revisions, contract.Revisions) {
		return false
	}
	report := evidence.Compatibility
	if len(report.Axes) != 4 {
		return false
	}
	wantVersions := []string{contract.Revisions.ManagedPack.Version, contract.Revisions.EngramBinary.Version, contract.Revisions.CodexPlugin.Version}
	wantNames := []string{protocolcontract.AxisManagedPack, protocolcontract.AxisEngramBinary, protocolcontract.AxisCodexPlugin}
	declarations := make([]protocolcontract.Declaration, 3)
	for index := range declarations {
		axis := report.Axes[index]
		if axis.Name != wantNames[index] || axis.Version != wantVersions[index] || !strings.Contains(axis.Provenance, contract.SourceRevision) {
			return false
		}
		declarations[index] = protocolcontract.Declaration{Version: axis.Version, Provenance: axis.Provenance, Supported: axis.Supported, Legacy: axis.Legacy}
	}
	want := protocolcontract.Evaluate(declarations[0], declarations[1], declarations[2])
	return reflect.DeepEqual(report, want)
}

func (study *Study) verifyManifest(manifest *Manifest, cohort CohortContract) error {
	if manifest.StudyID != study.Contract.StudyID || manifest.StudyVersion != study.Contract.StudyVersion ||
		manifest.CohortID != cohort.ID || manifest.FirstSamplingUnit != cohort.FirstSamplingUnit ||
		manifest.SamplingUnits != cohort.SamplingUnits || manifest.Hash != cohort.ManifestSHA256 ||
		manifest.Namespace != cohort.Namespace || manifest.SelectionSeed != cohort.SelectionSeed {
		return fmt.Errorf("Recall study manifest identity, hash, or sample metadata changed")
	}
	classes := make([]string, 0, len(study.Contract.TaskClasses))
	for _, class := range study.Contract.TaskClasses {
		classes = append(classes, class.ID)
	}
	if !reflect.DeepEqual(manifest.TaskClassCycle, classes) {
		return fmt.Errorf("Recall study manifest task classes changed")
	}
	for _, task := range manifest.Tasks {
		if task.InputSHA256 != taskCommitmentDigest(study.Contract, manifest, task) {
			return fmt.Errorf("Recall study manifest task input commitment changed")
		}
	}
	return nil
}

// VerifyTaskInput binds one future execution input byte-for-byte to its frozen
// fixture, instruction, verifier, and expected result. Only issue #110's
// consented executor supplies these private bytes; metadata-only commands never
// call this seam or materialize held-out inputs.
func (study *Study) VerifyTaskInput(manifest *Manifest, input TaskInput) error {
	if study == nil || manifest == nil {
		return fmt.Errorf("Recall study task input requires a frozen study and manifest")
	}
	cohort, ok := study.Contract.cohort(manifest.CohortID)
	if !ok {
		return fmt.Errorf("Recall study task input names an unknown cohort")
	}
	if err := study.verifyManifest(manifest, cohort); err != nil {
		return err
	}
	if input.StudyID != study.Contract.StudyID || input.StudyVersion != study.Contract.StudyVersion ||
		input.CohortID != manifest.CohortID || input.SourceRevision != study.Contract.SourceRevision ||
		input.TaskProtocolSHA256 != study.Contract.TaskProtocol.ArtifactSHA256 ||
		input.FixtureSeed != taskFixtureSeed(manifest, input.SamplingUnitID, input.TaskClass) {
		return fmt.Errorf("Recall study task input identity changed")
	}
	for _, task := range manifest.Tasks {
		if task.SamplingUnitID == input.SamplingUnitID {
			provided := taskCommitmentFromInput(study.Contract, manifest, input)
			if task.TaskClass != input.TaskClass || provided != task {
				return fmt.Errorf("Recall study task input commitment changed")
			}
			return nil
		}
	}
	return fmt.Errorf("Recall study task input is not a frozen cohort member")
}

func taskCommitmentFromInput(contract Contract, manifest *Manifest, input TaskInput) TaskCommitment {
	commitment := TaskCommitment{
		SamplingUnitID:    input.SamplingUnitID,
		TaskClass:         input.TaskClass,
		FixtureSHA256:     hashText(input.FixturePath + "\x00" + input.FixtureUTF8),
		InstructionSHA256: hashText(input.InstructionUTF8),
		VerifierSHA256:    hashText(input.VerifierID + "\x00" + input.VerifierUTF8),
		ExpectedSHA256:    hashText(input.ExpectedResultUTF8),
	}
	commitment.InputSHA256 = taskCommitmentDigest(contract, manifest, commitment)
	return commitment
}

func taskCommitmentDigest(contract Contract, manifest *Manifest, task TaskCommitment) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		"recall-task-input-v1", contract.StudyID, contract.StudyVersion, manifest.CohortID,
		task.SamplingUnitID, task.TaskClass, contract.SourceRevision, contract.TaskProtocol.ArtifactSHA256,
		taskFixtureSeed(manifest, task.SamplingUnitID, task.TaskClass), task.FixtureSHA256, task.InstructionSHA256,
		task.VerifierSHA256, task.ExpectedSHA256,
	}, "\x00")))
	return hex.EncodeToString(digest[:])
}

func hashText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// frozenTaskInput materializes one exact disposable task. It is deliberately
// unexported: #109 can freeze and test the input, while only #110 may expose a
// consented execution path that obtains it.
func frozenTaskInput(contract Contract, manifest *Manifest, unitID, taskClass string) TaskInput {
	seed := taskFixtureSeed(manifest, unitID, taskClass)
	short := func(domain string) string { return hashText(domain + "\x00" + seed)[:16] }
	a, b, c := short("a"), short("b"), short("c")
	input := TaskInput{StudyID: contract.StudyID, StudyVersion: contract.StudyVersion, CohortID: manifest.CohortID,
		SamplingUnitID: unitID, TaskClass: taskClass, SourceRevision: contract.SourceRevision,
		TaskProtocolSHA256: contract.TaskProtocol.ArtifactSHA256, FixtureSeed: seed}
	switch taskClass {
	case "repository-question":
		input.FixturePath = "docs/recall-fact.json"
		input.FixtureUTF8 = fmt.Sprintf("{\"fact_key\":%q,\"fact_value\":%q}\n", a, b)
		input.InstructionUTF8 = fmt.Sprintf("Read docs/recall-fact.json and return the fact_value for fact_key %s as {\"answer\":\"VALUE\"}.\n", a)
		input.VerifierID = "exact-json-answer-v1"
		input.VerifierUTF8 = "parse one JSON object; require exactly the string field answer; compare it byte-for-byte with the expected answer"
		input.ExpectedResultUTF8 = fmt.Sprintf("{\"answer\":%q}\n", b)
	case "implementation":
		input.FixturePath = "config/recall-setting.json"
		input.FixtureUTF8 = fmt.Sprintf("{\"current\":%q,\"required\":%q}\n", a, b)
		input.InstructionUTF8 = "Update config/recall-setting.json so current equals required; preserve exactly those two keys and emit no other file changes.\n"
		input.VerifierID = "exact-file-json-v1"
		input.VerifierUTF8 = "parse the named fixture as JSON; require exactly current and required; require both values equal the frozen expected value; reject any other changed path"
		input.ExpectedResultUTF8 = fmt.Sprintf("{\"current\":%q,\"required\":%q}\n", b, b)
	case "diagnosis":
		input.FixturePath = "diagnostics/recall-case.json"
		input.FixtureUTF8 = fmt.Sprintf("{\"expected\":%q,\"observed\":%q,\"root_cause_code\":%q}\n", a, b, c)
		input.InstructionUTF8 = "Diagnose diagnostics/recall-case.json and return its root cause as {\"root_cause_code\":\"CODE\"}; do not modify files.\n"
		input.VerifierID = "exact-json-root-cause-v1"
		input.VerifierUTF8 = "require no file changes; parse one JSON object; require exactly root_cause_code; compare it byte-for-byte with the frozen code"
		input.ExpectedResultUTF8 = fmt.Sprintf("{\"root_cause_code\":%q}\n", c)
	case "verification":
		input.FixturePath = "verification/recall-check.json"
		input.FixtureUTF8 = fmt.Sprintf("{\"expected_sha256\":%q,\"actual_sha256\":%q}\n", a, a)
		input.InstructionUTF8 = "Verify verification/recall-check.json and return {\"matches\":true} only when the two digests match; do not modify files.\n"
		input.VerifierID = "exact-json-verdict-v1"
		input.VerifierUTF8 = "require no file changes; parse one JSON object; require exactly the boolean field matches; compare with the equality of the frozen fixture digests"
		input.ExpectedResultUTF8 = "{\"matches\":true}\n"
	case "routine-non-durable":
		input.FixturePath = "maintenance/recall-items.json"
		input.FixtureUTF8 = fmt.Sprintf("{\"items\":[%q,%q,%q]}\n", c, a, b)
		input.InstructionUTF8 = "Sort maintenance/recall-items.json items in ascending byte order; preserve exactly one items key and emit no other file changes.\n"
		input.VerifierID = "exact-sorted-json-v1"
		input.VerifierUTF8 = "parse the named fixture as JSON; require exactly items; require the frozen three strings in ascending byte order; reject any other changed path"
		values := []string{a, b, c}
		sort.Strings(values)
		input.ExpectedResultUTF8 = fmt.Sprintf("{\"items\":[%q,%q,%q]}\n", values[0], values[1], values[2])
	}
	return input
}

func taskFixtureSeed(manifest *Manifest, unitID, taskClass string) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{manifest.SelectionSeed, unitID, taskClass}, "\x00")))
	return hex.EncodeToString(digest[:])
}
