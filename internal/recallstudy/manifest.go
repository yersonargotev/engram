package recallstudy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

const (
	ManifestSchemaVersion     = "recall-study-manifest-v1"
	VerificationSchemaVersion = "recall-study-verification-v1"
	maxManifestBytes          = 1 << 20
)

type Manifest struct {
	SchemaVersion      string   `json:"schema_version"`
	StudyID            string   `json:"study_id"`
	StudyVersion       string   `json:"study_version"`
	CohortID           string   `json:"cohort_id"`
	Status             string   `json:"status"`
	Namespace          string   `json:"namespace"`
	FirstSamplingUnit  int      `json:"first_sampling_unit"`
	SamplingUnits      int      `json:"sampling_units"`
	TaskClassCycle     []string `json:"task_class_cycle"`
	SelectionSeed      string   `json:"selection_seed"`
	ConsentRequirement string   `json:"consent_requirement"`
	InputCommitment    string   `json:"input_commitment"`
	Hash               string   `json:"-"`
}

type FrozenManifest struct {
	Manifest Manifest
	Hash     string
}

type CompatibilityEvidence struct {
	Revisions RevisionsContract `json:"revisions"`
	Ready     bool              `json:"ready"`
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

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode Recall study manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("decode Recall study manifest: multiple JSON values are not allowed")
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
	if !input.Compatibility.Ready || !reflect.DeepEqual(input.Compatibility.Revisions, contract.Revisions) {
		return VerificationReport{}, fmt.Errorf("Recall study Compatibility tuple is unsupported or changed")
	}
	consent := input.Consent
	if consent.StudyID != contract.StudyID || consent.StudyVersion != contract.StudyVersion ||
		!consent.CalibrationGranted || !consent.HeldOutGranted || !validHexDigest(consent.ProofSHA256, sha256.Size) {
		return VerificationReport{}, fmt.Errorf("Recall study consent is missing or does not match the frozen study")
	}
	return VerificationReport{
		SchemaVersion: VerificationSchemaVersion, StudyID: contract.StudyID, StudyVersion: contract.StudyVersion,
		ContractSHA256: study.Hash, CalibrationSHA256: input.Calibration.Hash, HeldOutSHA256: input.HeldOut.Hash,
		SamplingUnits: contract.Cohorts.RequiredPerTreatment, PlannedRuns: contract.Cohorts.RequiredTotal,
		Ready: true, HeldOutInputsAccessed: false,
	}, nil
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
	return nil
}
