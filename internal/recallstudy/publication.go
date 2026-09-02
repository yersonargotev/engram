package recallstudy

import "fmt"

const (
	PublicationSchemaVersion    = "recall-study-publication-v1"
	DispositionQualify          = "qualify_general_availability"
	DispositionContinueCanary   = "continue_canary"
	DispositionRollback         = "rollback_prior_verified_tuple"
	recallStudyPublicationScope = "codex"
)

type Publication struct {
	SchemaVersion             string   `json:"schema_version"`
	StudyID                   string   `json:"study_id"`
	StudyVersion              string   `json:"study_version"`
	ContractSHA256            string   `json:"contract_sha256"`
	CalibrationManifestSHA256 string   `json:"calibration_manifest_sha256"`
	HeldOutManifestSHA256     string   `json:"held_out_manifest_sha256"`
	Scope                     string   `json:"scope"`
	Stage                     string   `json:"stage"`
	Valid                     bool     `json:"valid"`
	Disposition               string   `json:"disposition"`
	ReasonCode                string   `json:"reason_code,omitempty"`
	EvidenceGaps              []string `json:"evidence_gaps"`
	PlannedRuns               int      `json:"planned_runs"`
	ObservedRuns              int      `json:"observed_runs"`
	Report                    *Report  `json:"report,omitempty"`
	RolloutEnabled            bool     `json:"rollout_enabled"`
}

func (study *Study) Publish(calibration, heldOut RowSet) (Publication, error) {
	if study == nil {
		return Publication{}, fmt.Errorf("Recall study publication requires a contract")
	}
	if calibration.CohortID != study.Contract.Cohorts.Calibration.ID {
		return Publication{}, fmt.Errorf("Recall study publication requires the complete calibration cohort")
	}
	if heldOut.CohortID != study.Contract.Cohorts.HeldOut.ID {
		return Publication{}, fmt.Errorf("Recall study publication requires the complete held-out cohort")
	}
	if _, err := study.aggregateRows(calibration); err != nil {
		return Publication{}, fmt.Errorf("validate calibration rows for publication: %w", err)
	}
	if _, err := study.aggregateRows(heldOut); err != nil {
		return Publication{}, fmt.Errorf("validate held-out rows for publication: %w", err)
	}

	combinedRows := make([]RunRow, 0, len(calibration.Rows)+len(heldOut.Rows))
	combinedRows = append(combinedRows, calibration.Rows...)
	combinedRows = append(combinedRows, heldOut.Rows...)
	report, err := study.Report(RowSet{
		SchemaVersion:  RowSetSchemaVersion,
		StudyID:        study.Contract.StudyID,
		StudyVersion:   study.Contract.StudyVersion,
		ContractSHA256: study.Hash,
		CohortID:       CombinedCohortID,
		Rows:           combinedRows,
	})
	if err != nil {
		return Publication{}, fmt.Errorf("derive Recall study publication: %w", err)
	}

	disposition := DispositionQualify
	evidenceGaps := make([]string, 0, len(report.Gates.Gates))
	rollback := false
	for _, gate := range report.Gates.Gates {
		if gate.Passed {
			continue
		}
		evidenceGaps = append(evidenceGaps, gate.ID)
		switch gate.ID {
		case "checkpoint-non-inferiority", "stop-growth", "harm":
			rollback = true
		}
	}
	if rollback {
		disposition = DispositionRollback
	} else if !report.Gates.AllPassed {
		disposition = DispositionContinueCanary
	}

	return Publication{
		SchemaVersion:             PublicationSchemaVersion,
		StudyID:                   study.Contract.StudyID,
		StudyVersion:              study.Contract.StudyVersion,
		ContractSHA256:            study.Hash,
		CalibrationManifestSHA256: study.Contract.Cohorts.Calibration.ManifestSHA256,
		HeldOutManifestSHA256:     study.Contract.Cohorts.HeldOut.ManifestSHA256,
		Scope:                     recallStudyPublicationScope,
		Stage:                     CombinedCohortID,
		Valid:                     true,
		Disposition:               disposition,
		EvidenceGaps:              evidenceGaps,
		PlannedRuns:               len(combinedRows),
		ObservedRuns:              len(combinedRows),
		Report:                    &report,
		RolloutEnabled:            false,
	}, nil
}

func (study *Study) PublishCalibrationStatus(result ExecutionResult) (Publication, error) {
	if study == nil || result.CohortID != study.Contract.Cohorts.Calibration.ID ||
		result.Disposition != DispositionContinueCanary || result.NextStageReady || result.ReasonCode == "" ||
		result.PlannedRuns != study.Contract.Cohorts.Calibration.SamplingUnits*len(study.Contract.Treatments) ||
		result.ObservedRuns < 0 || result.ObservedRuns > result.PlannedRuns {
		return Publication{}, fmt.Errorf("Recall study invalid-calibration publication requires a failed frozen calibration result")
	}
	evidenceGaps := make([]string, 0, len(study.Contract.Gates))
	for _, gate := range study.Contract.Gates {
		evidenceGaps = append(evidenceGaps, gate.ID)
	}
	return Publication{
		SchemaVersion:             PublicationSchemaVersion,
		StudyID:                   study.Contract.StudyID,
		StudyVersion:              study.Contract.StudyVersion,
		ContractSHA256:            study.Hash,
		CalibrationManifestSHA256: study.Contract.Cohorts.Calibration.ManifestSHA256,
		HeldOutManifestSHA256:     study.Contract.Cohorts.HeldOut.ManifestSHA256,
		Scope:                     recallStudyPublicationScope,
		Stage:                     study.Contract.Cohorts.Calibration.ID,
		Valid:                     false,
		Disposition:               DispositionContinueCanary,
		ReasonCode:                result.ReasonCode,
		EvidenceGaps:              evidenceGaps,
		PlannedRuns:               result.PlannedRuns,
		ObservedRuns:              result.ObservedRuns,
		RolloutEnabled:            false,
	}, nil
}
