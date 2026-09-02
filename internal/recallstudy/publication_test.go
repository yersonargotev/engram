package recallstudy

import (
	"path/filepath"
	"testing"
)

func TestPublishSelectsExactlyOneCodexDispositionFromFrozenGates(t *testing.T) {
	study, calibration, heldOut := verifiedStudy(t)
	calibrationRows := completeRows(study, calibration, mustPlan(t, study, calibration))
	heldOutRows := completeRows(study, heldOut, mustPlan(t, study, heldOut))

	continued, err := study.Publish(calibrationRows, heldOutRows)
	if err != nil {
		t.Fatal(err)
	}
	if continued.Disposition != DispositionContinueCanary || continued.Report == nil || continued.Report.Gates.AllPassed || continued.RolloutEnabled {
		t.Fatalf("continue-canary publication = %#v", continued)
	}

	rollbackRows := heldOutRows
	rollbackRows.Rows = append([]RunRow(nil), heldOutRows.Rows...)
	for index := range rollbackRows.Rows {
		if rollbackRows.Rows[index].Treatment == "targeted-recall" {
			rollbackRows.Rows[index].CheckpointSucceeded = false
		}
	}
	rolledBack, err := study.Publish(calibrationRows, rollbackRows)
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.Disposition != DispositionRollback || rolledBack.RolloutEnabled {
		t.Fatalf("rollback publication = %#v", rolledBack)
	}

	qualifyingCalibration := qualifyingRows(calibrationRows)
	qualifyingHeldOut := qualifyingRows(heldOutRows)
	qualified, err := study.Publish(qualifyingCalibration, qualifyingHeldOut)
	if err != nil {
		t.Fatal(err)
	}
	if qualified.Disposition != DispositionQualify || qualified.Report == nil || !qualified.Report.Gates.AllPassed || qualified.RolloutEnabled {
		t.Fatalf("qualifying publication = %#v", qualified)
	}
}

func TestFrozenV1PublicationRecordsInvalidCalibrationWithoutHeldOutEvidence(t *testing.T) {
	root := filepath.Join("..", "..", "evals", "recall-study", "v1")
	study, err := Load(filepath.Join(root, "contract.json"), filepath.Join(root, "contract.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	calibration, err := LoadManifest(filepath.Join(root, "calibration", "manifest.json"), filepath.Join(root, "calibration", "manifest.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	heldOut, err := LoadManifest(filepath.Join(root, "held-out", "manifest.json"), filepath.Join(root, "held-out", "manifest.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	var publication Publication
	if err := readStrictJSON(filepath.Join(root, "publication.json"), maxRowSetBytes, &publication); err != nil {
		t.Fatal(err)
	}
	if publication.StudyID != study.Contract.StudyID || publication.StudyVersion != study.Contract.StudyVersion ||
		publication.ContractSHA256 != study.Hash || publication.CalibrationManifestSHA256 != calibration.Hash || publication.HeldOutManifestSHA256 != heldOut.Hash {
		t.Fatalf("publication identity = %#v", publication)
	}
	if publication.Valid || publication.Stage != calibration.Manifest.CohortID || publication.Disposition != DispositionContinueCanary ||
		publication.ReasonCode != "targeted_recall_not_observed" || publication.ObservedRuns != 0 || publication.PlannedRuns != 180 ||
		publication.Report != nil || publication.RolloutEnabled || len(publication.EvidenceGaps) != len(study.Contract.Gates) {
		t.Fatalf("frozen v1 publication = %#v", publication)
	}
}

func qualifyingRows(input RowSet) RowSet {
	result := input
	result.Rows = append([]RunRow(nil), input.Rows...)
	broadIndex := 0
	targetedIndex := 0
	for index := range result.Rows {
		row := &result.Rows[index]
		if row.Treatment == "targeted-recall" {
			if targetedIndex < 100 {
				row.RecallResultCount = 0
				row.Assessments = nil
				row.FalseEmptyReview = "rejected"
				targetedIndex++
				continue
			}
			row.Assessments = []Assessment{{ResultKey: row.Assessments[0].ResultKey, Utility: "orienting", Quality: "current", Source: "evaluator"}}
			targetedIndex++
			continue
		}
		if row.Treatment == "broad-chronological" {
			utility := "orienting"
			if broadIndex%2 == 0 {
				utility = "unused"
			}
			row.Assessments = []Assessment{{ResultKey: row.Assessments[0].ResultKey, Utility: utility, Quality: "current", Source: "evaluator"}}
			broadIndex++
		}
	}
	return result
}
