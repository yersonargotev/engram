package recallstudy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

type cohortRunner interface {
	Run(context.Context, PlannedRun, TaskInput) (cohortRun, error)
	Close() error
}

type cohortRun struct {
	Row     RunRow
	Cleanup func() error
}

type invalidExecutionError struct {
	reasonCode string
}

func (err *invalidExecutionError) Error() string {
	return "Recall study execution is invalid: " + err.reasonCode
}

// ExecutionRuntime identifies the frozen local adapters used by the consented
// runner. Every path is verified before task bytes are materialized.
type ExecutionRuntime struct {
	SourceRepo  string
	CodexBinary string
	AuthFile    string
	TempRoot    string
}

// ExecutionRequest is the complete consented input for one cohort execution.
// Execute revalidates the frozen study before it materializes any task bytes.
type ExecutionRequest struct {
	Verification    VerificationInput
	Cohort          *Manifest
	CalibrationRows *RowSet
	OutputPath      string
	Runtime         ExecutionRuntime

	runner    cohortRunner
	writeRows func(string, any) error
}

// ExecutionResult reports private progress without exposing task or row content.
type ExecutionResult struct {
	CohortID       string `json:"cohort_id"`
	PlannedRuns    int    `json:"planned_runs"`
	ObservedRuns   int    `json:"observed_runs"`
	Complete       bool   `json:"complete"`
	NextStageReady bool   `json:"next_stage_ready"`
	Disposition    string `json:"disposition,omitempty"`
	ReasonCode     string `json:"reason_code,omitempty"`
}

// Execute runs one frozen cohort after every prerequisite for that stage passes.
func (study *Study) Execute(ctx context.Context, request ExecutionRequest) (result ExecutionResult, err error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}
	if _, err := study.Verify(request.Verification); err != nil {
		return ExecutionResult{}, err
	}
	if request.Cohort == nil {
		return ExecutionResult{}, fmt.Errorf("Recall study execution requires one frozen cohort")
	}
	cohort, ok := study.Contract.cohort(request.Cohort.CohortID)
	if !ok {
		return ExecutionResult{}, fmt.Errorf("Recall study execution requires one frozen cohort")
	}
	if err := study.verifyManifest(request.Cohort, cohort); err != nil {
		return ExecutionResult{}, err
	}
	if request.Cohort.CohortID == study.Contract.Cohorts.HeldOut.ID {
		if request.CalibrationRows == nil {
			return ExecutionResult{}, fmt.Errorf("Recall study held-out execution requires successful calibration evidence")
		}
		if request.CalibrationRows.CohortID != study.Contract.Cohorts.Calibration.ID {
			return ExecutionResult{}, fmt.Errorf("Recall study held-out execution requires successful calibration evidence")
		}
		calibration, err := study.aggregateRows(*request.CalibrationRows)
		if err != nil || calibration.OperationalFailures != 0 || calibration.Omissions != 0 {
			return ExecutionResult{}, fmt.Errorf("Recall study held-out execution requires successful calibration evidence")
		}
	}
	if strings.TrimSpace(request.OutputPath) == "" {
		return ExecutionResult{}, fmt.Errorf("Recall study execution requires a private output path")
	}
	plan, err := study.Plan(request.Cohort)
	if err != nil {
		return ExecutionResult{}, err
	}
	rows := RowSet{
		SchemaVersion: RowSetSchemaVersion, StudyID: study.Contract.StudyID,
		StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash,
		CohortID: request.Cohort.CohortID,
	}
	if existing, readErr := ReadRowSet(request.OutputPath); readErr == nil {
		rows = existing
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return ExecutionResult{}, readErr
	}
	observed, err := study.validateExecutionProgress(rows, plan)
	if err != nil {
		return ExecutionResult{}, err
	}
	if len(observed) == len(plan) {
		return study.executionResult(rows, plan)
	}
	runner := request.runner
	if runner == nil {
		created, err := newProcessCohortRunner(ctx, study, request.Cohort, request.Verification.Compatibility.Compatibility, request.Runtime)
		if err != nil {
			return ExecutionResult{}, err
		}
		runner = created
	}
	defer func() {
		err = errors.Join(err, runner.Close())
	}()
	writeRows := request.writeRows
	if writeRows == nil {
		writeRows = WritePrivateJSON
	}
	for _, planned := range plan {
		if _, complete := observed[planned.RunID]; complete {
			continue
		}
		input := frozenTaskInput(study.Contract, request.Cohort, planned.SamplingUnitID, planned.TaskClass)
		if err := study.VerifyTaskInput(request.Cohort, input); err != nil {
			return ExecutionResult{}, err
		}
		run, err := runner.Run(ctx, planned, input)
		if err != nil {
			var invalid *invalidExecutionError
			if errors.As(err, &invalid) {
				if run.Cleanup != nil {
					if cleanupErr := run.Cleanup(); cleanupErr != nil {
						return ExecutionResult{}, errors.Join(err, cleanupErr)
					}
				}
				return ExecutionResult{
					CohortID: request.Cohort.CohortID, PlannedRuns: len(plan), ObservedRuns: len(rows.Rows),
					Disposition: DispositionContinueCanary, ReasonCode: invalid.reasonCode,
				}, nil
			}
			return ExecutionResult{}, err
		}
		if run.Cleanup == nil {
			return ExecutionResult{}, fmt.Errorf("Recall study runner did not transfer cell cleanup ownership")
		}
		if err := validateExecutedRow(planned, run.Row); err != nil {
			return ExecutionResult{}, err
		}
		rows.Rows = append(rows.Rows, run.Row)
		observed[planned.RunID] = run.Row
		rows.Rows = orderedExecutionRows(plan, observed)
		if err := writeRows(request.OutputPath, rows); err != nil {
			return ExecutionResult{}, fmt.Errorf("persist Recall study progress: %w", err)
		}
		if err := run.Cleanup(); err != nil {
			return ExecutionResult{}, fmt.Errorf("clean persisted Recall study cell: %w", err)
		}
	}
	return study.executionResult(rows, plan)
}

func (study *Study) executionResult(rows RowSet, plan []PlannedRun) (ExecutionResult, error) {
	aggregate, err := study.aggregateRows(rows)
	if err != nil {
		return ExecutionResult{}, err
	}
	result := ExecutionResult{
		CohortID: rows.CohortID, PlannedRuns: len(plan), ObservedRuns: len(rows.Rows), Complete: true,
		NextStageReady: aggregate.OperationalFailures == 0 && aggregate.Omissions == 0,
	}
	if rows.CohortID == study.Contract.Cohorts.Calibration.ID && !result.NextStageReady {
		result.Disposition = DispositionContinueCanary
		result.ReasonCode = "calibration_operational_evidence_incomplete"
	}
	return result, nil
}

func (study *Study) validateExecutionProgress(rows RowSet, plan []PlannedRun) (map[string]RunRow, error) {
	if rows.SchemaVersion != RowSetSchemaVersion || rows.StudyID != study.Contract.StudyID ||
		rows.StudyVersion != study.Contract.StudyVersion || rows.ContractSHA256 != study.Hash {
		return nil, fmt.Errorf("Recall study progress identity does not match the frozen contract")
	}
	wanted := make(map[string]PlannedRun, len(plan))
	for _, planned := range plan {
		wanted[planned.RunID] = planned
	}
	observed := make(map[string]RunRow, len(rows.Rows))
	for _, row := range rows.Rows {
		planned, ok := wanted[row.RunID]
		if !ok || observed[row.RunID].RunID != "" {
			return nil, fmt.Errorf("Recall study progress contains an unknown or duplicate run")
		}
		if err := validateExecutedRow(planned, row); err != nil {
			return nil, err
		}
		observed[row.RunID] = row
	}
	return observed, nil
}

func validateExecutedRow(planned PlannedRun, row RunRow) error {
	if row.RunID != planned.RunID || row.SamplingUnitID != planned.SamplingUnitID ||
		row.TaskClass != planned.TaskClass || row.Treatment != planned.Treatment {
		return fmt.Errorf("Recall study execution row does not match the frozen plan")
	}
	return validateRunRow(row)
}

func orderedExecutionRows(plan []PlannedRun, observed map[string]RunRow) []RunRow {
	rows := make([]RunRow, 0, len(observed))
	for _, planned := range plan {
		if row, ok := observed[planned.RunID]; ok {
			rows = append(rows, row)
		}
	}
	return rows
}
