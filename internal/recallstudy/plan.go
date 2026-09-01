package recallstudy

import (
	"crypto/sha256"
	"fmt"
	"sort"
)

type PlannedRun struct {
	Sequence       int    `json:"sequence"`
	RunID          string `json:"run_id"`
	SamplingUnitID string `json:"sampling_unit_id"`
	Cohort         string `json:"cohort"`
	TaskClass      string `json:"task_class"`
	Treatment      string `json:"treatment"`
}

type sortableRun struct {
	key [sha256.Size]byte
	run PlannedRun
}

// Plan expands a frozen cohort into paired treatment cells and returns only
// protocol identities. It never opens task inputs or retained evidence.
func (study *Study) Plan(manifest *Manifest) ([]PlannedRun, error) {
	if study == nil || manifest == nil {
		return nil, fmt.Errorf("Recall study plan requires a frozen manifest")
	}
	var cohort CohortContract
	switch manifest.CohortID {
	case study.Contract.Cohorts.Calibration.ID:
		cohort = study.Contract.Cohorts.Calibration
	case study.Contract.Cohorts.HeldOut.ID:
		cohort = study.Contract.Cohorts.HeldOut
	default:
		return nil, fmt.Errorf("Recall study manifest names an unknown cohort")
	}
	if err := study.verifyManifest(manifest, cohort); err != nil {
		return nil, err
	}

	runs := make([]sortableRun, 0, manifest.SamplingUnits*len(study.Contract.Treatments))
	for offset := 0; offset < manifest.SamplingUnits; offset++ {
		number := manifest.FirstSamplingUnit + offset
		unitID := fmt.Sprintf("%s-%04d", manifest.Namespace, number)
		class := manifest.TaskClassCycle[offset%len(manifest.TaskClassCycle)]
		for _, treatment := range study.Contract.Treatments {
			runID := fmt.Sprintf("%s-%s", unitID, treatment.ID)
			run := PlannedRun{RunID: runID, SamplingUnitID: unitID, Cohort: manifest.CohortID, TaskClass: class, Treatment: treatment.ID}
			key := sha256.Sum256([]byte(manifest.SelectionSeed + "\x00" + runID))
			runs = append(runs, sortableRun{key: key, run: run})
		}
	}
	sort.Slice(runs, func(i, j int) bool { return string(runs[i].key[:]) < string(runs[j].key[:]) })
	planned := make([]PlannedRun, len(runs))
	for index := range runs {
		planned[index] = runs[index].run
		planned[index].Sequence = index + 1
	}
	return planned, nil
}
