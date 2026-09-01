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

type plannedBlock struct {
	key       [sha256.Size]byte
	class     string
	unitID    string
	treatment []sortableRun
}

// Plan expands a frozen cohort into paired treatment cells and returns only
// protocol identities. It never opens task inputs or retained evidence.
func (study *Study) Plan(manifest *Manifest) ([]PlannedRun, error) {
	if study == nil || manifest == nil {
		return nil, fmt.Errorf("Recall study plan requires a frozen manifest")
	}
	cohort, ok := study.Contract.cohort(manifest.CohortID)
	if !ok {
		return nil, fmt.Errorf("Recall study manifest names an unknown cohort")
	}
	if err := study.verifyManifest(manifest, cohort); err != nil {
		return nil, err
	}

	byClass := make(map[string][]plannedBlock, len(manifest.TaskClassCycle))
	for offset := 0; offset < manifest.SamplingUnits; offset++ {
		number := manifest.FirstSamplingUnit + offset
		unitID := fmt.Sprintf("%s-%04d", manifest.Namespace, number)
		class := manifest.TaskClassCycle[offset%len(manifest.TaskClassCycle)]
		block := plannedBlock{class: class, unitID: unitID}
		block.key = sha256.Sum256([]byte(study.Contract.Randomization.Seed + "\x00" + manifest.SelectionSeed + "\x00" + class + "\x00" + unitID))
		for _, treatment := range study.Contract.Treatments {
			runID := fmt.Sprintf("%s-%s", unitID, treatment.ID)
			run := PlannedRun{RunID: runID, SamplingUnitID: unitID, Cohort: manifest.CohortID, TaskClass: class, Treatment: treatment.ID}
			key := sha256.Sum256([]byte(study.Contract.Randomization.Seed + "\x00" + unitID + "\x00" + treatment.ID))
			block.treatment = append(block.treatment, sortableRun{key: key, run: run})
		}
		sort.Slice(block.treatment, func(i, j int) bool { return string(block.treatment[i].key[:]) < string(block.treatment[j].key[:]) })
		byClass[class] = append(byClass[class], block)
	}
	for class := range byClass {
		sort.Slice(byClass[class], func(i, j int) bool { return string(byClass[class][i].key[:]) < string(byClass[class][j].key[:]) })
	}
	planned := make([]PlannedRun, 0, manifest.SamplingUnits*len(study.Contract.Treatments))
	for stratumIndex := 0; len(planned) < manifest.SamplingUnits*len(study.Contract.Treatments); stratumIndex++ {
		for _, class := range manifest.TaskClassCycle {
			blocks := byClass[class]
			if stratumIndex >= len(blocks) {
				continue
			}
			for _, treatment := range blocks[stratumIndex].treatment {
				planned = append(planned, treatment.run)
				planned[len(planned)-1].Sequence = len(planned)
			}
		}
	}
	return planned, nil
}
