package activationstudy

import (
	"crypto/sha256"
	"fmt"
	"sort"
)

// PlannedRun is one deterministic treatment cell. CellID is protocol identity,
// not a host session or local run identifier.
type PlannedRun struct {
	Sequence     int    `json:"sequence"`
	CellID       string `json:"cell_id"`
	PromptID     string `json:"prompt_id"`
	PromptClass  string `json:"prompt_class"`
	PromptText   string `json:"-"`
	Treatment    string `json:"treatment"`
	Repetition   int    `json:"repetition"`
	SessionShape string `json:"session_shape"`
}

type sortableRun struct {
	key [sha256.Size]byte
	run PlannedRun
}

// Plan expands the frozen paired corpus and assigns a stable pseudorandom order.
func (study *Study) Plan() []PlannedRun {
	contract := study.Contract
	runs := make([]sortableRun, 0, len(contract.Prompts)*len(contract.Treatments)*contract.Repetitions)
	for repetition := 1; repetition <= contract.Repetitions; repetition++ {
		for _, prompt := range contract.Prompts {
			for _, treatment := range contract.Treatments {
				cellID := fmt.Sprintf("%s-r%02d-%s", prompt.ID, repetition, treatment.ID)
				run := PlannedRun{
					CellID: cellID, PromptID: prompt.ID, PromptClass: prompt.Class,
					PromptText: prompt.Text, Treatment: treatment.ID, Repetition: repetition,
					SessionShape: contract.SessionShapes[0],
				}
				key := sha256.Sum256([]byte(contract.RandomizationSeed + "\x00" + cellID))
				runs = append(runs, sortableRun{key: key, run: run})
			}
		}
	}
	sort.Slice(runs, func(i, j int) bool {
		return string(runs[i].key[:]) < string(runs[j].key[:])
	})
	planned := make([]PlannedRun, len(runs))
	for index := range runs {
		planned[index] = runs[index].run
		planned[index].Sequence = index + 1
	}
	return planned
}
