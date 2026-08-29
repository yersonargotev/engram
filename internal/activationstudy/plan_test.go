package activationstudy

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestPlanPairsEveryPromptAcrossTreatmentsDeterministically(t *testing.T) {
	t.Parallel()

	contractPath, hashPath := writeFrozenContract(t, validContractJSON())
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatal(err)
	}

	first := study.Plan()
	second := study.Plan()
	if !reflect.DeepEqual(first, second) {
		t.Fatal("Plan() changed order for the same frozen seed")
	}
	if len(first) != 36 {
		t.Fatalf("len(Plan()) = %d, want 36", len(first))
	}

	pairs := make(map[string]map[string]bool)
	cellIDs := make(map[string]bool)
	for index, run := range first {
		if run.Sequence != index+1 {
			t.Fatalf("run %d sequence = %d, want %d", index, run.Sequence, index+1)
		}
		if strings.Contains(run.CellID, "/") || strings.Contains(run.CellID, "\\") {
			t.Fatalf("cell ID %q contains a local path separator", run.CellID)
		}
		if cellIDs[run.CellID] {
			t.Fatalf("duplicate cell ID %q", run.CellID)
		}
		cellIDs[run.CellID] = true
		pairID := fmt.Sprintf("%s:%d", run.PromptID, run.Repetition)
		if pairs[pairID] == nil {
			pairs[pairID] = make(map[string]bool)
		}
		pairs[pairID][run.Treatment] = true
	}
	if len(pairs) != 12 {
		t.Fatalf("paired prompt repetitions = %d, want 12", len(pairs))
	}
	for pairID, treatments := range pairs {
		if len(treatments) != 3 || !treatments["engram-normal"] || !treatments["engram-ablated"] || !treatments["neutral"] {
			t.Fatalf("pair %q treatments = %#v", pairID, treatments)
		}
	}

	changed := *study
	changed.Contract.RandomizationSeed = "a-different-frozen-seed"
	if reflect.DeepEqual(first, changed.Plan()) {
		t.Fatal("Plan() order did not change with a different frozen seed")
	}
}
