package recallstudy

import "testing"

func TestStudyTaskOutputFindsAnswerBeforeLifecycleContinuation(t *testing.T) {
	stream := []byte("" +
		`{"type":"item.completed","item":{"type":"agent_message","text":"{\"answer\":\"frozen\"}"}}` + "\n" +
		`{"type":"item.completed","item":{"type":"agent_message","text":"Checkpoint verified as already finalized."}}` + "\n")
	if !studyTaskOutputMatches([]byte("Checkpoint verified as already finalized."), stream, []byte(`{"answer":"frozen"}`)) {
		t.Fatal("task answer before lifecycle continuation was not verified")
	}
	if studyTaskOutputMatches([]byte(`{"answer":"wrong"}`), stream, []byte(`{"answer":"different"}`)) {
		t.Fatal("unobserved task answer passed verification")
	}
}
