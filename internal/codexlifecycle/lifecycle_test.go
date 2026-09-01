package codexlifecycle

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSelectTreatmentKeepsCanaryOptInAndRejectsUnknownValues(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want Selection
	}{
		{
			name: "default remains broad project context",
			want: Selection{Treatment: TreatmentBroadProjectContext, Valid: true, ReasonCode: "canary_disabled"},
		},
		{
			name: "cue only targeted Recall",
			raw:  "targeted-recall",
			want: Selection{Treatment: TreatmentCueOnlyTargetedRecall, Enabled: true, Valid: true, ReasonCode: "canary_targeted_recall"},
		},
		{
			name: "declared exact session variant",
			raw:  "targeted-recall-exact-session",
			want: Selection{Treatment: TreatmentCueOnlyTargetedRecallExactSession, Enabled: true, Valid: true, ReasonCode: "canary_targeted_recall_exact_session"},
		},
		{
			name: "unknown treatment is not enabled",
			raw:  "broad-but-labeled-canary",
			want: Selection{Treatment: TreatmentBroadProjectContext, ReasonCode: "canary_treatment_invalid"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := SelectTreatment(tc.raw); got != tc.want {
				t.Fatalf("SelectTreatment(%q) = %#v, want %#v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestBuildModelContextIsCueOnlyUnlessExactContextIsExplicitlyProvided(t *testing.T) {
	const cue = "canonical checkpoint cue"
	if got, truncated := BuildModelContext(cue, "", MaxInjectedUTF8Bytes); got != cue || truncated {
		t.Fatalf("cue-only context = %q, truncated=%t", got, truncated)
	}

	extra := strings.Repeat("memoria-á", MaxInjectedUTF8Bytes)
	got, truncated := BuildModelContext(cue, extra, MaxInjectedUTF8Bytes)
	if !truncated || len(got) > MaxInjectedUTF8Bytes || !utf8.ValidString(got) {
		t.Fatalf("bounded context bytes=%d truncated=%t valid_utf8=%t", len(got), truncated, utf8.ValidString(got))
	}
	if !strings.HasPrefix(got, cue+"\n\n") {
		t.Fatalf("bounded context lost canonical cue: %q", got)
	}
}
