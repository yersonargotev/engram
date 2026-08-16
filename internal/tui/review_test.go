package tui

import (
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/yersonargotev/engram/internal/store"
)

func makeMemoryDue(t *testing.T, s *store.Store, id int64) {
	t.Helper()
	if _, err := s.DB().Exec(`UPDATE observations SET review_after = ? WHERE id = ?`, "2000-01-01 00:00:00", id); err != nil {
		t.Fatalf("make memory due: %v", err)
	}
}

func TestLoadReviewObservationsIsProjectScoped(t *testing.T) {
	fx := newTestFixture(t)
	makeMemoryDue(t, fx.store, fx.secondObs)

	if err := fx.store.CreateSession("other-session", "other", "/tmp/other"); err != nil {
		t.Fatalf("create other session: %v", err)
	}
	otherID, err := fx.store.AddObservation(store.AddObservationParams{
		SessionID: "other-session",
		Type:      "decision",
		Title:     "Other project decision",
		Content:   "must not appear in the current project queue",
		Project:   "other",
		Scope:     "project",
	})
	if err != nil {
		t.Fatalf("add other memory: %v", err)
	}
	makeMemoryDue(t, fx.store, otherID)

	loaded, ok := loadReviewObservations(fx.store, "engram")().(reviewObservationsMsg)
	if !ok {
		t.Fatalf("message type = %T, want reviewObservationsMsg", loadReviewObservations(fx.store, "engram")())
	}
	if loaded.err != nil {
		t.Fatalf("load review observations: %v", loaded.err)
	}
	if loaded.project != "engram" {
		t.Fatalf("project = %q, want engram", loaded.project)
	}
	if len(loaded.observations) != 1 || loaded.observations[0].ID != fx.secondObs {
		t.Fatalf("review observations = %#v, want only %d", loaded.observations, fx.secondObs)
	}
	if loaded.observations[0].State() != store.ObservationStateNeedsReview {
		t.Fatalf("state = %q, want needs_review", loaded.observations[0].State())
	}
}

func TestLoadReviewObservationsReportsUnavailableInputs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		store   *store.Store
		project string
	}{
		{name: "store", project: "engram"},
		{name: "project", store: newTestFixture(t).store},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := loadReviewObservations(tc.store, tc.project)().(reviewObservationsMsg)
			if msg.err == nil {
				t.Fatal("expected load error")
			}
		})
	}
}

func TestDashboardOpensReviewQueue(t *testing.T) {
	m := NewWithProject(nil, "", "", "engram")
	reviewIndex := -1
	for i, item := range dashboardMenuItems {
		if item == "Review memories" {
			reviewIndex = i
			break
		}
	}
	if reviewIndex < 0 {
		t.Fatal("dashboard menu is missing Review memories")
	}

	m.Cursor = reviewIndex
	updatedModel, cmd := m.handleDashboardSelection()
	updated := updatedModel.(Model)
	if updated.Screen != ScreenReview || updated.PrevScreen != ScreenDashboard {
		t.Fatalf("review navigation = screen %v previous %v", updated.Screen, updated.PrevScreen)
	}
	if !updated.ReviewLoading || cmd == nil {
		t.Fatalf("review navigation loading = %t, cmd nil = %t", updated.ReviewLoading, cmd == nil)
	}
}

func TestDashboardReportsAmbiguousProject(t *testing.T) {
	m := NewWithProjectResolver(nil, "", "", func() (string, error) {
		return "", errors.New("ambiguous project: open one project directory")
	})
	m.Cursor = 2

	updatedModel, cmd := m.handleDashboardSelection()
	updated := updatedModel.(Model)
	if updated.Screen != ScreenReview || !updated.ReviewLoading || cmd == nil {
		t.Fatalf("ambiguous project = screen %v loading %t cmd nil %t", updated.Screen, updated.ReviewLoading, cmd == nil)
	}
	msg := cmd().(reviewObservationsMsg)
	updatedModel, _ = updated.Update(msg)
	updated = updatedModel.(Model)
	if updated.ReviewLoading || !strings.Contains(updated.ReviewError, "ambiguous project") {
		t.Fatalf("review error state = loading %t error %q", updated.ReviewLoading, updated.ReviewError)
	}
}

func TestReviewRefreshRetriesProjectDetection(t *testing.T) {
	attempts := 0
	m := NewWithProjectResolver(nil, "", "", func() (string, error) {
		attempts++
		if attempts == 1 {
			return "", errors.New("ambiguous project")
		}
		return "engram", nil
	})
	m.Screen = ScreenReview

	first := m.loadReviewObservations()().(reviewObservationsMsg)
	if first.err == nil {
		t.Fatal("first project detection should fail")
	}
	_, retry := m.handleReviewKeys("r")
	second := retry().(reviewObservationsMsg)
	if second.project != "engram" || second.err == nil || !strings.Contains(second.err.Error(), "store is unavailable") {
		t.Fatalf("retry result = project %q error %v", second.project, second.err)
	}
	if attempts != 2 {
		t.Fatalf("project detection attempts = %d, want 2", attempts)
	}
}

func TestReviewMessageClampsViewportAndResize(t *testing.T) {
	m := NewWithProject(nil, "", "", "engram")
	m.Screen = ScreenReview
	m.Height = 14
	m.Cursor = 4
	m.Scroll = 3
	m.ReviewLoading = true

	updatedModel, _ := m.Update(reviewObservationsMsg{
		project:      "engram",
		observations: []store.Observation{{ID: 1}, {ID: 2}},
	})
	updated := updatedModel.(Model)
	if updated.ReviewLoading || updated.ReviewError != "" {
		t.Fatalf("loaded state = loading %t error %q", updated.ReviewLoading, updated.ReviewError)
	}
	if updated.Cursor != 1 || updated.Scroll != 0 {
		t.Fatalf("clamped cursor/scroll = %d/%d, want 1/0", updated.Cursor, updated.Scroll)
	}

	updated.ReviewObservations = []store.Observation{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}}
	updated.Cursor = 4
	updated.Scroll = 3
	resizedModel, _ := updated.Update(tea.WindowSizeMsg{Width: 90, Height: 40})
	resized := resizedModel.(Model)
	if resized.Scroll != 0 {
		t.Fatalf("resize scroll = %d, want 0 when all items fit", resized.Scroll)
	}
}

func TestReviewNavigationPreservesQueueContext(t *testing.T) {
	m := NewWithProject(nil, "", "", "engram")
	m.Screen = ScreenReview
	m.Height = 14
	m.Cursor = 3
	m.Scroll = 1
	m.ReviewObservations = []store.Observation{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}

	updatedModel, cmd := m.handleReviewKeys("enter")
	updated := updatedModel.(Model)
	if updated.PrevScreen != ScreenReview || cmd == nil {
		t.Fatalf("open detail previous = %v, cmd nil = %t", updated.PrevScreen, cmd == nil)
	}
	if updated.Cursor != 3 || updated.Scroll != 1 {
		t.Fatalf("queue context changed on open: %d/%d", updated.Cursor, updated.Scroll)
	}

	updated.Screen = ScreenObservationDetail
	updated.SelectedObservation = &store.Observation{ID: 4}
	backModel, refresh := updated.handleObservationDetailKeys("esc")
	back := backModel.(Model)
	if back.Screen != ScreenReview || refresh == nil {
		t.Fatalf("detail back = screen %v, refresh nil %t", back.Screen, refresh == nil)
	}
	if back.Cursor != 3 || back.Scroll != 1 {
		t.Fatalf("queue context after back = %d/%d, want 3/1", back.Cursor, back.Scroll)
	}
}

func TestReviewTimelineBackPreservesQueueContext(t *testing.T) {
	m := NewWithProject(nil, "", "", "engram")
	m.Screen = ScreenTimeline
	m.PrevScreen = ScreenReview
	m.Cursor = 3
	m.Scroll = 8
	m.ReviewCursor = 3
	m.ReviewScroll = 1

	updatedModel, refresh := m.handleTimelineKeys("esc")
	updated := updatedModel.(Model)
	if updated.Screen != ScreenReview || updated.Cursor != 3 || updated.Scroll != 1 || !updated.ReviewLoading || refresh == nil {
		t.Fatalf("timeline back = screen %v cursor/scroll %d/%d loading %t refresh nil %t", updated.Screen, updated.Cursor, updated.Scroll, updated.ReviewLoading, refresh == nil)
	}
}

func TestReviewListNavigationRefreshAndBack(t *testing.T) {
	m := NewWithProject(nil, "", "", "engram")
	m.Screen = ScreenReview
	m.Height = 14
	m.ReviewObservations = []store.Observation{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}
	m.Cursor = 2

	downModel, _ := m.handleReviewKeys("down")
	down := downModel.(Model)
	if down.Cursor != 3 || down.Scroll != 1 {
		t.Fatalf("review down = cursor/scroll %d/%d, want 3/1", down.Cursor, down.Scroll)
	}
	upModel, _ := down.handleReviewKeys("k")
	up := upModel.(Model)
	if up.Cursor != 2 || up.Scroll != 1 {
		t.Fatalf("review up = cursor/scroll %d/%d, want 2/1", up.Cursor, up.Scroll)
	}

	refreshModel, refresh := up.handleReviewKeys("r")
	refreshing := refreshModel.(Model)
	if !refreshing.ReviewLoading || refresh == nil {
		t.Fatalf("review refresh = loading %t, cmd nil %t", refreshing.ReviewLoading, refresh == nil)
	}

	backModel, stats := up.handleReviewKeys("esc")
	back := backModel.(Model)
	if back.Screen != ScreenDashboard || back.Cursor != 0 || back.Scroll != 0 || stats == nil {
		t.Fatalf("review back = screen %v cursor/scroll %d/%d stats nil %t", back.Screen, back.Cursor, back.Scroll, stats == nil)
	}
}

func TestReviewMarkRequiresConfirmationAndCanCancel(t *testing.T) {
	m := NewWithProject(nil, "", "", "engram")
	m.Screen = ScreenObservationDetail
	m.PrevScreen = ScreenReview
	m.SelectedObservation = &store.Observation{ID: 42, Type: "decision", Title: "Keep me"}

	confirmModel, cmd := m.handleObservationDetailKeys("m")
	confirm := confirmModel.(Model)
	if !confirm.ReviewConfirming || cmd != nil {
		t.Fatalf("mark prompt = confirming %t, cmd nil %t", confirm.ReviewConfirming, cmd == nil)
	}

	cancelModel, cmd := confirm.handleObservationDetailKeys("n")
	cancelled := cancelModel.(Model)
	if cancelled.ReviewConfirming || cmd != nil {
		t.Fatalf("cancel = confirming %t, cmd nil %t", cancelled.ReviewConfirming, cmd == nil)
	}
}

func TestReviewMarkPersistsAndRefreshesQueue(t *testing.T) {
	fx := newTestFixture(t)
	makeMemoryDue(t, fx.store, fx.secondObs)
	m := NewWithProject(fx.store, "", fx.dataDir, "engram")
	m.Screen = ScreenObservationDetail
	m.PrevScreen = ScreenReview
	m.SelectedObservation, _ = fx.store.GetObservation(fx.secondObs)
	m.ReviewConfirming = true
	m.ReviewObservations = []store.Observation{*m.SelectedObservation}

	confirmModel, cmd := m.handleObservationDetailKeys("y")
	confirm := confirmModel.(Model)
	if !confirm.ReviewMutating || cmd == nil {
		t.Fatalf("confirm mark = mutating %t, cmd nil %t", confirm.ReviewMutating, cmd == nil)
	}
	msg, ok := cmd().(reviewMarkedMsg)
	if !ok {
		t.Fatalf("message type = %T, want reviewMarkedMsg", cmd())
	}
	if msg.err != nil || msg.observation == nil || msg.observation.State() != store.ObservationStateActive {
		t.Fatalf("mark result = %+v", msg)
	}

	updatedModel, refresh := confirm.Update(msg)
	updated := updatedModel.(Model)
	if updated.Screen != ScreenReview || updated.ReviewMutating || updated.ReviewConfirming {
		t.Fatalf("mark completion = screen %v mutating %t confirming %t", updated.Screen, updated.ReviewMutating, updated.ReviewConfirming)
	}
	if refresh == nil {
		t.Fatal("mark completion should refresh review queue")
	}
	if !strings.Contains(updated.ReviewNotice, "marked reviewed") {
		t.Fatalf("review notice = %q", updated.ReviewNotice)
	}
	if due := refresh().(reviewObservationsMsg).observations; len(due) != 0 {
		t.Fatalf("due memories after mark = %d, want 0", len(due))
	}
}

func TestReviewPinToggleUpdatesDetailWithoutCompletingItem(t *testing.T) {
	fx := newTestFixture(t)
	makeMemoryDue(t, fx.store, fx.secondObs)
	obs, _ := fx.store.GetObservation(fx.secondObs)
	m := NewWithProject(fx.store, "", fx.dataDir, "engram")
	m.Screen = ScreenObservationDetail
	m.PrevScreen = ScreenReview
	m.SelectedObservation = obs
	m.ReviewObservations = []store.Observation{*obs}

	pinModel, cmd := m.handleObservationDetailKeys("p")
	pinning := pinModel.(Model)
	if !pinning.ReviewMutating || cmd == nil {
		t.Fatalf("pin = mutating %t, cmd nil %t", pinning.ReviewMutating, cmd == nil)
	}
	msg := cmd().(reviewPinnedMsg)
	if msg.err != nil || msg.observation == nil || !msg.observation.Pinned {
		t.Fatalf("pin result = %+v", msg)
	}

	updatedModel, followup := pinning.Update(msg)
	updated := updatedModel.(Model)
	if followup != nil || updated.Screen != ScreenObservationDetail || updated.ReviewMutating {
		t.Fatalf("pin completion = screen %v mutating %t followup nil %t", updated.Screen, updated.ReviewMutating, followup == nil)
	}
	if updated.SelectedObservation == nil || !updated.SelectedObservation.Pinned {
		t.Fatal("selected observation was not refreshed as pinned")
	}
	if len(updated.ReviewObservations) != 1 || !updated.ReviewObservations[0].Pinned {
		t.Fatal("queue item was not updated in place")
	}
	if updated.ReviewObservations[0].State() != store.ObservationStateNeedsReview {
		t.Fatal("pinning must not complete the review item")
	}

	unpinModel, cmd := updated.handleObservationDetailKeys("p")
	unpinning := unpinModel.(Model)
	msg = cmd().(reviewPinnedMsg)
	if !unpinning.ReviewMutating || msg.err != nil || msg.observation == nil || msg.observation.Pinned {
		t.Fatalf("unpin result = mutating %t message %+v", unpinning.ReviewMutating, msg)
	}
}

func TestReviewMutationErrorsStayInDetail(t *testing.T) {
	m := NewWithProject(nil, "", "", "engram")
	m.Screen = ScreenObservationDetail
	m.PrevScreen = ScreenReview
	m.SelectedObservation = &store.Observation{ID: 42}
	m.ReviewMutating = true
	m.ReviewConfirming = true

	updatedModel, cmd := m.Update(reviewMarkedMsg{err: errors.New("mark failed")})
	updated := updatedModel.(Model)
	if cmd != nil || updated.Screen != ScreenObservationDetail || updated.ReviewMutating || updated.ReviewConfirming {
		t.Fatalf("mark error = screen %v mutating %t confirming %t cmd nil %t", updated.Screen, updated.ReviewMutating, updated.ReviewConfirming, cmd == nil)
	}
	if updated.ReviewError != "mark failed" {
		t.Fatalf("mark error = %q", updated.ReviewError)
	}

	updated.ReviewMutating = true
	updatedModel, cmd = updated.Update(reviewPinnedMsg{err: errors.New("pin failed")})
	updated = updatedModel.(Model)
	if cmd != nil || updated.Screen != ScreenObservationDetail || updated.ReviewMutating {
		t.Fatalf("pin error = screen %v mutating %t cmd nil %t", updated.Screen, updated.ReviewMutating, cmd == nil)
	}
	if updated.ReviewError != "pin failed" {
		t.Fatalf("pin error = %q", updated.ReviewError)
	}

	if msg := markReviewObservation(nil, 42)().(reviewMarkedMsg); msg.err == nil {
		t.Fatal("nil store mark should fail")
	}
	if msg := setReviewObservationPinned(nil, 42, true)().(reviewPinnedMsg); msg.err == nil {
		t.Fatal("nil store pin should fail")
	}
}

func TestViewReviewStatesAndDetailActions(t *testing.T) {
	m := NewWithProject(nil, "", "", "engram")
	m.Screen = ScreenReview
	m.ReviewLoading = true
	if out := m.View(); !strings.Contains(out, "Loading memories due for review") {
		t.Fatalf("loading view = %q", out)
	}

	m.ReviewLoading = false
	m.ReviewError = "database unavailable"
	if out := m.View(); !strings.Contains(out, "database unavailable") {
		t.Fatalf("error view = %q", out)
	}

	m.ReviewError = ""
	if out := m.View(); !strings.Contains(out, "No memories are due for review") {
		t.Fatalf("empty view = %q", out)
	}

	reviewAfter := "2000-01-01 00:00:00"
	m.Height = 14
	m.ReviewObservations = []store.Observation{
		{ID: 1, Type: "decision", Title: "Review this", Content: "still accurate?", CreatedAt: "1999-01-01", ReviewAfter: &reviewAfter, Pinned: true},
		{ID: 2, Type: "policy", Title: "Second", Content: "policy", CreatedAt: "1999-01-02", ReviewAfter: &reviewAfter},
		{ID: 3, Type: "preference", Title: "Third", Content: "preference", CreatedAt: "1999-01-03", ReviewAfter: &reviewAfter},
		{ID: 4, Type: "decision", Title: "Fourth", Content: "decision", CreatedAt: "1999-01-04", ReviewAfter: &reviewAfter},
	}
	out := m.View()
	for _, want := range []string{"Review memories", "engram", "needs_review", "pinned", "review due 2000-01-01", "showing 1-3 of 4", "r refresh"} {
		if !strings.Contains(out, want) {
			t.Fatalf("review view missing %q: %q", want, out)
		}
	}

	m.Screen = ScreenObservationDetail
	m.PrevScreen = ScreenReview
	m.SelectedObservation = &m.ReviewObservations[0]
	out = m.View()
	for _, want := range []string{"m mark reviewed", "p unpin", "local to this device"} {
		if !strings.Contains(out, want) {
			t.Fatalf("review detail missing %q: %q", want, out)
		}
	}

	m.ReviewConfirming = true
	out = m.View()
	if !strings.Contains(out, "Mark this memory reviewed?") || !strings.Contains(out, "y confirm") {
		t.Fatalf("confirmation view missing prompt: %q", out)
	}

	m.ReviewConfirming = false
	m.ReviewNotice = "Memory pinned"
	if out = m.View(); !strings.Contains(out, "Memory pinned") {
		t.Fatalf("notice view missing feedback: %q", out)
	}
	m.ReviewNotice = ""
	m.ReviewError = "pin failed"
	if out = m.View(); !strings.Contains(out, "Review action failed: pin failed") {
		t.Fatalf("error view missing feedback: %q", out)
	}
	m.ReviewError = ""
	m.ReviewMutating = true
	if out = m.View(); !strings.Contains(out, "Updating local memory state") {
		t.Fatalf("mutating view missing progress: %q", out)
	}
}
