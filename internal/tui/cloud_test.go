package tui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/yersonargotev/engram/internal/cloud"
	"github.com/yersonargotev/engram/internal/cloud/constants"
)

func TestCloudControlCenterLoadsRealStatus(t *testing.T) {
	fx := newTestFixture(t)
	if _, err := cloud.SaveClientServer(fx.dataDir, "https://cloud.example.test/"); err != nil {
		t.Fatalf("save client config: %v", err)
	}
	if err := fx.store.EnrollProject("engram"); err != nil {
		t.Fatalf("enroll project: %v", err)
	}
	if err := fx.store.MarkSyncBlocked(constants.TargetKeyCloud+":engram", constants.ReasonPaused, "paused by test"); err != nil {
		t.Fatalf("mark sync blocked: %v", err)
	}

	msg := loadCloudStatus(fx.store, fx.dataDir)()
	loaded, ok := msg.(cloudStatusMsg)
	if !ok {
		t.Fatalf("message type = %T, want cloudStatusMsg", msg)
	}
	if loaded.err != nil {
		t.Fatalf("unexpected status error: %v", loaded.err)
	}
	if !loaded.status.Configured || loaded.status.ServerURL != "https://cloud.example.test/" {
		t.Fatalf("unexpected config status: %+v", loaded.status)
	}
	if len(loaded.status.EnrolledProjects) != 1 || loaded.status.EnrolledProjects[0] != "engram" {
		t.Fatalf("enrolled projects = %#v", loaded.status.EnrolledProjects)
	}
	if loaded.status.SyncLifecycle != "degraded" || loaded.status.ReasonCode != constants.ReasonPaused {
		t.Fatalf("unexpected sync status: %+v", loaded.status)
	}
	if len(loaded.status.ProjectStates) != 1 || loaded.status.ProjectStates[0].Project != "engram" || loaded.status.ProjectStates[0].ReasonCode != constants.ReasonPaused {
		t.Fatalf("unexpected project sync states: %+v", loaded.status.ProjectStates)
	}
}

func TestCloudControlCenterRejectsInvalidResolvedServer(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "cloud.json"), []byte(`{"server_url":"file:///tmp/cloud","token":""}`), 0o644); err != nil {
		t.Fatalf("seed invalid config: %v", err)
	}
	msg := loadCloudStatus(nil, dataDir)().(cloudStatusMsg)
	if msg.err == nil || !strings.Contains(msg.err.Error(), "invalid cloud server URL") {
		t.Fatalf("invalid config error = %v", msg.err)
	}
}

func TestSyncLifecyclePrioritySurfacesWorstProjectState(t *testing.T) {
	ordered := []string{"", "idle", "healthy", "pending", "running", "degraded"}
	for i := 1; i < len(ordered); i++ {
		if syncLifecyclePriority(ordered[i]) <= syncLifecyclePriority(ordered[i-1]) {
			t.Fatalf("priority(%q) must exceed priority(%q)", ordered[i], ordered[i-1])
		}
	}
}

func TestCloudStatusKeepsGlobalBlockerWithHealthyEnrolledProject(t *testing.T) {
	fx := newTestFixture(t)
	if err := fx.store.EnrollProject("engram"); err != nil {
		t.Fatalf("enroll project: %v", err)
	}
	if err := fx.store.MarkSyncHealthy(constants.TargetKeyCloud + ":engram"); err != nil {
		t.Fatalf("mark project healthy: %v", err)
	}
	if err := fx.store.MarkSyncBlocked(constants.TargetKeyCloud, constants.ReasonAuthRequired, "token rejected"); err != nil {
		t.Fatalf("mark global auth blocker: %v", err)
	}

	loaded := loadCloudStatus(fx.store, fx.dataDir)().(cloudStatusMsg)
	if loaded.err != nil {
		t.Fatalf("load cloud status: %v", loaded.err)
	}
	if loaded.status.SyncLifecycle != "degraded" || loaded.status.ReasonCode != constants.ReasonAuthRequired {
		t.Fatalf("global blocker was hidden: %+v", loaded.status)
	}
	if len(loaded.status.ProjectStates) != 1 || loaded.status.ProjectStates[0].Lifecycle != "healthy" {
		t.Fatalf("project state missing: %+v", loaded.status.ProjectStates)
	}
}

func TestCloudSettingsActionsOpenWorkingScreens(t *testing.T) {
	m := NewWithDataDir(nil, "", t.TempDir())
	m.Screen = ScreenCloudSettings

	m.Cursor = 0
	updatedModel, _ := m.handleCloudSettingsKeys("enter")
	updated := updatedModel.(Model)
	if updated.Screen != ScreenCloudConfigure || updated.PrevScreen != ScreenCloudSettings || !updated.CloudServerInput.Focused() {
		t.Fatalf("configure action = screen %v previous %v focused %t", updated.Screen, updated.PrevScreen, updated.CloudServerInput.Focused())
	}

	m.Cursor = 1
	updatedModel, cmd := m.handleCloudSettingsKeys("enter")
	updated = updatedModel.(Model)
	if updated.Screen != ScreenCloudStatus || updated.PrevScreen != ScreenCloudSettings || cmd == nil || !updated.CloudLoading {
		t.Fatalf("status action = screen %v previous %v loading %t cmd nil %t", updated.Screen, updated.PrevScreen, updated.CloudLoading, cmd == nil)
	}

	m.Cursor = 2
	updatedModel, cmd = m.handleCloudSettingsKeys("enter")
	updated = updatedModel.(Model)
	if updated.Screen != ScreenCloudEnroll || updated.PrevScreen != ScreenCloudSettings || cmd == nil || !updated.CloudLoading {
		t.Fatalf("enroll action = screen %v previous %v loading %t cmd nil %t", updated.Screen, updated.PrevScreen, updated.CloudLoading, cmd == nil)
	}
}

func TestCloudNavigationPreservesParentContext(t *testing.T) {
	m := NewWithDataDir(nil, "", t.TempDir())
	m.Screen = ScreenDashboard
	m.Cursor = 4

	updatedModel, _ := m.Update(keyMsg("enter"))
	updated := updatedModel.(Model)
	if updated.Screen != ScreenCloudSettings || updated.PrevScreen != ScreenDashboard {
		t.Fatalf("cloud settings navigation = screen %v previous %v", updated.Screen, updated.PrevScreen)
	}

	updated.Cursor = 1
	updatedModel, _ = updated.Update(keyMsg("enter"))
	updated = updatedModel.(Model)
	if updated.Screen != ScreenCloudStatus || updated.PrevScreen != ScreenCloudSettings {
		t.Fatalf("cloud status navigation = screen %v previous %v", updated.Screen, updated.PrevScreen)
	}

	updatedModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyEsc})
	updated = updatedModel.(Model)
	if updated.Screen != ScreenCloudSettings || updated.PrevScreen != ScreenDashboard {
		t.Fatalf("cloud status back = screen %v previous %v", updated.Screen, updated.PrevScreen)
	}

	updatedModel, _ = updated.Update(tea.KeyMsg{Type: tea.KeyEsc})
	updated = updatedModel.(Model)
	if updated.Screen != ScreenDashboard {
		t.Fatalf("cloud settings back = screen %v, want %v", updated.Screen, ScreenDashboard)
	}
}

func TestCloudConfigureAndEnrollBackRestoreSettingsContext(t *testing.T) {
	configure := NewWithDataDir(nil, "", t.TempDir())
	configure.Screen = ScreenCloudSettings
	configure.PrevScreen = ScreenDashboard
	configure.Cursor = 0
	updatedModel, _ := configure.Update(keyMsg("enter"))
	configured := updatedModel.(Model)
	updatedModel, _ = configured.Update(tea.KeyMsg{Type: tea.KeyEsc})
	configured = updatedModel.(Model)
	if configured.Screen != ScreenCloudSettings || configured.PrevScreen != ScreenDashboard || configured.CloudServerInput.Focused() {
		t.Fatalf("configure back = screen %v previous %v focused %t", configured.Screen, configured.PrevScreen, configured.CloudServerInput.Focused())
	}

	enroll := NewWithDataDir(nil, "", t.TempDir())
	enroll.Screen = ScreenCloudSettings
	enroll.PrevScreen = ScreenDashboard
	enroll.Cursor = 2
	updatedModel, _ = enroll.Update(keyMsg("enter"))
	enrolled := updatedModel.(Model)
	updatedModel, refresh := enrolled.Update(tea.KeyMsg{Type: tea.KeyEsc})
	enrolled = updatedModel.(Model)
	if enrolled.Screen != ScreenCloudSettings || enrolled.PrevScreen != ScreenDashboard || refresh == nil {
		t.Fatalf("enroll back = screen %v previous %v refresh nil %t", enrolled.Screen, enrolled.PrevScreen, refresh == nil)
	}
}

func TestCloudConfigureValidatesPersistsAndReturnsToControlCenter(t *testing.T) {
	dataDir := t.TempDir()
	m := NewWithDataDir(nil, "", dataDir)
	m.Screen = ScreenCloudConfigure
	m.CloudServerInput.Focus()
	m.CloudServerInput.SetValue("file:///tmp/not-cloud")

	updatedModel, cmd := m.handleCloudConfigureInputKeys(keyMsg("enter"))
	updated := updatedModel.(Model)
	if cmd == nil {
		t.Fatal("configure enter should execute persistence command")
	}
	result := cmd()
	configured, ok := result.(cloudConfiguredMsg)
	if !ok {
		t.Fatalf("message type = %T, want cloudConfiguredMsg", result)
	}
	if configured.err == nil {
		t.Fatal("invalid URL should be rejected")
	}
	updatedModel, _ = updated.Update(configured)
	updated = updatedModel.(Model)
	if updated.Screen != ScreenCloudConfigure || !strings.Contains(updated.ErrorMsg, "scheme must be http or https") {
		t.Fatalf("invalid result should remain editable: screen %v error %q", updated.Screen, updated.ErrorMsg)
	}

	m.CloudServerInput.SetValue("https://cloud.example.test")
	updatedModel, cmd = m.handleCloudConfigureInputKeys(keyMsg("enter"))
	updated = updatedModel.(Model)
	configured = cmd().(cloudConfiguredMsg)
	if configured.err != nil {
		t.Fatalf("configure server: %v", configured.err)
	}
	updatedModel, refresh := updated.Update(configured)
	updated = updatedModel.(Model)
	if updated.Screen != ScreenCloudSettings || updated.PrevScreen != ScreenDashboard || updated.CloudNotice == "" || refresh == nil {
		t.Fatalf("configured result = screen %v previous %v notice %q refresh nil %t", updated.Screen, updated.PrevScreen, updated.CloudNotice, refresh == nil)
	}

	resolved, err := cloud.LoadClientConfig(dataDir)
	if err != nil {
		t.Fatalf("load persisted config: %v", err)
	}
	if resolved == nil || resolved.ServerURL != "https://cloud.example.test" {
		t.Fatalf("persisted config = %+v", resolved)
	}
}

func TestCloudConfigureTreatsQAsTextWhileInputIsFocused(t *testing.T) {
	m := NewWithDataDir(nil, "", t.TempDir())
	m.Screen = ScreenCloudConfigure
	m.CloudServerInput.Focus()
	m.CloudServerInput.SetValue("https://")
	updatedModel, _ := m.Update(keyMsg("q"))
	updated := updatedModel.(Model)
	if updated.Screen != ScreenCloudConfigure || updated.CloudServerInput.Value() != "https://q" {
		t.Fatalf("focused q should be typed: screen %v value %q", updated.Screen, updated.CloudServerInput.Value())
	}
}

func TestCloudEnrollmentListsProjectsAndEnrollsSelection(t *testing.T) {
	fx := newTestFixture(t)
	msg := loadCloudProjects(fx.store)()
	loaded, ok := msg.(cloudProjectsMsg)
	if !ok || loaded.err != nil {
		t.Fatalf("load projects = %T %+v", msg, loaded.err)
	}
	if len(loaded.projects) != 1 || loaded.projects[0].Name != "engram" || loaded.projects[0].Enrolled {
		t.Fatalf("project choices = %+v", loaded.projects)
	}

	m := NewWithDataDir(fx.store, "", fx.dataDir)
	m.Screen = ScreenCloudEnroll
	m.CloudProjects = loaded.projects
	updatedModel, cmd := m.handleCloudEnrollKeys("enter")
	updated := updatedModel.(Model)
	if cmd == nil || !updated.CloudLoading {
		t.Fatalf("enroll should start command: loading %t cmd nil %t", updated.CloudLoading, cmd == nil)
	}
	enrolled := cmd().(cloudProjectEnrolledMsg)
	if enrolled.err != nil {
		t.Fatalf("enroll project: %v", enrolled.err)
	}
	updatedModel, reload := updated.Update(enrolled)
	updated = updatedModel.(Model)
	if reload == nil || updated.CloudNotice == "" {
		t.Fatalf("enrollment result should reload with feedback: %+v", updated)
	}
	if ok, err := fx.store.IsProjectEnrolled("engram"); err != nil || !ok {
		t.Fatalf("project enrollment = %t, %v", ok, err)
	}
}

func TestCloudEnrollmentHandlesAlreadyEnrolledAndUnavailableStore(t *testing.T) {
	m := NewWithDataDir(nil, "", t.TempDir())
	m.Screen = ScreenCloudEnroll
	m.CloudProjects = []CloudProject{{Name: "engram", Enrolled: true}}
	updatedModel, cmd := m.handleCloudEnrollKeys("enter")
	updated := updatedModel.(Model)
	if cmd != nil || !strings.Contains(updated.CloudNotice, "already enrolled") {
		t.Fatalf("already-enrolled action = notice %q cmd nil %t", updated.CloudNotice, cmd == nil)
	}

	msg := loadCloudProjects(nil)().(cloudProjectsMsg)
	if msg.err == nil || !strings.Contains(msg.err.Error(), "store is unavailable") {
		t.Fatalf("nil-store project error = %v", msg.err)
	}
}

func TestCloudViewsExposeLoadingEmptyErrorAndReadyStates(t *testing.T) {
	m := NewWithDataDir(nil, "", t.TempDir())
	m.Screen = ScreenCloudSettings
	m.CloudLoading = true
	if out := m.viewCloudSettings(); !strings.Contains(out, "Loading cloud status") {
		t.Fatalf("loading view missing: %q", out)
	}

	m.CloudLoading = false
	m.CloudStatus = CloudStatus{Configured: false}
	if out := m.viewCloudSettings(); !strings.Contains(out, "Not configured") {
		t.Fatalf("unconfigured view missing: %q", out)
	}

	m.CloudStatus = CloudStatus{
		Configured:       true,
		ServerURL:        "https://cloud.example.test",
		AuthConfigured:   true,
		SyncLifecycle:    "blocked",
		ReasonCode:       constants.ReasonPaused,
		EnrolledProjects: []string{"engram"},
	}
	out := m.viewCloudSettings()
	for _, want := range []string{"https://cloud.example.test", "Auth ready", "1 enrolled", "blocked", constants.ReasonPaused} {
		if !strings.Contains(out, want) {
			t.Fatalf("ready view missing %q: %q", want, out)
		}
	}

	m.Screen = ScreenCloudEnroll
	m.CloudProjects = nil
	if out := m.viewCloudEnroll(); !strings.Contains(out, "No local projects") {
		t.Fatalf("empty enrollment view missing: %q", out)
	}
	m.ErrorMsg = "database unavailable"
	if out := m.View(); !strings.Contains(out, "database unavailable") {
		t.Fatalf("error view missing: %q", out)
	}
}

func TestCloudProjectListsScrollWithTerminalHeight(t *testing.T) {
	m := New(nil, "")
	m.Height = 15
	m.Screen = ScreenCloudEnroll
	for i := 0; i < 7; i++ {
		m.CloudProjects = append(m.CloudProjects, CloudProject{Name: "project-" + string(rune('a'+i))})
	}
	for i := 0; i < 4; i++ {
		updatedModel, _ := m.handleCloudEnrollKeys("down")
		m = updatedModel.(Model)
	}
	if m.CloudScroll == 0 {
		t.Fatal("moving beyond the enrollment viewport should scroll")
	}
	if out := m.viewCloudEnroll(); !strings.Contains(out, "showing 3-5 of 7 projects") {
		t.Fatalf("enrollment scroll cue missing: %q", out)
	}

	m.Screen = ScreenCloudStatus
	m.CloudScroll = 0
	m.CloudStatus.ProjectStates = nil
	for i := 0; i < 7; i++ {
		m.CloudStatus.ProjectStates = append(m.CloudStatus.ProjectStates, CloudProjectSyncStatus{Project: "project-" + string(rune('a'+i)), Lifecycle: "healthy"})
	}
	updatedModel, _ := m.handleCloudStatusKeys("down")
	m = updatedModel.(Model)
	if out := m.viewCloudStatus(); !strings.Contains(out, "showing 2-4 of 7 projects") {
		t.Fatalf("status scroll cue missing: %q", out)
	}
}

func TestCloudStatusRefreshAndResizeClampScroll(t *testing.T) {
	m := New(nil, "")
	m.Screen = ScreenCloudStatus
	m.Height = 15
	m.CloudScroll = 5
	updatedModel, _ := m.Update(cloudStatusMsg{status: CloudStatus{
		ProjectStates: []CloudProjectSyncStatus{{Project: "a"}, {Project: "b"}},
	}})
	m = updatedModel.(Model)
	if m.CloudScroll != 0 {
		t.Fatalf("refresh scroll = %d, want 0", m.CloudScroll)
	}

	m.CloudStatus.ProjectStates = []CloudProjectSyncStatus{{Project: "a"}, {Project: "b"}, {Project: "c"}, {Project: "d"}, {Project: "e"}}
	m.CloudScroll = 2
	updatedModel, _ = m.Update(tea.WindowSizeMsg{Width: 100, Height: 30})
	m = updatedModel.(Model)
	if m.CloudScroll != 0 {
		t.Fatalf("resize scroll = %d, want 0", m.CloudScroll)
	}
}

func keyMsg(key string) tea.KeyMsg {
	return newTeaKeyMsg(key)
}

// Aliases keep cloud flow tests focused on behavior rather than Bubble Tea key encoding.
func newTeaKeyMsg(key string) tea.KeyMsg {
	switch key {
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	default:
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(key)}
	}
}
