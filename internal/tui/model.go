// Package tui implements the Bubbletea terminal UI for Engram.
//
// Following the Gentleman Bubbletea patterns:
// - Screen constants as iota
// - Single Model struct holds ALL state
// - Update() with type switch
// - Per-screen key handlers returning (tea.Model, tea.Cmd)
// - Vim keys (j/k) for navigation
// - PrevScreen for back navigation
package tui

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/yersonargotev/engram/internal/cloud"
	"github.com/yersonargotev/engram/internal/cloud/constants"
	"github.com/yersonargotev/engram/internal/setup"
	"github.com/yersonargotev/engram/internal/store"
	"github.com/yersonargotev/engram/internal/version"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ─── Screens ─────────────────────────────────────────────────────────────────

type Screen int

const (
	ScreenDashboard Screen = iota
	ScreenSearch
	ScreenSearchResults
	ScreenRecent
	ScreenObservationDetail
	ScreenTimeline
	ScreenSessions
	ScreenSessionDetail
	ScreenSetup
	ScreenCloudSettings
	ScreenCloudConfigure
	ScreenCloudStatus
	ScreenCloudEnroll
)

type SessionDeleteState int

const (
	SessionDeleteStateNone SessionDeleteState = iota
	SessionDeleteStatePrompt
	SessionDeleteStateDeleting
)

// ─── Custom Messages ─────────────────────────────────────────────────────────

type updateCheckMsg struct {
	result version.CheckResult
}

type statsLoadedMsg struct {
	stats *store.Stats
	err   error
}

type searchResultsMsg struct {
	results []store.SearchResult
	query   string
	err     error
}

type recentObservationsMsg struct {
	observations []store.Observation
	err          error
}

type observationDetailMsg struct {
	observation *store.Observation
	err         error
}

type timelineMsg struct {
	timeline *store.TimelineResult
	err      error
}

type recentSessionsMsg struct {
	sessions []store.SessionSummary
	err      error
}

type sessionObservationsMsg struct {
	observations []store.Observation
	err          error
}

type sessionDeletedMsg struct {
	sessionID string
	err       error
}

type setupInstallMsg struct {
	result *setup.Result
	err    error
}

type CloudStatus struct {
	Configured       bool
	ServerURL        string
	AuthConfigured   bool
	SyncLifecycle    string
	ReasonCode       string
	ReasonMessage    string
	EnrolledProjects []string
	ProjectStates    []CloudProjectSyncStatus
}

type CloudProjectSyncStatus struct {
	Project       string
	Lifecycle     string
	ReasonCode    string
	ReasonMessage string
}

type CloudProject struct {
	Name     string
	Enrolled bool
}

type cloudStatusMsg struct {
	status CloudStatus
	err    error
}

type cloudProjectsMsg struct {
	projects []CloudProject
	err      error
}

type cloudConfiguredMsg struct {
	serverURL string
	err       error
}

type cloudProjectEnrolledMsg struct {
	project string
	err     error
}

// ─── Model ───────────────────────────────────────────────────────────────────

type Model struct {
	store      *store.Store
	Version    string
	Screen     Screen
	PrevScreen Screen
	Width      int
	Height     int
	Cursor     int
	Scroll     int

	// Update notification
	UpdateStatus version.CheckStatus
	UpdateMsg    string

	// Error display
	ErrorMsg string

	// Dashboard
	Stats *store.Stats

	// Search
	SearchInput   textinput.Model
	SearchQuery   string
	SearchResults []store.SearchResult

	// Recent observations
	RecentObservations []store.Observation

	// Observation detail
	SelectedObservation *store.Observation
	DetailScroll        int

	// Timeline
	Timeline *store.TimelineResult

	// Sessions
	Sessions             []store.SessionSummary
	SelectedSessionIdx   int
	SessionObservations  []store.Observation
	SessionDetailScroll  int
	SessionDeleteState   SessionDeleteState
	SessionDeleteID      string
	SessionDeleteProject string

	// Clipboard feedback
	CopyFeedback string // "✓ Copied!" or "" — shown for 2 s after copy

	// Setup
	SetupAgents           []setup.Agent
	SetupResult           *setup.Result
	SetupError            string
	SetupDone             bool
	SetupInstalling       bool
	SetupInstallingName   string // agent name being installed (for display)
	SetupAllowlistPrompt  bool   // true = showing y/n prompt for allowlist
	SetupAllowlistApplied bool   // true = allowlist was added successfully
	SetupAllowlistError   string // error message if allowlist injection failed
	SetupSpinner          spinner.Model

	// Cloud control center
	CloudDataDir     string
	CloudStatus      CloudStatus
	CloudProjects    []CloudProject
	CloudServerInput textinput.Model
	CloudLoading     bool
	CloudNotice      string
	CloudScroll      int
}

// New creates a new TUI model connected to the given store.
func New(s *store.Store, version string) Model {
	return NewWithDataDir(s, version, "")
}

// NewWithDataDir creates a model that can read and update the persisted cloud
// client configuration alongside the local store.
func NewWithDataDir(s *store.Store, version, dataDir string) Model {
	ti := textinput.New()
	ti.Placeholder = "Search memories..."
	ti.CharLimit = 256
	ti.Width = 60

	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(colorLavender)

	cloudInput := textinput.New()
	cloudInput.Placeholder = "https://cloud.example.com"
	cloudInput.CharLimit = 2048
	cloudInput.Width = 64

	return Model{
		store:            s,
		Version:          version,
		Screen:           ScreenDashboard,
		SearchInput:      ti,
		SetupSpinner:     sp,
		CloudDataDir:     strings.TrimSpace(dataDir),
		CloudServerInput: cloudInput,
	}
}

// Init loads initial data (stats for the dashboard).
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		loadStats(m.store),
		checkForUpdate(m.Version),
		tea.EnterAltScreen,
	)
}

// ─── Commands (data loading) ─────────────────────────────────────────────────

func checkForUpdate(v string) tea.Cmd {
	return func() tea.Msg {
		return updateCheckMsg{result: version.CheckLatest(v)}
	}
}

func loadStats(s *store.Store) tea.Cmd {
	return func() tea.Msg {
		stats, err := s.Stats()
		return statsLoadedMsg{stats: stats, err: err}
	}
}

func searchMemories(s *store.Store, query string) tea.Cmd {
	return func() tea.Msg {
		results, err := s.Search(query, store.SearchOptions{Limit: 50})
		return searchResultsMsg{results: results, query: query, err: err}
	}
}

func loadRecentObservations(s *store.Store) tea.Cmd {
	return func() tea.Msg {
		obs, err := s.AllObservations("", "", 50)
		return recentObservationsMsg{observations: obs, err: err}
	}
}

func loadObservationDetail(s *store.Store, id int64) tea.Cmd {
	return func() tea.Msg {
		obs, err := s.GetObservation(id)
		return observationDetailMsg{observation: obs, err: err}
	}
}

func loadTimeline(s *store.Store, obsID int64) tea.Cmd {
	return func() tea.Msg {
		tl, err := s.Timeline(obsID, 10, 10)
		return timelineMsg{timeline: tl, err: err}
	}
}

func loadRecentSessions(s *store.Store) tea.Cmd {
	return func() tea.Msg {
		sessions, err := s.AllSessions("", 50)
		return recentSessionsMsg{sessions: sessions, err: err}
	}
}

func loadSessionObservations(s *store.Store, sessionID string) tea.Cmd {
	return func() tea.Msg {
		obs, err := s.SessionObservations(sessionID, 200)
		return sessionObservationsMsg{observations: obs, err: err}
	}
}

func deleteSession(s *store.Store, sessionID string) tea.Cmd {
	return func() tea.Msg {
		if s == nil {
			return sessionDeletedMsg{sessionID: sessionID, err: errors.New("store is unavailable")}
		}
		err := s.DeleteSession(sessionID)
		return sessionDeletedMsg{sessionID: sessionID, err: err}
	}
}

func installAgent(agentName string) tea.Cmd {
	return func() tea.Msg {
		result, err := installAgentFn(agentName)
		return setupInstallMsg{result: result, err: err}
	}
}

func loadCloudStatus(s *store.Store, dataDir string) tea.Cmd {
	return func() tea.Msg {
		cfg, err := cloud.ResolveClientConfig(dataDir)
		if err != nil {
			return cloudStatusMsg{err: err}
		}
		status := CloudStatus{}
		if cfg != nil {
			status.ServerURL = strings.TrimSpace(cfg.ServerURL)
			if status.ServerURL != "" {
				validatedURL, err := cloud.ValidateServerURL(status.ServerURL)
				if err != nil {
					return cloudStatusMsg{err: fmt.Errorf("invalid cloud server URL: %w", err)}
				}
				status.ServerURL = validatedURL
				status.Configured = true
			}
			status.AuthConfigured = strings.TrimSpace(cfg.Token) != ""
		}
		if s == nil {
			return cloudStatusMsg{status: status}
		}
		enrolled, err := s.ListEnrolledProjects()
		if err != nil {
			return cloudStatusMsg{err: err}
		}
		for _, project := range enrolled {
			status.EnrolledProjects = append(status.EnrolledProjects, project.Project)
		}
		globalState, err := s.GetSyncState(constants.TargetKeyCloud)
		if err != nil {
			return cloudStatusMsg{err: err}
		}
		applySyncState(&status, globalState)
		if len(enrolled) > 0 {
			for _, project := range enrolled {
				targetKey := fmt.Sprintf("%s:%s", constants.TargetKeyCloud, project.Project)
				syncState, err := s.GetSyncState(targetKey)
				if err != nil {
					return cloudStatusMsg{err: err}
				}
				projectState := cloudProjectSyncStatus(project.Project, syncState)
				status.ProjectStates = append(status.ProjectStates, projectState)
				projectPriority := syncLifecyclePriority(projectState.Lifecycle)
				currentPriority := syncLifecyclePriority(status.SyncLifecycle)
				if projectPriority > currentPriority || (projectPriority == currentPriority && status.ReasonCode == "" && projectState.ReasonCode != "") {
					status.SyncLifecycle = projectState.Lifecycle
					status.ReasonCode = projectState.ReasonCode
					status.ReasonMessage = projectState.ReasonMessage
				}
			}
		}
		return cloudStatusMsg{status: status}
	}
}

func applySyncState(status *CloudStatus, state *store.SyncState) {
	if status == nil || state == nil {
		return
	}
	details := cloudSyncStateDetailsFrom(state)
	status.SyncLifecycle = details.lifecycle
	status.ReasonCode = details.reasonCode
	status.ReasonMessage = details.reasonMessage
}

func cloudProjectSyncStatus(project string, state *store.SyncState) CloudProjectSyncStatus {
	details := cloudSyncStateDetailsFrom(state)
	return CloudProjectSyncStatus{
		Project:       project,
		Lifecycle:     details.lifecycle,
		ReasonCode:    details.reasonCode,
		ReasonMessage: details.reasonMessage,
	}
}

type cloudSyncStateDetails struct {
	lifecycle     string
	reasonCode    string
	reasonMessage string
}

func cloudSyncStateDetailsFrom(state *store.SyncState) cloudSyncStateDetails {
	if state == nil {
		return cloudSyncStateDetails{}
	}
	details := cloudSyncStateDetails{lifecycle: state.Lifecycle}
	if state.ReasonCode != nil {
		details.reasonCode = strings.TrimSpace(*state.ReasonCode)
	}
	if state.ReasonMessage != nil {
		details.reasonMessage = strings.TrimSpace(*state.ReasonMessage)
	}
	return details
}

func syncLifecyclePriority(lifecycle string) int {
	switch lifecycle {
	case store.SyncLifecycleDegraded:
		return 5
	case store.SyncLifecycleRunning:
		return 4
	case store.SyncLifecyclePending:
		return 3
	case store.SyncLifecycleHealthy:
		return 2
	case store.SyncLifecycleIdle:
		return 1
	default:
		return 0
	}
}

func configureCloudServer(dataDir, rawURL string) tea.Cmd {
	return func() tea.Msg {
		cfg, err := cloud.SaveClientServer(dataDir, rawURL)
		if err != nil {
			return cloudConfiguredMsg{err: err}
		}
		return cloudConfiguredMsg{serverURL: cfg.ServerURL}
	}
}

func loadCloudProjects(s *store.Store) tea.Cmd {
	return func() tea.Msg {
		if s == nil {
			return cloudProjectsMsg{err: errors.New("store is unavailable")}
		}
		projectStats, err := s.ListProjectsWithStats()
		if err != nil {
			return cloudProjectsMsg{err: err}
		}
		enrolled, err := s.ListEnrolledProjects()
		if err != nil {
			return cloudProjectsMsg{err: err}
		}
		enrolledSet := make(map[string]bool, len(enrolled))
		names := make(map[string]struct{}, len(projectStats)+len(enrolled))
		for _, project := range enrolled {
			enrolledSet[project.Project] = true
			names[project.Project] = struct{}{}
		}
		for _, project := range projectStats {
			names[project.Name] = struct{}{}
		}
		projects := make([]CloudProject, 0, len(names))
		for name := range names {
			projects = append(projects, CloudProject{Name: name, Enrolled: enrolledSet[name]})
		}
		sort.Slice(projects, func(i, j int) bool { return projects[i].Name < projects[j].Name })
		return cloudProjectsMsg{projects: projects}
	}
}

func enrollCloudProject(s *store.Store, project string) tea.Cmd {
	return func() tea.Msg {
		if s == nil {
			return cloudProjectEnrolledMsg{project: project, err: errors.New("store is unavailable")}
		}
		err := s.EnrollProject(project)
		return cloudProjectEnrolledMsg{project: project, err: err}
	}
}

var installAgentFn = setup.Install
var addClaudeCodeAllowlistFn = setup.AddClaudeCodeAllowlist
