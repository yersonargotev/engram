// Engram — Persistent memory for AI coding agents.
//
// Usage:
//
//	engram serve          Start HTTP + MCP server
//	engram mcp            Start MCP server only (stdio transport)
//	engram search <query> Search memories from CLI
//	engram save           Save a memory from CLI
//	engram context        Show recent context
//	engram stats          Show memory stats
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yersonargotev/engram/internal/cloud"
	"github.com/yersonargotev/engram/internal/cloud/autosync"
	"github.com/yersonargotev/engram/internal/cloud/constants"
	"github.com/yersonargotev/engram/internal/cloud/remote"
	"github.com/yersonargotev/engram/internal/cloud/syncguidance"
	"github.com/yersonargotev/engram/internal/diagnostic"
	"github.com/yersonargotev/engram/internal/mcp"
	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/obsidian"
	projectpkg "github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/recallbaseline"
	"github.com/yersonargotev/engram/internal/server"
	"github.com/yersonargotev/engram/internal/setup"
	"github.com/yersonargotev/engram/internal/store"
	engramsync "github.com/yersonargotev/engram/internal/sync"
	"github.com/yersonargotev/engram/internal/timeutil"
	"github.com/yersonargotev/engram/internal/tui"
	versioncheck "github.com/yersonargotev/engram/internal/version"

	tea "github.com/charmbracelet/bubbletea"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

// version is set via ldflags at build time by goreleaser.
// Falls back to "dev" for local builds; init() tries Go module info first.
var version = "dev"
var commit = "dev"

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		if version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			version = strings.TrimPrefix(info.Main.Version, "v")
		}
		if commit == "dev" {
			for _, setting := range info.Settings {
				if setting.Key == "vcs.revision" && setting.Value != "" {
					commit = setting.Value
					break
				}
			}
		}
	}
}

var (
	storeNew      = store.New
	newHTTPServer = server.New
	startHTTP     = (*server.Server).Start

	newMCPServer           = mcp.NewServer
	newMCPServerWithTools  = mcp.NewServerWithTools
	newMCPServerWithConfig = mcp.NewServerWithConfig
	resolveMCPTools        = mcp.ResolveTools
	serveMCP               = mcpserver.ServeStdio

	// detectProject is injectable for testing; wraps project.DetectProject.
	detectProject = projectpkg.DetectProject
	// detectProjectFull is injectable for commands that require unambiguous identity.
	detectProjectFull = projectpkg.DetectProjectFull

	newTUIModel = func(s *store.Store, dataDir string) tui.Model {
		return tui.NewWithProjectResolver(s, version, dataDir, func() (string, error) {
			result := detectProjectFull(currentCWD())
			if result.Error == nil {
				return result.Project, nil
			}
			message := result.Error.Error()
			if len(result.AvailableProjects) > 0 {
				message += "; open one project directory: " + strings.Join(result.AvailableProjects, ", ")
			}
			return "", errors.New(message)
		})
	}
	newTeaProgram = tea.NewProgram
	runTeaProgram = (*tea.Program).Run

	checkForUpdates = versioncheck.CheckLatest

	setupSupportedAgents        = setup.SupportedAgents
	setupInstallAgent           = setup.InstallWithOptions
	setupInspectCodexStatus     = setup.InspectCodexStatusWithRevision
	setupAddClaudeCodeAllowlist = setup.AddClaudeCodeAllowlist
	scanInputLine               = fmt.Scanln

	storeDeleteObservation = func(s *store.Store, id int64, hard bool) error { return s.DeleteObservation(id, hard) }
	storeDeleteSession     = func(s *store.Store, id string) error { return s.DeleteSession(id) }
	storeDeleteProject     = func(s *store.Store, name string, hard bool) (*store.DeleteProjectResult, error) {
		return s.DeleteProject(name, hard)
	}
	storePruneProject = func(s *store.Store, name string) (*store.PruneResult, error) { return s.PruneProject(name) }
	storeTimeline     = func(s *store.Store, observationID int64, before, after int) (*store.TimelineResult, error) {
		return s.Timeline(observationID, before, after)
	}
	storeFormatContext = func(s *store.Store, project, scope string) (string, error) { return s.FormatContext(project, scope) }
	storeStats         = func(s *store.Store) (*store.Stats, error) { return s.Stats() }
	storeExport        = func(s *store.Store) (*store.ExportData, error) { return s.Export() }
	jsonMarshalIndent  = json.MarshalIndent
	runDiagnostics     = func(ctx context.Context, s *store.Store, project, check string) (diagnostic.Report, error) {
		runner := diagnostic.NewRunner()
		scope := diagnostic.Scope{Store: s, Project: project, Now: time.Now()}
		if strings.TrimSpace(check) != "" {
			return runner.RunOne(ctx, scope, check)
		}
		return runner.RunAll(ctx, scope)
	}

	syncStatus = func(sy *engramsync.Syncer) (localChunks int, remoteChunks int, pendingImport int, err error) {
		return sy.Status()
	}
	syncImport = func(sy *engramsync.Syncer) (*engramsync.ImportResult, error) { return sy.Import() }
	syncExport = func(sy *engramsync.Syncer, createdBy, project string) (*engramsync.SyncResult, error) {
		return sy.Export(createdBy, project)
	}
	newCloudAutosyncManager = func(s *store.Store, _ any) cloudAutosyncManager {
		mgr := autosync.New(s, nil, autosync.DefaultConfig())
		return autosyncManagerAdapter{manager: mgr}
	}

	// newAutosyncManager is the injectable factory used by tryStartAutosync.
	// BR2-3: Returns startableAutosyncManager (not *autosync.Manager) so tests can
	// inject a deterministic fake — preventing racy wg.Add/wg.Wait interleaving.
	newAutosyncManager = func(s *store.Store, transport autosync.CloudTransport, cfg autosync.Config) startableAutosyncManager {
		return autosync.New(s, transport, cfg)
	}

	processExit = os.Exit
	exitFunc    = exitWithRecallBaseline

	stdinScanner = func() *bufio.Scanner { return bufio.NewScanner(os.Stdin) }
	userHomeDir  = os.UserHomeDir

	// newObsidianExporter is injectable for testing.
	newObsidianExporter = obsidian.NewExporter

	// newObsidianWatcher is injectable for testing.
	newObsidianWatcher = obsidian.NewWatcher

	// agentRunnerFactory is injectable for testing. In production it delegates to
	// llm.NewRunner; tests substitute a fake to avoid real CLI invocations.
	agentRunnerFactory = defaultAgentRunnerFactory
)

type cloudSyncStatus struct {
	Phase               string
	LastError           string
	ConsecutiveFailures int
	BackoffUntil        *time.Time
	LastSyncAt          *time.Time
	ReasonCode          string
	ReasonMessage       string
}

type cloudAutosyncManager interface {
	Run(context.Context)
	NotifyDirty()
	Status() cloudSyncStatus
}

// startableAutosyncManager is the interface implemented by *autosync.Manager and used
// by tryStartAutosync. It combines autosyncStatusProvider with Run and Stop so that
// the factory variable newAutosyncManager can be stubbed in tests without spawning
// real goroutines — eliminating the racy wg.Add/wg.Wait interleaving.
// BR2-3: Using an interface return type (not *autosync.Manager) makes the factory
// injectable with deterministic fakes.
type startableAutosyncManager interface {
	autosyncStatusProvider // Status() autosync.Status
	Run(context.Context)
	Stop()
}

type autosyncManagerAdapter struct {
	manager *autosync.Manager
}

func (a autosyncManagerAdapter) Run(ctx context.Context) {
	a.manager.Run(ctx)
}

func (a autosyncManagerAdapter) NotifyDirty() {
	a.manager.NotifyDirty()
}

func (a autosyncManagerAdapter) Status() cloudSyncStatus {
	status := a.manager.Status()
	return cloudSyncStatus{
		Phase:               status.Phase,
		LastError:           status.LastError,
		ConsecutiveFailures: status.ConsecutiveFailures,
		BackoffUntil:        status.BackoffUntil,
		LastSyncAt:          status.LastSyncAt,
		ReasonCode:          status.ReasonCode,
		ReasonMessage:       status.ReasonMessage,
	}
}

// mutationTransportAdapter adapts remote.MutationTransport to autosync.CloudTransport.
// This bridges the type gap between packages without creating a circular import.
type mutationTransportAdapter struct {
	remote *remote.MutationTransport
}

func (a *mutationTransportAdapter) PushMutations(entries []autosync.MutationEntry) (*autosync.PushMutationsResult, error) {
	remoteEntries := make([]remote.MutationEntry, len(entries))
	for i, e := range entries {
		remoteEntries[i] = remote.MutationEntry{
			Project:   e.Project,
			Entity:    e.Entity,
			EntityKey: e.EntityKey,
			Op:        e.Op,
			Payload:   e.Payload,
		}
	}
	seqs, err := a.remote.PushMutations(remoteEntries)
	if err != nil {
		return nil, err
	}
	return &autosync.PushMutationsResult{AcceptedSeqs: seqs}, nil
}

func (a *mutationTransportAdapter) PullMutations(sinceSeq int64, limit int) (*autosync.PullMutationsResponse, error) {
	resp, err := a.remote.PullMutations(sinceSeq, limit)
	if err != nil {
		return nil, err
	}
	mutations := make([]autosync.PulledMutation, len(resp.Mutations))
	for i, m := range resp.Mutations {
		mutations[i] = autosync.PulledMutation{
			Seq:        m.Seq,
			Project:    m.Project,
			Entity:     m.Entity,
			EntityKey:  m.EntityKey,
			Op:         m.Op,
			Payload:    m.Payload,
			OccurredAt: m.OccurredAt,
		}
	}
	return &autosync.PullMutationsResponse{
		Mutations: mutations,
		HasMore:   resp.HasMore,
		LatestSeq: resp.LatestSeq,
	}, nil
}

type storeSyncStatusProvider struct {
	store          *store.Store
	defaultProject string
	cfg            store.Config
}

func (p storeSyncStatusProvider) Status(project string) server.SyncStatus {
	resolvedProject, _ := store.NormalizeProject(project)
	resolvedProject = strings.TrimSpace(resolvedProject)
	if resolvedProject == "" {
		resolvedProject, _ = store.NormalizeProject(p.defaultProject)
		resolvedProject = strings.TrimSpace(resolvedProject)
	}
	upgradeStage, upgradeCode, upgradeMessage := p.upgradeStatus(resolvedProject)
	enabled, disabledCode, disabledMessage := p.cloudSyncEnabled(resolvedProject)
	targetKey := cloudTargetKeyForProject(resolvedProject)
	if !enabled {
		if disabledCode == "cloud_not_configured" && resolvedProject != "" {
			enrolled, err := p.store.IsProjectEnrolled(resolvedProject)
			if err != nil {
				return server.SyncStatus{
					Enabled:              false,
					Phase:                store.SyncLifecycleIdle,
					ReasonCode:           "status_unavailable",
					ReasonMessage:        fmt.Sprintf("cloud enrollment status is unavailable: %v", err),
					UpgradeStage:         upgradeStage,
					UpgradeReasonCode:    upgradeCode,
					UpgradeReasonMessage: upgradeMessage,
				}
			}
			if !enrolled {
				return server.SyncStatus{
					Enabled:              false,
					Phase:                store.SyncLifecycleIdle,
					ReasonCode:           constants.ReasonBlockedUnenrolled,
					ReasonMessage:        fmt.Sprintf("project %q is not enrolled for cloud sync", resolvedProject),
					UpgradeStage:         upgradeStage,
					UpgradeReasonCode:    upgradeCode,
					UpgradeReasonMessage: upgradeMessage,
				}
			}
			state, err := p.store.GetSyncState(targetKey)
			if err == nil && hasMeaningfulSyncState(state) {
				status := syncStatusFromState(state)
				status.Enabled = true
				status.UpgradeStage = upgradeStage
				status.UpgradeReasonCode = upgradeCode
				status.UpgradeReasonMessage = upgradeMessage
				return status
			}
		}
		return server.SyncStatus{
			Enabled:              false,
			Phase:                store.SyncLifecycleIdle,
			ReasonCode:           disabledCode,
			ReasonMessage:        disabledMessage,
			UpgradeStage:         upgradeStage,
			UpgradeReasonCode:    upgradeCode,
			UpgradeReasonMessage: upgradeMessage,
		}
	}
	state, err := p.store.GetSyncState(targetKey)
	if err != nil {
		reason := "sync state is unavailable"
		lastErr := fmt.Sprintf("read sync state: %v", err)
		return server.SyncStatus{
			Enabled:              true,
			Phase:                store.SyncLifecycleDegraded,
			ReasonCode:           "status_unavailable",
			ReasonMessage:        reason,
			LastError:            lastErr,
			UpgradeStage:         upgradeStage,
			UpgradeReasonCode:    upgradeCode,
			UpgradeReasonMessage: upgradeMessage,
		}
	}
	status := syncStatusFromState(state)
	status.Enabled = true
	status.UpgradeStage = upgradeStage
	status.UpgradeReasonCode = upgradeCode
	status.UpgradeReasonMessage = upgradeMessage
	return status
}

func (p storeSyncStatusProvider) upgradeStatus(project string) (string, string, string) {
	project = strings.TrimSpace(project)
	if project == "" {
		return "", "", ""
	}
	state, err := p.store.GetCloudUpgradeState(project)
	if err != nil {
		return "", "upgrade_status_unavailable", fmt.Sprintf("cloud upgrade status is unavailable: %v", err)
	}
	if state == nil {
		return "", "", ""
	}
	return state.Stage, strings.TrimSpace(state.LastErrorCode), strings.TrimSpace(state.LastErrorMessage)
}

func (p storeSyncStatusProvider) cloudSyncEnabled(project string) (bool, string, string) {
	cc, err := resolveCloudRuntimeConfig(p.cfg)
	if err != nil {
		return false, "cloud_config_error", fmt.Sprintf("cloud config error: %v", err)
	}
	if cc == nil || strings.TrimSpace(cc.ServerURL) == "" {
		return false, "cloud_not_configured", "cloud sync is not configured"
	}
	if _, err := validateCloudServerURL(cc.ServerURL); err != nil {
		return false, "cloud_config_error", fmt.Sprintf("cloud config error: invalid cloud runtime server URL: %v", err)
	}
	if strings.TrimSpace(project) == "" {
		return false, "project_required", "cloud sync status requires an explicit project scope"
	}
	enrolled, err := p.store.IsProjectEnrolled(project)
	if err != nil {
		return false, "status_unavailable", fmt.Sprintf("cloud enrollment status is unavailable: %v", err)
	}
	if !enrolled {
		return false, constants.ReasonBlockedUnenrolled, fmt.Sprintf("project %q is not enrolled for cloud sync", project)
	}
	return true, "", ""
}

func syncStatusFromState(state *store.SyncState) server.SyncStatus {
	var lastSyncAt *time.Time
	if state != nil && state.Lifecycle == store.SyncLifecycleHealthy {
		lastSyncAt = parseSyncStateTimestamp(state.UpdatedAt)
	}
	return server.SyncStatus{
		Phase:               state.Lifecycle,
		LastError:           derefString(state.LastError),
		ConsecutiveFailures: state.ConsecutiveFailures,
		BackoffUntil:        parseRFC3339Ptr(state.BackoffUntil),
		LastSyncAt:          lastSyncAt,
		ReasonCode:          derefString(state.ReasonCode),
		ReasonMessage:       derefString(state.ReasonMessage),
	}
}

func hasMeaningfulSyncState(state *store.SyncState) bool {
	if state == nil {
		return false
	}
	if state.Lifecycle != "" && state.Lifecycle != store.SyncLifecycleIdle {
		return true
	}
	if state.LastEnqueuedSeq > 0 || state.LastAckedSeq > 0 || state.LastPulledSeq > 0 {
		return true
	}
	if state.ConsecutiveFailures > 0 {
		return true
	}
	if state.BackoffUntil != nil || state.LeaseOwner != nil || state.LeaseUntil != nil {
		return true
	}
	if state.ReasonCode != nil || state.ReasonMessage != nil || state.LastError != nil {
		return true
	}
	return false
}

func parseSyncStateTimestamp(value string) *time.Time {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return &parsed
	}
	if parsed, err := time.ParseInLocation("2006-01-02 15:04:05", trimmed, time.UTC); err == nil {
		return &parsed
	}
	return nil
}

func parseRFC3339Ptr(value *string) *time.Time {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, *value)
	if err != nil {
		return nil
	}
	return &parsed
}

func derefString(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return *ptr
}

func envBool(key string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func resolveCloudRuntimeConfig(cfg store.Config) (*cloudConfig, error) {
	cc, err := cloud.ResolveClientConfig(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("read cloud config: %w", err)
	}
	if cc == nil {
		cc = &cloudConfig{}
	}
	return cc, nil
}

func preflightCloudSync(s *store.Store, cfg store.Config, project string, mutateState bool) (*cloudConfig, error) {
	project = strings.TrimSpace(project)
	if project != "" {
		project, _ = store.NormalizeProject(project)
	}
	targetKey := cloudTargetKeyForProject(project)

	cc, err := resolveCloudRuntimeConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("cloud sync config error: %w", err)
	}
	hasServer := strings.TrimSpace(cc.ServerURL) != ""
	if !hasServer {
		message := "cloud server is missing: configure server URL with `engram cloud config --server <url>`"
		if mutateState {
			_ = s.MarkSyncBlocked(targetKey, constants.ReasonCloudConfigError, message)
		}
		return nil, fmt.Errorf("cloud sync %s: %s", constants.ReasonCloudConfigError, message)
	}
	if _, err := validateCloudServerURL(cc.ServerURL); err != nil {
		message := fmt.Sprintf("invalid cloud runtime server URL: %v", err)
		if mutateState {
			_ = s.MarkSyncBlocked(targetKey, constants.ReasonCloudConfigError, message)
		}
		return nil, fmt.Errorf("cloud sync %s: %s", constants.ReasonCloudConfigError, message)
	}
	if project != "" {
		enrolled, err := s.IsProjectEnrolled(project)
		if err != nil {
			return nil, fmt.Errorf("cloud sync enrollment check: %w", err)
		}
		if !enrolled {
			message := fmt.Sprintf("project %q is not enrolled for cloud sync", project)
			if mutateState {
				_ = s.MarkSyncBlocked(targetKey, constants.ReasonBlockedUnenrolled, message)
			}
			return nil, fmt.Errorf("cloud sync blocked_unenrolled: %s", message)
		}
		if err := preflightCloudSyncLegacyMutations(s, project, targetKey, mutateState); err != nil {
			return nil, err
		}
	}
	return cc, nil
}

func preflightCloudSyncLegacyMutations(s *store.Store, project, targetKey string, mutateState bool) error {
	report, err := s.DiagnoseCloudUpgradeLegacyMutations(project)
	if err != nil {
		return fmt.Errorf("cloud sync legacy mutation preflight: %w", err)
	}
	if report.BlockedCount == 0 && report.RepairableCount == 0 {
		return nil
	}

	reasonCode := store.UpgradeReasonRepairableLegacyMutationPayload
	message := fmt.Sprintf(
		"legacy mutation payloads require repair before cloud sync for project %q: run `engram cloud upgrade doctor --project %s` then `engram cloud upgrade repair --project %s --apply`",
		project, project, project,
	)
	if report.BlockedCount > 0 {
		reasonCode = store.UpgradeReasonBlockedLegacyMutationManual
		first := firstBlockedLegacyMutationFinding(report)
		message = fmt.Sprintf(
			"legacy mutation payloads require manual action before cloud sync for project %q (seq=%d entity=%s op=%s): %s; inspect with `engram cloud upgrade doctor --project %s` and run `engram cloud upgrade repair --project %s --apply` for deterministic repairs",
			project, first.Seq, first.Entity, first.Op, first.Message, project, project,
		)
	}
	if mutateState {
		_ = s.MarkSyncBlocked(targetKey, reasonCode, message)
	}
	return fmt.Errorf("cloud sync %s: %s", reasonCode, message)
}

func firstBlockedLegacyMutationFinding(report store.CloudUpgradeLegacyMutationReport) store.CloudUpgradeLegacyMutationFinding {
	for _, finding := range report.Findings {
		if !finding.Repairable {
			return finding
		}
	}
	if len(report.Findings) > 0 {
		return report.Findings[0]
	}
	return store.CloudUpgradeLegacyMutationFinding{}
}

func cloudTargetKeyForProject(project string) string {
	project = strings.TrimSpace(project)
	if project == "" {
		return constants.TargetKeyCloud
	}
	project, _ = store.NormalizeProject(project)
	if strings.TrimSpace(project) == "" {
		return constants.TargetKeyCloud
	}
	return fmt.Sprintf("%s:%s", constants.TargetKeyCloud, project)
}

func markCloudSyncFailure(s *store.Store, targetKey string, syncErr error) {
	if syncErr == nil {
		return
	}
	message := cloudSyncFailureMessage(syncguidance.ProjectFromTargetKey(targetKey), syncErr)
	var statusErr *remote.HTTPStatusError
	if errors.As(syncErr, &statusErr) {
		switch {
		case statusErr.IsAuthFailure():
			_ = s.MarkSyncAuthRequired(targetKey, message)
			return
		case statusErr.IsPolicyFailure():
			_ = s.MarkSyncBlocked(targetKey, constants.ReasonPolicyForbidden, message)
			return
		}
	}
	_ = s.MarkSyncFailure(targetKey, message, time.Now().UTC().Add(30*time.Second))
}

func cloudSyncFailureMessage(project string, syncErr error) string {
	if syncErr == nil {
		return ""
	}
	return syncguidance.AppendGuidance(syncErr.Error(), project, syncErr)
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		exitFunc(1)
	}
	// Self-tests must run before update checks, configuration resolution, orphan
	// migration, and autosync setup so released binaries cannot touch user data.
	if strings.EqualFold(strings.TrimSpace(os.Args[1]), "test") {
		if code := cmdTest(os.Args[2:]); code != testExitSuccess {
			exitFunc(code)
		}
		return
	}

	if shouldCheckForUpdates(os.Args[1:]) {
		printUpdateCheckResult(checkForUpdates(version))
	}
	if handleConfigFreeCommand(os.Args[1:]) {
		return
	}

	cfg, cfgErr := store.DefaultConfig()
	if cfgErr != nil {
		// Fallback: try to resolve home directory from environment variables
		// that os.UserHomeDir() might have missed (e.g. MCP subprocesses on
		// Windows where %USERPROFILE% is not propagated).
		if home := resolveHomeFallback(); home != "" {
			log.Printf("[engram] UserHomeDir failed, using fallback: %s", home)
			cfg = store.FallbackConfig(filepath.Join(home, ".engram"))
		} else {
			fatal(cfgErr)
		}
	}

	// Allow overriding data dir via env
	if dir := os.Getenv("ENGRAM_DATA_DIR"); dir != "" {
		cfg.DataDir = dir
	}

	// Migrate orphaned databases that ended up in wrong locations
	// (e.g. drive root on Windows due to previous bug).
	migrateOrphanedDB(cfg.DataDir)

	switch os.Args[1] {
	case "serve":
		runRecallBaselineCLI(cfg, "serve", func() { cmdServe(cfg) })
	case "mcp":
		runRecallBaselineCLI(cfg, "mcp", func() { cmdMCP(cfg) })
	case "tui":
		runRecallBaselineCLI(cfg, "tui", func() { cmdTUI(cfg) })
	case "search":
		cmdSearch(cfg)
	case "save":
		runRecallBaselineCLI(cfg, "save", func() { cmdSave(cfg) })
	case "get":
		cmdGet(cfg)
	case "update":
		runRecallBaselineCLI(cfg, "update", func() { cmdUpdate(cfg) })
	case "review":
		runRecallBaselineCLI(cfg, "review", func() { cmdReview(cfg) })
	case "pin":
		runRecallBaselineCLI(cfg, "pin", func() { cmdPin(cfg, true) })
	case "unpin":
		runRecallBaselineCLI(cfg, "unpin", func() { cmdPin(cfg, false) })
	case "current-project":
		runRecallBaselineCLI(cfg, "current_project", cmdCurrentProject)
	case "suggest-topic-key":
		runRecallBaselineCLI(cfg, "suggest_topic_key", cmdSuggestTopicKey)
	case "delete":
		runRecallBaselineCLI(cfg, "delete", func() { cmdDelete(cfg) })
	case "timeline":
		runRecallBaselineCLI(cfg, "timeline", func() { cmdTimeline(cfg) })
	case "conflicts":
		runRecallBaselineCLI(cfg, "conflicts", func() { cmdConflicts(cfg) })
	case "doctor":
		runRecallBaselineCLI(cfg, "doctor", func() { cmdDoctor(cfg) })
	case "context":
		cmdContext(cfg)
	case "checkpoint":
		if len(os.Args) >= 3 && strings.EqualFold(strings.TrimSpace(os.Args[2]), "verify-stop") {
			cmdCheckpoint(cfg)
			break
		}
		operation := "checkpoint_status"
		if len(os.Args) >= 3 && strings.EqualFold(strings.TrimSpace(os.Args[2]), "record") {
			operation = "checkpoint_record"
		}
		runRecallBaselineCLI(cfg, operation, func() { cmdCheckpoint(cfg) })
	case "capture":
		if len(os.Args) >= 3 && (strings.EqualFold(strings.TrimSpace(os.Args[2]), "subagent-hook") ||
			strings.EqualFold(strings.TrimSpace(os.Args[2]), "prompt-hook") ||
			strings.EqualFold(strings.TrimSpace(os.Args[2]), "prompt-persist")) {
			cmdCapture(cfg)
			break
		}
		operation := "capture"
		if len(os.Args) >= 3 {
			operation += "_" + strings.ToLower(strings.TrimSpace(os.Args[2]))
		}
		runRecallBaselineCLI(cfg, operation, func() { cmdCapture(cfg) })
	case "lifecycle":
		cmdLifecycle(cfg)
	case "legacy-prompts":
		runRecallBaselineCLI(cfg, "legacy_prompts", func() { cmdLegacyPrompts(cfg) })
	case "stats":
		runRecallBaselineCLI(cfg, "stats", func() { cmdStats(cfg) })
	case "export":
		runRecallBaselineCLI(cfg, "export", func() { cmdExport(cfg) })
	case "import":
		runRecallBaselineCLI(cfg, "import", func() { cmdImport(cfg) })
	case "sync":
		runRecallBaselineCLI(cfg, "sync", func() { cmdSync(cfg) })
	case "cloud":
		runRecallBaselineCLI(cfg, "cloud", func() { cmdCloud(cfg) })
	case "obsidian-export":
		runRecallBaselineCLI(cfg, "obsidian_export", func() { cmdObsidianExport(cfg) })
	case "projects":
		runRecallBaselineCLI(cfg, "projects", func() { cmdProjects(cfg) })
	case "setup":
		runRecallBaselineCLI(cfg, "setup", func() { cmdSetup(cfg) })
	case "protocol-mode":
		runRecallBaselineCLI(cfg, "protocol_mode", func() { cmdProtocolMode(cfg) })
	case "activation-study":
		cmdActivationStudy()
	case "recall-study":
		cmdRecallStudy()
	case "recall-baseline":
		cmdRecallBaseline(cfg)
	case "recall-feedback":
		runRecallBaselineCLI(cfg, "recall_feedback_report", func() { cmdRecallFeedback(cfg) })
	case "version", "--version", "-v":
		fmt.Printf("engram %s\n", version)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		printUsage()
		exitFunc(1)
	}
}

func shouldCheckForUpdates(args []string) bool {
	if len(args) == 0 {
		return false
	}
	if isCodexSetupStatus(args) {
		return false
	}
	for _, arg := range args {
		if arg == "--json" {
			return false
		}
	}
	command := strings.ToLower(strings.TrimSpace(args[0]))
	switch command {
	case "mcp", "serve", "protocol-mode", "activation-study", "recall-study", "recall-baseline", "lifecycle":
		return false
	case "capture":
		if len(args) < 2 {
			return true
		}
		subcommand := strings.ToLower(strings.TrimSpace(args[1]))
		return subcommand != "subagent-hook" && subcommand != "prompt-hook" && subcommand != "prompt-persist"
	case "checkpoint":
		return len(args) >= 2 && strings.ToLower(strings.TrimSpace(args[1])) != "verify-stop"
	case "cloud":
		return len(args) < 2 || strings.ToLower(strings.TrimSpace(args[1])) != "serve"
	}
	return true
}

func handleConfigFreeCommand(args []string) bool {
	if len(args) == 0 {
		return false
	}
	if isCodexSetupStatus(args) {
		cmdSetup(store.Config{})
		return true
	}
	switch strings.ToLower(strings.TrimSpace(args[0])) {
	case "activation-study":
		cmdActivationStudy()
		return true
	case "recall-study":
		cmdRecallStudy()
		return true
	case "recall-baseline":
		if len(args) >= 2 {
			subcommand := strings.ToLower(strings.TrimSpace(args[1]))
			if subcommand == "power" || subcommand == "help" || subcommand == "--help" || subcommand == "-h" {
				cmdRecallBaseline(store.Config{})
				return true
			}
		}
	case "recall-feedback":
		if len(args) >= 2 && (args[1] == "help" || args[1] == "--help" || args[1] == "-h") {
			printRecallFeedbackUsage()
			return true
		}
	case "version", "--version", "-v":
		fmt.Printf("engram %s\n", version)
		return true
	case "help", "--help", "-h":
		printUsage()
		return true
	case "save":
		if len(args) == 2 && (args[1] == "--help" || args[1] == "-h") {
			printSaveUsage()
			return true
		}
	case "checkpoint":
		opts, _ := parseCheckpointArgs(args[1:])
		if opts.Help {
			printCheckpointUsage()
			return true
		}
	case "capture":
		if len(args) >= 2 {
			subcommand := strings.ToLower(strings.TrimSpace(args[1]))
			if subcommand == "help" || subcommand == "--help" || subcommand == "-h" {
				cmdCapture(store.Config{})
				return true
			}
		}
	case "legacy-prompts":
		if len(args) >= 2 {
			subcommand := strings.ToLower(strings.TrimSpace(args[1]))
			if subcommand == "help" || subcommand == "--help" || subcommand == "-h" {
				cmdLegacyPrompts(store.Config{})
				return true
			}
		}
	case "cloud":
		if len(args) >= 2 {
			subcommand := strings.ToLower(strings.TrimSpace(args[1]))
			if subcommand == "--help" || subcommand == "-h" || subcommand == "help" {
				cmdCloud(store.Config{})
				return true
			}
		}
	}
	return false
}

func isCodexSetupStatus(args []string) bool {
	return len(args) >= 3 &&
		strings.EqualFold(strings.TrimSpace(args[0]), "setup") &&
		strings.EqualFold(strings.TrimSpace(args[1]), "status") &&
		strings.EqualFold(strings.TrimSpace(args[2]), "codex")
}

func printUpdateCheckResult(result versioncheck.CheckResult) {
	if result.Status != versioncheck.StatusUpToDate && result.Message != "" {
		fmt.Fprintln(os.Stderr, result.Message)
		fmt.Fprintln(os.Stderr)
	}
}

// ─── Commands ────────────────────────────────────────────────────────────────

func cmdServe(cfg store.Config) {
	port := 7437 // "ENGR" on phone keypad vibes
	if p := os.Getenv("ENGRAM_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	// Allow: engram serve 8080
	if len(os.Args) > 2 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil {
			port = n
		}
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	srv := newHTTPServer(s, port)
	srv.SetRecallProvenance(version, commit)

	// Wire the semantic runner factory and prompt builder for POST /conflicts/scan.
	// Both live in cmd/engram so internal/server avoids a direct dependency on internal/llm.
	srv.SetRunnerFactory(agentRunnerFactory)
	srv.SetPromptBuilder(func(a, b store.ObservationSnippet) string {
		return llmBuildPrompt(a, b)
	})

	// Graceful shutdown context — cancelled on SIGINT/SIGTERM.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Try to start autosync (opt-in via ENGRAM_CLOUD_AUTOSYNC=1).
	// BW7: tryStartAutosync returns (status provider, stop func) so the signal
	// handler can call mgrStop() before os.Exit, giving the manager time to
	// release its sync lease.
	fallback := storeSyncStatusProvider{store: s, defaultProject: resolveServeSyncStatusProject(), cfg: cfg}
	mgr, mgrStop := tryStartAutosync(ctx, s, cfg)
	if mgr != nil {
		srv.SetSyncStatus(&autosyncStatusAdapter{mgr: mgr, fallback: fallback})
	} else {
		srv.SetSyncStatus(fallback)
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("[engram] shutting down...")
		cancel()
		if mgrStop != nil {
			mgrStop() // BW7: wait for Manager to release lease before exiting
		}
		exitFunc(0)
	}()

	if err := startHTTP(srv); err != nil {
		fatal(err)
	}
}

func resolveServeSyncStatusProject() string {
	projectName, ok := projectpkg.ProcessOverride("")
	if !ok {
		if cwd, err := os.Getwd(); err == nil {
			projectName = detectProject(cwd)
		}
	}
	projectName, _ = store.NormalizeProject(projectName)
	return strings.TrimSpace(projectName)
}

// tryStartAutosync starts the autosync Manager if ENGRAM_CLOUD_AUTOSYNC=1 and
// both ENGRAM_CLOUD_TOKEN and ENGRAM_CLOUD_SERVER are present.
// REQ-210: only exact "1" is accepted. REQ-211: missing token/server → log+skip.
// Never fatal — autosync is optional.
// BW7: Returns (status provider, stop func) so the caller can invoke stop
// before os.Exit to ensure the Manager releases its sync lease.
func tryStartAutosync(ctx context.Context, s *store.Store, cfg store.Config) (autosyncStatusProvider, func()) {
	// REQ-210: opt-in requires exact "1".
	if strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_AUTOSYNC")) != "1" {
		return nil, nil
	}

	cc, err := resolveCloudRuntimeConfig(cfg)
	if err != nil {
		log.Printf("[autosync] ERROR: cannot read cloud config: %v", err)
		return nil, nil
	}

	token := strings.TrimSpace(cc.Token)
	serverURL := strings.TrimSpace(cc.ServerURL)

	// REQ-211: token required. The token is resolved from cloud.json first and
	// overridden by ENGRAM_CLOUD_TOKEN when set, so both sources are tried.
	// On Windows (Task Scheduler), the env var is often absent — the file path
	// is the expected source (issue #421).
	if token == "" {
		log.Printf("[autosync] ERROR: cloud token is not configured (set ENGRAM_CLOUD_TOKEN or store token in cloud.json via `engram cloud config`); autosync disabled")
		return nil, nil
	}
	// REQ-211: server URL required. Resolved from cloud.json or ENGRAM_CLOUD_SERVER.
	if serverURL == "" {
		log.Printf("[autosync] ERROR: cloud server URL is not configured (set ENGRAM_CLOUD_SERVER or run `engram cloud config --server <url>`); autosync disabled")
		return nil, nil
	}

	remoteMT, err := remote.NewMutationTransport(serverURL, token)
	if err != nil {
		log.Printf("[autosync] ERROR: invalid server URL %q: %v; autosync disabled", serverURL, err)
		return nil, nil
	}
	transport := &mutationTransportAdapter{remote: remoteMT}
	mgrCfg := autosync.DefaultConfig()
	// BR2-3: Call newAutosyncManager (injectable) instead of autosync.New directly,
	// so tests can stub the factory and avoid real goroutine/network side effects.
	mgr := newAutosyncManager(s, transport, mgrCfg)

	go mgr.Run(ctx)
	log.Printf("[autosync] started (server=%s)", serverURL)
	return mgr, mgr.Stop
}

func cmdMCP(cfg store.Config) {
	toolsFilter := ""
	// The --project flag below is the explicit process argument of the shared
	// override rule; projectpkg.ProcessOverride supplies the ENGRAM_PROJECT step.
	projectOverride, _ := projectpkg.ProcessOverride("")
	for i := 2; i < len(os.Args); i++ {
		if strings.HasPrefix(os.Args[i], "--tools=") {
			toolsFilter = strings.TrimPrefix(os.Args[i], "--tools=")
		} else if os.Args[i] == "--tools" && i+1 < len(os.Args) {
			toolsFilter = os.Args[i+1]
			i++
		} else if strings.HasPrefix(os.Args[i], "--project=") {
			projectOverride = strings.TrimSpace(strings.TrimPrefix(os.Args[i], "--project="))
			if projectOverride == "" {
				fatal(fmt.Errorf("--project requires a value"))
			}
		} else if os.Args[i] == "--project" {
			if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
				fatal(fmt.Errorf("--project requires a value"))
			}
			projectOverride = strings.TrimSpace(os.Args[i+1])
			if projectOverride == "" {
				fatal(fmt.Errorf("--project requires a value"))
			}
			i++
		}
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	// Match `engram serve` autosync startup semantics for stdio MCP agents.
	// Autosync remains opt-in via ENGRAM_CLOUD_AUTOSYNC=1 and never makes MCP
	// startup fatal when cloud config is missing or invalid.
	ctx, cancel := context.WithCancel(context.Background())
	_, mgrStop := tryStartAutosync(ctx, s, cfg)
	autosyncStopped := false
	stopAutosync := func() {
		if autosyncStopped {
			return
		}
		autosyncStopped = true
		cancel()
		if mgrStop != nil {
			mgrStop()
		}
	}
	defer stopAutosync()

	observeOperation, observeCheckpoint, closeObservers := newRecallBaselineMCPObservers(cfg)
	defer closeObservers()
	mcpCfg := mcp.MCPConfig{
		DefaultProject: projectOverride, BinaryVersion: version, BinaryRevision: commit, ObserveOperation: observeOperation,
		ObserveCheckpoint: observeCheckpoint,
	}
	allowlist := resolveMCPTools(toolsFilter)
	mcpSrv := newMCPServerWithConfig(s, mcpCfg, allowlist)

	if err := serveMCP(mcpSrv); err != nil {
		stopAutosync()
		fatal(err)
	}
}

func cmdTUI(cfg store.Config) {
	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	model := newTUIModel(s, cfg.DataDir)
	p := newTeaProgram(model)
	if _, err := runTeaProgram(p); err != nil {
		fatal(err)
	}
}

func cmdSearch(cfg store.Config) {
	started := time.Now()
	baselineOutcome := recallbaseline.OutcomeError
	var baselineBytes *int64
	defer func() { observeRecallBaselineCLI(cfg, "search", started, baselineOutcome, baselineBytes) }()
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "usage: engram search <query> [--type TYPE] [--project PROJECT] [--all-projects] [--match-mode all|any] [--scope SCOPE] [--limit N] [--host HOST --session-id ID --root-turn-id ID] [--json]", nil)
		return
	}

	var queryParts []string
	opts := store.SearchOptions{}
	allProjects := false
	turnIdentity := store.CheckpointIdentity{}

	for i := 2; i < len(os.Args); i++ {
		arg := os.Args[i]
		if name, value, inline := strings.Cut(arg, "="); inline {
			switch name {
			case "--host", "--session-id", "--root-turn-id":
				if value == "" {
					failCLI(jsonMode, "missing_flag_value", fmt.Sprintf("%s requires a value", name), nil)
					return
				}
				switch name {
				case "--host":
					turnIdentity.Host = value
				case "--session-id":
					turnIdentity.SessionID = value
				case "--root-turn-id":
					turnIdentity.RootTurnID = value
				}
				continue
			}
		}
		switch arg {
		case "--json":
		case "--all-projects":
			allProjects = true
		case "--type", "--project", "--limit", "--scope", "--match-mode", "--host", "--session-id", "--root-turn-id":
			if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
				failCLI(jsonMode, "missing_flag_value", fmt.Sprintf("%s requires a value", arg), nil)
				return
			}
			value := os.Args[i+1]
			i++
			switch arg {
			case "--type":
				opts.Type = value
			case "--project":
				opts.Project = value
			case "--scope":
				opts.Scope = value
			case "--match-mode":
				opts.MatchMode = value
			case "--host":
				turnIdentity.Host = value
			case "--session-id":
				turnIdentity.SessionID = value
			case "--root-turn-id":
				turnIdentity.RootTurnID = value
			case "--limit":
				n, err := strconv.Atoi(value)
				if err != nil || n < 1 || n > memoryops.MaximumRecallCandidateLimit {
					failCLI(jsonMode, "invalid_limit", fmt.Sprintf("limit must be between 1 and %d", memoryops.MaximumRecallCandidateLimit), nil)
					return
				}
				opts.Limit = n
			}
		default:
			if strings.HasPrefix(arg, "--") {
				failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", arg), nil)
				return
			}
			queryParts = append(queryParts, arg)
		}
	}

	query := strings.Join(queryParts, " ")
	if strings.TrimSpace(query) == "" {
		failCLI(jsonMode, "invalid_arguments", "search query is required", nil)
		return
	}
	var err error
	opts.MatchMode, err = memoryops.NormalizeRecallMatchMode(opts.MatchMode)
	if err != nil {
		failCLI(jsonMode, "invalid_match_mode", err.Error(), nil)
		return
	}
	opts.Scope, err = memoryops.NormalizeRecallScope(opts.Scope)
	if err != nil {
		failCLI(jsonMode, "invalid_recall_scope", err.Error(), nil)
		return
	}
	if allProjects && opts.Project != "" {
		failCLI(jsonMode, "incompatible_flags", "--all-projects cannot be combined with --project", nil)
		return
	}
	identityParts := 0
	for _, value := range []string{turnIdentity.Host, turnIdentity.SessionID, turnIdentity.RootTurnID} {
		if strings.TrimSpace(value) != "" {
			identityParts++
		}
	}
	var recallTurnIdentity *store.CheckpointIdentity
	if identityParts != 0 {
		if identityParts != 3 {
			failCLI(jsonMode, "invalid_checkpoint_identity", "--host, --session-id, and --root-turn-id must be provided together", nil)
			return
		}
		if err := store.ValidateCheckpointIdentity(turnIdentity); err != nil {
			failCLI(jsonMode, "invalid_checkpoint_identity", err.Error(), nil)
			return
		}
		recallTurnIdentity = &turnIdentity
	}
	projectSource := projectpkg.SourceCLIExplicit
	projectPath := ""
	if allProjects {
		opts.Project = ""
		projectSource = projectpkg.SourceAllProjects
	} else if opts.Project != "" {
		opts.Project, _ = store.NormalizeProject(opts.Project)
		if opts.Project == "" {
			failCLI(jsonMode, "invalid_project", "project must not be empty", nil)
			return
		}
	} else if opts.Scope != "project" {
		projectSource = projectpkg.SourcePersonalScope
	} else {
		res := projectpkg.DetectProjectFull(currentCWD())
		if res.Error != nil || res.Project == "" {
			failCLI(jsonMode, "ambiguous_project", "could not resolve current project", map[string]any{"available_projects": res.AvailableProjects})
			return
		}
		opts.Project, _ = store.NormalizeProject(res.Project)
		projectSource, projectPath = res.Source, res.Path
	}

	s, err := storeNew(cfg)
	service := memoryops.New(s)
	if err == nil {
		defer s.Close()
	} else {
		service = memoryops.New(nil)
	}

	identity := projectpkg.ClassifyIdentitySource(projectSource)
	recallResult, err := service.Recall(memoryops.RecallInput{
		Query:           query,
		Type:            opts.Type,
		Project:         opts.Project,
		Scope:           opts.Scope,
		Limit:           opts.Limit,
		MatchMode:       opts.MatchMode,
		AllProjects:     allProjects || (opts.Scope != "project" && opts.Project == ""),
		ProjectStrength: identity.Strength,
		DeliberateScope: allProjects || opts.Scope != "project",
		BinaryVersion:   version,
		BinaryRevision:  commit,
		TurnIdentity:    recallTurnIdentity,
	})
	if err != nil {
		failCLI(jsonMode, "search_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		payload := map[string]any{
			"query":                  query,
			"project":                opts.Project,
			"project_source":         projectSource,
			"project_path":           projectPath,
			"project_strength":       identity.Strength,
			"implicit_write_allowed": identity.AllowsImplicitWrite,
			"all_projects":           allProjects,
			"recall_id":              recallResult.RecallID,
			"results":                recallResult.Candidates,
			"result_ids":             recallResult.ResultIDs,
			"opaque_result_ids":      recallResult.OpaqueResultIDs,
			"result_count":           recallResult.ResultCount,
			"delivered_utf8_bytes":   recallResult.DeliveredUTF8Bytes,
			"elapsed_monotonic_ms":   recallResult.ElapsedMonotonicMS,
			"provenance":             recallResult.Provenance,
		}
		if recallResult.Warning != nil {
			payload["warning"] = recallResult.Warning
		}
		if len(recallResult.Diagnostics) > 0 {
			payload["diagnostics"] = recallResult.Diagnostics
		}
		if identity.Strength == projectpkg.IdentityStrengthWeak {
			payload["safe_next_action"] = projectpkg.ExplicitProjectSafeNextAction
		}
		if recallResult.Warning == nil {
			baselineOutcome = recallbaseline.OutcomeSuccess
		}
		baselineBytes = cliJSONBytes(payload)
		_ = writeCLIJSON(payload)
		return
	}

	if recallResult.Warning != nil {
		fmt.Fprintf(os.Stderr, "Warning: %s %s\n", recallResult.Warning.Message, recallResult.Warning.NextAction)
		return
	}
	if len(recallResult.Candidates) == 0 {
		output := fmt.Sprintf("No Memory candidates found for: %q\n", query)
		baselineOutcome = recallbaseline.OutcomeSuccess
		baselineBytes = recallbaseline.KnownBytes(int64(len([]byte(output))))
		fmt.Print(output)
		return
	}

	baselineOutcome = recallbaseline.OutcomeSuccess
	fmt.Printf("Found %d Memory candidates (recall %s):\n\n", len(recallResult.Candidates), recallResult.RecallID)
	for i, candidate := range recallResult.Candidates {
		projectDisplay := ""
		if candidate.Project != "" {
			projectDisplay = fmt.Sprintf(" | project: %s", candidate.Project)
		}
		fmt.Printf("[%d] #%d (%s) — %s\n    %s\n    result: %s\n    scope: %s%s\n",
			i+1, candidate.ID, candidate.Type, candidate.Title, candidate.Summary, candidate.ResultID, candidate.Scope, projectDisplay)
		for _, conflict := range candidate.Conflicts {
			fmt.Printf("    warning: unresolved conflict with #%d (%s) [%s]\n", conflict.MemoryID, conflict.Title, conflict.Status)
		}
		fmt.Println()
	}
}

type saveOptions struct {
	Title    string
	Content  string
	Type     string
	Project  string
	Scope    string
	TopicKey string
	JSONMode bool
}

type saveArgumentError struct {
	Code    string
	Message string
}

const (
	savePositionalUsage = "engram save <title> <content> [flags]"
	saveNamedUsage      = "engram save --title TITLE --content CONTENT [flags]"
)

func parseSaveArgs(args []string) (saveOptions, *saveArgumentError) {
	opts := saveOptions{Type: "manual", Scope: "project"}
	positionals := make([]string, 0, 2)
	titleSet, contentSet := false, false

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--json" {
			opts.JSONMode = true
			continue
		}
		if arg != "--title" && arg != "--content" && arg != "--type" && arg != "--project" && arg != "--scope" && arg != "--topic" && arg != "--topic-key" {
			if strings.HasPrefix(arg, "--") {
				return opts, &saveArgumentError{Code: "unknown_flag", Message: fmt.Sprintf("unknown flag %s", arg)}
			}
			positionals = append(positionals, arg)
			continue
		}
		if i+1 >= len(args) || strings.HasPrefix(args[i+1], "--") {
			return opts, &saveArgumentError{Code: "missing_flag_value", Message: fmt.Sprintf("%s requires a value", arg)}
		}
		value := args[i+1]
		i++
		switch arg {
		case "--title":
			opts.Title, titleSet = value, true
		case "--content":
			opts.Content, contentSet = value, true
		case "--type":
			opts.Type = value
		case "--project":
			opts.Project = value
		case "--scope":
			opts.Scope = value
		case "--topic", "--topic-key":
			opts.TopicKey = value
		}
	}

	if titleSet || contentSet {
		if len(positionals) > 0 {
			return opts, &saveArgumentError{Code: "invalid_arguments", Message: "cannot mix positional title/content with --title or --content"}
		}
		if !titleSet || !contentSet {
			return opts, &saveArgumentError{Code: "invalid_arguments", Message: "named save requires both --title and --content"}
		}
	} else {
		if len(positionals) != 2 {
			return opts, &saveArgumentError{Code: "invalid_arguments", Message: fmt.Sprintf("usage: %s or %s", savePositionalUsage, saveNamedUsage)}
		}
		opts.Title, opts.Content = positionals[0], positionals[1]
	}
	if strings.TrimSpace(opts.Content) == "" {
		return opts, &saveArgumentError{Code: "invalid_arguments", Message: "content is required"}
	}
	return opts, nil
}

func cmdSave(cfg store.Config) {
	opts, argErr := parseSaveArgs(os.Args[2:])
	if argErr != nil {
		failCLI(opts.JSONMode || hasArg("--json"), argErr.Code, argErr.Message, nil)
		return
	}

	// Reject titleless saves before opening the store or creating a session
	// (#459). The store applies the same rule as a backstop.
	if err := store.ValidateObservationTitle(opts.Title); err != nil {
		fatal(err)
		return
	}

	cwd, err := os.Getwd()
	if err != nil {
		fatal(err)
		return
	}
	// Identity precedence is the one process-level rule shared with the MCP and
	// HTTP entry points: the explicit --project flag, then the process override
	// (project.ProcessOverride reads ENGRAM_PROJECT), then cwd detection.
	project := opts.Project
	projectSource, projectPath := projectpkg.SourceCLIExplicit, ""
	if strings.TrimSpace(project) == "" {
		if override, ok := projectpkg.ProcessOverride(""); ok {
			project = override
			projectSource = projectpkg.SourceEnvironment
		} else {
			resolved := detectProjectFull(cwd)
			if resolved.Error != nil || strings.TrimSpace(resolved.Project) == "" {
				if resolved.Error != nil {
					fatal(fmt.Errorf("cannot save without an unambiguous project identity: %w; use --project <name>", resolved.Error))
				} else {
					fatal(errors.New("cannot save without an unambiguous project identity; use --project <name>"))
				}
				return
			}
			if authorityErr := projectpkg.RequireImplicitWriteAuthority(resolved); authorityErr != nil {
				failProjectResolution(resolved, authorityErr)
				return
			}
			project = resolved.Project
			projectSource, projectPath = resolved.Source, resolved.Path
		}
	}
	var warning string
	project, warning = store.NormalizeProject(project)
	if warning != "" && !opts.JSONMode {
		fmt.Fprintln(os.Stderr, warning)
	}
	if strings.TrimSpace(project) == "" {
		fatal(errors.New("cannot save without an unambiguous project identity; use --project <name>"))
		return
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(opts.JSONMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()

	sessionID := "manual-save-" + project
	result, err := memoryops.New(s).Save(memoryops.SaveInput{
		SessionID: sessionID,
		CWD:       cwd,
		Type:      opts.Type,
		Title:     opts.Title,
		Content:   opts.Content,
		Project:   project,
		Scope:     opts.Scope,
		TopicKey:  opts.TopicKey,
	})
	if err != nil {
		failCLI(opts.JSONMode, "save_failed", err.Error(), nil)
		return
	}
	obs, candidates := result.Observation, result.Candidates
	if result.CandidateDetectionError != nil && !opts.JSONMode {
		fmt.Fprintf(os.Stderr, "engram: conflict candidate detection failed (non-fatal): %v\n", result.CandidateDetectionError)
	}
	if opts.JSONMode {
		candidatePayload := make([]map[string]any, 0, len(candidates))
		for _, c := range candidates {
			candidatePayload = append(candidatePayload, map[string]any{"id": c.ID, "sync_id": c.SyncID, "title": c.Title, "type": c.Type, "topic_key": c.TopicKey, "score": c.Score, "judgment_id": c.JudgmentID})
		}
		identity := projectpkg.ClassifyIdentitySource(projectSource)
		payload := map[string]any{"observation": obs, "state": obs.State(), "project": project, "project_source": projectSource, "project_path": projectPath, "project_strength": identity.Strength, "implicit_write_allowed": identity.AllowsImplicitWrite, "suggested_topic_key": result.SuggestedTopicKey, "judgment_required": len(candidates) > 0, "candidates": candidatePayload}
		if warning != "" {
			payload["project_warning"] = warning
		}
		if result.CandidateDetectionError != nil {
			payload["warning"] = "conflict candidate detection failed"
		}
		_ = writeCLIJSON(payload)
		return
	}
	fmt.Printf("Memory saved: #%d %q (%s)\n", obs.ID, opts.Title, opts.Type)
	if opts.TopicKey == "" {
		if suggestion := result.SuggestedTopicKey; suggestion != "" {
			fmt.Printf("Suggested topic_key: %s\n", suggestion)
		}
	}
	if len(candidates) > 0 {
		fmt.Printf("CONFLICT REVIEW PENDING — %d candidate(s); use engram conflicts judge.\n", len(candidates))
	}
}

func cmdDelete(cfg store.Config) {
	if len(os.Args) < 3 {
		if hasArg("--json") {
			failCLI(true, "invalid_arguments", "usage: engram delete <observation_id> [--hard] [--json]", nil)
			return
		}
		fmt.Fprintln(os.Stderr, "usage: engram delete <observation_id> [--hard]")
		fmt.Fprintln(os.Stderr, "       engram delete session  <id>")
		fmt.Fprintln(os.Stderr, "       engram delete project  <name> [--hard]")
		exitFunc(1)
		return
	}

	sub := os.Args[2]
	switch sub {
	case "session":
		cmdDeleteSession(cfg)
	case "prompt":
		cmdDeletePrompt(cfg)
	case "project":
		cmdDeleteProject(cfg)
	default:
		// Backward-compat: treat the second arg as a numeric observation ID.
		cmdDeleteObservation(cfg)
	}
}

func cmdDeleteObservation(cfg store.Config) {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: engram delete <observation_id> [--hard]")
		exitFunc(1)
		return
	}

	id, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		failCLI(jsonMode, "invalid_observation_id", fmt.Sprintf("invalid observation id %q", os.Args[2]), nil)
		return
	}

	hard := false
	for i := 3; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--hard":
			hard = true
		case "--json":
		default:
			failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", os.Args[i]), nil)
			return
		}
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()

	if err := storeDeleteObservation(s, id, hard); err != nil {
		failCLI(jsonMode, "delete_failed", err.Error(), nil)
		return
	}

	kind := "soft-deleted"
	if hard {
		kind = "hard-deleted"
	}
	if jsonMode {
		_ = writeCLIJSON(map[string]any{"id": id, "deleted": true, "hard_delete": hard})
		return
	}
	fmt.Printf("Observation #%d %s\n", id, kind)
}

func cmdDeleteSession(cfg store.Config) {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: engram delete session <id>")
		exitFunc(1)
		return
	}

	id := os.Args[3]

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()

	if err := storeDeleteSession(s, id); err != nil {
		fatal(err)
		return
	}
	fmt.Printf("Session %q deleted\n", id)
}

func cmdDeletePrompt(cfg store.Config) {
	failCLI(hasArg("--json"), "legacy_prompt_archive_frozen", "prompt deletion moved to the separately confirmed engram legacy-prompts purge command", nil)
}

func cmdDeleteProject(cfg store.Config) {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: engram delete project <name> [--hard]")
		exitFunc(1)
		return
	}

	name := os.Args[3]
	hard := false
	for i := 4; i < len(os.Args); i++ {
		if os.Args[i] == "--hard" {
			hard = true
		}
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()

	result, err := storeDeleteProject(s, name, hard)
	if err != nil {
		fatal(err)
		return
	}

	kind := "soft-deleted"
	if hard {
		kind = "hard-deleted"
	}
	fmt.Printf("Project %q %s: %d observation(s), %d session(s), %d Memory proposal(s), %d checkpoint(s); Legacy prompts preserved\n",
		result.Project, kind, result.ObservationsDeleted,
		result.SessionsDeleted, result.MemoryProposalsDeleted, result.MemoryCheckpointsDeleted)
}

func cmdTimeline(cfg store.Config) {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "usage: engram timeline <observation_id> [--before N] [--after N] [--json]", nil)
		return
	}

	obsID, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		failCLI(jsonMode, "invalid_observation_id", fmt.Sprintf("invalid observation id %q", os.Args[2]), nil)
		return
	}

	before, after := 5, 5
	for i := 3; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--json":
		case "--before":
			if i+1 < len(os.Args) {
				if n, err := strconv.Atoi(os.Args[i+1]); err == nil {
					before = n
				}
				i++
			}
		case "--after":
			if i+1 < len(os.Args) {
				if n, err := strconv.Atoi(os.Args[i+1]); err == nil {
					after = n
				}
				i++
			}
		default:
			failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", os.Args[i]), nil)
			return
		}
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()

	result, err := storeTimeline(s, obsID, before, after)
	if err != nil {
		failCLI(jsonMode, "timeline_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(result)
		return
	}

	// Session header
	if result.SessionInfo != nil {
		summary := ""
		if result.SessionInfo.Summary != nil {
			summary = fmt.Sprintf(" — %s", truncate(*result.SessionInfo.Summary, 100))
		}
		fmt.Printf("Session: %s (%s)%s\n", result.SessionInfo.Project, result.SessionInfo.StartedAt, summary)
		fmt.Printf("Total observations in session: %d\n\n", result.TotalInRange)
	}

	// Before
	if len(result.Before) > 0 {
		fmt.Println("─── Before ───")
		for _, e := range result.Before {
			fmt.Printf("  #%d [%s] %s — %s\n", e.ID, e.Type, e.Title, truncate(e.Content, 150))
		}
		fmt.Println()
	}

	// Focus
	fmt.Printf(">>> #%d [%s] %s <<<\n", result.Focus.ID, result.Focus.Type, result.Focus.Title)
	fmt.Printf("    %s\n", truncate(result.Focus.Content, 500))
	fmt.Printf("    %s\n\n", timeutil.FormatLocal(result.Focus.CreatedAt))

	// After
	if len(result.After) > 0 {
		fmt.Println("─── After ───")
		for _, e := range result.After {
			fmt.Printf("  #%d [%s] %s — %s\n", e.ID, e.Type, e.Title, truncate(e.Content, 150))
		}
	}
}

func cmdContext(cfg store.Config) {
	started := time.Now()
	baselineOutcome := recallbaseline.OutcomeError
	var baselineBytes *int64
	defer func() { observeRecallBaselineCLI(cfg, "context", started, baselineOutcome, baselineBytes) }()
	project := ""
	scope := ""
	jsonMode := hasArg("--json")

	for i := 2; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--json":
		case "--scope":
			if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
				failCLI(jsonMode, "invalid_arguments", "--scope requires a value", nil)
				return
			}
			scope = strings.TrimSpace(os.Args[i+1])
			i++
		default:
			if strings.HasPrefix(os.Args[i], "--") {
				failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", os.Args[i]), nil)
				return
			}
			if project == "" {
				project = os.Args[i]
			} else {
				failCLI(jsonMode, "invalid_arguments", "context accepts only one project", nil)
				return
			}
		}
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()

	ctx, err := storeFormatContext(s, project, scope)
	if err != nil {
		failCLI(jsonMode, "context_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		payload := map[string]any{"project": project, "scope": scope, "context": ctx}
		baselineOutcome = recallbaseline.OutcomeSuccess
		baselineBytes = cliJSONBytes(payload)
		_ = writeCLIJSON(payload)
		return
	}

	if ctx == "" {
		output := "No previous session memories found.\n"
		baselineOutcome = recallbaseline.OutcomeSuccess
		baselineBytes = recallbaseline.KnownBytes(int64(len(output)))
		fmt.Print(output)
		return
	}

	baselineOutcome = recallbaseline.OutcomeSuccess
	baselineBytes = recallbaseline.KnownBytes(int64(len([]byte(ctx))))
	fmt.Print(ctx)
}

func cmdStats(cfg store.Config) {
	jsonMode := hasArg("--json")
	for _, arg := range os.Args[2:] {
		if arg != "--json" {
			failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", arg), nil)
			return
		}
	}
	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()

	stats, err := storeStats(s)
	if err != nil {
		failCLI(jsonMode, "stats_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(stats)
		return
	}

	projects := "none yet"
	if len(stats.Projects) > 0 {
		projects = strings.Join(stats.Projects, ", ")
	}

	fmt.Printf("Engram Memory Stats\n")
	fmt.Printf("  Sessions:     %d\n", stats.TotalSessions)
	fmt.Printf("  Observations: %d\n", stats.TotalObservations)
	fmt.Printf("  Projects:     %s\n", projects)
	fmt.Printf("  Database:     %s/engram.db\n", cfg.DataDir)
}

func cmdExport(cfg store.Config) {
	outFile := "engram-export.json"
	if len(os.Args) > 2 {
		outFile = os.Args[2]
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	data, err := storeExport(s)
	if err != nil {
		fatal(err)
	}

	out, err := jsonMarshalIndent(data, "", "  ")
	if err != nil {
		fatal(err)
	}

	if err := os.WriteFile(outFile, out, 0644); err != nil {
		fatal(err)
	}

	fmt.Printf("Exported to %s\n", outFile)
	fmt.Printf("  Sessions:     %d\n", len(data.Sessions))
	fmt.Printf("  Observations: %d\n", len(data.Observations))
}

func cmdImport(cfg store.Config) {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: engram import <file.json>")
		exitFunc(1)
	}

	inFile := os.Args[2]
	raw, err := os.ReadFile(inFile)
	if err != nil {
		fatal(fmt.Errorf("read %s: %w", inFile, err))
	}

	var data store.ExportData
	if err := json.Unmarshal(raw, &data); err != nil {
		fatal(fmt.Errorf("parse %s: %w", inFile, err))
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	result, err := s.Import(&data)
	if err != nil {
		fatal(err)
	}

	fmt.Printf("Imported from %s\n", inFile)
	fmt.Printf("  Sessions:     %d\n", result.SessionsImported)
	fmt.Printf("  Observations: %d\n", result.ObservationsImported)
}

func cmdSync(cfg store.Config) {
	// Parse flags
	doImport := false
	doStatus := false
	doAll := false
	doCloud := false
	project := ""
	projectProvided := false
	for i := 2; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--help", "-h", "help":
			printSyncUsage()
			return
		case "--import":
			doImport = true
		case "--status":
			doStatus = true
		case "--all":
			doAll = true
		case "--cloud":
			doCloud = true
		case "--project":
			if i+1 < len(os.Args) {
				project = os.Args[i+1]
				projectProvided = true
				i++
			}
		}
	}

	// Default project using authoritative detection (so sync only exports
	// memories for THIS project, not everything in the global DB).
	// --all skips project filtering entirely — exports everything.
	if !doAll && project == "" {
		cwd, err := os.Getwd()
		if err != nil {
			fatal(fmt.Errorf("project_detection_failed: %w", err))
			return
		}
		resolved := detectProjectFull(cwd)
		if !doStatus {
			if authorityErr := projectpkg.RequireImplicitWriteAuthority(resolved); authorityErr != nil {
				failProjectResolution(resolved, authorityErr)
				return
			}
		}
		project = resolved.Project
	}
	if project != "" {
		normalizedProject, warning := store.NormalizeProject(project)
		project = normalizedProject
		if warning != "" {
			fmt.Fprintln(os.Stderr, warning)
		}
	}

	syncDir := ".engram"

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	cloudEnabled := doCloud || envBool("ENGRAM_CLOUD_SYNC")
	if cloudEnabled {
		if doAll {
			fatal(fmt.Errorf("cloud sync requires a single explicit --project scope; --all is not supported"))
		}
		if !projectProvided || strings.TrimSpace(project) == "" {
			fatal(fmt.Errorf("cloud sync requires an explicit non-empty --project value"))
		}
	}
	cloudTargetKey := cloudTargetKeyForProject(project)
	var sy *engramsync.Syncer

	markCloudHealthy := func() {
		if !cloudEnabled {
			return
		}
		if err := s.MarkSyncHealthy(cloudTargetKey); err != nil {
			fatal(fmt.Errorf("cloud sync health update: %w", err))
		}
	}

	markCloudSyncOutcome := func() {
		if !cloudEnabled {
			return
		}
		hasPending, err := s.HasPendingSyncMutationsForProject(project)
		if err != nil {
			fatal(fmt.Errorf("cloud sync state update: %w", err))
		}
		pendingImports := 0
		remoteStatusVerified := false
		if _, _, pending, statusErr := syncStatus(sy); statusErr == nil {
			pendingImports = pending
			remoteStatusVerified = true
		}
		if hasPending || (remoteStatusVerified && pendingImports > 0) {
			if err := s.MarkSyncPending(cloudTargetKey); err != nil {
				fatal(fmt.Errorf("cloud sync pending-state update: %w", err))
			}
			return
		}
		if !remoteStatusVerified {
			return
		}
		markCloudHealthy()
	}

	sy = engramsync.NewLocalWithProject(s, syncDir, project)
	if cloudEnabled {
		cc, err := preflightCloudSync(s, cfg, project, !doStatus)
		if err != nil {
			fatal(err)
		}
		transport, err := remote.NewRemoteTransport(cc.ServerURL, cc.Token, project)
		if err != nil {
			if !doStatus {
				markCloudSyncFailure(s, cloudTargetKey, err)
			}
			fatal(errors.New(cloudSyncFailureMessage(project, err)))
		}
		sy = engramsync.NewCloudWithTransport(s, transport, project)
	}

	if doStatus {
		local, remote, pending, err := syncStatus(sy)
		if err != nil {
			fatal(err)
		}
		if cloudEnabled {
			fmt.Printf("Cloud sync status (project=%q):\n", project)
			fmt.Printf("  Local chunks:    %d\n", local)
			fmt.Printf("  Remote chunks:   %d\n", remote)
			fmt.Printf("  Pending import:  %d\n", pending)
			return
		}
		fmt.Printf("Sync status:\n")
		fmt.Printf("  Local chunks:    %d\n", local)
		fmt.Printf("  Remote chunks:   %d\n", remote)
		fmt.Printf("  Pending import:  %d\n", pending)
		return
	}

	if doImport {
		result, err := syncImport(sy)
		if err != nil {
			if cloudEnabled {
				markCloudSyncFailure(s, cloudTargetKey, err)
			}
			if cloudEnabled {
				fatal(errors.New(cloudSyncFailureMessage(project, err)))
			}
			fatal(err)
		}
		markCloudSyncOutcome()

		if result.ChunksImported == 0 {
			fmt.Println("No new chunks to import.")
			if result.ChunksSkipped > 0 {
				fmt.Printf("  (%d chunks already imported)\n", result.ChunksSkipped)
			}
			printImportRelationCounts(result)
			return
		}

		if cloudEnabled {
			fmt.Printf("Imported %d new remote chunk(s) for project %q\n", result.ChunksImported, project)
		} else {
			fmt.Printf("Imported %d new chunk(s) from .engram/\n", result.ChunksImported)
		}
		fmt.Printf("  Sessions:     %d\n", result.SessionsImported)
		fmt.Printf("  Observations: %d\n", result.ObservationsImported)
		if result.ChunksSkipped > 0 {
			fmt.Printf("  Skipped:      %d (already imported)\n", result.ChunksSkipped)
		}
		printImportRelationCounts(result)
		return
	}

	// Export: DB → new chunk
	username := engramsync.GetUsername()
	if doAll {
		fmt.Println("Exporting ALL memories (all projects)...")
	} else {
		if cloudEnabled {
			fmt.Printf("Exporting memories for project %q to cloud...\n", project)
		} else {
			fmt.Printf("Exporting memories for project %q...\n", project)
		}
	}
	result, err := syncExport(sy, username, project)
	if err != nil {
		if cloudEnabled {
			markCloudSyncFailure(s, cloudTargetKey, err)
			fatal(errors.New(cloudSyncFailureMessage(project, err)))
		}
		fatal(err)
	}
	markCloudSyncOutcome()

	if result.IsEmpty {
		if doAll {
			fmt.Println("Nothing new to sync — all memories already exported.")
		} else {
			fmt.Printf("Nothing new to sync for project %q — all memories already exported.\n", project)
		}
		return
	}

	if result.ChunksExported > 1 {
		fmt.Printf("Created %d chunks (last %s)\n", result.ChunksExported, result.ChunkID)
	} else {
		fmt.Printf("Created chunk %s\n", result.ChunkID)
	}
	fmt.Printf("  Sessions:     %d\n", result.SessionsExported)
	fmt.Printf("  Observations: %d\n", result.ObservationsExported)
	if result.MutationsExported > 0 {
		fmt.Printf("  Mutations:    %d\n", result.MutationsExported)
	}
	if cloudEnabled {
		fmt.Printf("Cloud sync complete for project %q.\n", project)
		return
	}
	fmt.Println()
	fmt.Println("Add to git:")
	fmt.Printf("  git add .engram/ && git commit -m \"sync engram memories\"\n")
}

func printImportRelationCounts(result *engramsync.ImportResult) {
	fmt.Printf("  Relations replayed: %d\n", result.RelationsReplayed)
	fmt.Printf("  Relations deferred: %d\n", result.RelationsDeferred)
	fmt.Printf("  Relations dead:     %d\n", result.RelationsDead)
}

func printSyncUsage() {
	fmt.Println("usage: engram sync [--import | --status] [--all] [--cloud --project PROJECT]")
	fmt.Println("Local sync exports project-scoped chunks to .engram/ by default.")
	fmt.Println("Cloud sync requires an explicit --project and never runs from --help.")
}

// storeAdapter wraps *store.Store to satisfy obsidian.StoreReader.
// The real store.Stats() returns (*store.Stats, error); the interface expects *store.Stats.
type storeAdapter struct{ s *store.Store }

func (a *storeAdapter) Export() (*store.ExportData, error) { return a.s.Export() }
func (a *storeAdapter) Stats() *store.Stats {
	st, _ := a.s.Stats()
	return st
}

func cmdObsidianExport(cfg store.Config) {
	// Parse flags
	var (
		vault       string
		project     string
		limit       int
		since       string
		force       bool
		graphConfig string
		watch       bool
		interval    string
	)

	for i := 2; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--vault":
			if i+1 < len(os.Args) {
				vault = os.Args[i+1]
				i++
			}
		case "--project":
			if i+1 < len(os.Args) {
				project = os.Args[i+1]
				i++
			}
		case "--limit":
			if i+1 < len(os.Args) {
				if n, err := strconv.Atoi(os.Args[i+1]); err == nil {
					limit = n
				}
				i++
			}
		case "--since":
			if i+1 < len(os.Args) {
				since = os.Args[i+1]
				i++
			}
		case "--force":
			force = true
		case "--graph-config":
			if i+1 < len(os.Args) {
				graphConfig = os.Args[i+1]
				i++
			}
		case "--watch":
			watch = true
		case "--interval":
			if i+1 < len(os.Args) {
				interval = os.Args[i+1]
				i++
			}
		default:
			fmt.Fprintf(os.Stderr, "engram: unknown flag: %s\n", os.Args[i])
			exitFunc(1)
		}
	}

	if vault == "" {
		fmt.Fprintln(os.Stderr, "error: flag --vault is required")
		exitFunc(1)
	}

	// Default --graph-config to "preserve"
	if graphConfig == "" {
		graphConfig = string(obsidian.GraphConfigPreserve)
	}

	graphMode, err := obsidian.ParseGraphConfigMode(graphConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid --graph-config value: %s (accepted: preserve, force, skip)\n", graphConfig)
		exitFunc(1)
	}

	// Validate --interval requires --watch
	if interval != "" && !watch {
		fmt.Fprintln(os.Stderr, "error: --interval requires --watch")
		exitFunc(1)
	}

	// Parse and validate --interval (default 10m when --watch is set)
	var watchInterval time.Duration
	if watch {
		intervalStr := interval
		if intervalStr == "" {
			intervalStr = "10m"
		}
		d, parseErr := time.ParseDuration(intervalStr)
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "error: invalid --interval value %q: %v\n", intervalStr, parseErr)
			exitFunc(1)
		}
		if d < time.Minute {
			fmt.Fprintf(os.Stderr, "error: --interval must be at least 1m (minimum), got %v\n", d)
			exitFunc(1)
		}
		watchInterval = d
	}

	exportCfg := obsidian.ExportConfig{
		VaultPath:   vault,
		Project:     project,
		Limit:       limit,
		Force:       force,
		GraphConfig: graphMode,
	}

	if since != "" {
		// Try common date formats: full RFC3339, date-only (YYYY-MM-DD)
		var sinceTime time.Time
		var parseErr error
		for _, layout := range []string{time.RFC3339, "2006-01-02"} {
			sinceTime, parseErr = time.Parse(layout, since)
			if parseErr == nil {
				break
			}
		}
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "error: invalid --since value %q (expected YYYY-MM-DD or RFC3339)\n", since)
			exitFunc(1)
		}
		exportCfg.Since = sinceTime
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	exp := newObsidianExporter(&storeAdapter{s: s}, exportCfg)

	if watch {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		w := newObsidianWatcher(obsidian.WatcherConfig{
			Exporter: exp,
			Interval: watchInterval,
			Logf:     log.Printf,
		})

		if w != nil {
			if runErr := w.Run(ctx); runErr != nil {
				log.Printf("[engram] shutting down watch mode: %v", runErr)
			} else {
				log.Printf("[engram] shutting down watch mode")
			}
		}
		exitFunc(0)
		return
	}

	result, err := exp.Export()
	if err != nil {
		fatal(err)
	}

	fmt.Printf("Obsidian export complete\n")
	fmt.Printf("  Created: %d\n", result.Created)
	fmt.Printf("  Updated: %d\n", result.Updated)
	fmt.Printf("  Deleted: %d\n", result.Deleted)
	fmt.Printf("  Skipped: %d\n", result.Skipped)
	fmt.Printf("  Hubs:    %d\n", result.HubsCreated)
	if len(result.Errors) > 0 {
		fmt.Fprintf(os.Stderr, "  Errors: %d\n", len(result.Errors))
		for _, e := range result.Errors {
			fmt.Fprintf(os.Stderr, "    - %v\n", e)
		}
	}
}

func cmdProjects(cfg store.Config) {
	// Route: engram projects list | engram projects consolidate [--project NAME | --all] [--dry-run]
	subCmd := "list"
	if len(os.Args) > 2 {
		subCmd = os.Args[2]
	}
	switch subCmd {
	case "consolidate":
		cmdProjectsConsolidate(cfg)
	case "merge":
		cmdProjectsMerge(cfg)
	case "prune":
		cmdProjectsPrune(cfg)
	case "rescue-ownership":
		cmdProjectsRescueOwnership(cfg)
	case "list", "":
		cmdProjectsList(cfg)
	default:
		fmt.Fprintf(os.Stderr, "unknown projects subcommand: %s\n", subCmd)
		printProjectsUsage()
		exitFunc(1)
	}
}

func cmdProjectsMerge(cfg store.Config) {
	jsonMode := hasArg("--json")
	var sources []string
	target := ""
	dryRun, yes := false, false
	for i := 3; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--json":
		case "--dry-run":
			dryRun = true
		case "--yes":
			yes = true
		case "--from":
			if i+1 >= len(os.Args) {
				failCLI(jsonMode, "missing_flag_value", "--from requires a value", nil)
				return
			}
			sources = append(sources, os.Args[i+1])
			i++
		case "--to":
			if i+1 >= len(os.Args) {
				failCLI(jsonMode, "missing_flag_value", "--to requires a value", nil)
				return
			}
			target = os.Args[i+1]
			i++
		default:
			failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", os.Args[i]), nil)
			return
		}
	}
	if len(sources) == 0 || strings.TrimSpace(target) == "" {
		failCLI(jsonMode, "invalid_arguments", "at least one --from and --to are required", nil)
		return
	}
	target, _ = store.NormalizeProject(target)
	if target == "" {
		failCLI(jsonMode, "invalid_project", "target project must not be empty", nil)
		return
	}
	normalized := make([]string, 0, len(sources))
	seen := map[string]bool{}
	for _, src := range sources {
		n, _ := store.NormalizeProject(src)
		if n != "" && n != target && !seen[n] {
			normalized = append(normalized, n)
			seen[n] = true
		}
	}
	if len(normalized) == 0 {
		failCLI(jsonMode, "invalid_arguments", "sources must differ from target", nil)
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	preview, err := memoryops.New(s).Merge(memoryops.MergeInput{Sources: normalized, Canonical: target, DryRun: true})
	if err != nil {
		failCLI(jsonMode, "merge_failed", err.Error(), nil)
		return
	}
	if dryRun {
		if jsonMode {
			_ = writeCLIJSON(preview)
			return
		}
		fmt.Printf("Would merge %v into %q: %d observations, %d sessions, %d Memory proposals\n", preview.SourcesMerged, target, preview.ObservationsUpdated, preview.SessionsUpdated, preview.MemoryProposalsUpdated)
		return
	}
	if !yes {
		if jsonMode {
			failCLI(true, "confirmation_required", "project merge requires --yes in JSON mode", map[string]any{"preview": preview})
			return
		}
		fmt.Printf("Merge %v into %q? [y/N] ", normalized, target)
		var answer string
		_, _ = fmt.Scanln(&answer)
		if strings.ToLower(strings.TrimSpace(answer)) != "y" && strings.ToLower(strings.TrimSpace(answer)) != "yes" {
			fmt.Println("Aborted.")
			return
		}
	}
	result, err := memoryops.New(s).Merge(memoryops.MergeInput{Sources: normalized, Canonical: target})
	if err != nil {
		failCLI(jsonMode, "merge_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(result)
		return
	}
	fmt.Printf("Merged %v into %q: %d observations, %d sessions, %d Memory proposals\n", result.SourcesMerged, result.Canonical, result.ObservationsUpdated, result.SessionsUpdated, result.MemoryProposalsUpdated)

}

func printProjectsUsage() {
	fmt.Fprintln(os.Stderr, "usage: engram projects list")
	fmt.Fprintln(os.Stderr, "       engram projects consolidate [--project NAME | --all] [--dry-run]")
	fmt.Fprintln(os.Stderr, "       engram projects merge --from SOURCE [--from SOURCE...] --to TARGET [--dry-run] [--yes] [--json]")
	fmt.Fprintln(os.Stderr, "       engram projects prune [--dry-run] [--paths-only]")
	fmt.Fprintln(os.Stderr, "       engram projects rescue-ownership --project <name> [--session <id>]... [--observation <id>]... [--prompt <id>]...")
}

// cmdProjectsRescueOwnership assigns explicit ownership to legacy rows that
// carry none. It reaches the local store directly, so it is available in a
// zero-config install where ENGRAM_HTTP_TOKEN is unset and the HTTP rescue
// endpoint is not served. Every ownership error names this command.
func cmdProjectsRescueOwnership(cfg store.Config) {
	params := store.ProjectRescueParams{}
	for i := 3; i < len(os.Args); i++ {
		next := func() (string, bool) {
			if i+1 >= len(os.Args) {
				return "", false
			}
			i++
			return os.Args[i], true
		}
		switch os.Args[i] {
		case "--project":
			if value, ok := next(); ok {
				params.TargetProject = value
			}
		case "--session":
			if value, ok := next(); ok {
				params.SessionIDs = append(params.SessionIDs, value)
			}
		case "--observation":
			if value, ok := next(); ok {
				id, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					fatal(fmt.Errorf("invalid --observation id %q: %w", value, err))
					return
				}
				params.ObservationIDs = append(params.ObservationIDs, id)
			}
		case "--prompt":
			if value, ok := next(); ok {
				id, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					fatal(fmt.Errorf("invalid --prompt id %q: %w", value, err))
					return
				}
				params.PromptIDs = append(params.PromptIDs, id)
			}
		}
	}

	if strings.TrimSpace(params.TargetProject) == "" {
		fmt.Fprintln(os.Stderr, "--project <name> is required")
		printProjectsUsage()
		exitFunc(1)
		return
	}
	if len(params.SessionIDs) == 0 && len(params.ObservationIDs) == 0 && len(params.PromptIDs) == 0 {
		fmt.Fprintln(os.Stderr, "select at least one --session, --observation, or --prompt to rescue")
		printProjectsUsage()
		exitFunc(1)
		return
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()

	result, err := s.RescueNullProjectOwnership(params)
	if err != nil {
		fatal(err)
		return
	}

	fmt.Printf("Rescued ownership into %q: %d sessions, %d observations\n",
		params.TargetProject, result.RescuedSessions, result.RescuedObservations)
	if result.Complete {
		fmt.Println("Everything selected now belongs to the target project.")
	} else {
		fmt.Printf("%d selected item(s) were left behind:\n", len(result.Blocked))
		for _, blocked := range result.Blocked {
			owner := blocked.OwnedBy
			if owner == "" {
				owner = "-"
			}
			fmt.Printf("  %-11s %-24s %s (owner: %s)\n", blocked.Kind, blocked.ID, blocked.Reason, owner)
		}
	}
	if result.Journaled {
		fmt.Println("Local sync journal updated; autosync reports reconciliation state.")
	}
}

func cmdProjectsList(cfg store.Config) {
	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	projects, err := s.ListProjectsWithStats()
	if err != nil {
		fatal(err)
	}

	if len(projects) == 0 {
		fmt.Println("No projects found.")
		return
	}

	fmt.Printf("Projects (%d):\n", len(projects))
	for _, p := range projects {
		sessionWord := "sessions"
		if p.SessionCount == 1 {
			sessionWord = "session"
		}
		fmt.Printf("  %-30s %4d obs   %3d %s\n",
			p.Name,
			p.ObservationCount,
			p.SessionCount, sessionWord,
		)
	}
}

// projectGroup represents a set of project names that should be merged.
type projectGroup struct {
	Names     []string
	Canonical string // normalized operational canonical
}

// groupSimilarProjects groups only project names that normalize to the same value.
// Similarity signals and shared directories are deliberately not merge eligibility.
func groupSimilarProjects(projects []store.ProjectStats) []projectGroup {
	byNormalizedName := make(map[string][]store.ProjectStats)
	for _, p := range projects {
		normalized, _ := store.NormalizeProject(p.Name)
		if normalized != "" {
			byNormalizedName[normalized] = append(byNormalizedName[normalized], p)
		}
	}

	// Build groups — skip singletons (no normalization-equivalent names).
	var groups []projectGroup
	for canonical, members := range byNormalizedName {
		if len(members) < 2 {
			continue
		}
		grpNames := make([]string, len(members))
		for i, member := range members {
			grpNames[i] = member.Name
		}
		sort.Strings(grpNames)
		groups = append(groups, projectGroup{
			Names:     grpNames,
			Canonical: canonical,
		})
	}
	// Sort groups by canonical name for deterministic output
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Canonical < groups[j].Canonical
	})
	return groups
}

func findNormalizationEquivalentProjects(name string, existing []string) []projectpkg.ProjectMatch {
	normalized, _ := store.NormalizeProject(name)
	if normalized == "" {
		return nil
	}

	var matches []projectpkg.ProjectMatch
	for _, candidate := range existing {
		candidateNormalized, _ := store.NormalizeProject(candidate)
		if candidate == name || candidateNormalized != normalized {
			continue
		}
		matches = append(matches, projectpkg.ProjectMatch{
			Name:      candidate,
			MatchType: "normalization-equivalent",
		})
	}
	return matches
}

// mergedRecordCount reports how many records a merge actually moved. The store
// validates every source against the canonical name and fail-closes on the ones
// it cannot prove normalization-equivalent, so a merge can succeed while moving
// nothing at all. Callers must report that outcome honestly instead of
// announcing a completed merge.
func mergedRecordCount(result *store.MergeResult) int64 {
	if result == nil {
		return 0
	}
	return result.ObservationsUpdated + result.SessionsUpdated
}

// reportUnmergedSources names the selected sources the store left untouched, so
// a partially applied merge never reads as a complete one.
func reportUnmergedSources(sources []string, result *store.MergeResult) {
	merged := make(map[string]bool, len(result.SourcesMerged))
	for _, name := range result.SourcesMerged {
		merged[name] = true
	}

	// SourcesMerged holds the trimmed spelling the store actually rewrote, while
	// sources holds the raw spellings the operator selected. Only a source that
	// is literally the canonical name was a no-op by request.
	var skipped []string
	seen := make(map[string]bool, len(sources))
	for _, source := range sources {
		if strings.TrimSpace(source) == "" || source == result.Canonical {
			continue
		}
		if merged[strings.TrimSpace(source)] || seen[source] {
			continue
		}
		seen[source] = true
		skipped = append(skipped, source)
	}
	if len(skipped) == 0 {
		return
	}
	fmt.Printf("  Not merged (no records moved): %s\n", strings.Join(skipped, ", "))
}

func cmdProjectsConsolidate(cfg store.Config) {
	doAll := false
	dryRun := false
	explicitProject := ""
	projectProvided := false
	for i := 3; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--all":
			doAll = true
		case "--dry-run":
			dryRun = true
		case "--project":
			projectProvided = true
			if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
				fatal(errors.New("--project requires a value"))
				return
			}
			explicitProject = strings.TrimSpace(os.Args[i+1])
			i++
		}
	}
	if projectProvided && explicitProject == "" {
		fatal(errors.New("--project requires a non-empty value"))
		return
	}
	if doAll && projectProvided {
		fatal(errors.New("--all cannot be combined with --project"))
		return
	}

	var detectedProject projectpkg.DetectionResult
	if !doAll {
		if projectProvided {
			detectedProject = projectpkg.DetectionResult{Project: explicitProject, Source: projectpkg.SourceExplicitOverride}
		} else {
			cwd, err := os.Getwd()
			if err != nil {
				fatal(err)
				return
			}
			detectedProject = detectProjectFull(cwd)
			if detectedProject.Error != nil || strings.TrimSpace(detectedProject.Project) == "" {
				if detectedProject.Error != nil {
					fatal(fmt.Errorf("project detection failed: %w", detectedProject.Error))
				} else {
					fatal(errors.New("project detection failed: empty project"))
				}
				return
			}
			if !dryRun {
				if err := projectpkg.RequireImplicitWriteAuthority(detectedProject); err != nil {
					failProjectResolution(detectedProject, err)
					return
				}
			}
		}
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	if !doAll {
		// Single-project mode: detect canonical project for cwd, find variants
		canonical, _ := store.NormalizeProject(detectedProject.Project)

		allNames, err := s.ListProjectNames()
		if err != nil {
			fatal(err)
		}

		// Check if the detected canonical actually exists in the DB.
		canonicalExists := false
		for _, n := range allNames {
			if n == canonical {
				canonicalExists = true
				break
			}
		}
		if !canonicalExists {
			fmt.Printf("Note: %q has no existing memories. Merging will move memories into this new project name.\n", canonical)
		}

		// Only normalization-equivalent legacy names are safe automatic candidates.
		similar := findNormalizationEquivalentProjects(canonical, allNames)

		allStats, _ := s.ListProjectsWithStats()
		statsMap := make(map[string]store.ProjectStats)
		for _, ps := range allStats {
			statsMap[ps.Name] = ps
		}

		if len(similar) == 0 {
			fmt.Printf("No similar project names found for %q. Nothing to consolidate.\n", canonical)
			return
		}

		fmt.Printf("Detected project: %q\n\n", canonical)
		fmt.Printf("Found similar project names:\n")
		for i, sm := range similar {
			obs := 0
			if ps, ok := statsMap[sm.Name]; ok {
				obs = ps.ObservationCount
			}
			fmt.Printf("  [%d] %-30s %3d obs  (%s)\n", i+1, sm.Name, obs, sm.MatchType)
		}

		if dryRun {
			fmt.Printf("\n[dry-run] Would merge %d project(s) into %q\n", len(similar), canonical)
			return
		}

		fmt.Printf("\nSelect which to merge into %q (comma-separated numbers, 'all', or 'none'): ", canonical)
		var answer string
		scanInputLine(&answer)
		answer = strings.TrimSpace(strings.ToLower(answer))

		if answer == "none" || answer == "n" || answer == "" {
			fmt.Println("Cancelled.")
			return
		}

		var sources []string
		if answer == "all" || answer == "a" {
			for _, sm := range similar {
				sources = append(sources, sm.Name)
			}
		} else {
			// Parse comma-separated indices
			for _, part := range strings.Split(answer, ",") {
				part = strings.TrimSpace(part)
				idx := 0
				if _, err := fmt.Sscanf(part, "%d", &idx); err != nil || idx < 1 || idx > len(similar) {
					fmt.Fprintf(os.Stderr, "Invalid selection: %q (expected 1-%d)\n", part, len(similar))
					return
				}
				sources = append(sources, similar[idx-1].Name)
			}
		}

		if len(sources) == 0 {
			fmt.Println("Nothing selected.")
			return
		}

		fmt.Printf("\nMerging %d project(s) into %q...\n", len(sources), canonical)
		result, err := s.MergeProjects(sources, canonical)
		if err != nil {
			fatal(err)
		}

		if mergedRecordCount(result) == 0 {
			fmt.Printf("Nothing merged into %q: the store moved no records for the %d selected project(s).\n",
				result.Canonical, len(sources))
			return
		}

		fmt.Printf("Done! Merged %d project(s) into %q:\n", len(result.SourcesMerged), result.Canonical)
		fmt.Printf("  Observations: %d\n", result.ObservationsUpdated)
		fmt.Printf("  Sessions:     %d\n", result.SessionsUpdated)
		fmt.Printf("  Proposals:    %d\n", result.MemoryProposalsUpdated)
		reportUnmergedSources(sources, result)
		return
	}

	// --all mode: group all projects by normalization equivalence.
	projects, err := s.ListProjectsWithStats()
	if err != nil {
		fatal(err)
	}

	groups := groupSimilarProjects(projects)

	if len(groups) == 0 {
		fmt.Println("No similar project name groups found.")
		return
	}

	fmt.Printf("Found %d group(s) of similar project names:\n\n", len(groups))

	// Build stats map for obs counts
	projectStatsMap := make(map[string]store.ProjectStats)
	for _, p := range projects {
		projectStatsMap[p.Name] = p
	}

	for i, g := range groups {
		fmt.Printf("Group %d:\n", i+1)
		for j, name := range g.Names {
			obs := 0
			if ps, ok := projectStatsMap[name]; ok {
				obs = ps.ObservationCount
			}
			marker := "  "
			if name == g.Canonical {
				marker = "→ "
			}
			fmt.Printf("  %s[%d] %-30s %3d obs\n", marker, j+1, name, obs)
		}
		fmt.Printf("  Suggested canonical: %q (→)\n", g.Canonical)

		if dryRun {
			fmt.Printf("  [dry-run] Would merge into %q\n\n", g.Canonical)
			continue
		}

		fmt.Printf("\n  Options:\n")
		fmt.Printf("    all     — merge everything into %q\n", g.Canonical)
		fmt.Printf("    1,3,... — merge only selected numbers into %q\n", g.Canonical)
		fmt.Printf("    rename  — choose a different canonical name\n")
		fmt.Printf("    skip    — don't touch this group\n")
		fmt.Printf("  Choice: ")
		var answer string
		scanInputLine(&answer)
		answer = strings.TrimSpace(strings.ToLower(answer))

		canonical := g.Canonical

		if answer == "skip" || answer == "s" || answer == "n" || answer == "" {
			fmt.Println("  Skipped.")
			fmt.Println()
			continue
		}

		renameTarget := ""
		if answer == "rename" || answer == "r" {
			fmt.Printf("  Enter canonical name: ")
			var input string
			scanInputLine(&input)
			input = strings.TrimSpace(input)
			if input == "" {
				fmt.Println("  Empty input, skipping.")
				fmt.Println()
				continue
			}
			// Merging only ever targets the group's normalization-equivalent
			// canonical; the rename is applied afterwards as an explicit
			// project migration so sync identity follows the new name.
			renameTarget, _ = store.NormalizeProject(input)
			answer = "all" // after rename, merge everything then migrate
		}
		mergeCanonical, _ := store.NormalizeProject(canonical)

		// Determine which sources to merge
		var sources []string
		if answer == "all" || answer == "a" || answer == "y" || answer == "yes" {
			for _, name := range g.Names {
				if name != mergeCanonical {
					sources = append(sources, name)
				}
			}
		} else {
			// Parse comma-separated indices
			for _, part := range strings.Split(answer, ",") {
				part = strings.TrimSpace(part)
				idx := 0
				if _, err := fmt.Sscanf(part, "%d", &idx); err != nil || idx < 1 || idx > len(g.Names) {
					fmt.Fprintf(os.Stderr, "  Invalid selection: %q (expected 1-%d)\n", part, len(g.Names))
					fmt.Println()
					continue
				}
				selected := g.Names[idx-1]
				if selected != mergeCanonical {
					sources = append(sources, selected)
				}
			}
		}
		if len(sources) == 0 {
			fmt.Println("  Nothing to merge.")
			fmt.Println()
			continue
		}

		result, err := s.MergeProjects(sources, mergeCanonical)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Error merging: %v\n", err)
			fmt.Println()
			continue
		}
		if mergedRecordCount(result) == 0 {
			fmt.Printf("  Nothing merged into %q: the store moved no records for the %d selected project(s).\n",
				mergeCanonical, len(sources))
		} else {
			fmt.Printf("  Merged: %d obs, %d sessions, %d proposals\n",
				result.ObservationsUpdated, result.SessionsUpdated,
				result.MemoryProposalsUpdated)
			reportUnmergedSources(sources, result)
		}

		if renameTarget != "" && renameTarget != mergeCanonical {
			migrateResult, err := s.MigrateProject(mergeCanonical, renameTarget)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  Error renaming %q → %q: %v\n", mergeCanonical, renameTarget, err)
				fmt.Println()
				continue
			}
			fmt.Printf("  Renamed %q → %q: %d obs, %d sessions\n",
				mergeCanonical, renameTarget,
				migrateResult.ObservationsUpdated, migrateResult.SessionsUpdated)
		}
		fmt.Println()
	}
}

func cmdProjectsPrune(cfg store.Config) {
	dryRun := false
	pathsOnly := false
	for i := 3; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--dry-run":
			dryRun = true
		case "--paths-only":
			pathsOnly = true
		}
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
	}
	defer s.Close()

	allStats, err := s.ListProjectsWithStats()
	if err != nil {
		fatal(err)
	}

	// Find projects with 0 observations.
	var candidates []store.ProjectStats
	for _, ps := range allStats {
		if ps.ObservationCount != 0 || (pathsOnly && !isPathLikeProjectName(ps.Name)) {
			continue
		}
		candidates = append(candidates, ps)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })

	if len(candidates) == 0 {
		if pathsOnly {
			fmt.Println("No path-named projects to prune.")
			return
		}
		fmt.Println("No empty projects to prune.")
		return
	}

	fmt.Printf("Found %d project(s) with 0 observations:\n\n", len(candidates))
	for i, ps := range candidates {
		fmt.Printf("  [%d] %-30s %3d sessions\n", i+1, ps.Name, ps.SessionCount)
	}

	if dryRun {
		fmt.Printf("\n[dry-run] Would prune %d project(s)\n", len(candidates))
		return
	}

	fmt.Printf("\nSelect which to prune (comma-separated numbers, 'all', or 'none'): ")
	var answer string
	scanInputLine(&answer)
	answer = strings.TrimSpace(strings.ToLower(answer))

	if answer == "none" || answer == "n" || answer == "" {
		fmt.Println("Cancelled.")
		return
	}

	var selected []store.ProjectStats
	if answer == "all" || answer == "a" {
		selected = candidates
	} else {
		for _, part := range strings.Split(answer, ",") {
			part = strings.TrimSpace(part)
			idx := 0
			if _, err := fmt.Sscanf(part, "%d", &idx); err != nil || idx < 1 || idx > len(candidates) {
				fmt.Fprintf(os.Stderr, "Invalid selection: %q (expected 1-%d)\n", part, len(candidates))
				return
			}
			selected = append(selected, candidates[idx-1])
		}
	}

	if len(selected) == 0 {
		fmt.Println("Nothing selected.")
		return
	}

	totalSessions := int64(0)
	successful := 0
	for _, ps := range selected {
		result, err := storePruneProject(s, ps.Name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error pruning %q: %v\n", ps.Name, err)
			continue
		}
		successful++
		totalSessions += result.SessionsDeleted
	}

	fmt.Printf("\nPruned %d project(s): %d sessions removed; Legacy prompts preserved.\n", successful, totalSessions)
}

func isPathLikeProjectName(name string) bool {
	return strings.ContainsAny(name, `/\`)
}

// cmdSetup classifies os.Args[2:] with a two-pass, order-independent
// algorithm (see openspec/changes/setup-protocol-flag/proposal.md,
// Approach; JD-014 residual fix). The FIRST pass scans every token and only
// accumulates classification state — it never dispatches mid-loop. This
// guarantees a token like --protocol=<v> is always parsed regardless of
// what precedes it (e.g. an earlier unrecognized hyphen-prefixed token no
// longer short-circuits the loop before later tokens are read). The SECOND
// pass dispatches once, in a fixed priority order, using the fully
// accumulated state: helpSeen > extraBareSeen > unknownFlagSeen > slug
// present > protocol-only > no args.
func cmdSetup(cfg store.Config) {
	args := os.Args[2:]
	if len(args) >= 2 && strings.EqualFold(strings.TrimSpace(args[0]), "status") && strings.EqualFold(strings.TrimSpace(args[1]), "codex") {
		cmdSetupStatusCodex(args[2:])
		return
	}

	var (
		helpSeen        bool
		protocolRaw     string
		protocolFlag    bool
		slug            string
		slugSeen        bool
		extraBareSeen   bool
		unknownFlagSeen bool
		development     bool
	)

	for i := 0; i < len(args); i++ {
		token := args[i]
		switch {
		case token == "--help" || token == "-h" || token == "help":
			helpSeen = true
		case token == "--protocol":
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				protocolRaw = args[i+1]
				i++
			} else {
				// Dangling --protocol: either it's the last token, or the
				// next token is itself a flag (e.g. `--protocol --help`).
				// Do NOT consume the next token as the value — leave it to
				// be classified normally on the next iteration so
				// `--protocol --help` still shows usage.
				protocolRaw = ""
			}
			protocolFlag = true
		case strings.HasPrefix(token, "--protocol="):
			protocolRaw = strings.TrimPrefix(token, "--protocol=")
			protocolFlag = true
		case token == "--development":
			development = true
		case strings.HasPrefix(token, "-"):
			// Unrecognized hyphen-prefixed token: record it but keep
			// scanning so a --protocol appearing later is still parsed
			// (JD-014 residual).
			unknownFlagSeen = true
		default:
			if slugSeen {
				extraBareSeen = true
			} else {
				slug = token
				slugSeen = true
			}
		}
	}

	switch {
	case helpSeen:
		printSetupUsage()
		return
	case extraBareSeen:
		fmt.Fprintln(os.Stderr, "usage: engram setup [<agent>] [--protocol=slim|full] [--development]")
		exitFunc(1)
		return
	case unknownFlagSeen:
		// Preserve the legacy fallback to the interactive menu (keeps
		// TestCmdSetupHyphenArgFallsBackToInteractive green), but forward
		// the already-parsed --protocol mode (if any) instead of dropping
		// it (JD-014), regardless of the unknown flag's position.
		mode := ""
		if protocolFlag {
			mode = resolveProtocolModeFlag(protocolRaw)
		}
		cmdSetupInteractive(cfg, mode, setup.InstallOptions{Version: version, Commit: commit, Development: development})
		return
	case slugSeen:
		result, err := setupInstallAgent(slug, setup.InstallOptions{Version: version, Commit: commit, Development: development})
		if err != nil {
			fatal(err)
		}
		if protocolFlag {
			applyProtocolMode(cfg, slug, resolveProtocolModeFlag(protocolRaw))
		}
		printSetupResult(result)
		printPostInstall(result)
	default:
		// No slug: interactive menu. Mode (if any) applies to whichever
		// slug the user selects.
		mode := ""
		if protocolFlag {
			mode = resolveProtocolModeFlag(protocolRaw)
		}
		cmdSetupInteractive(cfg, mode, setup.InstallOptions{Version: version, Commit: commit, Development: development})
	}
}

func cmdSetupStatusCodex(args []string) {
	jsonMode := false
	for _, arg := range args {
		if arg == "--json" {
			jsonMode = true
		}
	}
	for _, arg := range args {
		switch arg {
		case "--json":
		case "--help", "-h", "help":
			fmt.Println("usage: engram setup status codex [--json]")
			return
		default:
			failCLI(jsonMode, "invalid_argument", "usage: engram setup status codex [--json]", map[string]any{"argument": arg})
			return
		}
	}

	status, err := setupInspectCodexStatus(version, commit, currentCWD())
	if err != nil {
		failCLI(jsonMode, "codex_status_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		if err := writeCLIJSON(status); err != nil {
			failCLI(true, "encode_error", err.Error(), nil)
		}
		return
	}
	printCodexIntegrationStatus(status)
}

func printCodexIntegrationStatus(status setup.CodexIntegrationStatus) {
	fmt.Printf("Codex integration mode: %s\n", status.Mode)
	if status.Compatibility.SchemaVersion != "" {
		fmt.Printf("Protocol compatibility: %s (%s) — %s\n", status.Compatibility.Status, status.Compatibility.ReasonCode, status.Compatibility.Reason)
		for _, axis := range status.Compatibility.Axes {
			label := strings.ReplaceAll(axis.Name, "_", " ")
			switch axis.Name {
			case "managed_pack":
				label = "Managed Pack"
			case "engram_binary":
				label = "Engram binary"
			case "codex_plugin":
				label = "Codex plugin"
			case "protocol_contract":
				label = "Protocol contract"
			}
			rangeText := "undeclared"
			if axis.Supported != nil {
				rangeText = fmt.Sprintf("%d..%d", axis.Supported.Minimum, axis.Supported.Maximum)
			}
			fmt.Printf("  - %s: %s; Protocol %s; %s\n", label, axis.Version, rangeText, axis.Provenance)
		}
		if status.Compatibility.Intersection != nil {
			fmt.Printf("  Protocol intersection: %d..%d\n", status.Compatibility.Intersection.Minimum, status.Compatibility.Intersection.Maximum)
		}
	}
	if status.LifecycleCanary.Treatment != "" {
		fmt.Printf("Codex lifecycle treatment: %s (canary enabled: %t; source: %s)\n",
			status.LifecycleCanary.Treatment, status.LifecycleCanary.Enabled, status.LifecycleCanary.SelectionSource)
		fmt.Printf("  Activation cue: %s; injection limit: %d UTF-8 bytes\n",
			status.LifecycleCanary.ActivationCue, status.LifecycleCanary.InjectionLimitUTF8Bytes)
		fmt.Printf("  Capture state: prompt=%s; subagent=%s\n",
			status.PromptCapture.CurrentConsent, status.SubagentCapture.State)
		metrics := status.LifecycleCanary.Metrics
		if metrics.State == setup.CodexLifecycleMetricsObserved {
			fmt.Printf("  Lifecycle metrics: %s; events=%d; p50=%gms; p95=%gms; total injected=%d UTF-8 bytes; average=%g\n",
				metrics.State, metrics.Events, metrics.P50LatencyMillis, metrics.P95LatencyMillis,
				metrics.TotalInjectedUTF8Bytes, metrics.AverageInjectedUTF8Bytes)
		} else {
			fmt.Printf("  Lifecycle metrics: %s (%s)\n", metrics.State, metrics.ReasonCode)
		}
	}
	for _, check := range status.Checks {
		fmt.Printf("  - %s: %s — %s\n", check.Capability, check.Status, check.Reason)
		for _, evidence := range check.Evidence {
			fmt.Printf("      %s: %s\n", evidence.Name, evidence.Value)
		}
	}
}

// cmdSetupInteractive renders the agent picker and installs the chosen
// agent. mode is the already-resolved --protocol value ("slim"/"full") from
// a slug-less invocation, or "" when --protocol was not given at all.
func cmdSetupInteractive(cfg store.Config, mode string, options setup.InstallOptions) {
	agents := setupSupportedAgents()

	fmt.Println("engram setup — Install agent plugin")
	fmt.Println()
	fmt.Println("Which agent do you want to set up?")
	fmt.Println()

	for i, a := range agents {
		fmt.Printf("  [%d] %s\n", i+1, a.Description)
		fmt.Printf("      Install to: %s\n\n", a.InstallDir)
	}

	fmt.Print("Enter choice (1-", len(agents), "): ")
	var input string
	scanInputLine(&input)

	choice, err := strconv.Atoi(strings.TrimSpace(input))
	if err != nil || choice < 1 || choice > len(agents) {
		fmt.Fprintln(os.Stderr, "Invalid choice.")
		exitFunc(1)
	}

	selected := agents[choice-1]
	fmt.Printf("\nInstalling %s plugin...\n", selected.Name)

	result, err := setupInstallAgent(selected.Name, options)
	if err != nil {
		fatal(err)
	}
	if mode != "" {
		applyProtocolMode(cfg, selected.Name, mode)
	}

	printSetupResult(result)
	printPostInstall(result)
}

func printSetupResult(result *setup.Result) {
	if result.Agent != "codex" || len(result.Checks) == 0 {
		fmt.Printf("✓ Installed %s plugin (%d files)\n", result.Agent, result.Files)
		fmt.Printf("  → %s\n", result.Destination)
		return
	}

	if result.Complete {
		fmt.Printf("✓ Codex setup complete (%d files changed)\n", result.Files)
	} else {
		fmt.Printf("⚠ Codex setup incomplete (%d files changed)\n", result.Files)
	}
	fmt.Printf("  → %s\n", result.Destination)
	for _, check := range result.Checks {
		fmt.Printf("  - %s: %s — %s\n", check.Capability, check.Status, check.Detail)
	}
	if len(result.Preserved) > 0 {
		fmt.Printf("  - preserved: %s\n", strings.Join(result.Preserved, ", "))
	}
}

// printSetupUsage prints `engram setup --help` output. Its Flags section
// MUST contain the literal "--protocol" (Guarantee 1); it must never read
// stdin (Guarantee 2 — safe under a detached/non-TTY stdin).
func printSetupUsage() {
	fmt.Println("usage: engram setup [<agent>] [--protocol=slim|full] [--development]")
	fmt.Println("       engram setup status codex [--json]")
	fmt.Println()
	fmt.Println("Install an agent plugin (claude-code, opencode, codex, ...).")
	fmt.Println("Without <agent>, shows an interactive menu.")
	fmt.Println("The status command reports a read-only Codex capability snapshot.")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --protocol=<slim|full>  Set the session-start protocol verbosity for the")
	fmt.Println("                          installed agent slug (default: full). Unknown or")
	fmt.Println("                          missing values fall back to full with a warning.")
	fmt.Println("                          slim currently only takes effect for claude-code,")
	fmt.Println("                          and only when the installed engram is >= 1.4.0.")
	fmt.Println("  --development           Allow Codex setup to follow the moving main branch.")
	fmt.Println("  --help, -h              Show this help and exit.")
}

// resolveProtocolModeFlag normalizes a --protocol value to "slim" or "full".
// Unknown or empty values fall back to "full" with a non-fatal stderr
// warning — an invalid --protocol value never fails `engram setup`.
func resolveProtocolModeFlag(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch v {
	case setup.ProtocolModeSlim:
		return setup.ProtocolModeSlim
	case setup.ProtocolModeFull:
		return setup.ProtocolModeFull
	default:
		fmt.Fprintf(os.Stderr, "warning: unknown --protocol value %q, defaulting to full\n", raw)
		return setup.ProtocolModeFull
	}
}

// applyProtocolMode persists the resolved protocol mode for slug, using the
// SAME cfg.DataDir main() resolved (ENGRAM_DATA_DIR override included) so the
// `protocol-mode` subcommand's read path matches this write path (JD-005). A
// write failure is reported as a non-fatal warning — it never fails setup.
func applyProtocolMode(cfg store.Config, slug, mode string) {
	if err := setup.WriteProtocolMode(cfg.DataDir, slug, mode); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not persist protocol mode: %v\n", err)
	}
}

// cmdProtocolMode implements `engram protocol-mode <slug>`: prints "slim" to
// stdout ONLY when the persisted mode for slug is "slim" AND the running
// binary's version meets the slim floor (>= 1.4.0); any other case
// (unrecognized slug, missing/corrupted mode file, version below floor,
// unparseable version) prints "full". All branching lives here in Go so it
// runs under `go test` — the Claude Code hook scripts only read this single
// line of stdout.
func cmdProtocolMode(cfg store.Config) {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: engram protocol-mode <slug>")
		exitFunc(1)
		return
	}
	slug := os.Args[2]

	mode := setup.ReadProtocolMode(cfg.DataDir, slug)
	if mode == setup.ProtocolModeSlim && meetsProtocolVersionFloor(version) {
		fmt.Println(setup.ProtocolModeSlim)
		return
	}
	fmt.Println(setup.ProtocolModeFull)
}

// protocolVersionFloor is the minimum engram version required to honor a
// persisted "slim" protocol-mode: the slim status block relies on the
// MCP serverInstructions duplication fix shipped in this release.
var protocolVersionFloor = [3]int{1, 4, 0}

// meetsProtocolVersionFloor reports whether v (e.g. "1.4.0", "v1.5.2", or the
// build-time "dev" placeholder) is >= protocolVersionFloor. Any unparseable
// or empty value returns false — the caller then falls back to "full".
func meetsProtocolVersionFloor(v string) bool {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if v == "" || v == "dev" {
		return false
	}

	segments := strings.SplitN(v, ".", 3)
	var parts [3]int
	for i, s := range segments {
		if i >= 3 {
			break
		}
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return false
		}
		parts[i] = n
	}

	for i := 0; i < 3; i++ {
		if parts[i] != protocolVersionFloor[i] {
			return parts[i] > protocolVersionFloor[i]
		}
	}
	return true
}

func printPostInstall(result *setup.Result) {
	switch result.Agent {
	case "opencode":
		fmt.Println("\nNext steps:")
		fmt.Println("  1. Restart OpenCode — plugin + MCP server are ready")
		fmt.Println("  2. The plugin auto-starts the Engram HTTP server when needed")
		if result.TUIPluginEnabled {
			fmt.Println("\nAlso enabled: opencode-subagent-statusline in tui.json — sub-agent activity in the sidebar/footer.")
		}
	case "pi":
		fmt.Println("\nNext steps:")
		fmt.Println("  1. Restart Pi so packages and MCP config are reloaded")
		fmt.Println("  2. Verify with: pi list")
	case "claude-code":
		// Offer to add engram tools to the permissions allowlist
		fmt.Print("\nAdd engram tools to ~/.claude/settings.json allowlist?\n")
		fmt.Print("This prevents Claude Code from asking permission on every tool call.\n")
		fmt.Print("Add to allowlist? (y/N): ")
		var answer string
		scanInputLine(&answer)
		answer = strings.TrimSpace(strings.ToLower(answer))
		if answer == "y" || answer == "yes" {
			if err := setupAddClaudeCodeAllowlist(); err != nil {
				fmt.Fprintf(os.Stderr, "  warning: could not update allowlist: %v\n", err)
				fmt.Fprintln(os.Stderr, "  You can add them manually to permissions.allow in ~/.claude/settings.json")
			} else {
				fmt.Println("  ✓ Engram tools added to allowlist")
			}
		} else {
			fmt.Println("  Skipped. You can add them later to permissions.allow in ~/.claude/settings.json")
		}

		fmt.Println("\nNext steps:")
		fmt.Println("  1. Restart Claude Code — the plugin is active immediately")
		fmt.Println("  2. Verify with: claude plugin list")
		fmt.Println("  3. MCP config written to ~/.claude/mcp/engram.json using absolute binary path")
		fmt.Println("     (survives plugin auto-updates; re-run 'engram setup claude-code' if you move the binary)")
	default:
		// Every other agent's "next steps" are declared as data in the registry,
		// so the message is rendered generically instead of one case per agent.
		printNextSteps(setup.PostInstallSteps(result.Agent))
	}
}

// printNextSteps renders a numbered "Next steps" list, or nothing when empty.
func printNextSteps(steps []string) {
	if len(steps) == 0 {
		return
	}
	fmt.Println("\nNext steps:")
	for i, step := range steps {
		fmt.Printf("  %d. %s\n", i+1, step)
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func printSaveUsage() {
	fmt.Printf(`Usage:
  %s
  %s

Save a memory and surface pending conflict candidates.

Flags:
  --title TITLE       Memory title (named form)
  --content CONTENT   Memory content (named form)
  --type TYPE         Memory type (default: manual)
  --project PROJECT   Project name (defaults to current project detection)
  --scope SCOPE       Memory scope (default: project)
  --topic-key KEY     Stable topic key (--topic is an alias)
  --json              Write structured JSON output
`, savePositionalUsage, saveNamedUsage)
}

func printUsage() {
	fmt.Printf(`engram v%s — Persistent memory for AI coding agents

Usage:
  engram <command> [arguments]

Commands:
  serve [port]       Start HTTP API server (default: 7437)
  mcp [--tools=PROFILE] [--project NAME]
                     Start MCP server (stdio transport, for any AI agent)
                       Profiles: agent (5 tools), curation (11 tools), lifecycle (4 tools),
                                 admin (4 tools), all (default, 24)
                       Combine: --tools=agent,curation,lifecycle,admin or pick individual tools
                       Example: engram mcp --tools=agent
                       --project NAME  Set process-level default project (overrides cwd detection).
                                       Also accepted as ENGRAM_PROJECT=NAME env var.
  tui                Launch interactive terminal UI
  test [suite] [--quick] [--json]
                     Run isolated local reliability and performance self-tests
                       suites: reliability, performance (default: both)
  search <query>     Search memories [--type TYPE] [--project PROJECT] [--scope SCOPE] [--limit N]
                       [--all-projects] [--match-mode all|any]
                       [--host HOST --session-id ID --root-turn-id ID] [--json]
  save <title> <content>
                     Save a memory [--type TYPE] [--project PROJECT] [--scope SCOPE]
                       [--topic-key KEY] [--json]
  save --title TITLE --content CONTENT
                     Equivalent named-input form with the same flags
  get <obs_id>       Explicit curation: get complete Memory and relations [--json]
  get --recall-id ID --result-id ID [--position BYTES]
                       Get one selected Memory segment (max 16 KiB) [--json]
  update <obs_id>    Partially update a memory [--title V] [--content V] [--type V]
                       [--scope V] [--topic-key V|--clear-topic-key] [--json]
  review list        List due memories [--project P|--all-projects] [--limit N] [--json]
  review mark <id>   Mark one memory reviewed (local only) [--json]
  pin|unpin <id>     Change local context priority [--json]
  current-project    Inspect project detection without failing on ambiguity [--json]
  suggest-topic-key  Suggest a key [--type V] [--title V|--content V] [--json]
  delete <obs_id>    Delete an observation [--hard] (soft-delete by default; --hard removes permanently)
  delete session <id>
                     Delete a session by ID (session must have no observations)
  delete project <name> [--hard]
                     Cascade-delete a project: soft-deletes observations (or hard if --hard),
                     preserves Legacy prompts; with --hard removes only unreferenced sessions
  timeline <obs_id>  Show chronological context around an observation [--before N] [--after N]
  conflicts <sub>   Inspect and manage memory conflict relations
                       list     [--project P]  [--status S]  [--since RFC3339]  [--limit N]
                       show     <relation_id>
                       stats    [--project P]
                       scan     [--project P]  [--since RFC3339]  [--limit N]  [--cursor ID]
                                [--dry-run]  [--apply]  [--max-insert N]  [--semantic]  [--concurrency N]  [--timeout-per-call SECONDS]
                                [--max-semantic N]  [--yes]
				       deferred [--status S]  [--limit N]  [--inspect SYNC_ID]  [--replay]
				                [--recover SYNC_ID [--json]]
                       judge    <judgment-id> --relation R [--confidence N] [--json]
                       compare  <id-a> <id-b> --relation R --confidence N --reasoning TEXT [--json]
  doctor             Run read-only operational diagnostics [--json] [--project P] [--check CODE]
  context [project]  Show recent context from previous sessions [--scope SCOPE] [--json]
  activation-study   Verify, run, or analyze the frozen Codex activation cohort
  recall-study       Verify, execute, or publish the frozen paired Recall study
  recall-baseline    Record and report content-free local operational evidence
                       record|report|power|purge (see recall-baseline --help)
  recall-feedback    Report aggregate-only local Recall utility and quality metrics
                       report [--json]
  checkpoint record  Record a root-turn Memory checkpoint
                       --host HOST --session-id ID --root-turn-id ID
                       --disposition saved|needs_review|skipped [reference flags] [--json]
  checkpoint status  Inspect one exact root-turn Memory checkpoint
                       --host HOST --session-id ID --root-turn-id ID [--json]
  checkpoint verify-stop
                     Verify one Codex Stop event against the checkpoint ledger
                       --host HOST (reads the Stop event from stdin)
  capture status     Inspect Diagnostic capture consent without reading captured content
                       [--project PROJECT] [--type prompt|subagent_output] [--session-id ID] [--json]
  capture enable     Enable explicit local Diagnostic capture consent
                       --project PROJECT --type prompt|subagent_output [--session-id ID --expires-at RFC3339]
                       [--retention-days 1..30] [--json]
  capture disable    Revoke consent without purging captured content
                       --project PROJECT --type prompt|subagent_output [--session-id ID] [--json]
  capture purge      Permanently purge Diagnostic captures without changing consent
                       --project PROJECT --type prompt|subagent_output [--yes] [--json]
  legacy-prompts     Explicitly manage the frozen local Legacy prompt archive
                       inventory|access|export|purge (see legacy-prompts --help)
  stats              Show memory system statistics
  export [file]      Export all memories to JSON (default: engram-export.json)
  import <file>      Import memories from a JSON export file
  projects list      List all projects with Memory and session counts
  projects consolidate [--project NAME | --all] [--dry-run]
                     Merge similar project names into one canonical name
                       --project  Explicit canonical project for single-project mode
                       --all      Scan ALL projects for similar name groups
                       --dry-run  Preview what would be merged (no changes)
  projects merge --from SOURCE [--from SOURCE...] --to TARGET [--dry-run] [--yes] [--json]
                     Deterministically merge named projects
  projects prune [--dry-run] [--paths-only]
                     Remove projects with no observations
                       --dry-run     Preview projects without removing data
                       --paths-only  Limit pruning to project names containing / or \
  projects rescue-ownership --project NAME [--session ID] [--observation ID] [--prompt ID]
                     Assign explicit ownership to legacy unowned records
                     (--prompt is retained only to report legacy_prompt_frozen)
  setup [agent]      Install/setup agent integration (opencode, pi, claude-code,
                     gemini-cli, codex, antigravity-cli, windsurf, qwen, kiro,
                     cursor, vscode-copilot, kilocode)
  setup status codex Report the read-only Codex integration capability snapshot [--json]
  sync               Export new memories as compressed chunk to .engram/
                         --import   Import new chunks from .engram/ into local DB
                         --status   Show sync status
                         --project  Filter export to a specific project
                         --all      Export ALL projects (ignore directory-based filter)
		                 --cloud    Run sync against configured cloud endpoint (requires explicit --project)
	  cloud <subcommand> Cloud integration commands (opt-in)
	                        status     Show cloud config status
	                        enroll     Enroll a project for cloud sync
	                        config     Set cloud server URL
	                        serve      Run cloud backend + dashboard
  obsidian-export    Export memories to an Obsidian-compatible markdown vault
                       --vault         Path to Obsidian vault root (required)
                       --project       Filter export to a single project (optional)
                       --limit         Cap exported observations at N (optional)
                       --since         Export only observations after this date, e.g. 2026-01-01 (optional)
                       --force         Ignore incremental state, full re-export (optional)
                       --graph-config  Graph layout mode: preserve|force|skip (default: preserve)
                       --watch         Enable auto-sync mode (runs on interval until Ctrl+C)
                       --interval      Sync interval for --watch mode (default: 10m, minimum: 1m)

  version            Print version
  help               Show this help

Environment:
  ENGRAM_DATA_DIR    Override data directory (default: ~/.engram)
  ENGRAM_PORT        Override HTTP server port (default: 7437)
  ENGRAM_CODEX_RECALL_CANARY
                     Opt into targeted-recall or targeted-recall-exact-session.
                     Unset preserves broad context; unknown values do not enable a canary.
  ENGRAM_PROJECT     Process-level default project override, applied by every entry point
                     with one precedence rule: explicit request project (engram save --project,
                     an MCP tool project argument) > process override (engram mcp --project,
                     then ENGRAM_PROJECT) > cwd detection.
                     For "engram save": owns the observation when --project is omitted.
                     For "engram serve": fallback for GET /sync/status with no project param.
                     For "engram mcp": sets DefaultProject, overriding cwd detection for all tools.
  ENGRAM_HTTP_TOKEN  Optional Bearer auth for local HTTP server (engram serve).
                     When set, the following routes require Authorization: Bearer <token>:
                       DELETE /sessions/{id}, DELETE /observations/{id}, DELETE /prompts/{id},
                       GET /export, POST /import
                     POST /projects/rescue-ownership
                       always require a configured token and matching Bearer credential; unset returns 503.
                     Comparison is constant-time. Token is read per-request (no restart needed).
                     Other routes remain open when unset (zero-config default).
  ENGRAM_TIMEZONE    Timezone for timestamp display in TUI and cloud dashboard.
                     Accepts any IANA zone name (e.g. America/New_York, Europe/Berlin).
                     Falls back to system local time when unset or invalid.
  ENGRAM_AGENT_CLI   LLM runner for conflicts scan --semantic (claude or opencode)
  ENGRAM_CLOUD_AUTOSYNC
                     Set to 1 to enable background autosync; also requires
                     ENGRAM_CLOUD_TOKEN and ENGRAM_CLOUD_SERVER
  ENGRAM_CLOUD_SERVER
                     Cloud server URL used by autosync and engram sync --cloud
  ENGRAM_DATABASE_URL
                     Postgres DSN for engram cloud serve
  ENGRAM_CLOUD_HOST  Bind host for engram cloud serve (default: 127.0.0.1)
  ENGRAM_CLOUD_MAX_PUSH_BYTES
                     Max cloud push payload bytes (default: 8388608)
  ENGRAM_CLOUD_TOKEN Bearer token required in authenticated cloud serve mode
  ENGRAM_CLOUD_INSECURE_NO_AUTH
                     Set to 1 ONLY for local insecure cloud serve mode (no auth)
                     Cannot be combined with ENGRAM_CLOUD_TOKEN
                     Cannot be combined with ENGRAM_CLOUD_ADMIN
  ENGRAM_CLOUD_ALLOWED_PROJECTS
                     Comma-separated project allowlist enforced by cloud server.
                     Required for cloud serve in BOTH token auth and insecure no-auth mode.
                     Use * to allow all projects (dev/internal deploys).
  ENGRAM_JWT_SECRET  Required in authenticated cloud serve mode (ENGRAM_CLOUD_TOKEN set);
                     must be explicitly set to a non-default value
  ENGRAM_CLOUD_ADMIN Optional admin-only dashboard token in authenticated mode
                     Ignored/rejected in insecure mode (ENGRAM_CLOUD_INSECURE_NO_AUTH=1)

MCP Configuration (add to your agent's config):
  {
    "mcp": {
      "engram": {
        "type": "stdio",
        "command": "engram",
        "args": ["mcp", "--tools=agent"]
      }
    }
  }
`, version)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "engram: %s\n", err)
	exitFunc(1)
}

// resolveHomeFallback tries platform-specific environment variables to find
// a home directory when os.UserHomeDir() fails. This commonly happens on
// Windows when engram is launched as an MCP subprocess without full env
// propagation.
func resolveHomeFallback() string {
	// Windows: try common env vars that might be set even when
	// %USERPROFILE% is missing.
	for _, env := range []string{"USERPROFILE", "HOME", "LOCALAPPDATA"} {
		if v := os.Getenv(env); v != "" {
			if env == "LOCALAPPDATA" {
				// LOCALAPPDATA is C:\Users\<user>\AppData\Local — go up two levels.
				parent := filepath.Dir(filepath.Dir(v))
				if parent != "." && parent != v {
					return parent
				}
			}
			return v
		}
	}

	// Unix: $HOME should always work, but try passwd-style fallback.
	if v := os.Getenv("HOME"); v != "" {
		return v
	}

	return ""
}

// migrateOrphanedDB checks for engram databases that ended up in wrong
// locations (e.g. drive root on Windows when UserHomeDir failed silently)
// and moves them to the correct location if the correct location has no DB.
func migrateOrphanedDB(correctDir string) {
	correctDB := filepath.Join(correctDir, "engram.db")

	// If the correct DB already exists, nothing to migrate.
	if _, err := os.Stat(correctDB); err == nil {
		return
	}

	// Known wrong locations: relative ".engram" resolved from common roots.
	// On Windows this typically ends up at C:\.engram or D:\.engram.
	candidates := []string{
		filepath.Join(string(filepath.Separator), ".engram", "engram.db"),
	}

	// On Windows, check all drive letter roots.
	if filepath.Separator == '\\' {
		for _, drive := range "CDEFGHIJ" {
			candidates = append(candidates,
				filepath.Join(string(drive)+":\\", ".engram", "engram.db"),
			)
		}
	}

	for _, candidate := range candidates {
		if candidate == correctDB {
			continue
		}
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() {
			continue
		}

		// Found an orphaned DB — migrate it.
		log.Printf("[engram] found orphaned database at %s, migrating to %s", candidate, correctDB)

		if err := os.MkdirAll(correctDir, 0755); err != nil {
			log.Printf("[engram] migration failed (create dir): %v", err)
			return
		}

		// Move DB and WAL/SHM files if they exist.
		for _, suffix := range []string{"", "-wal", "-shm"} {
			src := candidate + suffix
			dst := correctDB + suffix
			if _, statErr := os.Stat(src); statErr != nil {
				continue
			}
			if renameErr := os.Rename(src, dst); renameErr != nil {
				log.Printf("[engram] migration failed (move %s): %v", filepath.Base(src), renameErr)
				return
			}
		}

		// Clean up empty orphaned directory.
		orphanDir := filepath.Dir(candidate)
		entries, _ := os.ReadDir(orphanDir)
		if len(entries) == 0 {
			os.Remove(orphanDir)
		}

		log.Printf("[engram] migration complete — memories recovered")
		return
	}
}

func truncate(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max]) + "..."
}
