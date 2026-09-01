package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

const legacyPromptExportSchemaVersion = 1

type legacyPromptScopeFlags struct {
	Project string
	Unowned bool
	All     bool
}

type legacyPromptExportEnvelope struct {
	SchemaVersion int                                 `json:"schema_version"`
	Archive       string                              `json:"archive"`
	Result        *memoryops.LegacyPromptExportResult `json:"result"`
}

func cmdLegacyPrompts(cfg store.Config) {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "legacy-prompts requires inventory, access, export, or purge", nil)
		return
	}
	switch strings.ToLower(strings.TrimSpace(os.Args[2])) {
	case "inventory":
		cmdLegacyPromptsInventory(cfg, os.Args[3:])
	case "access":
		cmdLegacyPromptsAccess(cfg, os.Args[3:])
	case "export":
		cmdLegacyPromptsExport(cfg, os.Args[3:])
	case "purge":
		cmdLegacyPromptsPurge(cfg, os.Args[3:])
	case "help", "--help", "-h":
		printLegacyPromptsUsage()
	default:
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unknown legacy-prompts command %q", os.Args[2]), nil)
	}
}

func cmdLegacyPromptsInventory(cfg store.Config, args []string) {
	set, scope, jsonMode := newLegacyPromptFlagSet("inventory", true)
	if !parseLegacyPromptFlags(set, args, *jsonMode) {
		return
	}
	s, ok := openLegacyPromptStore(cfg, *jsonMode)
	if !ok {
		return
	}
	defer s.Close()
	result, err := memoryops.New(s).InventoryLegacyPrompts(memoryops.LegacyPromptInventoryInput{
		Project: scope.Project, Unowned: scope.Unowned, All: scope.All,
	})
	if err != nil {
		failCLI(*jsonMode, "legacy_prompt_inventory_failed", err.Error(), nil)
		return
	}
	if *jsonMode {
		_ = writeCLIJSON(result)
		return
	}
	fmt.Printf("Legacy prompt archive: %d prompts across %d sessions", result.Count, result.Sessions)
	if result.OldestAt != "" {
		fmt.Printf(" (%s to %s)", result.OldestAt, result.NewestAt)
	}
	fmt.Println(".")
}

func cmdLegacyPromptsAccess(cfg store.Config, args []string) {
	set, scope, jsonMode := newLegacyPromptFlagSet("access", false)
	limit := memoryops.DefaultLegacyPromptAccessLimit
	var cursor int64
	set.IntVar(&limit, "limit", limit, "maximum records")
	set.Int64Var(&cursor, "cursor", 0, "exclusive archive ID cursor")
	if !parseLegacyPromptFlags(set, args, *jsonMode) {
		return
	}
	s, ok := openLegacyPromptStore(cfg, *jsonMode)
	if !ok {
		return
	}
	defer s.Close()
	result, err := memoryops.New(s).AccessLegacyPrompts(memoryops.LegacyPromptAccessInput{
		Project: scope.Project, Unowned: scope.Unowned, All: scope.All, Cursor: cursor, Limit: limit,
	})
	if err != nil {
		failCLI(*jsonMode, "legacy_prompt_access_failed", err.Error(), nil)
		return
	}
	if *jsonMode {
		_ = writeCLIJSON(result)
		return
	}
	for _, prompt := range result.Prompts {
		fmt.Printf("[%d] %s %s\n%s\n\n", prompt.ID, prompt.SessionID, prompt.CreatedAt, prompt.Content)
	}
	if result.NextCursor != 0 {
		fmt.Printf("Next cursor: %d\n", result.NextCursor)
	}
}

func cmdLegacyPromptsExport(cfg store.Config, args []string) {
	set, scope, jsonMode := newLegacyPromptFlagSet("export", true)
	output := ""
	set.StringVar(&output, "output", "", "private JSON output path")
	if !parseLegacyPromptFlags(set, args, *jsonMode) {
		return
	}
	if strings.TrimSpace(output) == "" {
		failCLI(*jsonMode, "invalid_arguments", "legacy-prompts export requires --output", nil)
		return
	}
	s, ok := openLegacyPromptStore(cfg, *jsonMode)
	if !ok {
		return
	}
	defer s.Close()
	result, err := memoryops.New(s).ExportLegacyPrompts(memoryops.LegacyPromptExportInput{
		Project: scope.Project, Unowned: scope.Unowned, All: scope.All,
	})
	if err != nil {
		failCLI(*jsonMode, "legacy_prompt_export_failed", err.Error(), nil)
		return
	}
	envelope := legacyPromptExportEnvelope{SchemaVersion: legacyPromptExportSchemaVersion, Archive: "legacy_prompts", Result: result}
	if err := writePrivateJSONAtomic(output, envelope); err != nil {
		failCLI(*jsonMode, "legacy_prompt_export_failed", err.Error(), nil)
		return
	}
	response := map[string]any{"exported": len(result.Prompts), "output": output, "schema_version": legacyPromptExportSchemaVersion}
	if *jsonMode {
		_ = writeCLIJSON(response)
		return
	}
	fmt.Printf("Exported %d Legacy prompts to %s (private mode).\n", len(result.Prompts), output)
}

func cmdLegacyPromptsPurge(cfg store.Config, args []string) {
	set, scope, jsonMode := newLegacyPromptFlagSet("purge", false)
	yes := false
	set.BoolVar(&yes, "yes", false, "confirm destructive purge")
	if !parseLegacyPromptFlags(set, args, *jsonMode) {
		return
	}
	s, ok := openLegacyPromptStore(cfg, *jsonMode)
	if !ok {
		return
	}
	defer s.Close()
	service := memoryops.New(s)
	preview, err := service.InventoryLegacyPrompts(memoryops.LegacyPromptInventoryInput{
		Project: scope.Project, Unowned: scope.Unowned, All: scope.All,
	})
	if err != nil {
		failCLI(*jsonMode, "legacy_prompt_inventory_failed", err.Error(), nil)
		return
	}
	if !yes {
		if *jsonMode {
			failCLI(true, "confirmation_required", "legacy-prompts purge requires --yes in JSON mode", map[string]any{"preview": preview})
			return
		}
		fmt.Printf("Permanently purge %d Legacy prompts? [y/N] ", preview.Count)
		var answer string
		_, _ = scanInputLine(&answer)
		if answer = strings.ToLower(strings.TrimSpace(answer)); answer != "y" && answer != "yes" {
			fmt.Println("Aborted.")
			return
		}
	}
	result, err := service.PurgeLegacyPrompts(memoryops.LegacyPromptPurgeInput{
		Project: scope.Project, Unowned: scope.Unowned, All: scope.All,
	})
	if err != nil {
		failCLI(*jsonMode, "legacy_prompt_purge_failed", err.Error(), nil)
		return
	}
	if *jsonMode {
		_ = writeCLIJSON(result)
		return
	}
	fmt.Printf("Purged %d Legacy prompts locally.\n", result.Deleted)
}

func newLegacyPromptFlagSet(name string, allowAll bool) (*flag.FlagSet, *legacyPromptScopeFlags, *bool) {
	set := flag.NewFlagSet("legacy-prompts "+name, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	scope := &legacyPromptScopeFlags{}
	jsonMode := new(bool)
	set.StringVar(&scope.Project, "project", "", "exact project name")
	set.BoolVar(&scope.Unowned, "unowned", false, "select unowned archive rows")
	if allowAll {
		set.BoolVar(&scope.All, "all", false, "select the complete archive")
	}
	set.BoolVar(jsonMode, "json", false, "emit JSON")
	return set, scope, jsonMode
}

func parseLegacyPromptFlags(set *flag.FlagSet, args []string, jsonMode bool) bool {
	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printLegacyPromptsUsage()
			return false
		}
		failCLI(jsonMode || hasArg("--json"), "invalid_arguments", err.Error(), nil)
		return false
	}
	if set.NArg() != 0 {
		failCLI(jsonMode || hasArg("--json"), "invalid_arguments", fmt.Sprintf("unexpected argument %q", set.Arg(0)), nil)
		return false
	}
	return true
}

func openLegacyPromptStore(cfg store.Config, jsonMode bool) (*store.Store, bool) {
	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return nil, false
	}
	return s, true
}

func writePrivateJSONAtomic(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".engram-legacy-prompts-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	cleanup := true
	defer func() {
		_ = temporary.Close()
		if cleanup {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(append(encoded, '\n')); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func printLegacyPromptsUsage() {
	fmt.Println(`usage: engram legacy-prompts inventory (--project PROJECT | --unowned | --all) [--json]
       engram legacy-prompts access (--project PROJECT | --unowned) [--limit N] [--cursor ID] [--json]
       engram legacy-prompts export (--project PROJECT | --unowned | --all) --output FILE [--json]
       engram legacy-prompts purge (--project PROJECT | --unowned) [--yes] [--json]`)
}
