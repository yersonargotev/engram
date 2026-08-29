package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/store"
)

func cmdGet(cfg store.Config) {
	if len(os.Args) < 3 {
		failCLI(hasArg("--json"), "invalid_arguments", "usage: engram get <observation_id> [--json]", nil)
		return
	}
	jsonMode := hasArg("--json")
	for _, arg := range os.Args[3:] {
		if arg != "--json" {
			failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", arg), nil)
			return
		}
	}
	id, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil || id <= 0 {
		failCLI(jsonMode, "invalid_observation_id", fmt.Sprintf("invalid observation id %q", os.Args[2]), nil)
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	result, err := memoryops.New(s).Get(id)
	if err != nil {
		failCLI(jsonMode, "observation_not_found", fmt.Sprintf("observation #%d not found", id), nil)
		return
	}
	obs := result.Observation
	if jsonMode {
		_ = writeCLIJSON(map[string]any{"observation": obs, "state": obs.State(), "pinned": obs.Pinned, "relations": result.Relations})
		return
	}
	fmt.Printf("#%d [%s] %s\n%s\nSession: %s\n", obs.ID, obs.Type, obs.Title, obs.Content, obs.SessionID)
	if obs.Project != nil {
		fmt.Printf("Project: %s\n", *obs.Project)
	}
	fmt.Printf("Scope: %s\nCreated: %s\n", obs.Scope, obs.CreatedAt)
}

func cmdUpdate(cfg store.Config) {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "usage: engram update <observation_id> [fields]", nil)
		return
	}
	id, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil || id <= 0 {
		failCLI(jsonMode, "invalid_observation_id", fmt.Sprintf("invalid observation id %q", os.Args[2]), nil)
		return
	}
	var p store.UpdateObservationParams
	for i := 3; i < len(os.Args); i++ {
		arg := os.Args[i]
		if arg == "--json" {
			continue
		}
		if arg == "--clear-topic-key" {
			empty := ""
			p.TopicKey = &empty
			continue
		}
		if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
			failCLI(jsonMode, "missing_flag_value", fmt.Sprintf("%s requires a value", arg), nil)
			return
		}
		v := os.Args[i+1]
		i++
		switch arg {
		case "--title":
			p.Title = &v
		case "--content":
			p.Content = &v
		case "--type":
			p.Type = &v
		case "--scope":
			p.Scope = &v
		case "--topic-key", "--topic":
			p.TopicKey = &v
		default:
			failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", arg), nil)
			return
		}
	}
	if p.Title == nil && p.Content == nil && p.Type == nil && p.Scope == nil && p.TopicKey == nil {
		failCLI(jsonMode, "invalid_arguments", "provide at least one field to update", nil)
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	obs, err := s.UpdateObservation(id, p)
	if err != nil {
		failCLI(jsonMode, "update_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(map[string]any{"observation": obs, "state": obs.State(), "pinned": obs.Pinned})
		return
	}
	fmt.Printf("Memory updated: #%d %q (%s, scope=%s)\n", obs.ID, obs.Title, obs.Type, obs.Scope)
}

func cmdReview(cfg store.Config) {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "usage: engram review <list|mark> ...", nil)
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	switch os.Args[2] {
	case "list":
		projectName, allProjects, limit := "", false, 10
		for i := 3; i < len(os.Args); i++ {
			switch os.Args[i] {
			case "--json":
			case "--all-projects":
				allProjects = true
			case "--project":
				if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
					failCLI(jsonMode, "missing_flag_value", "--project requires a value", nil)
					return
				}
				projectName = os.Args[i+1]
				i++
			case "--limit":
				if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
					failCLI(jsonMode, "missing_flag_value", "--limit requires a value", nil)
					return
				}
				limit, err = strconv.Atoi(os.Args[i+1])
				i++
				if err != nil || limit < 1 {
					failCLI(jsonMode, "invalid_limit", "limit must be positive", nil)
					return
				}
			default:
				failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", os.Args[i]), nil)
				return
			}
		}
		if allProjects && projectName != "" {
			failCLI(jsonMode, "incompatible_flags", "--all-projects cannot be combined with --project", nil)
			return
		}
		if !allProjects && projectName == "" {
			res := project.DetectProjectFull(currentCWD())
			if res.Error != nil || res.Project == "" {
				failCLI(jsonMode, "ambiguous_project", "could not resolve current project", map[string]any{"available_projects": res.AvailableProjects})
				return
			}
			projectName = res.Project
		}
		items, err := memoryops.New(s).ReviewList(memoryops.ReviewListInput{Project: projectName, Limit: limit, AllProjects: allProjects})
		if err != nil {
			failCLI(jsonMode, "review_failed", err.Error(), nil)
			return
		}
		if jsonMode {
			_ = writeCLIJSON(map[string]any{"project": projectName, "all_projects": allProjects, "observations": items, "local_only": true})
			return
		}
		if len(items) == 0 {
			fmt.Println("No memories need review.")
			return
		}
		for _, o := range items {
			fmt.Printf("#%d [%s] %s — state: %s\n", o.ID, o.Type, o.Title, o.State())
		}
	case "mark":
		if len(os.Args) < 4 {
			failCLI(jsonMode, "invalid_arguments", "usage: engram review mark <observation_id> [--json]", nil)
			return
		}
		id, e := strconv.ParseInt(os.Args[3], 10, 64)
		if e != nil || id <= 0 {
			failCLI(jsonMode, "invalid_observation_id", fmt.Sprintf("invalid observation id %q", os.Args[3]), nil)
			return
		}
		obs, err := memoryops.New(s).ReviewMark(id)
		if err != nil {
			failCLI(jsonMode, "review_failed", err.Error(), nil)
			return
		}
		if jsonMode {
			_ = writeCLIJSON(map[string]any{"observation": obs, "state": obs.State(), "local_only": true})
			return
		}
		fmt.Printf("Memory marked reviewed: #%d %q\n", obs.ID, obs.Title)
	default:
		failCLI(jsonMode, "invalid_action", "review action must be list or mark", nil)
	}
}

func cmdPin(cfg store.Config, pinned bool) {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "observation id is required", nil)
		return
	}
	for _, arg := range os.Args[3:] {
		if arg != "--json" {
			failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", arg), nil)
			return
		}
	}
	id, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil || id <= 0 {
		failCLI(jsonMode, "invalid_observation_id", fmt.Sprintf("invalid observation id %q", os.Args[2]), nil)
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	obs, err := memoryops.New(s).SetPinned(id, pinned)
	if err != nil {
		failCLI(jsonMode, "pin_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(map[string]any{"id": obs.ID, "sync_id": obs.SyncID, "pinned": obs.Pinned, "local_only": true})
		return
	}
	state := "unpinned"
	if pinned {
		state = "pinned"
	}
	fmt.Printf("Memory #%d %s (local only)\n", id, state)
}

func cmdCurrentProject() {
	jsonMode := hasArg("--json")
	for _, arg := range os.Args[2:] {
		if arg != "--json" {
			failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", arg), nil)
			return
		}
	}
	cwd := currentCWD()
	res := project.DetectProjectFull(cwd)
	policy := project.IdentityPolicyForResult(res)
	payload := map[string]any{
		"project":                res.Project,
		"project_source":         res.Source,
		"project_path":           res.Path,
		"project_strength":       policy.Strength,
		"implicit_write_allowed": policy.AllowsImplicitWrite,
		"cwd":                    cwd,
		"available_projects":     res.AvailableProjects,
	}
	if policy.Strength == project.IdentityStrengthWeak {
		payload["safe_next_action"] = project.ExplicitProjectSafeNextAction
	}
	if res.Warning != "" {
		payload["warning"] = res.Warning
	}
	if res.Error != nil {
		payload["error_hint"] = res.Error.Error()
	}
	if jsonMode {
		_ = writeCLIJSON(payload)
		return
	}
	if res.Project != "" {
		fmt.Printf("%s\n", res.Project)
		return
	}
	fmt.Println("Current project is ambiguous.")
	for _, p := range res.AvailableProjects {
		fmt.Printf("  %s\n", p)
	}
}

func cmdSuggestTopicKey() {
	jsonMode := hasArg("--json")
	typ, title, content := "", "", ""
	for i := 2; i < len(os.Args); i++ {
		arg := os.Args[i]
		if arg == "--json" {
			continue
		}
		if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
			failCLI(jsonMode, "missing_flag_value", fmt.Sprintf("%s requires a value", arg), nil)
			return
		}
		v := os.Args[i+1]
		i++
		switch arg {
		case "--type":
			typ = v
		case "--title":
			title = v
		case "--content":
			content = v
		default:
			failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", arg), nil)
			return
		}
	}
	if strings.TrimSpace(title) == "" && strings.TrimSpace(content) == "" {
		failCLI(jsonMode, "invalid_arguments", "provide --title or --content", nil)
		return
	}
	key := store.SuggestTopicKey(typ, title, content)
	if key == "" {
		failCLI(jsonMode, "suggestion_failed", "could not suggest topic_key", nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(map[string]any{"topic_key": key})
		return
	}
	fmt.Printf("Suggested topic_key: %s\n", key)
}

func hasArg(want string) bool {
	for _, arg := range os.Args[2:] {
		if arg == want {
			return true
		}
	}
	return false
}
func currentCWD() string { cwd, _ := os.Getwd(); return cwd }
