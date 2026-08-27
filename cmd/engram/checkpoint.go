package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

type checkpointCLIOptions struct {
	Action      string
	Host        string
	SessionID   string
	RootTurnID  string
	Disposition string
	ReasonCode  string
	JSONMode    bool
	Help        bool
}

type checkpointArgumentError struct {
	Message string
}

func (e *checkpointArgumentError) Error() string { return e.Message }

func cmdCheckpoint(cfg store.Config) {
	opts, err := parseCheckpointArgs(os.Args[2:])
	if err != nil {
		failCLI(opts.JSONMode || hasArg("--json"), "invalid_arguments", err.Error(), nil)
		return
	}
	if opts.Help {
		printCheckpointUsage()
		return
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(opts.JSONMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	service := memoryops.New(s)

	switch opts.Action {
	case "record":
		result, recordErr := service.RecordCheckpoint(memoryops.CheckpointRecordInput{
			Host:        opts.Host,
			SessionID:   opts.SessionID,
			RootTurnID:  opts.RootTurnID,
			Disposition: opts.Disposition,
			ReasonCode:  opts.ReasonCode,
		})
		if recordErr != nil {
			failCLI(opts.JSONMode, memoryops.CheckpointErrorCode(recordErr), recordErr.Error(), nil)
			return
		}
		if opts.JSONMode {
			_ = writeCLIJSON(result)
			return
		}
		fmt.Printf("Memory checkpoint %s: %s (%s)\n", result.Idempotency, result.Checkpoint.Disposition, result.Checkpoint.ReasonCode)
	case "status":
		result, statusErr := service.CheckpointStatus(memoryops.CheckpointStatusInput{
			Host:       opts.Host,
			SessionID:  opts.SessionID,
			RootTurnID: opts.RootTurnID,
		})
		if statusErr != nil {
			failCLI(opts.JSONMode, memoryops.CheckpointErrorCode(statusErr), statusErr.Error(), nil)
			return
		}
		if opts.JSONMode {
			_ = writeCLIJSON(result)
			return
		}
		fmt.Printf("Memory checkpoint: %s (%s)\n", result.Checkpoint.Disposition, result.Checkpoint.ReasonCode)
	}
}

func parseCheckpointArgs(args []string) (checkpointCLIOptions, error) {
	opts := checkpointCLIOptions{}
	if len(args) == 0 {
		return opts, &checkpointArgumentError{Message: "usage: engram checkpoint record|status [flags]"}
	}
	opts.Action = strings.ToLower(strings.TrimSpace(args[0]))
	if opts.Action == "help" || opts.Action == "--help" || opts.Action == "-h" {
		opts.Help = true
		return opts, nil
	}
	if opts.Action != "record" && opts.Action != "status" {
		return opts, &checkpointArgumentError{Message: "checkpoint action must be record or status"}
	}

	for i := 1; i < len(args); i++ {
		rawArg := args[i]
		if rawArg == "--json" {
			opts.JSONMode = true
			continue
		}
		if rawArg == "--help" || rawArg == "-h" {
			opts.Help = true
			return opts, nil
		}

		arg, value, hasInlineValue := strings.Cut(rawArg, "=")
		if !hasInlineValue && i+1 >= len(args) {
			return opts, &checkpointArgumentError{Message: fmt.Sprintf("%s requires a value", arg)}
		}
		if !hasInlineValue {
			if args[i+1] == "--help" || args[i+1] == "-h" {
				opts.Help = true
				return opts, nil
			}
			if strings.HasPrefix(args[i+1], "-") {
				return opts, &checkpointArgumentError{Message: fmt.Sprintf("%s requires a value; use %s=VALUE for values beginning with '-'", arg, arg)}
			}
			value = args[i+1]
			i++
		}
		switch arg {
		case "--host":
			opts.Host = value
		case "--session-id":
			opts.SessionID = value
		case "--root-turn-id":
			opts.RootTurnID = value
		case "--disposition":
			opts.Disposition = value
		case "--reason":
			opts.ReasonCode = value
		default:
			return opts, &checkpointArgumentError{Message: fmt.Sprintf("unknown checkpoint flag %s", arg)}
		}
	}
	if opts.Action == "status" && (opts.Disposition != "" || opts.ReasonCode != "") {
		return opts, &checkpointArgumentError{Message: "checkpoint status does not accept --disposition or --reason"}
	}
	return opts, nil
}

func printCheckpointUsage() {
	fmt.Println(`Usage:
  engram checkpoint record --host HOST --session-id ID --root-turn-id ID \
    --disposition skipped --reason no_durable_knowledge [--json]
  engram checkpoint status --host HOST --session-id ID --root-turn-id ID [--json]`)
}
