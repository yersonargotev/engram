package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/recallbaseline"
	"github.com/yersonargotev/engram/internal/store"
)

type checkpointCLIOptions struct {
	Action      string
	Host        string
	SessionID   string
	RootTurnID  string
	Disposition string
	ReasonCode  string
	Project     string
	MemoryIDs   []int64
	Memories    []memoryops.CheckpointMemoryInput
	Proposal    *memoryops.CheckpointProposalInput
	JSONMode    bool
	Help        bool
}

type checkpointArgumentError struct {
	Code    string
	Message string
}

func (e *checkpointArgumentError) Error() string { return e.Message }

func cmdCheckpoint(cfg store.Config) {
	opts, err := parseCheckpointArgs(os.Args[2:])
	if err != nil {
		code := err.Code
		if code == "" {
			code = "invalid_arguments"
		}
		failCLI(opts.JSONMode || hasArg("--json"), code, err.Error(), nil)
		return
	}
	if opts.Help {
		printCheckpointUsage()
		return
	}
	if opts.Action == "verify-stop" {
		cmdCheckpointVerifyStop(cfg, opts.Host, os.Stdin)
		return
	}

	s, storeErr := storeNew(cfg)
	if storeErr != nil {
		failCLI(opts.JSONMode, "store_error", storeErr.Error(), nil)
		return
	}
	defer s.Close()
	service := memoryops.New(s)

	switch opts.Action {
	case "preflight":
		result, preflightErr := service.PreflightCheckpoint(memoryops.CheckpointPreflightInput{
			Project: opts.Project, Memories: opts.Memories,
		})
		if preflightErr != nil {
			failCLI(opts.JSONMode, memoryops.CheckpointErrorCode(preflightErr), preflightErr.Error(), nil)
			return
		}
		if opts.JSONMode {
			_ = writeCLIJSON(result)
			return
		}
		fmt.Printf("Terminal Memory preflight: %d exact duplicate(s), %d semantic candidate(s) (limit %d)\n",
			len(result.ExactDuplicates), len(result.Candidates), result.CandidateLimit)
		for _, duplicate := range result.ExactDuplicates {
			fmt.Printf("  Input %d reuses Memory #%d (%s, project %s)\n", duplicate.InputIndex+1,
				duplicate.Reference.MemoryID, duplicate.Reference.MemorySyncID, duplicate.Reference.Project)
		}
		for _, candidate := range result.Candidates {
			fmt.Printf("  Input %d candidate Memory #%d (%s, project %s): %s\n", candidate.InputIndex+1,
				candidate.Reference.MemoryID, candidate.Reference.MemorySyncID, candidate.Reference.Project, candidate.Title)
		}
	case "record":
		result, recordErr := service.RecordCheckpoint(memoryops.CheckpointRecordInput{
			Host:        opts.Host,
			SessionID:   opts.SessionID,
			RootTurnID:  opts.RootTurnID,
			Disposition: opts.Disposition,
			ReasonCode:  opts.ReasonCode,
			Project:     opts.Project,
			MemoryIDs:   opts.MemoryIDs,
			Memories:    opts.Memories,
			Proposal:    opts.Proposal,
			CWD:         currentCWD(),
		})
		if recordErr != nil {
			outcome := recallbaseline.OutcomeUnknown
			if memoryops.CheckpointErrorCode(recordErr) == memoryops.CheckpointErrorCodeConflict {
				outcome = recallbaseline.OutcomeConflict
			}
			recordRecallBaselineCheckpoint(cfg, opts.Host, opts.SessionID, opts.RootTurnID, outcome)
			failCLI(opts.JSONMode, memoryops.CheckpointErrorCode(recordErr), recordErr.Error(), nil)
			return
		}
		defer recordRecallBaselineCheckpoint(cfg, opts.Host, opts.SessionID, opts.RootTurnID, recallbaseline.OutcomeCompleted)
		if opts.JSONMode {
			_ = writeCLIJSON(result)
			return
		}
		switch result.Checkpoint.Disposition {
		case store.CheckpointDispositionSaved:
			fmt.Printf("Memory checkpoint %s: saved (%d Memories)\n", result.Idempotency, len(result.Checkpoint.References))
		case store.CheckpointDispositionNeedsReview:
			fmt.Printf("Memory checkpoint %s: needs_review (%d Memories; proposal %s)\n",
				result.Idempotency, len(result.Checkpoint.References), result.Checkpoint.Proposal.ID)
			printCheckpointReferences(result.Checkpoint.References)
		default:
			fmt.Printf("Memory checkpoint %s: %s (%s)\n", result.Idempotency, result.Checkpoint.Disposition, result.Checkpoint.ReasonCode)
		}
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
		switch result.Checkpoint.Disposition {
		case store.CheckpointDispositionSaved:
			fmt.Printf("Memory checkpoint: saved (%d Memories)\n", len(result.Checkpoint.References))
			for _, reference := range result.Checkpoint.References {
				fmt.Printf("  Memory #%d (%s, project %s)\n", reference.MemoryID, reference.MemorySyncID, reference.Project)
			}
		case store.CheckpointDispositionNeedsReview:
			fmt.Printf("Memory checkpoint: needs_review (%d Memories; proposal %s, project %s)\n",
				len(result.Checkpoint.References), result.Checkpoint.Proposal.ID, result.Checkpoint.Proposal.Project)
			printCheckpointReferences(result.Checkpoint.References)
		default:
			fmt.Printf("Memory checkpoint: %s (%s)\n", result.Checkpoint.Disposition, result.Checkpoint.ReasonCode)
		}
	}
}

func printCheckpointReferences(references []store.CheckpointReference) {
	for _, reference := range references {
		fmt.Printf("  Memory #%d (%s, project %s)\n", reference.MemoryID, reference.MemorySyncID, reference.Project)
	}
}

func recordRecallBaselineCheckpoint(cfg store.Config, host, sessionID, rootTurnID string, outcome recallbaseline.Outcome) {
	recordRecallBaselineEvents(cfg, recallbaseline.Event{
		Kind: recallbaseline.EventCheckpoint, Surface: recallbaseline.SurfaceLifecycle,
		Operation: "terminal_checkpoint", Outcome: outcome,
		Link: recallbaseline.Linkage{Host: host, SessionID: sessionID, RootTurnID: rootTurnID},
	})
}

type checkpointStopEvent struct {
	SessionID      *string `json:"session_id"`
	TurnID         *string `json:"turn_id"`
	StopHookActive *bool   `json:"stop_hook_active"`
}

type checkpointStopResponse struct {
	Decision      string `json:"decision,omitempty"`
	Reason        string `json:"reason,omitempty"`
	SystemMessage string `json:"systemMessage,omitempty"`
}

func cmdCheckpointVerifyStop(cfg store.Config, host string, input io.Reader) {
	started := time.Now()
	finish := func(event *checkpointStopEvent, stopOutcome, checkpointOutcome recallbaseline.Outcome, response checkpointStopResponse) {
		finishCheckpointStopWithBaseline(cfg, host, event, stopOutcome, checkpointOutcome, response, started)
	}
	event, err := decodeCheckpointStopEvent(input)
	if err != nil {
		finish(nil, recallbaseline.OutcomeIntegrationFailure, recallbaseline.OutcomeUnknown,
			checkpointStopIntegrationFailure("Stop input is missing a string session_id, string turn_id, or boolean stop_hook_active."))
		return
	}

	s, err := storeNew(cfg)
	if err != nil {
		finish(&event, recallbaseline.OutcomeIntegrationFailure, recallbaseline.OutcomeUnknown,
			checkpointStopIntegrationFailure("the verifier could not inspect checkpoint status."))
		return
	}
	defer s.Close()

	outcome, err := memoryops.New(s).VerifyCheckpoint(memoryops.CheckpointVerificationInput{
		Host:           host,
		SessionID:      *event.SessionID,
		RootTurnID:     *event.TurnID,
		RecoveryActive: *event.StopHookActive,
	})
	if err != nil {
		finish(&event, recallbaseline.OutcomeIntegrationFailure, recallbaseline.OutcomeUnknown,
			checkpointStopIntegrationFailure("the verifier could not inspect checkpoint status."))
		return
	}

	switch outcome {
	case memoryops.CheckpointVerificationComplete:
		finish(&event, recallbaseline.OutcomeCompleted, recallbaseline.OutcomeCompleted, checkpointStopResponse{})
	case memoryops.CheckpointVerificationContinuationRequired:
		identity, marshalErr := json.Marshal(store.CheckpointIdentity{
			Host: host, SessionID: *event.SessionID, RootTurnID: *event.TurnID,
		})
		if marshalErr != nil {
			finish(&event, recallbaseline.OutcomeIntegrationFailure, recallbaseline.OutcomeUnknown,
				checkpointStopIntegrationFailure("the verifier could not encode the original checkpoint identity."))
			return
		}
		reason := "Finalize the missing Engram checkpoint for the original root user turn " + string(identity) + " using the Engram memory skill. Preserve this identity unchanged; do not checkpoint this continuation."
		finish(&event, recallbaseline.OutcomeContinuationRequired, recallbaseline.OutcomeMissing,
			checkpointStopResponse{Decision: "block", Reason: reason})
	case memoryops.CheckpointVerificationRecoveryExhausted:
		finish(&event, recallbaseline.OutcomeRecoveryExhausted, recallbaseline.OutcomeMissing,
			checkpointStopIntegrationFailure("checkpoint is still missing after the single recovery continuation."))
	default:
		finish(&event, recallbaseline.OutcomeIntegrationFailure, recallbaseline.OutcomeUnknown,
			checkpointStopIntegrationFailure("checkpoint verification returned an unexpected outcome."))
	}
}

func finishCheckpointStopWithBaseline(
	cfg store.Config,
	host string,
	event *checkpointStopEvent,
	stopOutcome, checkpointOutcome recallbaseline.Outcome,
	response checkpointStopResponse,
	started time.Time,
) {
	link := recallbaseline.Linkage{}
	if event != nil {
		link.Host = host
		if event.SessionID != nil {
			link.SessionID = *event.SessionID
		}
		if event.TurnID != nil {
			link.RootTurnID = *event.TurnID
		}
	}
	operationOutcome := recallbaseline.OutcomeSuccess
	if stopOutcome == recallbaseline.OutcomeIntegrationFailure || stopOutcome == recallbaseline.OutcomeRecoveryExhausted {
		operationOutcome = recallbaseline.OutcomeError
	}
	var deliveredBytes *int64
	if encoded, err := json.MarshalIndent(response, "", "  "); err == nil {
		bytes := int64(len(encoded) + 1)
		deliveredBytes = &bytes
	}
	events := []recallbaseline.Event{
		{Kind: recallbaseline.EventStop, Surface: recallbaseline.SurfaceLifecycle, Operation: "stop", Outcome: stopOutcome},
		{Kind: recallbaseline.EventOperation, Surface: recallbaseline.SurfaceCLI, Operation: "checkpoint_verify_stop", Outcome: operationOutcome,
			Latency: recallbaseline.KnownLatency(time.Since(started)), DeliveredUTF8Bytes: deliveredBytes},
	}
	if link.Host != "" && link.SessionID != "" && link.RootTurnID != "" {
		events = append(events, recallbaseline.Event{
			Kind: recallbaseline.EventCheckpoint, Surface: recallbaseline.SurfaceLifecycle,
			Operation: "terminal_checkpoint", Outcome: checkpointOutcome, Link: link,
		})
	}
	_ = writeCLIJSON(response)
	recordRecallBaselineEvents(cfg, events...)
}

func decodeCheckpointStopEvent(input io.Reader) (checkpointStopEvent, error) {
	var event checkpointStopEvent
	decoder := json.NewDecoder(input)
	if err := decoder.Decode(&event); err != nil {
		return event, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return event, fmt.Errorf("multiple Stop input values")
		}
		return event, err
	}
	if event.SessionID == nil || event.TurnID == nil || event.StopHookActive == nil ||
		strings.TrimSpace(*event.SessionID) == "" || strings.TrimSpace(*event.TurnID) == "" {
		return event, fmt.Errorf("incomplete Stop input")
	}
	return event, nil
}

func checkpointStopIntegrationFailure(message string) checkpointStopResponse {
	return checkpointStopResponse{SystemMessage: "Engram checkpoint verifier integration failure: " + message}
}

func parseCheckpointArgs(args []string) (checkpointCLIOptions, *checkpointArgumentError) {
	opts := checkpointCLIOptions{}
	if len(args) == 0 {
		return opts, &checkpointArgumentError{Message: "usage: engram checkpoint record|status [flags]"}
	}
	opts.Action = strings.ToLower(strings.TrimSpace(args[0]))
	if opts.Action == "help" || opts.Action == "--help" || opts.Action == "-h" {
		opts.Help = true
		return opts, nil
	}
	if opts.Action != "preflight" && opts.Action != "record" && opts.Action != "status" && opts.Action != "verify-stop" {
		return opts, &checkpointArgumentError{Message: "checkpoint action must be preflight, record, status, or verify-stop"}
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
		case "--project":
			opts.Project = value
		case "--memory-id":
			memoryID, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return opts, &checkpointArgumentError{
					Code:    memoryops.CheckpointErrorCodeInvalidReferences,
					Message: "invalid checkpoint references: memory_ids must contain integers",
				}
			}
			opts.MemoryIDs = append(opts.MemoryIDs, memoryID)
		case "--memory-json":
			var memory memoryops.CheckpointMemoryInput
			if err := json.Unmarshal([]byte(value), &memory); err != nil {
				return opts, &checkpointArgumentError{
					Code:    memoryops.CheckpointErrorCodeInvalidReferences,
					Message: "invalid checkpoint references: memories must be an array of Memory objects",
				}
			}
			opts.Memories = append(opts.Memories, memory)
		case "--proposal-id":
			return opts, &checkpointArgumentError{
				Code: memoryops.CheckpointErrorCodeInvalidReferences, Message: "invalid checkpoint references: proposal_id is not supported",
			}
		case "--proposal-json":
			if opts.Proposal != nil {
				return opts, &checkpointArgumentError{
					Code: memoryops.CheckpointErrorCodeInvalidReferences, Message: "invalid checkpoint references: proposal may be provided once",
				}
			}
			var proposal memoryops.CheckpointProposalInput
			if err := json.Unmarshal([]byte(value), &proposal); err != nil {
				return opts, &checkpointArgumentError{
					Code:    memoryops.CheckpointErrorCodeInvalidReferences,
					Message: "invalid checkpoint references: proposal must be a Memory proposal object",
				}
			}
			opts.Proposal = &proposal
		default:
			return opts, &checkpointArgumentError{Message: fmt.Sprintf("unknown checkpoint flag %s", arg)}
		}
	}
	if opts.Action == "status" && (opts.Disposition != "" || opts.ReasonCode != "" || opts.Project != "" || len(opts.MemoryIDs) > 0 || len(opts.Memories) > 0 || opts.Proposal != nil) {
		return opts, &checkpointArgumentError{Message: "checkpoint status accepts only identity flags"}
	}
	if opts.Action == "preflight" && (opts.Host != "" || opts.SessionID != "" || opts.RootTurnID != "" ||
		opts.Disposition != "" || opts.ReasonCode != "" || len(opts.MemoryIDs) > 0 || opts.Proposal != nil) {
		return opts, &checkpointArgumentError{Message: "checkpoint preflight accepts only --project, --memory-json, and --json"}
	}
	if opts.Action == "verify-stop" && (opts.SessionID != "" || opts.RootTurnID != "" || opts.Disposition != "" || opts.ReasonCode != "" || opts.Project != "" || len(opts.MemoryIDs) > 0 || len(opts.Memories) > 0 || opts.Proposal != nil || opts.JSONMode) {
		return opts, &checkpointArgumentError{Message: "checkpoint verify-stop accepts only --host"}
	}
	return opts, nil
}

func printCheckpointUsage() {
	fmt.Println(`Usage:
	engram checkpoint preflight --project PROJECT --memory-json JSON [--memory-json JSON ...] [--json]
	engram checkpoint record --host HOST --session-id ID --root-turn-id ID \
	  --disposition skipped --reason no_durable_knowledge [--json]
	engram checkpoint record --host HOST --session-id ID --root-turn-id ID \
	  --disposition saved --project PROJECT \
	  [--memory-id ID ...] [--memory-json JSON ...] [--json]
	engram checkpoint record --host HOST --session-id ID --root-turn-id ID \
	  --disposition needs_review --project PROJECT \
	  [--memory-id ID ...] [--memory-json JSON ...] \
	  --proposal-json '{"title":"...","content":"..."}' [--json]
	engram checkpoint status --host HOST --session-id ID --root-turn-id ID [--json]
	engram checkpoint verify-stop --host HOST`)
}
