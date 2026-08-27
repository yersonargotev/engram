package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	mcppkg "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

// CheckpointToolHandler exposes the transport-neutral checkpoint write for MCP
// and for adapter-parity tests.
func CheckpointToolHandler(s *store.Store) server.ToolHandlerFunc {
	return func(_ context.Context, req mcppkg.CallToolRequest) (*mcppkg.CallToolResult, error) {
		memoryIDs, err := checkpointMemoryIDsArg(req, "memory_ids")
		if err != nil {
			return checkpointToolError(err), nil
		}
		memories, err := checkpointMemoriesArg(req, "memories")
		if err != nil {
			return checkpointToolError(err), nil
		}
		proposal, err := checkpointProposalArg(req, "proposal")
		if err != nil {
			return checkpointToolError(err), nil
		}
		result, err := memoryops.New(s).RecordCheckpoint(memoryops.CheckpointRecordInput{
			Host:        checkpointStringArg(req, "host"),
			SessionID:   checkpointStringArg(req, "session_id"),
			RootTurnID:  checkpointStringArg(req, "root_turn_id"),
			Disposition: checkpointStringArg(req, "disposition"),
			ReasonCode:  checkpointStringArg(req, "reason"),
			Project:     checkpointStringArg(req, "project"),
			MemoryIDs:   memoryIDs,
			Memories:    memories,
			ProposalID:  checkpointStringArg(req, "proposal_id"),
			Proposal:    proposal,
			CWD:         currentWorkingDirectory(),
		})
		if err != nil {
			return checkpointToolError(err), nil
		}
		return checkpointToolJSON(result), nil
	}
}

// queuedCheckpointToolHandler preserves the checkpoint JSON error contract for
// transport-level queue failures as well as domain and persistence failures.
func queuedCheckpointToolHandler(q *writeQueue, h server.ToolHandlerFunc) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcppkg.CallToolRequest) (*mcppkg.CallToolResult, error) {
		result, err := q.Do(ctx, func(runCtx context.Context) (*mcppkg.CallToolResult, error) {
			return h(runCtx, req)
		})
		if err != nil {
			return checkpointToolError(err), nil
		}
		return result, nil
	}
}

// CheckpointStatusToolHandler exposes exact root-turn inspection for MCP and
// for adapter-parity tests.
func CheckpointStatusToolHandler(s *store.Store) server.ToolHandlerFunc {
	return func(_ context.Context, req mcppkg.CallToolRequest) (*mcppkg.CallToolResult, error) {
		result, err := memoryops.New(s).CheckpointStatus(memoryops.CheckpointStatusInput{
			Host:       checkpointStringArg(req, "host"),
			SessionID:  checkpointStringArg(req, "session_id"),
			RootTurnID: checkpointStringArg(req, "root_turn_id"),
		})
		if err != nil {
			return checkpointToolError(err), nil
		}
		return checkpointToolJSON(result), nil
	}
}

func checkpointToolJSON(value any) *mcppkg.CallToolResult {
	out, err := jsonMarshal(value)
	if err != nil {
		return checkpointToolError(fmt.Errorf("encode checkpoint response: %w", err))
	}
	return mcppkg.NewToolResultText(string(out))
}

func checkpointToolError(err error) *mcppkg.CallToolResult {
	out, marshalErr := jsonMarshal(map[string]any{
		"code":    memoryops.CheckpointErrorCode(err),
		"message": err.Error(),
	})
	if marshalErr != nil {
		out = []byte(`{"code":"checkpoint_failed","message":"encode checkpoint error response"}`)
	}
	result := mcppkg.NewToolResultText(string(out))
	result.IsError = true
	return result
}

func checkpointStringArg(req mcppkg.CallToolRequest, key string) string {
	value, _ := req.GetArguments()[key].(string)
	return value
}

func checkpointMemoryIDsArg(req mcppkg.CallToolRequest, key string) ([]int64, error) {
	value, exists := req.GetArguments()[key]
	if !exists || value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case []int64:
		return typed, nil
	case []any:
		ids := make([]int64, 0, len(typed))
		for _, item := range typed {
			var id int64
			switch number := item.(type) {
			case int:
				id = int64(number)
			case int64:
				id = number
			case float64:
				id = int64(number)
				if float64(id) != number {
					return nil, fmt.Errorf("%w: memory_ids must contain integers", store.ErrCheckpointInvalidReferences)
				}
			default:
				return nil, fmt.Errorf("%w: memory_ids must contain integers", store.ErrCheckpointInvalidReferences)
			}
			ids = append(ids, id)
		}
		return ids, nil
	default:
		return nil, fmt.Errorf("%w: memory_ids must be an array", store.ErrCheckpointInvalidReferences)
	}
}

func checkpointMemoriesArg(req mcppkg.CallToolRequest, key string) ([]memoryops.CheckpointMemoryInput, error) {
	value, exists := req.GetArguments()[key]
	if !exists || value == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode memories", store.ErrCheckpointInvalidReferences)
	}
	var memories []memoryops.CheckpointMemoryInput
	if err := json.Unmarshal(encoded, &memories); err != nil {
		return nil, fmt.Errorf("%w: memories must be an array of Memory objects", store.ErrCheckpointInvalidReferences)
	}
	return memories, nil
}

func checkpointProposalArg(req mcppkg.CallToolRequest, key string) (*memoryops.CheckpointProposalInput, error) {
	value, exists := req.GetArguments()[key]
	if !exists || value == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode proposal", store.ErrCheckpointInvalidReferences)
	}
	var proposal memoryops.CheckpointProposalInput
	if err := json.Unmarshal(encoded, &proposal); err != nil {
		return nil, fmt.Errorf("%w: proposal must be a Memory proposal object", store.ErrCheckpointInvalidReferences)
	}
	return &proposal, nil
}
