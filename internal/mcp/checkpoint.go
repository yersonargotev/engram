package mcp

import (
	"context"
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
		result, err := memoryops.New(s).RecordCheckpoint(memoryops.CheckpointRecordInput{
			Host:        checkpointStringArg(req, "host"),
			SessionID:   checkpointStringArg(req, "session_id"),
			RootTurnID:  checkpointStringArg(req, "root_turn_id"),
			Disposition: checkpointStringArg(req, "disposition"),
			ReasonCode:  checkpointStringArg(req, "reason"),
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
