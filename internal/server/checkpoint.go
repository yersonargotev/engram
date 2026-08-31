package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/yersonargotev/engram/internal/memoryops"
)

const checkpointHTTPErrorCodeInvalidRequest = "invalid_checkpoint_request"

func (s *Server) handleRecordCheckpoint(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input memoryops.CheckpointRecordInput
	if err := decoder.Decode(&input); err != nil {
		writeCheckpointHTTPRequestError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		writeCheckpointHTTPRequestError(w, err)
		return
	}

	result, err := memoryops.New(s.store).RecordCheckpoint(input)
	if err != nil {
		writeCheckpointHTTPError(w, err)
		return
	}
	status := http.StatusOK
	if result.Idempotency == memoryops.CheckpointIdempotencyCreated {
		s.notifyWrite()
		status = http.StatusCreated
	}
	jsonResponse(w, status, result)
}

func (s *Server) handleCheckpointStatus(w http.ResponseWriter, r *http.Request) {
	result, err := memoryops.New(s.store).CheckpointStatus(memoryops.CheckpointStatusInput{
		Host:       r.URL.Query().Get("host"),
		SessionID:  r.URL.Query().Get("session_id"),
		RootTurnID: r.URL.Query().Get("root_turn_id"),
	})
	if err != nil {
		writeCheckpointHTTPError(w, err)
		return
	}
	jsonResponse(w, http.StatusOK, result)
}

func writeCheckpointHTTPRequestError(w http.ResponseWriter, err error) {
	jsonResponse(w, http.StatusBadRequest, map[string]any{
		"code":    checkpointHTTPErrorCodeInvalidRequest,
		"message": "invalid checkpoint request",
		"details": map[string]any{"cause": err.Error()},
	})
}

func writeCheckpointHTTPError(w http.ResponseWriter, err error) {
	code := memoryops.CheckpointErrorCode(err)
	status := http.StatusInternalServerError
	switch code {
	case memoryops.CheckpointErrorCodeInvalidDisposition,
		memoryops.CheckpointErrorCodeInvalidIdentity,
		memoryops.CheckpointErrorCodeInvalidReason,
		memoryops.CheckpointErrorCodeInvalidReferences,
		memoryops.CheckpointErrorCodeProjectMismatch:
		status = http.StatusBadRequest
	case memoryops.CheckpointErrorCodeMemoryNotFound, memoryops.CheckpointErrorCodeNotFound:
		status = http.StatusNotFound
	case memoryops.CheckpointErrorCodeConflict:
		status = http.StatusConflict
	}
	jsonResponse(w, status, map[string]any{
		"code":    code,
		"message": err.Error(),
		"details": map[string]any{"operation": "checkpoint"},
	})
}
