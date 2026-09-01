package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

const checkpointHTTPErrorCodeInvalidRequest = "invalid_checkpoint_request"

func (s *Server) handlePreflightCheckpoint(w http.ResponseWriter, r *http.Request) {
	raw, err := readCheckpointJSON(r.Body)
	if err != nil {
		writeCheckpointHTTPRequestError(w, err)
		return
	}
	var input memoryops.CheckpointPreflightInput
	if err := decodeCheckpointJSON(raw, &input); err != nil {
		writeCheckpointHTTPRequestError(w, err)
		return
	}

	result, err := memoryops.New(s.store).PreflightCheckpoint(input)
	if err != nil {
		writeCheckpointHTTPError(w, err)
		return
	}
	jsonResponse(w, http.StatusOK, result)
}

func (s *Server) handleRecordCheckpoint(w http.ResponseWriter, r *http.Request) {
	raw, err := readCheckpointJSON(r.Body)
	if err != nil {
		writeCheckpointHTTPRequestError(w, err)
		return
	}
	service := memoryops.New(s.store)
	var replayInput memoryops.CheckpointReplayInput
	if err := json.Unmarshal(raw, &replayInput); err != nil {
		writeCheckpointHTTPRequestError(w, err)
		return
	}
	replayed, replayErr := service.ReplayCheckpoint(replayInput)
	if replayErr == nil {
		jsonResponse(w, http.StatusOK, replayed)
		return
	}
	if !errors.Is(replayErr, store.ErrCheckpointNotFound) && !errors.Is(replayErr, store.ErrCheckpointInvalidIdentity) {
		writeCheckpointHTTPError(w, replayErr)
		return
	}

	var input memoryops.CheckpointRecordInput
	if err := decodeCheckpointJSON(raw, &input); err != nil {
		writeCheckpointHTTPRequestError(w, err)
		return
	}

	result, err := service.RecordCheckpoint(input)
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

func readCheckpointJSON(body io.Reader) (json.RawMessage, error) {
	decoder := json.NewDecoder(body)
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return nil, err
	}
	return raw, nil
}

func decodeCheckpointJSON(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
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
