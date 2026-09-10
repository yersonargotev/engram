package memoryops

import (
	"context"

	"github.com/yersonargotev/engram/internal/store"
)

// RecallSupersession describes an explicit judged relation, not applicability.
// Unavailable endpoints retain direction without exposing their identity/content.
type RecallSupersession struct {
	Direction         string `json:"direction"`
	EndpointAvailable bool   `json:"endpoint_available"`
	MemoryID          int64  `json:"memory_id,omitempty"`
	SyncID            string `json:"sync_id,omitempty"`
	Title             string `json:"title,omitempty"`
}

func (s *Service) recallSupersessions(ctx context.Context, relations store.ObservationRelations, project, scope string) ([]RecallSupersession, int, error) {
	type endpoint struct{ direction, syncID string }
	endpoints := make([]endpoint, 0)
	for _, relation := range relations.AsTarget {
		if relation.Relation == store.RelationSupersedes && relation.JudgmentStatus == store.JudgmentStatusJudged {
			endpoints = append(endpoints, endpoint{"superseded_by", relation.SourceID})
		}
	}
	for _, relation := range relations.AsSource {
		if relation.Relation == store.RelationSupersedes && relation.JudgmentStatus == store.JudgmentStatusJudged {
			endpoints = append(endpoints, endpoint{"supersedes", relation.TargetID})
		}
	}
	const limit = 3
	omitted := 0
	if len(endpoints) > limit {
		omitted = len(endpoints) - limit
		endpoints = endpoints[:limit]
	}
	ids := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		ids = append(ids, endpoint.syncID)
	}
	eligible, err := s.store.RecallEligibleConflictTargetsContext(ctx, ids, store.SearchOptions{Project: project, Scope: scope, IncludeHistory: true})
	if err != nil {
		return nil, 0, err
	}
	result := make([]RecallSupersession, 0, len(endpoints))
	for _, endpoint := range endpoints {
		relation := RecallSupersession{Direction: endpoint.direction}
		if target, ok := eligible[endpoint.syncID]; ok {
			relation.EndpointAvailable = true
			relation.MemoryID = target.ID
			relation.SyncID = target.SyncID
			relation.Title = truncateRecallUTF8(target.Title, 256)
		}
		result = append(result, relation)
	}
	return result, omitted, nil
}
