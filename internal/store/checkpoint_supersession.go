package store

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

var (
	ErrCheckpointInvalidSupersession = errors.New("invalid checkpoint supersession")
	ErrCheckpointSupersessionStale   = errors.New("checkpoint supersession target changed; reevaluate the target")
)

// CheckpointSupersession binds an explicit verdict to one settled replacement.
// Exactly one replacement selector is required. Input indexes are zero-based
// indexes into the enclosing Memories array, before reference deduplication.
type CheckpointSupersession struct {
	ReplacementMemoryID   int64  `json:"replacement_memory_id,omitempty"`
	ReplacementInputIndex *int   `json:"replacement_input_index,omitempty"`
	TargetMemoryID        int64  `json:"target_memory_id"`
	TargetVersion         string `json:"target_version"`
	Reason                string `json:"reason"`
}

// UnmarshalJSON rejects misspelled declarations rather than silently losing
// the caller's explicit retirement intent.
func (p *CheckpointSupersession) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return ErrCheckpointInvalidSupersession
	}
	type declaration CheckpointSupersession
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded declaration
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("%w: %v", ErrCheckpointInvalidSupersession, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ErrCheckpointInvalidSupersession
	}
	*p = CheckpointSupersession(decoded)
	return nil
}

// CheckpointSupersessionTarget reads one coherent evaluated Memory and opaque
// state fingerprint. It does not reserve state or authorize a later write.
func (s *Store) CheckpointSupersessionTarget(id int64) (*Observation, string, error) {
	var memory *Observation
	var version string
	err := s.withTx(func(tx *sql.Tx) error {
		var err error
		memory, version, err = s.checkpointSupersessionTargetTx(tx, id)
		return err
	})
	return memory, version, err
}

func (s *Store) checkpointSupersessionTargetTx(tx *sql.Tx, id int64) (*Observation, string, error) {
	memory, err := s.getObservationTx(tx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", ErrCheckpointMemoryNotFound
	}
	if err != nil {
		return nil, "", err
	}
	rows, err := tx.Query(`SELECT sync_id FROM memory_relations WHERE source_id = ? OR target_id = ? ORDER BY sync_id`, memory.SyncID, memory.SyncID)
	if err != nil {
		return nil, "", err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, "", err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, "", err
	}
	relations := make([]Relation, 0, len(ids))
	for _, id := range ids {
		relation, err := s.getRelationTx(tx, id)
		if err != nil {
			return nil, "", err
		}
		relations = append(relations, *relation)
	}
	snapshot, err := json.Marshal(struct {
		Memory    *Observation
		Pinned    bool
		State     string
		Relations []Relation
	}{memory, memory.Pinned, memory.State(), relations})
	if err != nil {
		return nil, "", err
	}
	return memory, fmt.Sprintf("sha256:%x", sha256.Sum256(snapshot)), nil
}

func (s *Store) validateCheckpointSupersessionsTx(tx *sql.Tx, p checkpointMemorySet) (map[int64]*Observation, error) {
	targets := make(map[int64]*Observation, len(p.Supersessions))
	for _, declaration := range p.Supersessions {
		if declaration.TargetMemoryID <= 0 || strings.TrimSpace(declaration.TargetVersion) == "" || strings.TrimSpace(declaration.Reason) == "" ||
			(declaration.ReplacementMemoryID != 0) == (declaration.ReplacementInputIndex != nil) {
			return nil, ErrCheckpointInvalidSupersession
		}
		if declaration.ReplacementInputIndex != nil && (*declaration.ReplacementInputIndex < 0 || *declaration.ReplacementInputIndex >= len(p.Memories)) {
			return nil, ErrCheckpointInvalidSupersession
		}
		if declaration.ReplacementMemoryID != 0 {
			found := false
			for _, id := range p.MemoryIDs {
				if id == declaration.ReplacementMemoryID {
					found = true
				}
			}
			if !found || declaration.ReplacementMemoryID == declaration.TargetMemoryID {
				return nil, ErrCheckpointInvalidSupersession
			}
		}
		if _, exists := targets[declaration.TargetMemoryID]; exists {
			return nil, ErrCheckpointInvalidSupersession
		}
		target, version, err := s.checkpointSupersessionTargetTx(tx, declaration.TargetMemoryID)
		if err != nil {
			return nil, err
		}
		project := ""
		if target.Project != nil {
			project, _ = NormalizeProject(*target.Project)
		}
		if project != p.Project {
			return nil, &CheckpointProjectMismatchError{RequestedProject: p.Project, MemoryProject: &project}
		}
		if version != declaration.TargetVersion {
			return nil, ErrCheckpointSupersessionStale
		}
		if err := validateCheckpointSupersessionEndpointTx(tx, target); err != nil {
			return nil, err
		}
		targets[target.ID] = target
		if declaration.ReplacementMemoryID != 0 {
			replacement, err := s.getObservationTx(tx, declaration.ReplacementMemoryID)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrCheckpointMemoryNotFound
			}
			if err != nil {
				return nil, err
			}
			replacementProject := ""
			if replacement.Project != nil {
				replacementProject, _ = NormalizeProject(*replacement.Project)
			}
			if replacementProject != p.Project {
				return nil, &CheckpointProjectMismatchError{RequestedProject: p.Project, MemoryProject: &replacementProject}
			}
			if err := validateCheckpointSupersessionPairTx(tx, replacement, target); err != nil {
				return nil, err
			}
		}
	}
	// No inline write may overwrite evaluated historical evidence, even when it
	// is a different inline input from the declared replacement.
	for _, input := range p.Memories {
		if len(targets) == 0 {
			break
		}
		fields, err := s.prepareObservationFields(input)
		if err != nil {
			return nil, err
		}
		for _, target := range targets {
			if fields.Scope != target.Scope {
				continue
			}
			if fields.TopicKey != "" && target.TopicKey != nil && fields.TopicKey == *target.TopicKey {
				return nil, ErrCheckpointInvalidSupersession
			}
			var hash string
			if err := tx.QueryRow(`SELECT normalized_hash FROM observations WHERE id = ?`, target.ID).Scan(&hash); err != nil {
				return nil, err
			}
			if fields.NormalizedHash == hash {
				return nil, ErrCheckpointInvalidSupersession
			}
		}
	}
	for _, declaration := range p.Supersessions {
		if _, isTarget := targets[declaration.ReplacementMemoryID]; isTarget {
			return nil, ErrCheckpointInvalidSupersession
		}
	}
	return targets, nil
}

func validateCheckpointSupersessionEndpointTx(tx *sql.Tx, memory *Observation) error {
	if memory.State() != ObservationStateActive {
		return ErrCheckpointInvalidSupersession
	}
	var superseded bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM memory_relations WHERE target_id = ? AND relation = 'supersedes' AND judgment_status = 'judged')`, memory.SyncID).Scan(&superseded); err != nil {
		return err
	}
	if superseded {
		return ErrCheckpointInvalidSupersession
	}
	return nil
}

func validateCheckpointSupersessionPairTx(tx *sql.Tx, replacement, target *Observation) error {
	if replacement.ID == target.ID {
		return ErrCheckpointInvalidSupersession
	}
	if err := validateCheckpointSupersessionEndpointTx(tx, replacement); err != nil {
		return err
	}
	var reverse bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM memory_relations WHERE source_id = ? AND target_id = ?)`, target.SyncID, replacement.SyncID).Scan(&reverse); err != nil {
		return err
	}
	// Independent curation preserves the canonical existing pair orientation.
	// A checkpoint cannot silently turn its requested directed verdict around.
	if reverse {
		return ErrCheckpointInvalidSupersession
	}
	return nil
}
