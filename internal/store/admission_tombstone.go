package store

import (
	"database/sql"
	"fmt"
)

// migrateAdmissionTombstone permanently enforces the v3 invariant that an
// opened database contains no dedicated Admission persistence. It runs on
// every open because an older Engram binary can recreate these tables.
func (s *Store) migrateAdmissionTombstone() error {
	return s.withTx(func(tx *sql.Tx) error {
		for _, table := range []string{
			"admission_shadow_reviews",
			"admission_study_omissions",
			"admission_shadow_proposals",
			"admission_shadow_runs",
			"admission_studies",
		} {
			if _, err := s.execHook(tx, "DROP TABLE IF EXISTS "+table); err != nil {
				return fmt.Errorf("drop Admission table %s: %w", table, err)
			}
		}
		return nil
	})
}
