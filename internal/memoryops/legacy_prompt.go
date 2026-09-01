package memoryops

import (
	"errors"
	"fmt"

	"github.com/yersonargotev/engram/internal/store"
)

const (
	DefaultLegacyPromptAccessLimit = store.DefaultLegacyPromptPageSize
	MaxLegacyPromptAccessLimit     = store.MaxLegacyPromptPageSize
)

var (
	ErrLegacyPromptInvalidScope  = errors.New("invalid Legacy prompt scope")
	ErrLegacyPromptInvalidCursor = errors.New("invalid Legacy prompt cursor")
	ErrLegacyPromptInvalidLimit  = errors.New("invalid Legacy prompt limit")
)

type LegacyPromptInventoryInput struct {
	Project string
	Unowned bool
	All     bool
}

type LegacyPromptInventoryResult struct {
	Project  string `json:"project,omitempty"`
	Unowned  bool   `json:"unowned,omitempty"`
	All      bool   `json:"all,omitempty"`
	Count    int64  `json:"count"`
	Sessions int64  `json:"sessions"`
	OldestAt string `json:"oldest_at,omitempty"`
	NewestAt string `json:"newest_at,omitempty"`
}

type LegacyPromptAccessInput struct {
	Project string
	Unowned bool
	All     bool
	Cursor  int64
	Limit   int
}

type LegacyPromptAccessResult struct {
	Project    string         `json:"project,omitempty"`
	Unowned    bool           `json:"unowned,omitempty"`
	Cursor     int64          `json:"cursor,omitempty"`
	Limit      int            `json:"limit"`
	Prompts    []store.Prompt `json:"prompts"`
	NextCursor int64          `json:"next_cursor,omitempty"`
}

type LegacyPromptExportInput struct {
	Project string
	Unowned bool
	All     bool
}

type LegacyPromptExportResult struct {
	Project string         `json:"project,omitempty"`
	Unowned bool           `json:"unowned,omitempty"`
	All     bool           `json:"all,omitempty"`
	Prompts []store.Prompt `json:"prompts"`
}

type LegacyPromptPurgeInput struct {
	Project string
	Unowned bool
	All     bool
}

type LegacyPromptPurgeResult struct {
	Project string `json:"project,omitempty"`
	Unowned bool   `json:"unowned,omitempty"`
	Deleted int64  `json:"deleted"`
}

// InventoryLegacyPrompts reports only counts and archive time boundaries. It
// never reads prompt content into the public result.
func (s *Service) InventoryLegacyPrompts(input LegacyPromptInventoryInput) (*LegacyPromptInventoryResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	scope, err := validateLegacyPromptScope(input.Project, input.Unowned, input.All, true)
	if err != nil {
		return nil, err
	}
	inventory, err := s.store.InventoryLegacyPrompts(scope)
	if err != nil {
		return nil, fmt.Errorf("inventory Legacy prompts: %w", err)
	}
	return &LegacyPromptInventoryResult{
		Project:  scope.Project,
		Unowned:  scope.Unowned,
		All:      scope.All,
		Count:    inventory.Count,
		Sessions: inventory.Sessions,
		OldestAt: inventory.OldestAt,
		NewestAt: inventory.NewestAt,
	}, nil
}

// AccessLegacyPrompts is the explicit, bounded content-reading operation for a
// single project or the unowned archive.
func (s *Service) AccessLegacyPrompts(input LegacyPromptAccessInput) (*LegacyPromptAccessResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	scope, err := validateLegacyPromptScope(input.Project, input.Unowned, input.All, false)
	if err != nil {
		return nil, err
	}
	if input.Cursor < 0 {
		return nil, ErrLegacyPromptInvalidCursor
	}
	limit := input.Limit
	if limit == 0 {
		limit = DefaultLegacyPromptAccessLimit
	}
	if limit < 1 || limit > MaxLegacyPromptAccessLimit {
		return nil, fmt.Errorf("%w: must be between 1 and %d", ErrLegacyPromptInvalidLimit, MaxLegacyPromptAccessLimit)
	}
	page, err := s.store.AccessLegacyPrompts(scope, input.Cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("access Legacy prompts: %w", err)
	}
	return &LegacyPromptAccessResult{
		Project: scope.Project, Unowned: scope.Unowned, Cursor: input.Cursor, Limit: limit,
		Prompts: page.Prompts, NextCursor: page.NextCursor,
	}, nil
}

// ExportLegacyPrompts provides intact records to the caller while keeping file
// format and atomic output ownership in the adapter layer.
func (s *Service) ExportLegacyPrompts(input LegacyPromptExportInput) (*LegacyPromptExportResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	scope, err := validateLegacyPromptScope(input.Project, input.Unowned, input.All, true)
	if err != nil {
		return nil, err
	}
	prompts, err := s.store.ExportLegacyPrompts(scope)
	if err != nil {
		return nil, fmt.Errorf("export Legacy prompts: %w", err)
	}
	return &LegacyPromptExportResult{
		Project: scope.Project, Unowned: scope.Unowned, All: scope.All, Prompts: prompts,
	}, nil
}

// PurgeLegacyPrompts is the explicit archive deletion operation. Confirmation
// remains an adapter concern; this operation never creates sync evidence.
func (s *Service) PurgeLegacyPrompts(input LegacyPromptPurgeInput) (*LegacyPromptPurgeResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	scope, err := validateLegacyPromptScope(input.Project, input.Unowned, input.All, false)
	if err != nil {
		return nil, err
	}
	deleted, err := s.store.PurgeLegacyPrompts(scope)
	if err != nil {
		return nil, fmt.Errorf("purge Legacy prompts: %w", err)
	}
	return &LegacyPromptPurgeResult{Project: scope.Project, Unowned: scope.Unowned, Deleted: deleted}, nil
}

func validateLegacyPromptScope(project string, unowned, all, allowAll bool) (store.LegacyPromptScope, error) {
	project, _ = store.NormalizeProject(project)
	selected := 0
	if project != "" {
		selected++
	}
	if unowned {
		selected++
	}
	if all {
		selected++
	}
	if selected != 1 || (all && !allowAll) {
		return store.LegacyPromptScope{}, ErrLegacyPromptInvalidScope
	}
	return store.LegacyPromptScope{Project: project, Unowned: unowned, All: all}, nil
}
