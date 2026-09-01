package memoryops

import (
	"errors"
	"strings"
	"testing"
)

func TestLegacyPromptOperationsValidateExplicitScopes(t *testing.T) {
	service := newTestService(t)

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "inventory requires one scope",
			call: func() error {
				_, err := service.InventoryLegacyPrompts(LegacyPromptInventoryInput{})
				return err
			},
		},
		{
			name: "access rejects competing scopes",
			call: func() error {
				_, err := service.AccessLegacyPrompts(LegacyPromptAccessInput{Project: "alpha", Unowned: true})
				return err
			},
		},
		{
			name: "access rejects negative cursor",
			call: func() error {
				_, err := service.AccessLegacyPrompts(LegacyPromptAccessInput{Project: "alpha", Cursor: -1})
				return err
			},
		},
		{
			name: "access rejects excessive limit",
			call: func() error {
				_, err := service.AccessLegacyPrompts(LegacyPromptAccessInput{Project: "alpha", Limit: MaxLegacyPromptAccessLimit + 1})
				return err
			},
		},
		{
			name: "purge rejects all",
			call: func() error {
				_, err := service.PurgeLegacyPrompts(LegacyPromptPurgeInput{All: true})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, ErrLegacyPromptInvalidScope) && !errors.Is(err, ErrLegacyPromptInvalidCursor) && !errors.Is(err, ErrLegacyPromptInvalidLimit) {
				t.Fatalf("error = %v, want a Legacy prompt validation error", err)
			}
		})
	}
}

func TestLegacyPromptServiceProvidesInventoryAccessExportAndPurge(t *testing.T) {
	service := newTestService(t)
	seedServiceLegacyPrompt(t, service, "legacy-1", "session-a", "alpha secret", "alpha", "2026-01-01 00:00:00")
	seedServiceLegacyPrompt(t, service, "legacy-2", "session-u", "unowned secret", nil, "2026-01-02 00:00:00")
	seedServiceLegacyPrompt(t, service, "legacy-3", "session-b", "beta secret", "beta", "2026-01-03 00:00:00")

	inventory, err := service.InventoryLegacyPrompts(LegacyPromptInventoryInput{Project: " Alpha "})
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if inventory.Project != "alpha" || inventory.Count != 1 || inventory.Sessions != 1 {
		t.Fatalf("inventory = %#v", inventory)
	}
	allInventory, err := service.InventoryLegacyPrompts(LegacyPromptInventoryInput{All: true})
	if err != nil {
		t.Fatalf("inventory all: %v", err)
	}
	if !allInventory.All || allInventory.Count != 3 || allInventory.Sessions != 3 {
		t.Fatalf("all inventory = %#v", allInventory)
	}

	page, err := service.AccessLegacyPrompts(LegacyPromptAccessInput{Unowned: true})
	if err != nil {
		t.Fatalf("access: %v", err)
	}
	if page.Limit != DefaultLegacyPromptAccessLimit || len(page.Prompts) != 1 || page.Prompts[0].Content != "unowned secret" {
		t.Fatalf("page = %#v", page)
	}

	exported, err := service.ExportLegacyPrompts(LegacyPromptExportInput{All: true})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(exported.Prompts) != 3 || !exported.All {
		t.Fatalf("export = %#v", exported)
	}

	purged, err := service.PurgeLegacyPrompts(LegacyPromptPurgeInput{Project: "alpha"})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged.Project != "alpha" || purged.Deleted != 1 {
		t.Fatalf("purge = %#v", purged)
	}
	remaining, err := service.ExportLegacyPrompts(LegacyPromptExportInput{All: true})
	if err != nil {
		t.Fatalf("export remaining: %v", err)
	}
	if len(remaining.Prompts) != 2 {
		t.Fatalf("remaining prompts = %d, want 2", len(remaining.Prompts))
	}
}

func TestLegacyPromptServiceRequiresStore(t *testing.T) {
	var service *Service
	if _, err := service.InventoryLegacyPrompts(LegacyPromptInventoryInput{Project: "alpha"}); !errors.Is(err, ErrStoreRequired) {
		t.Fatalf("inventory error = %v, want ErrStoreRequired", err)
	}
}

func seedServiceLegacyPrompt(t *testing.T, service *Service, syncID, sessionID, content string, project any, createdAt string) {
	t.Helper()
	sessionProject := ""
	if project, ok := project.(string); ok {
		sessionProject = strings.TrimSpace(project)
	}
	if _, err := service.store.DB().Exec(`
		INSERT OR IGNORE INTO sessions (id, project, directory)
		VALUES (?, ?, ?)`, sessionID, sessionProject, "/tmp/"+sessionID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := service.store.DB().Exec(`
		INSERT INTO user_prompts (sync_id, session_id, content, project, created_at)
		VALUES (?, ?, ?, ?, ?)`, syncID, sessionID, content, project, createdAt); err != nil {
		t.Fatalf("seed prompt: %v", err)
	}
}
