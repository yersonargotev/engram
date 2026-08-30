package store

import (
	"database/sql"
	"fmt"
	"strings"
)

// MemoryProposal is immutable checkpoint audit evidence retained for explicit
// review. It is local-only and remains distinct from a Memory.
type MemoryProposal struct {
	ID        string `json:"id"`
	Project   string `json:"project"`
	Title     string `json:"title"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
}

type MemoryProposalInput struct {
	Title   string
	Content string
}

func createMemoryProposalTx(tx *sql.Tx, project string, input MemoryProposalInput) (*MemoryProposal, error) {
	project, _ = NormalizeProject(project)
	input.Title = RedactPrivateBlocks(input.Title)
	input.Content = RedactPrivateBlocks(input.Content)
	if project == "" || strings.TrimSpace(input.Title) == "" || strings.TrimSpace(input.Content) == "" {
		return nil, ErrCheckpointInvalidReferences
	}

	proposal := &MemoryProposal{
		ID:      newSyncID("proposal"),
		Project: project,
		Title:   input.Title,
		Content: input.Content,
	}
	if _, err := tx.Exec(`
		INSERT INTO memory_proposals (id, project, title, content)
		VALUES (?, ?, ?, ?)`,
		proposal.ID, proposal.Project, proposal.Title, proposal.Content,
	); err != nil {
		return nil, fmt.Errorf("insert Memory proposal: %w", err)
	}
	if err := tx.QueryRow(`SELECT created_at FROM memory_proposals WHERE id = ?`, proposal.ID).Scan(&proposal.CreatedAt); err != nil {
		return nil, fmt.Errorf("read Memory proposal timestamp: %w", err)
	}
	return proposal, nil
}
