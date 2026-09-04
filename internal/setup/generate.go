package setup

// Sync embedded plugin copies from the source of truth (plugin/ directory).
// Claude Code and Codex are installed via marketplace; OpenCode and the
// portable Agent Plugin are embedded for hosts that load files from disk.
// Run: go generate ./internal/setup/
//go:generate sh -c "rm -rf plugins/opencode && mkdir -p plugins/opencode && cp ../../plugin/opencode/engram.ts plugins/opencode/"
//go:generate sh -c "rm -rf plugins/engram && mkdir -p plugins/engram/skills/engram-memory && cp ../../plugin/engram/plugin.json ../../plugin/engram/mcp.json plugins/engram/ && cp ../../plugin/engram/skills/engram-memory/SKILL.md plugins/engram/skills/engram-memory/"
