---
name: engram-plugin-thin
description: >
  Adapter boundary rules for plugin integrations.
  Trigger: Changes in plugin scripts/hooks for Claude, OpenCode, Gemini, Codex, or Cursor.
license: Apache-2.0
metadata:
  author: yersonargotev
  version: "1.0"
---

## When to Use

Use this skill when:
- Editing plugin hooks/scripts/adapters
- Adding passive/active memory capture integrations
- Wiring agent-specific setup behavior

---

## Boundary Rules

1. Keep adapters thin: parse input, call API/tool, return.
2. Put complex logic in Go core (`store/server/mcp`).
3. Avoid extra runtime dependencies in plugin scripts.
4. Reuse a shared contract across all supported agents.

---

## Compatibility Checklist

- [ ] Claude Code flow still works
- [ ] OpenCode flow still works
- [ ] Gemini/Codex config paths remain valid
- [ ] Cursor local Agent Plugin install remains valid
- [ ] Docs reflect real integration behavior
