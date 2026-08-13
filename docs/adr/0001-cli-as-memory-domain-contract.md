# Treat the CLI as a memory domain contract

Engram's CLI will be a supported interface for humans, skills, and scripts, with
stable machine-readable output and the same domain rules as MCP for equivalent
memory operations. The CLI does not need one command per MCP tool: the first
parity scope covers curated memory workflows and deliberately excludes automatic
prompt capture, session start/end, session summaries, and passive capture, which
remain agent lifecycle operations until a concrete CLI automation use case is
approved. Existing command names remain compatible while missing operations are
added to the current flat grammar and the existing `conflicts` and `projects`
groups. Approved memory commands expose clean JSON for automation. An explicit
CLI `--project` is a trusted human or script selection after format validation;
when omitted, Engram uses automatic project detection. Completion requires all
approved memory, review, relation, and project operations plus CLI–MCP contract
tests and aligned public documentation. Existing flags remain compatible while
clearer canonical aliases may be added. Multi-record destructive operations
require interactive confirmation or explicit `--yes`, with `--dry-run` where a
preview is meaningful. Semantic scan may discover and judge candidate pairs;
`conflicts compare` only persists a verdict already supplied by its caller.
JSON payloads are public additive contracts with a common structured error and
no speculative schema version. Search exposes explicit all-project and token
matching controls, with complete content in JSON. Updates are partial, cannot
move a memory between projects, and use an explicit control to clear a topic
key. Project inspection reports ambiguity successfully; operations that require
a resolved project fail until the caller supplies a selection.
Saving succeeds even when it surfaces pending conflict candidates; structured
output carries the judgment workflow forward. Review defaults to the current
project, while cross-project review is explicit. Review timestamps and pins are
local-only. Relation commands preserve MCP validation and `not_conflict`
semantics. Explicit project merge remains separate from heuristic consolidation,
supports previews, and requires confirmation for mutation.
Full retrieval includes memory metadata and relations; existing read operations
gain structured output, and topic-key suggestion remains read-only. CLI and MCP
stay thin and share domain operations rather than calling each other's handlers.
Correct but previously ignored invalid input may become a deterministic error;
valid commands and human-readable output remain compatible where possible. The
external `engram-memory` skill adopts the richer contract in a separate Packy
change after Engram ships it, while its current helper remains supported.
