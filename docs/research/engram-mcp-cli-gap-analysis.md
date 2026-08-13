# Engram MCP vs CLI: gap analysis

Date: 2026-08-13

> Historical research note: the product decisions resulting from this analysis
> are recorded in [ADR-0001](../adr/0001-cli-as-memory-domain-contract.md). The
> approved CLI parity described there has now been implemented; this file
> preserves the pre-implementation evidence and gap inventory.

## Scope

This comparison uses only primary repository sources: implementation, tests,
and shipped documentation. A gap means that MCP exposes a public capability
whose parameters, semantics, controls, or output contract have no equivalent
CLI operation. This document does not decide which gaps should be implemented.

## Evidence summary

The implementation exposes 22 MCP tools: 18 in the agent profile and four in
admin; this is asserted by [the profile test](../../internal/mcp/mcp_test.go#L2373-L2394).
Their schemas are registered in [internal/mcp/mcp.go](../../internal/mcp/mcp.go#L261-L933).
The CLI dispatcher is [cmd/engram/main.go](../../cmd/engram/main.go#L591-L669)
and its published usage is [printUsage](../../cmd/engram/main.go#L2637-L2705).

MCP has explicit read-only/destructive/idempotency annotations on every tool
([tests](../../internal/mcp/mcp_test.go#L2456-L2498)), project-aware JSON
envelopes and structured ambiguous-project recovery
([tests](../../internal/mcp/mcp_test.go#L4003-L4166)). The CLI is mostly
human-readable text; doctor is the demonstrated exception, with a test that
matches its JSON envelope to MCP ([doctor parity test](../../cmd/engram/doctor_test.go#L237-L260)).

## Comparison matrix

| MCP capability | CLI today | Concrete gap / definition required | Priority |
| --- | --- | --- | --- |
| Search: query, project auto-resolution, all-projects, AND/OR match mode, bounded results | `search` has type/project/scope/limit and prints truncated prose | Define project policy, `all_projects`, `match_mode`, lifecycle/relation annotations, max limit, and JSON output. [MCP](../../internal/mcp/mcp.go#L261-L295); [CLI](../../cmd/engram/main.go#L921-L995); [match tests](../../internal/mcp/mcp_test.go#L7390-L7450) | P0 |
| Save: session selection, prompt capture, candidate feedback, lifecycle state, guarded recovery | `save` writes a `manual-save[-project]` session directly | Define whether save shares MCP duplicate/upsert/candidate/lifecycle behavior, safe project resolution, session ID, and machine-readable result usable by relation workflow. [MCP docs](../../DOCS.md#L837-L851); [CLI](../../cmd/engram/main.go#L997-L1066) | P0 |
| Pending-conflict judgment | `conflicts` can list/show/scan but no direct judgment command | Define command grammar for judgment ID, relation, reason/evidence/confidence/session; validation and confirmation for low-confidence/high-impact judgments. [MCP](../../internal/mcp/mcp.go#L824-L875); [CLI](../../cmd/engram/conflicts.go#L14-L51) | P0 |
| Direct semantic comparison | Semantic scan chooses pairs and invokes a runner | Add/decline a caller-supplied pair verdict: two IDs, relation, confidence, bounded reasoning, optional model, cross-project rule, and `not_conflict` result. [MCP](../../internal/mcp/mcp.go#L878-L933); [scan](../../cmd/engram/conflicts.go#L302-L467) | P0 |
| Full observation retrieval | Search and timeline truncate at 300/500 chars | No full get-by-ID command. Define text/JSON complete record, including topic/tool metadata. [MCP](../../internal/mcp/mcp.go#L609-L625); [CLI truncation](../../cmd/engram/main.go#L979-L994) [and](../../cmd/engram/main.go#L1281-L1284) | P1 |
| Partial update | No CLI route | Define mutable/clearable fields and output for update by ID. [MCP](../../internal/mcp/mcp.go#L375-L403); [test](../../internal/mcp/mcp_test.go#L1447-L1479) | P1 |
| Review lifecycle | No CLI route | Define stale-list and mark-reviewed grammar, project/limit, output, and local-only/non-sync semantics. [MCP](../../internal/mcp/mcp.go#L410-L425); [test](../../internal/mcp/mcp_test.go#L946-L1027) | P1 |
| Current project inspection | No non-error inspection command | Define response fields, ambiguity exit behavior, alternatives and interaction with override. [MCP response](../../DOCS.md#L793-L808) | P1 |
| Explicit deterministic project merge | `projects consolidate` is heuristic/interactive | Define whether explicit source-to-target merge is required; if so sources, target, preview/confirmation, errors, output. [MCP](../../internal/mcp/mcp.go#L767-L788); [CLI](../../cmd/engram/main.go#L1827-L1847) | P1 |
| Prompt persistence and session lifecycle | CLI only has implicit manual save sessions and deletion | Missing prompt-save, session start/end, and structured session summary contracts. Define IDs, directory/project ownership, summary template, output. [MCP](../../internal/mcp/mcp.go#L476-L505) [and](../../internal/mcp/mcp.go#L628-L735) | P2 |
| Passive learning capture | No CLI route | Define input source (argument/file/stdin), accepted headings, source/session controls, deduplication and idempotent script behavior. [MCP](../../internal/mcp/mcp.go#L737-L765); [tests](../../internal/mcp/mcp_test.go#L676-L771) | P2 |
| Topic-key suggestion; pin/unpin | Save accepts a raw topic; no pin controls | Define standalone suggestion output and local-only pin UX/documentation. [MCP](../../internal/mcp/mcp.go#L431-L449) [and](../../internal/mcp/mcp.go#L507-L537) | P2 |

Priorities are sequencing, not product approval: P0 blocks a coherent,
automatable memory-and-relation workflow; P1 preserves retrieval, curation and
project-control parity; P2 is agent-oriented workflow support whose CLI value
needs an explicit product decision.

## Cross-cutting definitions required

1. **Canonical inventory.** Source/test says 22 tools, but README and DOCS say
   20 and CLI help says 19/15-agent/4-admin. This is documentation and contract
   drift that must be reconciled before claiming a parity target.
   [Test](../../internal/mcp/mcp_test.go#L2373-L2385),
   [README](../../README.md#L135-L146), [DOCS](../../DOCS.md#L812-L814),
   [CLI help](../../cmd/engram/main.go#L2643-L2649).

2. **Structured CLI output.** Define a consistent JSON flag, schema/version,
   stdout/stderr and exit-code discipline for commands intended for automation.
   Doctor provides the existing baseline. [Test](../../cmd/engram/doctor_test.go#L237-L260).

3. **Project controls.** MCP validates overrides and has structured recovery;
   CLI behavior varies: save accepts raw project while conflicts falls back to
   cwd. Define one CLI resolution and ambiguity policy. [Save](../../cmd/engram/main.go#L1041-L1060),
   [conflicts resolver](../../cmd/engram/conflicts.go#L53-L73).

4. **Destructive/judgment permissions.** MCP marks deletion and merge
   destructive and instructs escalation for sensitive judgments; CLI semantic
   scan separately prompts unless `--yes`. Define consistent confirmation,
   noninteractive, provenance and dry-run policy. [MCP judge](../../internal/mcp/mcp.go#L824-L847),
   [CLI prompt](../../cmd/engram/conflicts.go#L413-L426).

5. **Tests.** For each approved CLI operation, add command tests for success,
   invalid input, project controls, JSON/text output and noninteractive
   destructive behavior. Use common store fixtures for any semantics claimed
   equal to MCP. MCP already covers review, compare and recovery behavior;
   [review tests](../../internal/mcp/mcp_test.go#L946-L1049),
   [compare tests](../../internal/mcp/mcp_compare_test.go#L55-L256).

## Non-conclusion

This analysis does not recommend exposing every MCP tool through CLI. Prompt
capture, passive capture and session activity may remain intentionally
agent-facing; the missing decision is whether they need a stable CLI contract.
