# Engram

Engram is a persistent memory system for coding agents. Its domain separates
durable knowledge from the transient activity that produces it.

## Language

**Memory**:
A durable, structured observation that can be recalled and curated independently
of the coding session that produced it.
_Avoid_: Note, record

**Memory proposal**:
Immutable local checkpoint audit evidence for a `needs_review` disposition. It
contains an Engram-derived identity, project, creation time, and the caller's
redacted title and content. It is not a Memory, and no workflow converts it into
one.
_Avoid_: Memory candidate, draft memory

**Activation study**:
A frozen, versioned matched-prompt evaluation that measures agent skill and CLI
invocation across disposable repository treatments using bounded event evidence.
_Avoid_: Agent benchmark, activation experiment

**Memory operation**:
An action that reads, creates, changes, organizes, reviews, or relates memories.
It remains meaningful outside an active agent session.
_Avoid_: MCP operation, CLI operation

**Curated memory workflow**:
A selective workflow in which a human, skill, or agent recalls relevant project
knowledge and preserves only durable findings.
_Avoid_: Session capture, activity logging

**Memory checkpoint**:
A root-user-turn assessment that records whether durable knowledge was saved,
skipped with an explicit reason, or left for review. It guarantees a disposition,
not that every turn creates a Memory.
_Avoid_: Automatic save, mandatory memory

**Root user turn**:
The causal unit that begins with one user message and ends when all agent,
tool, subagent, and continuation work caused by that message has settled.
_Avoid_: Model turn, tool turn, session

**Checkpoint verifier**:
A host capability that detects a missing terminal Memory checkpoint for a root
user turn without deciding what knowledge is durable.
_Avoid_: Memory judge, automatic save

**Checkpoint guarantee**:
The promise that a host cannot complete a root user turn without a terminal
Memory checkpoint. Hosts without a reliable continuation capability are
best-effort and must not claim this guarantee.
_Avoid_: Universal support, eventual capture

**Activation cue**:
A short, always-delivered host instruction that states the Memory checkpoint
invariant and points to the canonical skill for the detailed rubric. It does not
repeat the full memory protocol or decide the checkpoint outcome.
_Avoid_: Full protocol, Memory policy

**Distribution authority**:
The canonical source that identifies, versions, and publishes Engram setup and
integration artifacts. `yersonargotev/engram` is Engram's sole distribution
authority; host registries, caches, and upstream repositories are delivery or
source relationships rather than competing authorities. Stable setup identifies
a release and exact source revision rather than following a moving branch.
_Avoid_: Install location, upstream relationship

**Agent lifecycle operation**:
An action that records or coordinates transient agent activity, such as prompts,
session boundaries, or passive capture.
_Avoid_: Memory operation

**Dead mutation recovery**:
A deliberate local attempt to apply a previously abandoned remote mutation
without emitting a new outbound mutation.
_Avoid_: Republish, requeue
