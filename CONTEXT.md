# Engram

Engram is a persistent memory system for coding agents. Its domain separates
durable knowledge from the transient activity that produces it.

## Language

**Memory**:
A durable, structured observation that can be recalled and curated independently
of the coding session that produced it.
_Avoid_: Note, record

**Memory proposal**:
Potentially durable knowledge that has not been accepted as a Memory.
_Avoid_: Memory candidate, draft memory

**Protected proposal**:
A Memory proposal whose false rejection blocks any move toward automatic rejection.
_Avoid_: Critical memory, mandatory save

**Evidence bundle**:
Bounded, provenance-bearing evidence used to formulate or assess Memory proposals.
_Avoid_: Transcript, session dump

**Evidence acquisition**:
A read-only operation that builds an Evidence bundle from explicitly selected,
already-persisted sources and reports any omissions or truncation.
_Avoid_: Session ingestion, automatic capture

**Admission assessment**:
An explainable `admit`, `review`, or `reject` recommendation for a Memory proposal.
It does not itself create or discard a Memory.
_Avoid_: Classifier result, admission decision

**Shadow admission run**:
An explicitly requested, local evaluation that retains Memory proposals and
Admission assessments for later correction and measurement without promoting
them to Memories.
_Avoid_: Session capture, automatic admission

**Admission correction**:
An append-only human verdict about one retained Memory proposal. It supplies
evaluation evidence and does not itself promote, reject, or delete a Memory.
_Avoid_: Memory review, approval

**Promotion**:
The explicit acceptance of a Memory proposal as a Memory.
_Avoid_: Automatic save, candidate persistence

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
_Avoid_: Admission hook, memory judge, automatic save

**Checkpoint guarantee**:
The promise that a host cannot complete a root user turn without a terminal
Memory checkpoint. Hosts without a reliable continuation capability are
best-effort and must not claim this guarantee.
_Avoid_: Universal support, eventual capture

**Activation cue**:
A short, always-delivered host instruction that states the Memory checkpoint
invariant and points to the canonical skill for the detailed rubric. It does not
repeat the full memory protocol or decide the checkpoint outcome.
_Avoid_: Full protocol, admission policy

**Distribution authority**:
The canonical source that identifies, versions, and publishes Engram setup and
integration artifacts. `yersonargotev/engram` is Engram's sole distribution
authority; host registries, caches, and upstream repositories are delivery or
source relationships rather than competing authorities. Stable setup identifies
a release and exact source revision rather than following a moving branch.
_Avoid_: Install location, upstream relationship

**Task briefing**:
A selection of durable project memories intended to help an agent perform its
current task. Transient work signals may guide selection but are not part of the
briefing itself.
_Avoid_: Repository snapshot, activity summary

**Task intent**:
An explicit description of the work an agent is currently trying to accomplish.
When supplied, it is the authoritative selection signal for a task briefing.
_Avoid_: Prompt history, branch name

**Repository signal**:
Transient evidence from the working tree, such as its branch, diff, or affected
paths, that supplements task intent when selecting a task briefing.
_Avoid_: Memory, task intent

**Selection evidence**:
A traceable connection between a task or repository signal and a memory that
justifies the memory's inclusion or position in a task briefing.
_Avoid_: Relevance score, model rationale

**Agent lifecycle operation**:
An action that records or coordinates transient agent activity, such as prompts,
session boundaries, or passive capture.
_Avoid_: Memory operation

**Dead mutation recovery**:
A deliberate local attempt to apply a previously abandoned remote mutation
without emitting a new outbound mutation.
_Avoid_: Republish, requeue
