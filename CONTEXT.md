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

**Recall strategy**:
A bounded policy for deciding when and which durable project knowledge is exposed
to an agent for the current task.
_Avoid_: History injection, session replay

**Recall scope**:
The explicit project, personal, or cross-project boundary within which a Recall
strategy may select Memories.
_Avoid_: Implicit fallback, global recall

**Recall budget**:
The explicit item and byte limits within which one Recall strategy may expose
Memory candidates or content. Exceeding it requires a deliberate continuation.
_Avoid_: Silent pagination, unlimited context

**Recall utility**:
An explicit assessment of whether an exposed Memory was decisive, orienting,
duplicate, or unused in a root user turn. An unassessed Memory remains unknown.
_Avoid_: Retrieval count, inferred use

**Recall authority**:
The ordering that treats current user intent and maintained or runtime evidence as
authoritative while Memories remain advisory and contradictions stay explicit.
_Avoid_: Memory source of truth, silent override

**Recall feedback**:
A content-free local assessment that attributes Recall utility to an exposed
Memory. Missing feedback preserves an unknown assessment.
_Avoid_: Usage inference, retrieval telemetry

**Protocol contract**:
The versioned set of machine-verifiable Memory, Recall, and checkpoint invariants
that every Engram surface must preserve.
_Avoid_: MCP policy, hook policy

**Content capture**:
Explicitly enabled persistence of raw transient agent activity, such as a full
prompt or subagent result. It is neither a Memory nor a Memory checkpoint.
_Avoid_: Memory save, automatic context

**Capture consent**:
Explicit permission to persist one type of Content capture for one project or an
expiring session. It is independent of Memory and checkpoint activation.
_Avoid_: Global consent, implied consent

**Diagnostic capture**:
Short-lived, local-only Content capture reserved for troubleshooting and kept
outside Memory and Recall.
_Avoid_: Session history, Memory evidence

**Legacy prompt archive**:
Frozen prompt state created before the Content capture consent boundary. It
remains available only through explicit legacy access and outside current Recall.
_Avoid_: Diagnostic capture, active prompt history

**Session summary**:
An optional Memory that synthesizes durable outcomes of a session when explicitly
requested. It is neither lifecycle completion nor a Memory checkpoint.
_Avoid_: Mandatory close, automatic summary

**Memory checkpoint**:
A root-user-turn assessment that records whether durable knowledge was saved,
skipped with an explicit reason, or left for review. It guarantees a disposition,
not that every turn creates a Memory.
_Avoid_: Automatic save, mandatory memory

**Mixed Memory checkpoint**:
A `needs_review` Memory checkpoint that atomically references one or more settled
Memories and exactly one unresolved Memory proposal.
_Avoid_: Partial save, proposal promotion

**Terminal Memory commit**:
The default act of creating or referencing settled Memories atomically with the
terminal Memory checkpoint for a root user turn.
_Avoid_: Proactive save, immediate save

**Terminal Memory preflight**:
A bounded, non-persisting assessment before a Terminal Memory commit that reuses
exact duplicates and surfaces only material semantic candidates.
_Avoid_: Memory save, pending checkpoint

**Root user turn**:
The causal unit that begins with one user message and ends when all agent,
tool, subagent, and continuation work caused by that message has settled.
_Avoid_: Model turn, tool turn, session

**Checkpoint verifier**:
A host capability that detects a missing terminal Memory checkpoint for a root
user turn without deciding what knowledge is durable.
_Avoid_: Memory judge, automatic save

**Checkpoint guarantee**:
The promise that a host makes one bounded, identity-preserving attempt to obtain
a missing terminal Memory checkpoint and never hides an incomplete result. It
guarantees visible enforcement, not indefinite blocking or successful persistence.
_Avoid_: Mandatory completion barrier, retry loop

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

**Compatibility tuple**:
An exact Managed Pack, Engram binary, host plugin, and Protocol contract version
combination that the Distribution authority has verified to work together.
_Avoid_: Engram version, latest compatible

**Agent lifecycle operation**:
An action that records or coordinates transient agent activity, such as prompts,
session boundaries, or passive capture.
_Avoid_: Memory operation

**Dead mutation recovery**:
A deliberate local attempt to apply a previously abandoned remote mutation
without emitting a new outbound mutation.
_Avoid_: Republish, requeue
