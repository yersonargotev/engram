# Engram

Engram is a persistent memory system for coding agents. Its domain separates
durable knowledge from the transient activity that produces it.

## Language

**Memory**:
A durable, structured observation that can be recalled and curated independently
of the coding session that produced it.
_Avoid_: Note, record

**Memory operation**:
An action that reads, creates, changes, organizes, reviews, or relates memories.
It remains meaningful outside an active agent session.
_Avoid_: MCP operation, CLI operation

**Curated memory workflow**:
A selective workflow in which a human, skill, or agent recalls relevant project
knowledge and preserves only durable findings.
_Avoid_: Session capture, activity logging

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
