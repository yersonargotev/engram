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

**Agent lifecycle operation**:
An action that records or coordinates transient agent activity, such as prompts,
session boundaries, or passive capture.
_Avoid_: Memory operation

**Dead mutation recovery**:
A deliberate local attempt to apply a previously abandoned remote mutation
without emitting a new outbound mutation.
_Avoid_: Republish, requeue
