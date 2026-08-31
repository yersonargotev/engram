#!/bin/bash
# Engram — Post-compaction hook for Claude Code
#
# When compaction happens, re-inject the Terminal Memory cue and context.

ENGRAM_PORT="${ENGRAM_PORT:-7437}"
ENGRAM_URL="http://127.0.0.1:${ENGRAM_PORT}"

# Load shared helpers
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/_helpers.sh"

# Read hook input from stdin
INPUT=$(cat)
SESSION_ID=$(echo "$INPUT" | jq -r '.session_id // empty')
CWD=$(echo "$INPUT" | jq -r '.cwd // empty')
PROJECT=$(resolve_project "$CWD") || PROJECT=""

# Ensure session exists
if [ -n "$SESSION_ID" ] && [ -n "$PROJECT" ]; then
  curl -sf "${ENGRAM_URL}/sessions" \
    -X POST \
    -H "Content-Type: application/json" \
    -d "$(jq -n --arg id "$SESSION_ID" --arg project "$PROJECT" --arg dir "$CWD" \
      '{id: $id, project: $project, directory: $dir}')" \
    > /dev/null 2>&1
fi

# Fetch context from previous sessions
CONTEXT=""
if [ -n "$PROJECT" ]; then
  ENCODED_PROJECT=$(printf '%s' "$PROJECT" | jq -sRr @uri)
  CONTEXT=$(curl -sf "${ENGRAM_URL}/context?project=${ENCODED_PROJECT}" --max-time 3 2>/dev/null | jq -r '.context // empty')
fi

# Resolve protocol verbosity mode for this slug. All slim/full branching
# (including the engram-version floor check) lives in Go — see `engram
# protocol-mode`. A missing/old engram binary or an unrecognized subcommand
# never yields "slim" here, so this always defaults safely to full. $mode is
# NEVER echoed/logged to this hook's own stdout.
mode=$(engram protocol-mode claude-code 2>/dev/null)
if [ "$mode" != "slim" ]; then
  mode="full"
fi

# Inject Memory Protocol + compaction instruction + context. Only the static
# protocol prose is gated on $mode — the "CRITICAL INSTRUCTION" header below
# and the numbered recovery steps that follow it stay unconditional (they are
# the compaction-recovery contract itself, not the duplicated protocol text).
if [ "$mode" != "slim" ]; then
cat <<'PROTOCOL'
## Engram Terminal Memory — ACTIVE PROTOCOL

For every settled root user turn, make exactly one Terminal Memory commit:
`saved`, `needs_review`, or `skipped(no_durable_knowledge)`. Compaction does not
create a new root turn; reuse the supplied opaque identity and finalize after
all remaining causal work settles.

### DEFAULT AGENT TOOLS — exactly five
mem_current_project, mem_search, mem_get_observation, mem_checkpoint, mem_checkpoint_status

Current user intent, maintained source, and runtime evidence override Memory.
The engram-memory skill owns the disposition rubric. Session summary and
independent save remain optional curation workflows.

PROTOCOL
fi

printf "\nContinue the same root user turn after compaction. Recover only context that can change the work, then make its Terminal Memory commit after the work settles. Project hint: '%s'.\n" "$PROJECT"

# Inject memory context if available
if [ -n "$CONTEXT" ]; then
  printf "\n%s\n" "$CONTEXT"
fi

exit 0
