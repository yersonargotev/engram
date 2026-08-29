#!/bin/bash
# Engram — UserPromptSubmit hook for Codex
#
# Captures the user prompt best-effort and forwards Codex's opaque root-turn
# identity as model-visible developer context. Memory semantics live in the
# canonical skill, not in this adapter.

ENGRAM_PORT="${ENGRAM_PORT:-7437}"
ENGRAM_URL="http://127.0.0.1:${ENGRAM_PORT}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/_helpers.sh"

INPUT=$(cat)

json_string() {
  local key="$1"
  printf '%s' "$INPUT" | jq -r --arg key "$key" \
    'if type == "object" and (.[$key] | type) == "string" then .[$key] else "" end' \
    2>/dev/null
}

CWD=$(json_string "cwd")
SESSION_ID=$(json_string "session_id")
ROOT_TURN_ID=$(json_string "turn_id")
PROMPT=$(json_string "prompt")

# Prompt capture remains lifecycle telemetry. It never creates a Memory or a
# checkpoint and cannot block prompt submission. Project policy stays in the
# server; detached standard streams keep the foreground hook prompt.
if [ -n "$PROMPT" ] && [ -n "$SESSION_ID" ]; then
  (
    PROJECT=$(resolve_project "$CWD") || exit 0
    curl -sf -X POST "${ENGRAM_URL}/prompts" --max-time 2 \
      -H 'Content-Type: application/json' \
      -d "$(jq -n --arg session "$SESSION_ID" --arg project "$PROJECT" --arg content "$PROMPT" \
        '{session_id:$session, project:$project, content:$content}')" \
      >/dev/null 2>&1 || true
  ) </dev/null >/dev/null 2>&1 &
fi

if [ -z "$SESSION_ID" ] || [ -z "$ROOT_TURN_ID" ]; then
  printf '%s\n' '{}'
  exit 0
fi

IDENTITY=$(jq -cn \
  --arg host "codex" \
  --arg session "$SESSION_ID" \
  --arg turn "$ROOT_TURN_ID" \
  '{host:$host,session_id:$session,root_turn_id:$turn}')
CONTEXT="Engram checkpoint identity for this root user turn: ${IDENTITY}. Pass these opaque values unchanged to mem_checkpoint or engram checkpoint record."

jq -n --arg context "$CONTEXT" \
  '{hookSpecificOutput:{hookEventName:"UserPromptSubmit",additionalContext:$context}}'

exit 0
