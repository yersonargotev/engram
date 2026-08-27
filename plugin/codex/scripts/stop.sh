#!/bin/bash
# Engram — Stop checkpoint verifier for Codex
#
# This adapter only verifies the core-owned checkpoint for the exact Codex
# root-turn identity. It never chooses or records a Memory disposition.

INPUT=$(cat)
SESSION_ID=$(printf '%s' "$INPUT" | jq -r 'if type == "object" and (.session_id | type) == "string" then .session_id else "" end' 2>/dev/null)
ROOT_TURN_ID=$(printf '%s' "$INPUT" | jq -r 'if type == "object" and (.turn_id | type) == "string" then .turn_id else "" end' 2>/dev/null)
STOP_HOOK_ACTIVE=$(printf '%s' "$INPUT" | jq -r 'if type == "object" and (.stop_hook_active | type) == "boolean" then (.stop_hook_active | tostring) else "" end' 2>/dev/null)

checkpoint_failure() {
  jq -n --arg message "$1" '{systemMessage:("Engram checkpoint verifier integration failure: " + $message)}'
}

if [ -z "$SESSION_ID" ] || [ -z "$ROOT_TURN_ID" ] || [ -z "$STOP_HOOK_ACTIVE" ]; then
  checkpoint_failure "Stop input is missing a string session_id, string turn_id, or boolean stop_hook_active."
  exit 0
fi

STATUS_DIR=$(mktemp -d "${TMPDIR:-/tmp}/engram-stop.XXXXXX") || {
  checkpoint_failure "temporary status capture is unavailable."
  exit 0
}
STATUS_OUTPUT_FILE="${STATUS_DIR}/stdout"
STATUS_ERROR_FILE="${STATUS_DIR}/stderr"
trap 'rm -f "$STATUS_OUTPUT_FILE" "$STATUS_ERROR_FILE"; rmdir "$STATUS_DIR" 2>/dev/null || true' EXIT

engram checkpoint status \
  --host=codex \
  --session-id="$SESSION_ID" \
  --root-turn-id="$ROOT_TURN_ID" \
  --json >"$STATUS_OUTPUT_FILE" 2>"$STATUS_ERROR_FILE" &
STATUS_PID=$!
STATUS_WAIT_TICKS=0
STATUS_TIMED_OUT=0
while kill -0 "$STATUS_PID" 2>/dev/null; do
  if [ "$STATUS_WAIT_TICKS" -ge 20 ]; then
    STATUS_TIMED_OUT=1
    kill "$STATUS_PID" 2>/dev/null || true
    break
  fi
  sleep 0.1
  STATUS_WAIT_TICKS=$((STATUS_WAIT_TICKS + 1))
done
wait "$STATUS_PID" 2>/dev/null
STATUS_CODE=$?
STATUS_OUTPUT=$(cat "$STATUS_OUTPUT_FILE")
STATUS_ERROR=$(cat "$STATUS_ERROR_FILE")

if [ "$STATUS_TIMED_OUT" -eq 1 ]; then
  checkpoint_failure "checkpoint status timed out after 2 seconds."
  exit 0
fi

if [ "$STATUS_CODE" -eq 0 ] && printf '%s' "$STATUS_OUTPUT" | jq -s -e \
  --arg session "$SESSION_ID" \
  --arg turn "$ROOT_TURN_ID" '
    length == 1 and
    .[0].checkpoint.identity.host == "codex" and
    .[0].checkpoint.identity.session_id == $session and
    .[0].checkpoint.identity.root_turn_id == $turn and
    (.[0].checkpoint.disposition == "saved" or
     .[0].checkpoint.disposition == "skipped" or
     .[0].checkpoint.disposition == "needs_review")
  ' >/dev/null 2>&1; then
  printf '%s\n' '{}'
  exit 0
fi

if [ "$STATUS_CODE" -ne 0 ] && printf '%s' "$STATUS_ERROR" | jq -s -e \
  'length == 1 and .[0].code == "checkpoint_not_found"' >/dev/null 2>&1; then
  if [ "$STOP_HOOK_ACTIVE" = "true" ]; then
    checkpoint_failure "checkpoint is still missing after the single recovery continuation."
    exit 0
  fi
  IDENTITY=$(jq -cn \
    --arg host "codex" \
    --arg session "$SESSION_ID" \
    --arg turn "$ROOT_TURN_ID" \
    '{host:$host,session_id:$session,root_turn_id:$turn}')
  REASON="Finalize the missing Engram checkpoint for the original root user turn ${IDENTITY} using the Engram memory skill. Preserve this identity unchanged; do not checkpoint this continuation."
  jq -n --arg reason "$REASON" '{decision:"block",reason:$reason}'
  exit 0
fi

checkpoint_failure "checkpoint status did not return the expected terminal result."
exit 0
