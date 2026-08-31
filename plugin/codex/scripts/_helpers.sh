#!/bin/bash
# Engram — Shared helpers for Codex hooks
# WARNING: Do not read from stdin here — scripts source this before reading their hook input.

# Resolve the project through the server, which owns project policy.
# An unavailable, malformed, empty, or ambiguous response is not a project.
resolve_project() {
  local dir="$1"
  [ -n "$dir" ] || return 1

  local encoded_cwd response
  encoded_cwd=$(printf '%s' "$dir" | jq -sRr @uri) || return 1
  response=$(curl -sf "${ENGRAM_URL}/project/current?cwd=${encoded_cwd}" --max-time 2 2>/dev/null) || return 1
  printf '%s' "$response" | jq -er '
    if (.project | type) == "string"
      and (.project | gsub("^[[:space:]]+|[[:space:]]+$"; "") | length) > 0
      and (.project_source | type) == "string"
      and (.project_strength | type) == "string"
      and (.project_strength as $strength | ["strong", "explicit"] | index($strength) != null)
      and ((.project_strength == "explicit") or (.implicit_write_allowed == true))
      and (has("error_hint") | not)
    then .project
    else error("canonical project resolution failed")
    end
  ' 2>/dev/null
}

# Emit bounded, content-free operational metadata only during an explicitly
# enabled baseline window. Core owns validation, salted linkage, and retention.
record_baseline() {
  [ "${ENGRAM_RECALL_BASELINE:-0}" = "1" ] || return 0
  (
    engram recall-baseline record "$@" >/dev/null 2>&1 || true
  ) </dev/null >/dev/null 2>&1 &
}
