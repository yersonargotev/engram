#!/bin/bash
# Shared Codex checkpoint transport helpers.
# Memory semantics live in skills/memory/SKILL.md; this adapter only extracts
# the canonical cue and renders valid hook output.

CHECKPOINT_HELPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECKPOINT_SKILL_PATH="${CHECKPOINT_HELPER_DIR}/../skills/memory/SKILL.md"

checkpoint_activation_cue() {
  awk '
    $0 == "<!-- engram:checkpoint-cue:start -->" { capture = 1; next }
    $0 == "<!-- engram:checkpoint-cue:end -->" { capture = 0; found = 1; exit }
    capture { print }
    END { if (!found) exit 1 }
  ' "$CHECKPOINT_SKILL_PATH"
}

emit_session_start_context() {
  local extra_context="${1:-}"
  local cue
  cue=$(checkpoint_activation_cue) || return 0

  local model_context="$cue"
  if [ -n "$extra_context" ]; then
    model_context="${model_context}

${extra_context}"
  fi

  jq -n --arg context "$model_context" \
    '{hookSpecificOutput:{hookEventName:"SessionStart",additionalContext:$context}}'
}
