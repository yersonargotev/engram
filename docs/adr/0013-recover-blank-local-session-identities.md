# Recover blank local session identities explicitly

While triaging [issue #185](https://github.com/yersonargotev/engram/issues/185),
the maintainer selected local, project-scoped recovery of one exact legacy blank
source identity to an operator-supplied nonblank canonical identity. Recovery
must work without enrollment, credentials, or network calls; it must never
generate an identity, enroll a project, or upload as a side effect.

Explicit recovery may change only the owning-session link of dependent Legacy
prompt archive rows, preserving content, IDs, ownership, and dates. This deliberate
exception permits identity recovery without allowing historical content edits,
capture activation, or prompt replication. Only identity fields of exactly
attributable active pending outbound session/observation entries may change;
acknowledged, quarantined, and abandoned history, prompt copies/deletion metadata,
sync cursors, host checkpoint identities, and relation judgment provenance remain
unchanged. Keeping every active pending payload immutable was rejected because
it would retain references to the corrupt source identity.

Source, owned-reference, eligible pending-journal changes, and a minimal recovery
receipt commit atomically after a consistent backup and stale-target checks. An
exact repeat returns already-recovered only with matching receipt and valid
destination identity/ownership; destination existence alone is not proof. If
customized archive objects cannot be shown to preserve the linkage-only contract
and unrelated data, reject the operation without mutation and identify the
blocking object. Customized schema repair and directory recovery remain separate
work. This contract does not authorize applying recovery to existing user data.
