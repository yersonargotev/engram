# Recover dead remote mutations locally

Engram recovers a dead remote mutation by applying its stored payload locally
through the canonical store rules. Recovery never emits a new outbound
mutation: doing so would change provenance and could create a synchronization
feedback loop. A successful manual recovery retains an `applied` tombstone so
repeated or concurrent requests are idempotent; a later remote mutation with
the same identity supersedes any terminal tombstone and starts a new queue
episode. Automatic deferred replay keeps its existing delete-on-success
behavior because it does not need the same operator-facing receipt.

## Considered options

Resetting a dead row to `deferred` was rejected because it makes the result
asynchronous and obscures whether the operator action succeeded. Republishing
the payload as a local outbound mutation was rejected because recovery is a
pull-side concern and must preserve the mutation's remote origin.
