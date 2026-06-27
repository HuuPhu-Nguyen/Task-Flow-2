# Recovery Scope Decision

This document records the recovery behavior decision that should guide future
persistence work. It does not describe implemented behavior beyond what is
already documented in `docs/EXECUTION_GUARANTEES.md`.

## Current Position

SQLite remains the default and only implemented `JobStateStore`.

Current startup recovery can rebuild resumable `RUNNING` jobs from persisted job
and task snapshots, restore completed task results when result payload snapshots
exist, reset assigned tasks to `PENDING`, and mark legacy or otherwise
non-resumable running jobs failed. Task leases, durable attempt rows, and
PostgreSQL/Flyway are not implemented.

## Decision

- Explicit attempt history: accepted as future recovery behavior.
- Lease-based restart recovery: accepted as future recovery behavior.
- PostgreSQL plus Flyway: deferred until there is a real multi-process,
  operator-managed, or external database requirement.

The behavior work comes first. A new database technology should not be added
just to make recovery sound stronger. PostgreSQL/Flyway can be reconsidered
after the attempt and lease contracts are defined and there is a concrete reason
SQLite is no longer enough for the project goal.

## Attempt History Scope

Attempt history should be implemented before claiming detailed retry audit or
restart attribution. The contract should define:

- A persisted attempt identity per task attempt.
- Attempt sequence number.
- Assigned peer id.
- Attempt start time.
- Completion, failure, timeout, or release time.
- Failure reason when available.
- Whether the attempt ended terminally or left work eligible for retry.
- How existing `retry_count` maps to attempt rows during migration.

Minimum tests before implementation:

- Assignment creates a new attempt row before dispatch.
- Successful completion closes only the current attempt.
- Timeout and peer-disconnect release close the current attempt and preserve
  retry count.
- Processor failure records a failed attempt.
- Stale or duplicate task results do not mutate a closed or superseded attempt.
- Startup recovery preserves completed attempt rows and resumes retryable work
  without inventing false successes.

## Lease-Based Restart Recovery Scope

Lease-based recovery should be implemented before claiming coordinated restart
ownership for in-flight work. The contract should define:

- Lease owner identity for a coordinator process.
- Lease acquisition timing and required persistence before task dispatch.
- Lease heartbeat or renewal interval.
- Lease expiration policy.
- What happens to assigned tasks whose lease owner is alive, expired, or
  unknown at startup.
- Duplicate-result handling when a peer completes work after its old lease has
  expired and the task has been reassigned.
- Coordinator restart behavior for pending, assigned, completed, failed, and
  partially persisted tasks.

Minimum tests before implementation:

- A task is dispatched only after its lease owner is persisted.
- Restart with an unexpired lease does not immediately reassign work owned by
  another live coordinator.
- Restart with an expired lease releases assigned work for retry.
- A late result from an expired lease is rejected after reassignment.
- Completed task result snapshots remain authoritative after restart.
- Non-resumable jobs are still failed explicitly with an inspectable reason.

## PostgreSQL/Flyway Deferral

PostgreSQL/Flyway is not part of the next recovery slice by default.

Reconsider it only when at least one of these requirements exists:

- Multiple coordinator processes need a shared externally managed state store.
- Operators need standard database backup, migration, inspection, or retention
  workflows that SQLite cannot satisfy.
- Recovery tests require transaction or locking behavior that SQLite cannot
  model for the intended deployment.

If PostgreSQL/Flyway is accepted later, it should implement the same
`JobStateStore` recovery contract as SQLite plus any attempt-history and lease
extensions already accepted. It should not change public recovery guarantees by
itself.

## Public Claim Rule

Until attempt history and leases are implemented and tested, public docs should
continue to say that assigned tasks are reset to `PENDING` on startup because
leases are not implemented. They should also keep PostgreSQL/Flyway described as
deferred infrastructure work, not as an implemented or required recovery
guarantee.
