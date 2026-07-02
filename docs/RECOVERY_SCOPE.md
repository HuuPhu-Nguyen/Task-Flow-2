# Recovery Scope Decision

This document records the recovery behavior decision that should guide future
persistence work. It does not describe implemented behavior beyond what is
already documented in `docs/EXECUTION_GUARANTEES.md`.

## Current Position

SQLite remains the default and only implemented `JobStateStore`.

Current startup recovery can rebuild resumable `RUNNING` jobs from persisted job
and task snapshots, restore completed task results when result payload snapshots
exist, preserve assigned tasks whose leases have not expired, release assigned
tasks whose leases are expired or missing to `PENDING`, and mark legacy or
otherwise non-resumable running jobs failed. Durable attempt rows exist in
SQLite schema version 7, task leases exist in SQLite schema version 8, and
coordinator RabbitMQ broker outbox rows exist in SQLite schema version 9.
PostgreSQL/Flyway is not implemented.

## Decision

- Explicit attempt history: implemented for the SQLite state store.
- Lease-based restart recovery: implemented for the SQLite state store.
- PostgreSQL plus Flyway: deferred until there is a real multi-process,
  operator-managed, or external database requirement.

The behavior work comes first. A new database technology should not be added
just to make recovery sound stronger. PostgreSQL/Flyway can be reconsidered
when there is a concrete reason SQLite is no longer enough for the project
goal.

## Attempt History Scope

Attempt history now records a durable audit row for each task assignment made
through SQLite-backed persistence. It is an audit trail that complements task
leases; lease ownership and expiry live on task rows.

The implemented contract records:

- A persisted attempt identity per task attempt.
- Attempt sequence number.
- Assigned peer id.
- Attempt start time.
- Completion, failure, timeout, or release time.
- Failure reason when available.
- Whether the attempt ended terminally or left work eligible for retry.
- Existing `retry_count` remains task-row state; attempt numbers are assigned
  from the existing rows for that task instead of being inferred from
  `retry_count`.

Covered behavior:

- Assignment creates a new attempt row before dispatch.
- Successful completion closes only the current attempt.
- Timeout and peer-disconnect release close the current attempt and preserve
  retry count.
- Processor failure records a failed attempt.
- Stale or duplicate task results do not mutate a closed or superseded attempt.
- Startup recovery preserves completed attempt rows and resumes retryable work
  without inventing false successes; assigned tasks with expired or missing
  leases close their running attempt with `lease_expired`.

## Lease-Based Restart Recovery Scope

Lease-based recovery is implemented for the SQLite state store as a fixed-expiry
task ownership model. It does not introduce multiple active coordinators,
external coordinator membership, or a PostgreSQL-backed locking model.

The implemented contract records:

- Lease owner identity for a coordinator process.
- Lease acquisition timing before task dispatch.
- Lease expiry time for assigned work.
- Startup behavior for assigned tasks with unexpired leases.
- Startup release behavior for assigned tasks with expired or missing leases.
- Duplicate-result handling when a peer completes work after its old lease has
  expired and the task has been reassigned.
- Coordinator restart behavior for pending, assigned, completed, failed, and
  partially persisted tasks.

Covered behavior:

- A task is dispatched only after its lease owner is persisted.
- Restart with an unexpired lease does not immediately reassign the assigned
  task.
- Restart with an expired lease releases assigned work for retry.
- Startup release preserves `retry_count`; active scheduler lease expiry uses
  the normal retry counter.
- A late result from an expired lease is rejected after reassignment.
- Completed task result snapshots remain authoritative after restart.
- Non-resumable jobs are still failed explicitly with an inspectable reason.
- Active scheduler lease expiry closes the current attempt with
  `lease_expired` and schedules retry or terminal failure according to the
  normal retry limit.

## PostgreSQL/Flyway Deferral

PostgreSQL/Flyway is not implemented.

Reconsider it only when at least one of these requirements exists:

- Multiple coordinator processes need a shared externally managed state store.
- Operators need standard database backup, migration, inspection, or retention
  workflows that SQLite cannot satisfy.
- Recovery tests require transaction or locking behavior that SQLite cannot
  model for the intended deployment.

If PostgreSQL/Flyway is accepted later, it should implement the same
`JobStateStore` recovery contract as SQLite, including attempt history and any
lease extensions already accepted. It should not change public recovery
guarantees by itself.

## Public Claim Rule

Public docs may claim SQLite-backed task leases for assigned work, unexpired
lease preservation on startup, expired-lease release, and stale-result rejection
after reassignment. They should not claim multi-coordinator locking,
PostgreSQL/Flyway storage, or an operator-managed external database until those
are implemented and tested.
