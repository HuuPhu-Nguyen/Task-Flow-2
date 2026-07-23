# Recovery Scope Decision

This document records the recovery behavior decision that should guide future
persistence work. It does not describe implemented behavior beyond what is
already documented in `docs/EXECUTION_GUARANTEES.md`.

## Current Position

SQLite remains the default and only implemented `JobStateStore`.

Current startup recovery can rebuild resumable `RUNNING` jobs and replayable
`FINALIZING` jobs from persisted job and task snapshots, restore completed task
results when result payload snapshots exist, preserve assigned tasks whose
complete assignment identities have not expired, release expired or incomplete
legacy assignments to `PENDING`, and mark otherwise non-resumable jobs failed.
Hydration reconstructs task status,
retry count, last assignment generation, current assignment UUID/participant,
lease metadata, and committed result payload from SQLite rather than from any
pre-crash scheduler object. Durable attempt rows exist in
SQLite schema version 7, task leases exist in SQLite schema version 8,
coordinator RabbitMQ broker outbox rows exist in SQLite schema version 9, and
current assignment generation plus audit identity/lease fields exist in schema
version 10. Schema version 11 adds the `FINALIZING` state used to replay
semantic aggregation after the last successful task transaction. Schema
version 12 adds the canonical job-submission request hash used to classify an
exact requester-scoped replay after coordinator restart. Migrated rows retain
an empty hash and remain non-replayable as original submissions because task
snapshots may be plugin-transformed rather than the original submitted values.
PostgreSQL/Flyway is not implemented.

Coordinator shutdown now stops RabbitMQ intake before draining the bounded
scheduler mailbox. A delivery that was admitted drains to its typed
scheduler/store disposition; a delivery that reaches the closed ingress gate
remains unacknowledged and returns to RabbitMQ when the channel closes.
SQLite closes only after the scheduler, peer monitor, and outbox replayer stop.
Healthy-connection acknowledgement windows remain separate from the managed
single-broker restart proof. The latter starts with RabbitMQ unavailable,
stops it again during active work, preserves SQLite assignment/outbox state,
restores topology and consumers, rejects the stale pre-outage result, and
completes with the current result after recovery.

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

- Assignment UUID and lease deadline for every new task attempt; migrated legacy
  attempts retain null UUID/zero lease values because that identity did not
  previously exist.
- Attempt sequence number.
- Assigned peer id.
- Attempt start time.
- Completion, failure, timeout, or release time.
- Failure reason when available.
- Whether the attempt ended terminally or left work eligible for retry.
- Existing `retry_count` remains separate task-row state. The latest assignment
  generation is persisted on the task row; schema-v9 pending/legacy rows use
  their attempt audit as a startup fallback, and reconciliation backfills that
  last known generation before the next assignment advances it.

Covered behavior:

- For RabbitMQ, the scheduler obtains an assignment UUID candidate from its
  injected generator. SQLite reads only a `PENDING` task, calculates persisted
  generation plus one, validates the supplied UUID, and commits task state,
  attempt audit, and the exact `TASK_ASSIGN` outbox envelope together before
  dispatch. Retrying publication reuses that envelope without advancing the
  generation.
- Successful completion conditionally matches task ID, `ASSIGNED` state,
  attempt number, assignment ID, and participant ID, then closes only that
  exact running attempt in the same SQLite transaction before memory changes.
  When that result completes the exact expected task set and every result
  snapshot is present, the transaction also moves the job from `RUNNING` to
  `FINALIZING`.
- Runtime retry release, timeout, peer disconnect, lease expiry, dispatch
  failure, and terminal failure conditionally match the same complete
  assignment tuple and close that exact attempt before task/capacity projection.
  Retry-policy failures store the decided retry count; dispatch and startup
  releases preserve it.
- Stale or duplicate task results do not mutate a closed or superseded attempt.
- Startup recovery preserves completed attempt rows and resumes retryable work
  without inventing false successes; assigned tasks with expired leases close
  their running attempt with `lease_expired`, while assigned legacy rows without
  a complete identity are closed and released during restart reconciliation.

## Lease-Based Restart Recovery Scope

Lease-based recovery is implemented for the SQLite state store as a fixed-expiry
task ownership model. It does not introduce multiple active coordinators,
external coordinator membership, or a PostgreSQL-backed locking model.

The implemented contract records:

- Lease owner identity for a coordinator process.
- Lease acquisition timing before task dispatch.
- Lease expiry time for assigned work.
- Startup behavior for assigned tasks with unexpired leases.
- Startup release behavior for assigned tasks with expired leases or incomplete
  assignment identities.
- Duplicate-result handling when a peer completes work after its old lease has
  expired and the task has been reassigned.
- Coordinator restart behavior for pending, assigned, completed, failed, and
  partially persisted tasks.

Covered behavior:

- A task is dispatched only after its lease owner is persisted.
- Restart with an unexpired lease does not immediately reassign the assigned
  task when its persisted attempt number, assignment UUID, and worker are valid.
- Restart with an expired lease releases assigned work for retry.
- Restart with an incomplete legacy assignment identity releases the row even
  when its old lease deadline has not yet elapsed.
- Startup release preserves `retry_count`; active scheduler lease expiry uses
  the normal retry counter.
- A late result from an expired lease is rejected after reassignment.
- Completed task result snapshots remain authoritative after restart.
- A `FINALIZING` job is reconstructed from its ordered durable task snapshots;
  plugin aggregation must be deterministic for those inputs. Successful
  terminal state, semantic payload, and the RabbitMQ final-result outbox row
  then commit atomically. The non-outbox fallback commits terminal state before
  publication.
- Schema-v10 `RUNNING` jobs with an exact fully completed, result-bearing task
  set migrate to `FINALIZING`; incomplete legacy snapshots do not.
- A schema-v12 submission replay is classified from the stored token hash,
  optional public key, and canonical request hash without rerunning plugin task
  creation. Exact replay can return running status or a completed persisted
  result; conflicts do not mutate the recovered job/task set.
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
after reassignment. They may also claim replayable single-coordinator job
finalization from durable ordered task results and atomic terminal/outbox
commit, plus broker redelivery of unsettled coordinator deliveries after
connection close and bounded single-broker restart recovery for active work.
They must not claim exactly-once broker delivery, zero-downtime cluster
failover, multi-coordinator locking, PostgreSQL/Flyway storage, or an
operator-managed external database until those are implemented and tested.
