# 0010: Use At-Least-Once Execution With Generation-Fenced Result Commitment

Status: Accepted

Date: 2026-07-22

Scope: Frozen architecture for Phases 0–8; the database/scheduler mechanism,
same-participant ABA proof, and bounded executor deduplication are implemented
through TF-0107.

## Context

RabbitMQ can redeliver, executor participants can crash after work, publisher
confirmations can be lost, and leases can expire while an old execution still
runs. Preventing every duplicate execution would require controlling arbitrary
plugin side effects and every external dependency, which the coordinator cannot
do.

The safety requirement is instead that a task has one authoritative committed
result and that an obsolete assignment cannot win, including reassignment to
the same worker identity (the ABA case).

## Decision

TaskFlow uses at-least-once message delivery and at-least-once task execution
with generation-fenced, single-authoritative result commitment.

Every assignment generation has a monotonic attempt number, coordinator-created
assignment ID, worker ID, and persisted lease deadline. A task result echoes its
attempt number and assignment ID. SQLite conditionally commits a result only
when task ID, `ASSIGNED` state, attempt number, assignment ID, and worker ID all
match the current persisted assignment. Duplicate current results and stale
results are typed non-commit outcomes and are acknowledged without retry.

Assignment generation, task retry count, and broker delivery-attempt count are
separate concepts. Coordinator fencing does not make arbitrary plugin external
side effects exactly once.

## Alternatives Considered

- **Claim exactly-once execution:** rejected because process crashes and
  external side effects cannot be atomically committed with SQLite and
  RabbitMQ in the general plugin model.
- **Fence only by worker ID:** rejected because reassignment to the same worker
  permits an old result to pass the ABA check.
- **At-most-once delivery with no retry:** rejected because transient failure
  would turn uncertain delivery or execution into lost accepted work.
- **Use only an in-memory generation check:** rejected because restart or a
  storage failure could make memory disagree with authoritative state.

## Consequences

- Executor participants suppress a duplicate delivery while its assignment is
  cached as running and replay the exact cached result after completion.
  Eviction or restart may still cause duplicate execution.
- Plugins must declare whether retry is pure, idempotent, requires an
  idempotency key, or is unsafe.
- The database, not scheduler memory, decides whether a result commits.
- Assignment/outbox replay republishes the same identity; publication retry does
  not create a new generation.
- Logs and metrics must distinguish committed, duplicate, and stale results.
- The SQLite/store boundary rejects same-participant ABA results. Deterministic
  scheduler/SQLite and live RabbitMQ scenarios prove the complete obsolete-X,
  current-Y disposition and one-result outcome.

## Conditions That Would Invalidate This Decision

A replacement ADR is required if accepted workloads demand transactional
exactly-once external effects or prohibit any duplicate execution. That change
would require a narrower plugin contract and an external idempotency or
transaction protocol; changing a message acknowledgement flag is insufficient.

Conversely, choosing to abandon retries and accepted-work liveness would require
an explicit at-most-once contract and revised invariants. Neither a faster
transport nor a different database invalidates generation fencing by itself.

## Evidence And Implementation Status

- [Guarantees and non-goals](../GUARANTEES.md)
- [Failure model](../FAILURE_MODEL.md)
- [Current execution behavior](../EXECUTION_GUARANTEES.md)
- `DatabaseManagerTest#matchingAssignmentCommitsExactlyOnceAndDuplicateIsTyped`
  proves the one-commit/typed-duplicate store boundary.
- `DatabaseManagerTest#sameWorkerAbaResultIsStaleAtStoreBoundary` proves the
  database predicate for same-participant reassignment.
- `TaskSchedulerPersistenceTest#staleResultIsAcknowledgedWithoutRequeueOrSuccessAccounting`
  and `#duplicateResultIsAcknowledgedWithoutRequeueOrSecondSuccess` prove the
  scheduler disposition and metric boundary.
- `AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit` and
  `RabbitMqCoordinatorLiveIntegrationTest#sameWorkerAbaResultCannotCommitThroughLiveBroker`
  prove the complete same-participant scenario through the scheduler, SQLite,
  and live RabbitMQ.
- `WorkerAssignmentDeduplicationIntegrationTest#duplicateRunningAssignmentExecutesOnce`
  and `#duplicateCompletedAssignmentRepublishesSameResult` prove the RabbitMQ
  executor acknowledgement/execution/publication decisions; engine tests prove
  bounds, TTL, cache-loss re-execution, and eviction counters.

## Related Documents

- [ADR 0007: Single authoritative coordinator](0007-single-authoritative-coordinator.md)
- [ADR 0008: SQLite single-writer state store](0008-sqlite-single-writer-state-store.md)
- [ADR 0009: RabbitMQ sole supported transport](0009-rabbitmq-sole-supported-transport.md)
