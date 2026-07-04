# 0003: Keep SQLite As The First State Store And Defer PostgreSQL/Flyway

Status: Accepted

Date: 2026-07-04

## Context

TaskFlow needs defensible persistence and recovery behavior before it needs
multiple database technologies. SQLite already supports the current local
coordinator use case and now stores job/task state, requester ownership data,
peer registry metadata, semantic final payloads, task-attempt history, task
leases, and RabbitMQ coordinator outbox rows.

Adding PostgreSQL/Flyway before a concrete external database requirement would
increase maintenance cost without improving current runtime behavior.

## Decision

SQLite remains the default and only implemented `JobStateStore`.

Attempt history, lease-based restart recovery, and RabbitMQ coordinator outbox
replay are implemented for SQLite first. PostgreSQL/Flyway is deferred until
there is a real multi-process, operator-managed, or external database
requirement.

## Consequences

- Public docs may claim only SQLite-backed persistence and recovery behavior.
- Recovery behavior work should define and test the `JobStateStore` contract
  before adding another database.
- PostgreSQL/Flyway must implement the same recovery contracts if accepted
  later; it should not change public guarantees by itself.
- Multi-coordinator locking and external database operations remain out of
  scope.
- Fresh-clone and CI evidence should keep exercising the SQLite implementation.

## Evidence

- `docs/RECOVERY_SCOPE.md`
- `docs/EXECUTION_GUARANTEES.md`
- `taskflow-core/src/main/java/server/db/JobStateStore.java`
- `taskflow-persistence-sqlite`
- `taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java`
- `taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java`

## Related Documents

- [Recovery Scope Decision](../RECOVERY_SCOPE.md)
- [Execution Guarantees](../EXECUTION_GUARANTEES.md)
- [Observability Scope](../OBSERVABILITY_SCOPE.md)
