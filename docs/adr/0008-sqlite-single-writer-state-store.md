# 0008: Retain SQLite For The Single-Writer State-Store Scope

Status: Accepted

Date: 2026-07-22

Scope: Frozen architecture for Phases 0–8; implementation exists and later
tasks strengthen its transition contracts.

## Context

The coordinator needs authoritative durable job/task state, requester ownership,
attempt audit, leases, terminal payloads, participant metadata, and outbox
intent. The supported deployment has one coordinator writer, and the existing
SQLite adapter already persists those records with schema-version and recovery
tests.

Choosing a larger database does not by itself fix stale writes, missing
transactions, or ambiguous recovery. Those guarantees come from explicit
conditional transitions, atomic outbox writes, migrations, and tests.

## Decision

SQLite remains the authoritative coordinator state store for Phases 0–8 because
the supported design has exactly one coordinator writer. Correctness-sensitive
transitions must commit in SQLite before their in-memory projections or
external effects are treated as successful.

SQLite transactions must enforce conditional assignment/result/terminal
transitions and atomic coordinator outbox intent. Schema changes remain
versioned and transactionally migrated. The database file is local to the
coordinator deployment; a shared network filesystem is not a coordinator
consensus mechanism.

## Alternatives Considered

- **PostgreSQL immediately:** rejected because no supported multi-writer or
  operator-managed database requirement currently justifies the operational and
  migration cost.
- **In-memory state with periodic snapshots:** rejected because crash windows
  could lose accepted work or allow memory to disagree with durable authority.
- **One SQLite file shared by multiple coordinators:** rejected because file
  locking does not provide leader fencing or a safe active-active contract.
- **RabbitMQ as the state authority:** rejected because broker delivery state is
  not a substitute for conditional job/task transitions and queryable recovery.

## Consequences

- The coordinator is the sole database writer; multi-coordinator operation is
  unsupported before the optional HA phase.
- SQLite adapter contract and migration tests are release evidence, not merely
  implementation detail.
- In-memory maps and indexes may accelerate scheduling but must be rebuildable
  projections.
- Outbound intent that must survive a crash is written in the same transaction
  as its authoritative state transition.
- Operational limits such as database size, write latency, backup, and recovery
  time must be measured rather than hidden behind a “lightweight” label.

## Conditions That Would Invalidate This Decision

A replacement ADR is required when multiple coordinators must write shared
state, measured SQLite write/recovery behavior cannot meet an accepted workload,
or operators require external database HA, backup, retention, or concurrent
inspection semantics that SQLite cannot satisfy.

The replacement store must run the same persistence contract suite and preserve
all conditional transitions, outbox atomicity, migration rules, and recovery
semantics. A preference for a résumé technology or hypothetical scale does not
invalidate this decision.

## Evidence And Implementation Status

- [Recovery scope](../RECOVERY_SCOPE.md)
- [Guarantees and non-goals](../GUARANTEES.md)
- [Failure model](../FAILURE_MODEL.md)
- `taskflow-core/src/main/java/server/db/JobStateStore.java`
- `taskflow-core/src/test/java/server/db/PersistenceContractTest.java`
- `taskflow-persistence-sqlite/src/test/java/server/db/SqlitePersistenceContractTest.java`
- `taskflow-persistence-sqlite`

## Related Documents

- [ADR 0007: Single authoritative coordinator](0007-single-authoritative-coordinator.md)
- [Superseded ADR 0003: SQLite-first recovery](0003-sqlite-first-recovery.md)
