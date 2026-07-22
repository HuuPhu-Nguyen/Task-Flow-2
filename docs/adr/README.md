# Architecture Decision Records

This directory records durable TaskFlow architecture decisions. ADRs are short
summaries of decisions that are implemented or intentionally deferred elsewhere;
they do not replace the longer scope and guarantee documents linked from each
record.

## Records

- [0001: Use RabbitMQ as the default runtime while deprecating TCP](0001-rabbitmq-planned-primary-runtime.md) — superseded by ADR 0009
- [0002: Use role-split ServiceLoader plugins for task domains](0002-role-split-service-loader-plugins.md)
- [0003: Keep SQLite as the first state store and defer PostgreSQL/Flyway](0003-sqlite-first-recovery.md) — superseded by ADR 0008
- [0004: Use semantic final result payloads with compatibility task-result lists](0004-semantic-final-result-payloads.md)
- [0005: Use per-job result ownership and defer full account authentication](0005-per-job-result-ownership.md)
- [0006: Publish role-specific runtime packages](0006-role-specific-runtime-packages.md)
- [0007: Use one authoritative coordinator](0007-single-authoritative-coordinator.md)
- [0008: Retain SQLite for the single-writer state-store scope](0008-sqlite-single-writer-state-store.md)
- [0009: Use RabbitMQ as the sole supported transport](0009-rabbitmq-sole-supported-transport.md)
- [0010: Use at-least-once execution with generation-fenced result commitment](0010-at-least-once-generation-fenced-results.md)
- [0011: Use object storage for large payloads](0011-object-storage-large-payloads.md)
- [0012: Use simple weighted-capacity scheduling](0012-simple-weighted-capacity-scheduling.md)
- [0013: Use requester-scoped job-submission idempotency](0013-requester-scoped-job-submission-idempotency.md)

## Format

ADRs 0007–0013 use:

- Status
- Context
- Decision
- Alternatives considered
- Consequences
- Conditions that would invalidate the decision
- Evidence
- Related documents

Earlier records retain their historical format. A later decision supersedes an
accepted record instead of silently rewriting its original context.
