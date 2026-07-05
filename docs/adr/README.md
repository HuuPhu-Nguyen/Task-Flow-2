# Architecture Decision Records

This directory records durable TaskFlow architecture decisions. ADRs are short
summaries of decisions that are implemented or intentionally deferred elsewhere;
they do not replace the longer scope and guarantee documents linked from each
record.

## Records

- [0001: Use RabbitMQ as the planned primary runtime while deprecating TCP](0001-rabbitmq-planned-primary-runtime.md)
- [0002: Use role-split ServiceLoader plugins for task domains](0002-role-split-service-loader-plugins.md)
- [0003: Keep SQLite as the first state store and defer PostgreSQL/Flyway](0003-sqlite-first-recovery.md)
- [0004: Use semantic final result payloads with compatibility task-result lists](0004-semantic-final-result-payloads.md)
- [0005: Use per-job result ownership and defer full account authentication](0005-per-job-result-ownership.md)
- [0006: Publish role-specific runtime packages](0006-role-specific-runtime-packages.md)

## Format

Each ADR uses:

- Status
- Context
- Decision
- Consequences
- Evidence
- Related documents
