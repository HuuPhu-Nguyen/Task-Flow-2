# 0001: Use RabbitMQ As The Default Runtime While Deprecating TCP

Status: Accepted, amended 2026-07-05

Date: 2026-07-04

## Context

TaskFlow started with a TCP runtime that remains useful for local demos,
compatibility, and JavaFX manual smoke checks. The framework now also has a
RabbitMQ transport with explicit peer IDs, peer-targeted routes, publisher
confirms, manual acknowledgement, prefetch, DLQ workflow, coordinator outbox
replay, JavaFX service adapters, and broker-backed CI coverage.

RabbitMQ is a better long-term fit for a coordinator-mediated distributed
task-execution framework with dual-role participant nodes because it provides
queueing, acknowledgement, dead-lettering, operator inspection points, and
fewer direct coordinator-to-participant socket assumptions.
Even so, RabbitMQ still lacks full broker outage/restart coverage and some
operational hardening required for production-runtime claims.

## Decision

RabbitMQ is the default runtime for the coordinator, command-line participants
(the existing `taskflow-peer` artifact), and JavaFX GUI participants when
`TASKFLOW_TRANSPORT` is unset or blank.

TCP is deprecated as the legacy local compatibility/demo path and remains
available only when selected explicitly with `TASKFLOW_TRANSPORT=tcp`. TCP
removal must be a later cleanup with no unrelated feature work.

## Consequences

- Public docs may describe RabbitMQ as the default runtime, but not as a fully
  supported production runtime.
- `TASKFLOW_TRANSPORT` unset or blank selects RabbitMQ; `TASKFLOW_TRANSPORT=tcp`
  selects the deprecated compatibility path.
- RabbitMQ support-promotion gates must keep JavaFX desktop or automation
  evidence, result-request replay policy, broader broker-failure integration
  coverage, reliable broker-backed CI, and aligned docs/demos current.
- TCP code and docs stay maintained until removal gates pass.
- Future transport work should improve the RabbitMQ path rather than expanding
  TCP-specific behavior.

## Evidence

- `docs/RUNTIME_STRATEGY.md`
- `docs/RABBITMQ_SCOPE.md`
- `docs/EXECUTION_GUARANTEES.md`
- `.github/workflows/ci.yml`
- `taskflow-transport-rabbitmq`
- `taskflow-coordinator`
- `taskflow-peer`
- `taskflow-gui`

## Related Documents

- [Runtime Strategy](../RUNTIME_STRATEGY.md)
- [RabbitMQ Runtime Scope Decision](../RABBITMQ_SCOPE.md)
- [Execution Guarantees](../EXECUTION_GUARANTEES.md)
- [Participant Identity (`peer` Compatibility Names)](../PEER_IDENTITY.md)
- [Backpressure Scope](../BACKPRESSURE_SCOPE.md)
