# 0001: Use RabbitMQ As The Planned Primary Runtime While Keeping TCP As The Current Default

Status: Accepted

Date: 2026-07-04

## Context

TaskFlow started with a TCP runtime that remains useful for local demos,
compatibility, and JavaFX manual smoke checks. The framework now also has a
RabbitMQ transport with explicit peer IDs, peer-targeted routes, publisher
confirms, manual acknowledgement, prefetch, DLQ workflow, coordinator outbox
replay, JavaFX service adapters, and broker-backed CI coverage.

RabbitMQ is a better long-term fit for a coordinated peer-to-peer framework
because it provides queueing, acknowledgement, dead-lettering, operator
inspection points, and fewer direct coordinator-to-peer socket assumptions.
Even so, RabbitMQ still lacks full desktop GUI evidence, broader outage
coverage, and some result replay decisions.

## Decision

RabbitMQ is the planned primary runtime for the coordinator, command-line peers,
and JavaFX GUI peers.

TCP remains the current default local runtime and compatibility/demo path until
the RabbitMQ primary-runtime gates pass. TCP deprecation must happen before TCP
removal, and removal must be a later cleanup with no unrelated feature work.

## Consequences

- Public docs must describe RabbitMQ as planned primary or target broker
  runtime, not as the default or fully supported production runtime.
- `TASKFLOW_TRANSPORT=rabbitmq` remains explicit while `TASKFLOW_TRANSPORT` unset
  keeps TCP as the default.
- RabbitMQ replacement gates must include JavaFX desktop or automation evidence,
  result-request replay policy, broader broker-failure integration coverage,
  reliable broker-backed CI, and aligned docs/demos.
- TCP code and docs stay maintained until deprecation and removal gates pass.
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
- [Peer Identity](../PEER_IDENTITY.md)
- [Backpressure Scope](../BACKPRESSURE_SCOPE.md)
