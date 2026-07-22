# 0009: Use RabbitMQ As The Sole Supported Transport

Status: Accepted

Date: 2026-07-22

Scope: Frozen end-state for Phases 0–8; migration is incomplete until TF-0301
removes or quarantines the deprecated TCP compatibility path.

## Context

Maintaining two supported transports makes acknowledgement, redelivery,
routing, backpressure, outage recovery, and outbox guarantees ambiguous. The
coordinator-mediated architecture needs one delivery contract that expects
duplicates and integrates with durable publication intent.

RabbitMQ already provides the project's default broker path, publisher confirms,
manual acknowledgements, prefetch, peer-targeted compatibility routes, dead
lettering, and coordinator outbox replay. TCP remains in the current tree only
as a deprecated compatibility/demo path and does not define the target contract.

## Decision

RabbitMQ is the only supported runtime transport in the Phase 3–8 end-state.
All supported assignment, task-result, submission, and final-result delivery
semantics are defined against RabbitMQ acknowledgement, redelivery, routing,
retry/quarantine, and reconnect behavior.

TF-0301 must either remove TCP or isolate it in a legacy module excluded from
default builds and releases. Until that task passes, documentation must say
that RabbitMQ is the default but transitional and that TCP is deprecated; it
must not claim that the sole-transport migration is already complete.

RabbitMQ carries control messages and small bounded inline payloads. It is not
the large binary data plane.

## Alternatives Considered

- **Keep TCP and RabbitMQ equally supported:** rejected because two delivery
  contracts multiply failure behavior and invite transport-specific scheduler
  semantics.
- **Retain TCP as the primary runtime:** rejected because direct socket
  lifecycle/routing does not supply the broker-backed acknowledgement,
  redelivery, dead-letter, and operator boundaries required by the target.
- **Create a generic pluggable transport framework:** rejected because no second
  supported transport requirement justifies the abstraction or duplicate
  contract suites.
- **Allow direct participant-to-participant authoritative coordination:**
  rejected because it violates the single coordinator authority.

## Consequences

- Runtime delivery code can converge on one typed disposition and one broker
  contract suite.
- RabbitMQ, its topology, and broker-backed integration evidence become runtime
  prerequisites for supported distributed operation.
- Publisher confirms do not mean consumer processing or exactly-once delivery;
  duplicates remain normal and must be classified safely.
- TCP-specific configuration, packaging, tests, and claims leave the supported
  surface when TF-0301 completes.
- Full broker outage/restart, bounded poison handling, and acknowledgement crash
  windows must pass before production-strength RabbitMQ claims are made.

## Conditions That Would Invalidate This Decision

A replacement ADR is required if an explicit supported deployment cannot run a
broker, a regulatory or platform constraint prohibits RabbitMQ, or measured
RabbitMQ behavior cannot meet an accepted requirement after the documented
recovery/overload design is implemented.

Such a change must choose one replacement supported transport or fund a complete
second contract suite; it may not silently reintroduce transport-dependent
correctness. Temporary broker unavailability or an unfinished migration does
not invalidate the decision.

## Evidence And Implementation Status

- [Runtime strategy](../RUNTIME_STRATEGY.md)
- [RabbitMQ scope](../RABBITMQ_SCOPE.md)
- [Guarantees and non-goals](../GUARANTEES.md)
- [Failure model](../FAILURE_MODEL.md)
- `taskflow-transport-rabbitmq`

## Related Documents

- [Superseded ADR 0001: RabbitMQ default and TCP deprecation](0001-rabbitmq-planned-primary-runtime.md)
- [ADR 0011: Object storage for large payloads](0011-object-storage-large-payloads.md)
