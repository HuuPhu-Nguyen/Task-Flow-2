# 0009: Use RabbitMQ As The Sole Supported Transport

Status: Accepted

Date: 2026-07-22

Scope: Frozen end-state for Phases 0–8; the TF-0301 transport migration is
complete.

## Context

Maintaining two supported transports makes acknowledgement, redelivery,
routing, backpressure, outage recovery, and outbox guarantees ambiguous. The
coordinator-mediated architecture needs one delivery contract that expects
duplicates and integrates with durable publication intent.

RabbitMQ provides publisher confirms, manual acknowledgements, prefetch,
peer-targeted compatibility routes, dead lettering, and coordinator outbox
replay. Before TF-0301, a deprecated socket path remained beside that broker
contract and made the supported failure semantics ambiguous.

## Decision

RabbitMQ is the only supported runtime transport in the Phase 3–8 end-state.
All supported assignment, task-result, submission, and final-result delivery
semantics are defined against RabbitMQ acknowledgement, redelivery, routing,
retry/quarantine, and reconnect behavior.

TF-0301 removed the legacy socket source, tests, selector, scripts, documentation,
and package surface. Coordinator, command-line participant, and JavaFX entry
points now start RabbitMQ directly. Historical peer-registry values remain
readable as unknown transport metadata only and cannot activate a runtime.

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

- Runtime delivery code uses one five-value typed disposition contract plus
  bounded TTL retry queues, observable attempt/reason metadata, and final
  automatic quarantine.
- RabbitMQ, its topology, and broker-backed integration evidence become runtime
  prerequisites for supported distributed operation.
- Publisher confirms do not mean consumer processing or exactly-once delivery;
  duplicates remain normal and must be classified safely.
- Legacy socket configuration, packaging, tests, and claims are absent from the
  supported surface.
- Healthy-broker acknowledgement crash windows now have live pre-ack,
  post-commit/pre-ack, and shutdown-ownership evidence. A managed
  Testcontainers/Toxiproxy test also proves bounded unavailable startup and
  single-broker restart recovery during active work. The broader Phase 7
  process-kill/chaos matrix and remaining operational gates must pass before
  production-strength RabbitMQ claims are made.

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
- `RabbitMqOnlyRuntimeArchitectureTest`
- `TaskCoordinatorServerTest`, `PeerNodeTest`, and `PeerAppLauncherTest`
- `taskflow-transport-rabbitmq`

## Related Documents

- [Superseded ADR 0001: RabbitMQ default and TCP deprecation](0001-rabbitmq-planned-primary-runtime.md)
- [ADR 0011: Object storage for large payloads](0011-object-storage-large-payloads.md)
