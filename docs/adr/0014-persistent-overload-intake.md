# 0014: Reserve Result Progress And Reduce Submission Intake

Status: Accepted

Date: 2026-07-27

Scope: TF-0406 persistent-overload behavior for the single authoritative
coordinator.

## Context

The configured scheduler mailbox was one bounded FIFO. Under persistent
`JOB_SUBMIT` pressure, submissions could occupy every slot while an already
accepted task result entered finite broker retry. Per-cycle deadline batching
prevented message traffic from excluding expiry forever, but it did not reserve
result progress. TF-0405 bounded accepted state and returned typed
pre-acceptance rejection; it did not isolate result intake or expose current
overload pressure.

Any change must preserve exact replay before dynamic admission, typed fresh
rejection before J0/T0, finite retry/quarantine, deferred acknowledgement
ownership, protocol version 2, and SQLite schema version 12.

## Decision

The scheduler mailbox has two bounded lanes behind its existing
`BlockingQueue<MessageEnvelope>` surface:

- the ordinary submission/control lane has exactly the configured
  `inboundQueueCapacity`;
- one fixed slot is reserved for `TASK_RESULT`.

The scheduler selects a queued result before ordinary FIFO work. The result
still consumes one existing message-stage unit, and deadlines still run after
the bounded message stage. A second result that observes the reserve occupied
receives the existing `RETRY_TRANSIENT` path; no envelope is evicted.

The RabbitMQ coordinator subscribes to `JOB_SUBMIT` with route-local prefetch
`1` on one dedicated channel and explicit stable consumer tag. Result and
heartbeat intake retain configured prefetch on the primary channel. Submission
intake stays live rather than being cancelled, so exact replay, typed rejection,
and recovery after load falls have no consumer-transition race.

Core maintains one immutable, process-local overload snapshot with stable
reasons for both lanes, pending outbox, active jobs, and active tasks. It is
refreshed at the owning mutation boundaries and emitted through structured
transition/metrics events. A failed outbox count retains the prior known value
and marks observation unhealthy. The snapshot is neither durable state nor
admission authority.

## Alternatives Considered

- **Cancel and resubscribe the submission consumer:** rejected because
  cancellation overlaps deferred acknowledgements, automatic recovery, and
  shutdown ownership, creating a new delivery-lifecycle state machine.
- **Use one unbounded priority queue:** rejected because it violates I7 and
  converts overload into heap growth.
- **Give both routes the full configured capacity:** rejected because it
  doubles the configured memory bound.
- **Split the configured capacity:** rejected because capacity `1` would leave
  one route without a usable slot.
- **Create a second scheduler:** rejected because it duplicates authoritative
  transition ordering and recovery ownership.
- **Add a public reserve/prefetch knob:** rejected because the smallest safe
  values are contract constants and an unsafe value would weaken the guarantee.

## Consequences

- Total scheduler-envelope capacity is exactly configured capacity plus one.
- Persistent submissions cannot occupy the one result reserve.
- Result priority is bounded by the existing message budget and can still
  receive finite retry when its reserve is occupied.
- New work at an admission limit remains explicitly rejected before J0/T0;
  exact replay remains available.
- Active/outbox/lane cleanup automatically clears its overload reason, and the
  next eligible submission can commit without restart.
- No protocol field, database table, schema version, retry schedule, or public
  configuration default changes.

## Conditions That Would Invalidate This Decision

A replacement ADR is required if measured supported workloads need more than
one independently progressing accepted-result lane, if RabbitMQ route-local
QoS cannot recover without duplicate consumers on a supported deployment, or
if one authoritative coordinator is replaced. The replacement must retain an
explicit finite memory bound, acknowledgement ownership, exact replay before
admission, and preservation of already accepted work.

## Evidence And Implementation Status

- [`SchedulerMailboxTest`](../../taskflow-core/src/test/java/server/scheduler/SchedulerMailboxTest.java)
- [`SchedulerOverloadStatusTest`](../../taskflow-core/src/test/java/server/scheduler/SchedulerOverloadStatusTest.java)
- [`SchedulerOverloadTest`](../../taskflow-core/src/test/java/server/scheduler/SchedulerOverloadTest.java)
- [`RabbitMqTransportDeliveryDispositionTest`](../../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqTransportDeliveryDispositionTest.java)
- [`RabbitMqTransportLiveTest`](../../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqTransportLiveTest.java)
- [`RabbitMqCoordinatorLiveIntegrationTest`](../../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java)
- [Persistent-overload report](../reports/persistent-overload.md)

## Related Documents

- [ADR 0009: RabbitMQ sole supported transport](0009-rabbitmq-sole-supported-transport.md)
- [ADR 0012: Simple weighted-capacity scheduling](0012-simple-weighted-capacity-scheduling.md)
- [Backpressure scope](../BACKPRESSURE_SCOPE.md)
- [Failure model](../FAILURE_MODEL.md)
