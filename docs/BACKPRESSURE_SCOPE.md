# Backpressure Scope

This document records TaskFlow's current RabbitMQ backpressure behavior and why
adaptive throttling remains deferred.

## Implemented Boundaries

- Scheduler ingress is bounded by `inboundQueueCapacity` /
  `TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY`.
- Scheduler CPU work is also bounded per cycle: message, combined deadline,
  dispatch, and terminal/outbox stages have separate positive limits, all
  defaulting to `100`. The exact order ensures mailbox pressure cannot exclude
  deadlines and deadline pressure cannot exclude already queued results.
- Cross-job dispatch uses a persistent round-robin pass and a positive
  per-job assignment quota, default `1`, no greater than the dispatch batch.
  Capacity-blocked dispatch visits at most one configured batch at a time.
- A no-capacity probe removes the job from runnable rotation and adds one
  capacity-wait entry. A capacity-availability generation or deterministic
  500 ms recheck makes the prior waiting generation eligible; capability
  heartbeats wake the scheduler without polling or creating an overflow queue.
- RabbitMQ coordinator deliveries for `JOB_SUBMIT` and `TASK_RESULT` remain
  unacknowledged through scheduler admission and processing. If the mailbox is
  full, the delivery receives `RETRY_TRANSIENT` and already accepted work is
  unchanged.
- RabbitMQ consumers apply `TASKFLOW_RABBITMQ_PREFETCH`, limiting outstanding
  unacknowledged deliveries per consumer channel.
- Initial broker connection retry has one process-owned attempt in flight at a
  time. Each attempt has a configured timeout and failures use capped
  exponential delay, preventing startup or recovery from creating an
  unbounded connection-attempt storm.
- Executor participants defer `TASK_ASSIGN` acknowledgement until the matching
  `TASK_RESULT` publication is broker-confirmed. A transient failed publication
  gives the assignment `RETRY_TRANSIENT`.
- Participant liveness events are internal coordinator events. If the bounded
  scheduler mailbox cannot admit one, the coordinator logs an explicit
  dropped-event error instead of claiming it was handled.
- Coordinator shutdown atomically closes broker ingress before consumer
  cancellation. Envelopes already admitted to the bounded mailbox drain to a
  typed settlement; a delivery racing after intake closure remains
  unacknowledged and returns to RabbitMQ when the transport channel closes.
  No overflow or shutdown-only in-memory queue is created.
- The SQLite broker-outbox replayer remains independent of the scheduler
  thread, but its pending-row query uses the same configured
  `schedulerOutboxBatchSize` bound.

These mechanisms define one broker-facing overload contract. The current
RabbitMQ adapter maps `RETRY_TRANSIENT` to publisher-confirmed TTL retry queues.
The default delays are 1, 5, and 30 seconds, with four total processing
deliveries including the initial one. Persistent failure reaches final
quarantine instead of spinning on immediate redelivery while the original
routing binding remains available. This does not claim recovery after an
ephemeral peer route disappears or adaptive capacity management.

## Current Evidence

- `SchedulerMailboxTest` covers bounded mailbox creation, accepted broker
  delivery deferral, full-mailbox transient disposition, and repeated overflow
  without replacing already accepted work.
- `SchedulerLoopTest` covers exact cycle order/budgets and both mailbox/deadline
  starvation boundaries with deterministic queues.
- `AssignmentServiceBatchTest` proves the 10,000-plus-ten round-one bound,
  quota persistence across dispatch batches, local retry priority, bounded
  no-capacity probes, capacity-signal restoration, and fake-clock recheck.
- `SchedulerWorkloadIndexTest`, `SchedulerLoopTest`, and
  `JobCompletionServiceBatchTest` prove capacity-wait isolation, external
  scheduler wake-up, stale deadline accounting, and due terminal retries.
- `RabbitMqTransportLiveTest` covers prefetch with unacknowledged deliveries,
  elapsed delayed-retry timing, the exact configured attempt bound, and final
  quarantine without immediate-redelivery spin.
- `RabbitMqCoordinatorConnectionTest` and `WorkerAssignmentDeduplicationIntegrationTest`
  cover deferred assignment acknowledgement and confirmed result publication.
- RabbitMQ coordinator tests cover acknowledgement of successful, duplicate,
  and stale outcomes plus typed delayed retry on retryable scheduler/storage
  failures.
- `RabbitMqCoordinatorLiveIntegrationTest` closes the coordinator connection
  before acknowledgement and after a durable result commit. RabbitMQ
  redelivers in both cases; the latter is classified and acknowledged as a
  harmless duplicate with one authoritative commit.
- `RabbitMqCoordinatorShutdownLiveIntegrationTest` proves one pre-stop
  delivery drains while a post-stop deferred delivery returns to broker
  ownership after transport close.
- `RabbitMqBrokerRecoveryIntegrationTest` proves the connection retry bound,
  offline coordinator outbox ownership, and exact replay after a real
  single-broker stop/restart while work is active.

## Deferred Adaptive Behavior

Adaptive broker/participant backpressure remains deferred because no measured
overload target currently requires dynamic tuning beyond the bounded mailbox,
bounded broker retry, deferred acknowledgements, and prefetch controls.

Do not add adaptive throttling knobs until a reproducible overload test or demo
shows a specific failure mode these boundaries do not address. Any future
design must define:

- observed metrics and thresholds;
- queue boundaries being controlled;
- participant capacity signals;
- broker queue-depth behavior;
- deadlock-avoidance rules;
- behavior when overload persists.
