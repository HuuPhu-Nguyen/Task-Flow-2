# Backpressure Scope

This document records TaskFlow's current bounded RabbitMQ backpressure and
persistent-overload behavior. Participant-driven adaptive rate control remains
deferred.

## Implemented Boundaries

- New J0/T0 work is rejected explicitly before durable acceptance when
  configured active-job, active-task, per-job task, inline-byte,
  per-reference-byte, or pending SQLite-outbox bounds activate. Active limits
  use resulting count `<=`; pending outbox rejects at the configured
  threshold. Exact idempotent replay bypasses capacity admission.
- Limit rejection returns failed protocol-v2 `JOB_RESULT` with a typed
  `admissionRejection`; pending-outbox count failure is reported as storage
  failure. Neither path mutates accepted state.
- Scheduler ingress has two bounded lanes. The ordinary submission/control
  lane has exactly `inboundQueueCapacity` /
  `TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY` slots. A fixed task-result lane
  adds one slot, so total envelope capacity is exactly the configured capacity
  plus one. A second result that finds the reserve occupied receives the
  existing `RETRY_TRANSIENT`; no queued envelope is evicted.
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
- The coordinator consumes `JOB_SUBMIT` with route-local prefetch `1` on one
  dedicated channel. `TASK_RESULT`, `HEARTBEAT`, and participant transports
  retain `TASKFLOW_RABBITMQ_PREFETCH`. The dedicated channel and explicit
  consumer tag participate in automatic topology recovery.
- The scheduler dequeues one queued task result before ordinary FIFO work.
  Every result still consumes one existing message-stage unit, and the bounded
  deadline stage still runs after each message batch.
- `TaskScheduler.getOverloadSnapshot()` exposes immutable active reasons for
  submission-lane saturation, observed result-reserve saturation, pending
  outbox, active jobs, and active tasks. Each reason contains its configured
  maximum, observed value, and activation time. Pending-outbox count failure
  retains the last known pressure and marks observation unhealthy.
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
ephemeral peer route disappears or participant-driven adaptive capacity
management.

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
- `SchedulerAdmissionTest`, `TaskSchedulerPersistenceTest`, and
  `DatabaseManagerTest` prove exact admission boundaries, no rejected J0/T0
  mutation, replay while full, and aggregate outbox counting/storage failure.
- `AdmissionOverloadExperiment#coordinatorHeapPlateausAtConfiguredBounds`
  proves the configured active projection and durable commit count stop at 64
  jobs / 4,096 tasks through 100,000 unique typed rejections; retained-heap
  samples are recorded in `reports/admission-overload.md`.
- `SchedulerOverloadTest` proves capacity-`1` result priority, deadline
  progress, exact typed rejection at an active bound, and fresh admission after
  cleanup without scheduler restart. Its fixed-heap baseline/changed results
  are recorded in `reports/persistent-overload.md`.
- `RabbitMqTransportLiveTest#routeLocalJobPrefetchDoesNotBlockTaskResultIntakeAgainstLiveBroker`
  and
  `RabbitMqCoordinatorLiveIntegrationTest#submissionFloodPreservesAcceptedResultsAndRecoversAdmissionAgainstLiveBroker`
  prove independent result intake and end-to-end flood recovery.

## Remaining Deferred Adaptive Behavior

TF-0406 uses a fixed reduced submission prefetch, a bounded result reserve, and
automatic recovery through continuously live intake. It intentionally does not
add consumer cancel/resubscribe transitions, broker-management polling, or
participant-specific throttling.

Do not add adaptive throttling knobs until a reproducible overload test or demo
shows a specific failure mode these boundaries do not address. Any future
design must define:

- observed metrics and thresholds;
- queue boundaries being controlled;
- participant capacity signals;
- broker queue-depth behavior;
- deadlock-avoidance rules;
- behavior when overload persists.
