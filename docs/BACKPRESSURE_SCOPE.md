# Backpressure Scope

This document records TaskFlow's current RabbitMQ backpressure behavior and why
adaptive throttling remains deferred.

## Implemented Boundaries

- Scheduler ingress is bounded by `inboundQueueCapacity` /
  `TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY`.
- RabbitMQ coordinator deliveries for `JOB_SUBMIT` and `TASK_RESULT` remain
  unacknowledged until scheduler admission. If the mailbox is full, the
  delivery is requeued and already accepted work is unchanged.
- RabbitMQ consumers apply `TASKFLOW_RABBITMQ_PREFETCH`, limiting outstanding
  unacknowledged deliveries per consumer channel.
- Executor participants defer `TASK_ASSIGN` acknowledgement until the matching
  `TASK_RESULT` publication is broker-confirmed. A failed publication requeues
  the assignment.
- Participant liveness events are internal coordinator events. If the bounded
  scheduler mailbox cannot admit one, the coordinator logs an explicit
  dropped-event error instead of claiming it was handled.

These mechanisms define one broker-facing overload contract. They do not claim
adaptive capacity management or bounded delayed poison-message retry.

## Current Evidence

- `SchedulerMailboxTest` covers bounded mailbox creation, accepted broker
  delivery deferral, full-mailbox requeue, and repeated overflow without
  replacing already accepted work.
- `RabbitMqTransportLiveTest` covers prefetch with unacknowledged deliveries.
- `RabbitMqCoordinatorConnectionTest` and `WorkerAssignmentDeduplicationIntegrationTest`
  cover deferred assignment acknowledgement and confirmed result publication.
- RabbitMQ coordinator tests cover acknowledgement of successful, duplicate,
  and stale outcomes plus requeue on retryable scheduler/storage failures.

## Deferred Adaptive Behavior

Adaptive broker/participant backpressure remains deferred because no measured
overload target currently requires dynamic tuning beyond the bounded mailbox,
broker requeue, deferred acknowledgements, and prefetch controls.

Do not add adaptive throttling knobs until a reproducible overload test or demo
shows a specific failure mode these boundaries do not address. Any future
design must define:

- observed metrics and thresholds;
- queue boundaries being controlled;
- participant capacity signals;
- broker queue-depth behavior;
- deadlock-avoidance rules;
- behavior when overload persists.
