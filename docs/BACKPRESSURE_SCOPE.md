# Backpressure Scope

This document records TaskFlow's current RabbitMQ backpressure behavior and why
adaptive throttling remains deferred.

## Implemented Boundaries

- Scheduler ingress is bounded by `inboundQueueCapacity` /
  `TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY`.
- RabbitMQ coordinator deliveries for `JOB_SUBMIT` and `TASK_RESULT` remain
  unacknowledged until scheduler admission. If the mailbox is full, the
  delivery receives `RETRY_TRANSIENT` and already accepted work is unchanged.
- RabbitMQ consumers apply `TASKFLOW_RABBITMQ_PREFETCH`, limiting outstanding
  unacknowledged deliveries per consumer channel.
- Executor participants defer `TASK_ASSIGN` acknowledgement until the matching
  `TASK_RESULT` publication is broker-confirmed. A transient failed publication
  gives the assignment `RETRY_TRANSIENT`.
- Participant liveness events are internal coordinator events. If the bounded
  scheduler mailbox cannot admit one, the coordinator logs an explicit
  dropped-event error instead of claiming it was handled.

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
- `RabbitMqTransportLiveTest` covers prefetch with unacknowledged deliveries,
  elapsed delayed-retry timing, the exact configured attempt bound, and final
  quarantine without immediate-redelivery spin.
- `RabbitMqCoordinatorConnectionTest` and `WorkerAssignmentDeduplicationIntegrationTest`
  cover deferred assignment acknowledgement and confirmed result publication.
- RabbitMQ coordinator tests cover acknowledgement of successful, duplicate,
  and stale outcomes plus typed delayed retry on retryable scheduler/storage
  failures.

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
