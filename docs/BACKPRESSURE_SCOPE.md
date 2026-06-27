# Backpressure Scope

This document records the current TaskFlow backpressure behavior and why
adaptive broker/peer throttling is deferred.

## Implemented Boundaries

- Scheduler ingress is bounded by `inboundQueueCapacity` /
  `TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY`.
- TCP peer handlers use blocking mailbox admission for scheduler-bound
  `JOB_SUBMIT`, `TASK_RESULT`, and `JOB_RESULT_REQUEST` messages. When the
  scheduler mailbox is full, the socket handler waits instead of accepting and
  dropping the message.
- RabbitMQ coordinator deliveries for `JOB_SUBMIT` and `TASK_RESULT` defer
  acknowledgement before scheduler admission. If the scheduler mailbox is full,
  the delivery is requeued and the accepted scheduler work remains unchanged.
- RabbitMQ consumers apply `TASKFLOW_RABBITMQ_PREFETCH`, so the broker limits
  how many deliveries can remain unacknowledged on a consumer channel.
- Peer disconnect and liveness events are internal coordinator events. If those
  events cannot be admitted because the scheduler mailbox is full, the
  coordinator logs an explicit dropped-event error instead of silently claiming
  the event was handled.

## Current Evidence

- `SchedulerMailboxTest` covers bounded mailbox creation, accepted broker
  delivery deferral, full-mailbox broker requeue, and repeated broker overflow
  without replacing already accepted scheduler work.
- `PeerHandlerTest` covers TCP scheduler-bound messages waiting for mailbox
  capacity.
- RabbitMQ transport live tests cover prefetch behavior with unacknowledged
  deliveries.

## Deferred Adaptive Behavior

Adaptive broker/peer backpressure remains deferred because there is not yet a
measured overload target that requires dynamic tuning beyond the implemented
bounded mailbox, blocking TCP admission, RabbitMQ requeue, and RabbitMQ prefetch
controls.

Do not add adaptive throttling knobs until a reproducible overload test or demo
shows a specific failure mode these boundaries do not address. Any future design
must define:

- observed metrics and thresholds;
- queue boundaries being controlled;
- peer capacity signals;
- broker queue-depth behavior;
- deadlock-avoidance rules;
- behavior when overload persists.
