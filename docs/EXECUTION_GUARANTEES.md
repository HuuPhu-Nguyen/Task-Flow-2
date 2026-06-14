# Execution Guarantees

This document defines the current runtime guarantees of TaskFlow.

## Delivery and Execution Semantics

- **Task execution model:** at-least-once.
- **Implication:** a task may run more than once in failure or timeout scenarios.
- **Idempotency guard:** a task result is accepted only when it comes from the currently assigned peer and the task is in `ASSIGNED` state.

## RabbitMQ Publication

- RabbitMQ transport channels enable publisher-confirm mode during startup.
- `publish` and `publishToPeer` return success only after RabbitMQ confirms the publish within `TASKFLOW_RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS`.
- A broker nack or publisher-confirm timeout is reported as a failed publish.
- Peer-targeted publishes also use RabbitMQ mandatory-return detection; an unroutable peer-targeted publish is reported as failed.
- Failed task-assignment publishes are retried by scheduler dispatch logic.
- Failed final `JOB_RESULT` publishes remain pending until delivery succeeds or `jobResultMaxDeliveryAttempts` is exhausted.
- This is not a durable outbox: coordinator crash replay around publication is still not implemented.

## Task State Machine

Each task moves through:

- `PENDING`
- `ASSIGNED`
- `COMPLETED` or `FAILED` (terminal)

Invalid/stale transitions are ignored (for example duplicate success from a peer that is no longer assigned).

## Retry and Timeout Policy

- **Timeout per assigned task:** 60 seconds.
- **Maximum retries per task:** 20 attempts.
- On timeout or explicit peer execution failure:
  - the attempt is counted as failed,
  - the task is retried if attempts remain,
  - otherwise the task moves to terminal `FAILED`.

## Capability-Aware Assignment

- Peers advertise supported task types in heartbeat metadata.
- The scheduler assigns a task only to peers that advertise support for that task type.
- If no capable peer is available, the task remains pending instead of being assigned to an incompatible peer.

## Job Completion/Failure

- A job is **successful** only when all tasks complete.
- A job is **failed** when any task reaches terminal `FAILED`.
- On job failure, non-terminal remaining tasks are persisted as failed in DB for consistent historical state.
- Final `JOB_RESULT` delivery is retried when the requester cannot be reached or the output transport reports failure.
- Final result delivery is bounded by `jobResultMaxDeliveryAttempts` / `TASKFLOW_JOB_RESULT_MAX_DELIVERY_ATTEMPTS`.
- If final result delivery is exhausted, the scheduler removes the job from active memory, logs `job_result_delivery_abandoned`, and persists the job as failed so it does not remain pending forever.

## Persistence

- SQLite is the current `JobStateStore` implementation.
- The SQLite schema is versioned and startup rejects schema versions newer than this runtime supports.
- SQLite foreign-key checks are enabled per connection, and `tasks.job_id` must reference an existing `jobs.job_id`.
- Existing unversioned task tables are migrated to the current foreign-key schema when they do not contain orphan task rows.
- Coordinator startup marks stale `RUNNING` jobs and non-terminal tasks failed; it does not yet resume in-flight attempts from persisted leases.

## Heartbeat and Peer Liveness

- Coordinator sends periodic `PING` and expects `PONG`.
- Missing heartbeats beyond timeout mark the peer stale and it is removed.

## Core Scheduler Metrics

Scheduler emits structured event logs and periodic metrics snapshots including:

- `queue_depth`
- `active_jobs`
- `dispatch_latency_ms` (average from task becoming pending to assignment)
- `retry_count`
- `task_success_rate` (successful attempts / total attempts)
- `success_count`
- `failure_count`

These metrics are intended for immediate operational visibility in Phase 1 and as migration inputs to dedicated metrics backends in later phases.
