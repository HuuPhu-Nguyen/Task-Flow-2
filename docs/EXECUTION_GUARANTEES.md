# Execution Guarantees

This document defines the current runtime guarantees of TaskFlow.

## Delivery and Execution Semantics

- **Task execution model:** at-least-once.
- **Implication:** a task may run more than once in failure or timeout scenarios.
- **Idempotency guard:** a task result is accepted only when it comes from the currently assigned peer and the task is in `ASSIGNED` state.

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
