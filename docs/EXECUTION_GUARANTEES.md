# Execution Guarantees

This document defines the current runtime guarantees of TaskFlow.

## Delivery and Execution Semantics

- **Task execution model:** at-least-once.
- **Implication:** a task may run more than once in failure or timeout scenarios.
- **Idempotency guard:** a task result is accepted only when it comes from the currently assigned peer and the task is in `ASSIGNED` state.

## Job Submission Validation

- Coordinator-side `TaskPlugin` implementations validate submitted parameters and payload shapes during job startup.
- Built-in server plugins reject missing or unsupported task options, empty payload lists, malformed payload objects, unsupported conversion file extensions, and invalid Base64 file data.
- Invalid submissions return a failed terminal `JOB_RESULT` before scheduler startup persists tasks or assigns peer work.

## Peer Submitter and Result Handling

- Peers may combine submitter, executor, and result-handler capabilities depending on their runtime profile and transport.
- Submitter paths use `ClientJobPlugin.buildPayloads(...)` for local input handling.
- Successful final `JOB_RESULT` payloads are handled by the matching `ClientJobPlugin.saveResults(...)` in the JavaFX GUI and the RabbitMQ command-line submitter.
- The JavaFX GUI is the supported peer UI for TCP submit, execute, receive-result, and save-result behavior, and it has service-level RabbitMQ support for live submit, execute, result routing, and save flows.
- The RabbitMQ command-line `submit` path is the supported headless submit-and-save flow today.
- The legacy TCP command-line peer can execute assigned work and has a low-level signed submit helper, but it does not provide a supported final-result saving workflow.
- `docs/PEER_LIFECYCLE.md` records the current lifecycle, evidence, and shared-service candidates.

## Runtime Direction

- RabbitMQ is the planned primary runtime for the coordinator, command-line peers, and JavaFX GUI peers.
- TCP remains the current default local runtime and compatibility/demo path until RabbitMQ replacement gates pass.
- This direction does not change current defaults: TCP remains default, RabbitMQ live broker tests are opt-in, and RabbitMQ is still transitional until its documented gates are implemented and tested.
- `docs/RUNTIME_STRATEGY.md` records the default-flip, support-promotion, TCP-deprecation, and TCP-removal gates.

## RabbitMQ Publication

- RabbitMQ transport channels enable publisher-confirm mode during startup.
- `publish` and `publishToPeer` return success only after RabbitMQ confirms the publish within `TASKFLOW_RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS`.
- A broker nack or publisher-confirm timeout is reported as a failed publish.
- Peer-targeted publishes also use RabbitMQ mandatory-return detection; an unroutable peer-targeted publish is reported as failed.
- Failed task-assignment publishes are retried by scheduler dispatch logic.
- Failed final `JOB_RESULT` publishes remain pending until delivery succeeds or `jobResultMaxDeliveryAttempts` is exhausted.
- This is not a durable outbox: coordinator crash replay around publication is still not implemented.
- RabbitMQ remains a transitional adapter; `docs/RABBITMQ_SCOPE.md` records the gates required before calling it the primary supported runtime.

## RabbitMQ Connection Recovery

- RabbitMQ client automatic connection recovery is enabled for transport connections.
- Opt-in live transport coverage verifies that an existing transport can consume and publish again after the broker closes its connection through the RabbitMQ management API.
- This does not guarantee durable replay for messages that were not broker-confirmed before an outage.
- This does not recover coordinator crashes around publication; a durable outbox/replay model is still not implemented.

## RabbitMQ Dead Lettering

- RabbitMQ topology declaration can configure a dead-letter exchange and queue for normal TaskFlow queues.
- Malformed broker deliveries are rejected, and handler failures can be rejected when `TASKFLOW_RABBITMQ_REQUEUE_ON_HANDLER_FAILURE=false`.
- Rejected deliveries are routed by RabbitMQ to the configured dead-letter queue when dead-lettering is enabled.
- TaskFlow does not yet provide a DLQ inspection, quarantine, or redrive workflow. Dead-lettered messages require operator review outside TaskFlow before any manual republish.

## JavaFX GUI Transport Scope

- The JavaFX GUI uses TCP by default and selects RabbitMQ when `TASKFLOW_TRANSPORT=rabbitmq` is set before launch.
- GUI job submission, live result reception, requester-token persistence, and GUI task execution are implemented behind `JobSubmissionClient`, `CoordinatorConnection`, result-routing, and worker-runtime boundaries for both TCP and RabbitMQ.
- RabbitMQ GUI task-assignment acknowledgements are deferred until the GUI peer publishes the corresponding `TASK_RESULT`; broker publish failure requeues the assignment.
- RabbitMQ GUI `JOB_RESULT` delivery is routed through the existing active-job router and plugin-backed save flow, but `JOB_RESULT_REQUEST` over RabbitMQ is not implemented because there is no broker route for that request yet.
- Current JavaFX RabbitMQ coverage is service-level and headless. Full JavaFX desktop automation remains deferred in `docs/GUI_AUTOMATION_SCOPE.md`.

## Scheduler Ingress and Backpressure

- Scheduler ingress uses a bounded mailbox controlled by `inboundQueueCapacity` / `TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY`, default `1000`.
- TCP peer handlers wait for bounded scheduler-mailbox capacity before admitting `JOB_SUBMIT` and `TASK_RESULT` messages, applying socket-level backpressure instead of dropping those messages.
- RabbitMQ job submissions and task results are requeued when the scheduler mailbox is full instead of being accepted into process memory.
- RabbitMQ transport channels apply `TASKFLOW_RABBITMQ_PREFETCH` with `basicQos`.
- Broker deliveries use manual acknowledgement.
- Deferred acknowledgements keep deliveries unacknowledged until the scheduler or peer explicitly settles them.
- Live broker coverage verifies `prefetch=1` prevents a second shared-route delivery while the first delivery remains unacknowledged.
- Adaptive backpressure across broker queue depth, peer capacity, and external autoscaling remains future work. `docs/BACKPRESSURE_SCOPE.md` records the current backpressure boundaries and the evidence required before adding adaptive throttling.

## Task State Machine

Each task moves through:

- `PENDING`
- `ASSIGNED`
- `COMPLETED` or `FAILED` (terminal)

Invalid/stale transitions are ignored (for example duplicate success from a peer that is no longer assigned).
The SQLite state store also guards these persisted transitions so terminal task/job rows are not overwritten by later updates.

## Retry and Timeout Policy

- **Timeout per assigned task:** 60 seconds.
- **Maximum retries per task:** 20 attempts.
- On timeout or explicit peer execution failure:
  - the attempt is counted as failed,
  - the task is retried if attempts remain,
  - otherwise the task moves to terminal `FAILED`.
- When a retry is scheduled, the persisted task row is returned to `PENDING`, its previous assignment/timing fields are cleared, and `retry_count` is incremented.

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

## Result Ownership

- Job submissions carry a per-job requester token.
- Job submissions may also carry a requester public key and Ed25519 signature. The JavaFX submitter and command-line submit paths sign submissions.
- The coordinator persists only the token hash and requester public key, not the raw token or private key.
- `JOB_RESULT_REQUEST` must include the matching requester token before the coordinator resends an in-memory pending terminal result, reports an active job as still running, or reconstructs a completed persisted result.
- Jobs submitted with a requester public key are identity-bound: result requests must include the same public key and a valid signature over the request fields.
- Requests with a missing or wrong token, missing identity signature, mismatched public key, or invalid signature return a failed `JOB_RESULT` instead of task results.
- Running jobs without a persisted requester token hash are treated as non-resumable at startup because ownership cannot be verified after restart.
- JavaFX submissions persist raw requester tokens and a requester identity keypair in a local user-profile token file. TCP result requests can use those tokens across later GUI processes; RabbitMQ GUI result-request replay is not implemented. The default path is `<user-home>/.taskflow/gui-requester-tokens.properties`, overrideable with `TASKFLOW_GUI_REQUESTER_TOKEN_STORE`.
- On POSIX-compatible filesystems, TaskFlow attempts to restrict that token file to owner read/write and its parent directory to owner read/write/execute. On Windows or unsupported filesystems, token-file protection depends on the normal user-profile and filesystem access controls.
- This is a per-job token plus local-signing-key ownership model, not full user/account authentication, login sessions, authorization roles, replay prevention, or a credential vault.

## Persistence

- SQLite is the current `JobStateStore` implementation, provided by `taskflow-persistence-sqlite`.
- The SQLite schema is versioned and startup rejects schema versions newer than this runtime supports.
- SQLite foreign-key checks are enabled per connection, and `tasks.job_id` must reference an existing `jobs.job_id`.
- Existing unversioned task tables are migrated to the current foreign-key schema when they do not contain orphan task rows.
- Schema version 2 stores job parameters plus task payload/result snapshots used for startup recovery.
- Schema version 3 stores requester token hashes used to authorize result requests across reconnects.
- Schema version 4 stores requester identity public keys used to require signed result requests for identity-bound jobs.
- Coordinator startup rebuilds resumable `RUNNING` jobs from persisted snapshots, restores completed task results when result payloads were persisted, and resets assigned tasks to `PENDING` because leases are not implemented.
- Legacy or otherwise non-resumable `RUNNING` jobs are marked `FAILED` on startup.
- If startup recovery cannot safely reconcile persisted state, the coordinator closes that state store, disables persistence for the run, and logs `database_disabled` instead of writing against unreconciled history.
- After startup, task assignment must be persisted before dispatching work to a peer.
- If retry, task-failure, or task-completion persistence fails after in-memory state changes, the scheduler fails the job with a terminal `JOB_RESULT` and attempts to persist terminal task/job state.
- Final job-status persistence happens after final result delivery. If that terminal write fails, the scheduler removes the job from active memory and logs `job_terminal_persistence_degraded` with the failed operation and policy.
- `JOB_RESULT_REQUEST` can resend an in-memory pending terminal result or reconstruct a completed persisted `JOB_RESULT` when the requester token matches, any required requester identity signature is valid, and every task result snapshot exists.
- Failed jobs and completed jobs with missing result snapshots are not reconstructed as successful persisted results.
- Explicit attempt history, lease-based recovery, and PostgreSQL/Flyway are not implemented. `docs/RECOVERY_SCOPE.md` records attempt history and leases as accepted future behavior scope, with PostgreSQL/Flyway deferred until there is a concrete external database requirement.

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

`docs/OBSERVABILITY_SCOPE.md` maps the current structured-log events and records that a dedicated metrics backend is deferred until recovery leases, RabbitMQ outbox/replay, or TaskFlow DLQ workflow work needs promoted operational visibility.
