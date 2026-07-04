# Execution Guarantees

This document defines the current runtime guarantees of TaskFlow.

## Delivery and Execution Semantics

- **Task execution model:** at-least-once.
- **Implication:** a task may run more than once in failure or timeout scenarios.
- **Idempotency guard:** a task result is accepted only when it comes from the currently assigned peer and the task is in `ASSIGNED` state.

## Job Submission Validation

- GUI and command-line submitters generate peer-scoped job IDs with the
  sanitized submitting peer ID, a timestamp, and a full UUID suffix.
- Protocol parsing validates required framework fields, safe peer/job/task
  identifiers, safe task-type names, task-count limits, inline job-payload
  size, and result-payload size through the shared `MessageValidator`.
- The scheduler rejects duplicate submitted job IDs that are already active.
  When persistence is enabled, it also rejects IDs already present in persisted
  job history.
- Coordinator-side `TaskPlugin` implementations validate submitted parameters and payload shapes during job startup.
- Built-in server plugins reject missing or unsupported task options, empty payload lists, malformed payload objects, unsupported conversion file extensions, invalid Base64 file data, and invalid conversion payload-reference shapes.
- Invalid submissions return a failed terminal `JOB_RESULT` before scheduler startup persists tasks or assigns peer work when the requester can be routed. Invalid non-submit broker deliveries are rejected instead of requeued indefinitely.

## Peer Submitter and Result Handling

- Peers may combine submitter, executor, and result-handler capabilities depending on their runtime profile and transport.
- TCP, RabbitMQ, command-line peers, and JavaFX GUI peers use explicit sanitized peer IDs. `TASKFLOW_PEER_ID` provides a stable configured ID; otherwise the runtime generates a unique process-scoped fallback ID for local use.
- TCP coordinator registration uses the peer-declared ID from heartbeat or first peer message, not the server-side socket address. Active duplicate TCP peer IDs are rejected without replacing the existing peer.
- Current RabbitMQ peer routes are keyed by peer ID; duplicate active RabbitMQ peers with the same ID are an invalid deployment configuration because the broker cannot disambiguate ownership of the shared peer route.
- When SQLite persistence is available, the coordinator records durable last-known peer metadata for peer ID, runtime type, transport, capabilities, heartbeat/disconnect times, status, and scheduling metric snapshots. Live sockets, connection handles, broker consumers, and channels are not persisted.
- Submitter paths use `ClientJobPlugin.buildPayloads(...)` for local input handling. Conversion submitters inline Base64 by default and can use local-file payload references when `TASKFLOW_PAYLOAD_STORAGE_DIR` is configured.
- Successful final `JOB_RESULT` payloads are handled by the matching `ClientJobPlugin.handleResult(...)` in the JavaFX GUI and the RabbitMQ command-line submitter. The default handler calls `saveResults(...)` for list-based file-result plugins.
- The JavaFX GUI is the supported peer UI for TCP submit, execute, receive-result, and save-result behavior, and it has service-level RabbitMQ support for live submit, execute, result routing, and save flows.
- The RabbitMQ command-line `submit` path is the supported headless submit-and-save flow today.
- The legacy TCP command-line peer can execute assigned work and has a low-level signed submit helper, but it does not provide a supported final-result saving workflow.
- `docs/PEER_LIFECYCLE.md` records the current lifecycle, evidence, and shared-service candidates.
- `docs/PEER_IDENTITY.md` records the peer identity contract, sanitization, duplicate-ID behavior, and generated fallback limits.

## Runtime Direction

- RabbitMQ is the planned primary runtime for the coordinator, command-line peers, and JavaFX GUI peers.
- TCP remains the current default local runtime and compatibility/demo path until RabbitMQ replacement gates pass.
- This direction does not change current defaults: TCP remains default, RabbitMQ live broker tests are opt-in for local runs, and RabbitMQ is still transitional until its remaining documented gates are implemented and tested. GitHub Actions runs a dedicated RabbitMQ integration job for the focused live broker gates.
- `docs/RUNTIME_STRATEGY.md` records the default-flip, support-promotion, TCP-deprecation, and TCP-removal gates.

## RabbitMQ Publication

- RabbitMQ transport channels enable publisher-confirm mode during startup.
- `publish` and `publishToPeer` return success only after RabbitMQ confirms the publish within `TASKFLOW_RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS`.
- A broker nack or publisher-confirm timeout is reported as a failed publish.
- Peer-targeted publishes also use RabbitMQ mandatory-return detection; an unroutable peer-targeted publish is reported as failed.
- With SQLite persistence available, coordinator `TASK_ASSIGN` and final `JOB_RESULT` publications are stored in a durable broker outbox transactionally with the corresponding task assignment or terminal job-state update.
- Pending outbox rows are replayed when the RabbitMQ coordinator starts and retried periodically while it runs. Confirmed publishes mark the outbox row sent; unconfirmed or unroutable peer-targeted publishes keep the row pending with a failed-attempt record.
- Replay can duplicate a task assignment or final job result if the coordinator crashes after RabbitMQ accepts a publish but before SQLite records the outbox row as sent. Scheduler task-result acceptance remains idempotent by current assigned peer and task state, and terminal job completion is persisted once.
- Peer-side `TASK_RESULT` publishes are not stored in the coordinator outbox. RabbitMQ peers and the GUI defer acknowledgement of `TASK_ASSIGN` until the corresponding `TASK_RESULT` publish is confirmed, so publish failure requeues the original assignment.
- RabbitMQ remains a transitional adapter; `docs/RABBITMQ_SCOPE.md` records the gates required before calling it the primary supported runtime.

## RabbitMQ Connection Recovery

- RabbitMQ client automatic connection recovery is enabled for transport connections.
- Opt-in live transport coverage verifies that an existing transport can consume and publish again after the broker closes its connection through the RabbitMQ management API.
- SQLite-backed coordinator outbox replay covers coordinator-originated `TASK_ASSIGN` and final `JOB_RESULT` messages that were queued before a coordinator crash. Live broker coverage verifies seeded pending outbox rows, replay after a simulated publish-before-sent-marking crash window, and duplicate task-result rejection after replayed task assignments.
- This does not guarantee full broker outage recovery, peer-side durable `TASK_RESULT` replay, or redelivery of messages outside the coordinator outbox.

## RabbitMQ Dead Lettering

- RabbitMQ topology declaration can configure a dead-letter exchange, dead-letter queue, and quarantine queue for normal TaskFlow queues.
- Malformed or validation-failing broker deliveries are rejected, and handler failures can be rejected when `TASKFLOW_RABBITMQ_REQUEUE_ON_HANDLER_FAILURE=false`.
- Rejected deliveries are routed by RabbitMQ to the configured dead-letter queue when dead-lettering is enabled.
- `peer.PeerNode dlq inspect` reads dead-letter metadata and body previews without acknowledging the DLQ entry.
- `peer.PeerNode dlq redrive` republishes only valid TaskFlow broker envelopes to the original routing key captured in RabbitMQ dead-letter metadata, increments `x-taskflow-redrive-count`, and acknowledges the DLQ entry only after RabbitMQ confirms the publish.
- Redrive refuses malformed or unknown-route poison messages and leaves them in the DLQ for an explicit quarantine or discard decision.
- `peer.PeerNode dlq quarantine` republishes a DLQ entry to the configured quarantine queue and acknowledges the original DLQ entry only after publish confirmation.
- `peer.PeerNode dlq discard` acknowledges and removes the DLQ entry without republishing it.

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
- **Lease per assigned task:** 120 seconds.
- **Maximum retries per task:** 20 attempts.
- During active scheduler operation, on timeout, lease expiry, or explicit peer execution failure:
  - the attempt is counted as failed,
  - the task is retried if attempts remain,
  - otherwise the task moves to terminal `FAILED`.
- When a retry is scheduled, the persisted task row is returned to `PENDING`, its previous assignment/timing/lease fields are cleared, and `retry_count` is incremented.
- During startup recovery, an expired or missing lease is released to `PENDING` without incrementing `retry_count`; the next runtime attempt receives a new attempt-history row.

## Capability-Aware Assignment

- Peers advertise supported task types in heartbeat metadata.
- The scheduler assigns a task only to peers that advertise support for that task type.
- If no capable peer is available, the task remains pending instead of being assigned to an incompatible peer.

## Job Completion/Failure

- A job is **successful** only when all tasks complete.
- A job is **failed** when any task reaches terminal `FAILED`.
- On job failure, non-terminal remaining tasks are persisted as failed in DB for consistent historical state.
- For non-outbox outputs, final `JOB_RESULT` delivery is retried when the requester cannot be reached or the output transport reports failure.
- Non-outbox final result delivery is bounded by `jobResultMaxDeliveryAttempts` / `TASKFLOW_JOB_RESULT_MAX_DELIVERY_ATTEMPTS`.
- If non-outbox final result delivery is exhausted, the scheduler removes the job from active memory, logs `job_result_delivery_abandoned`, and persists the job as failed so it does not remain pending forever.
- RabbitMQ with SQLite persistence commits the terminal job state and final `JOB_RESULT` outbox row together; failed publishes remain pending for replay instead of using the bounded non-outbox delivery counter.

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

## Transport Security Scope

- RabbitMQ credentials are supplied through environment variables and default to the local-only `guest` / `guest` demo credentials.
- Non-demo RabbitMQ deployments should use a dedicated vhost, a least-privilege RabbitMQ user, secret storage outside source control, restricted management API access, and trusted network boundaries.
- TaskFlow does not currently expose native RabbitMQ TLS/certificate options. Use a verified TLS-terminating tunnel/proxy for untrusted networks or add tested RabbitMQ Java client TLS configuration before claiming direct broker TLS support.
- These transport controls are separate from TaskFlow requester-token ownership and do not add user/account authentication.

## Persistence

- SQLite is the current `JobStateStore` implementation, provided by `taskflow-persistence-sqlite`.
- The SQLite schema is versioned and startup rejects schema versions newer than this runtime supports.
- SQLite foreign-key checks are enabled per connection, and `tasks.job_id` must reference an existing `jobs.job_id`.
- Existing unversioned task tables are migrated to the current foreign-key schema when they do not contain orphan task rows.
- Schema version 2 stores job parameters plus task payload/result snapshots used for startup recovery.
- Schema version 3 stores requester token hashes used to authorize result requests across reconnects.
- Schema version 4 stores requester identity public keys used to require signed result requests for identity-bound jobs.
- Schema version 5 stores peer registry metadata for last-known peer state across coordinator restart.
- Schema version 6 stores the completed job's final semantic result payload.
- Schema version 7 stores task attempt history rows for assignment, success, retry, terminal failure, dispatch failure, startup reconciliation, and restart release.
- Schema version 8 stores task lease owner and expiry for assigned work.
- Schema version 9 stores coordinator broker outbox rows for RabbitMQ `TASK_ASSIGN` and final `JOB_RESULT` publication replay.
- Coordinator startup rebuilds resumable `RUNNING` jobs from persisted snapshots, restores completed task results when result payloads were persisted, preserves assigned tasks with unexpired leases, and releases assigned tasks with expired or missing leases to `PENDING` with a `lease_expired` attempt reason.
- Legacy or otherwise non-resumable `RUNNING` jobs are marked `FAILED` on startup.
- If startup recovery cannot safely reconcile persisted state, the coordinator closes that state store, disables persistence for the run, and logs `database_disabled` instead of writing against unreconciled history.
- After startup, task assignment must be persisted before dispatching work to a peer.
- If retry, task-failure, or task-completion persistence fails after in-memory state changes, the scheduler fails the job with a terminal `JOB_RESULT` and attempts to persist terminal task/job state.
- For non-outbox outputs, final job-status persistence happens after final result delivery. For RabbitMQ with SQLite persistence, terminal job status and the final `JOB_RESULT` outbox row are committed together before immediate publish/replay. If a non-outbox terminal write fails after delivery, the scheduler removes the job from active memory and logs `job_terminal_persistence_degraded` with the failed operation and policy.
- `JOB_RESULT_REQUEST` can resend an in-memory pending terminal result or reconstruct a completed persisted `JOB_RESULT` when the requester token matches, any required requester identity signature is valid, and every task result snapshot exists. Reconstructed completed results include the schema-v6 semantic final payload when it was persisted, plus the compatibility ordered task-result list.
- Failed jobs and completed jobs with missing result snapshots are not reconstructed as successful persisted results.
- PostgreSQL/Flyway is not implemented. `docs/RECOVERY_SCOPE.md` records the SQLite lease behavior and keeps PostgreSQL/Flyway deferred until there is a concrete external database requirement.

## Heartbeat and Peer Liveness

- Coordinator sends periodic `PING` and expects `PONG`.
- TCP peers answer `PING` with their explicit peer ID and supported task types.
- Missing heartbeats beyond timeout mark the peer stale, remove it from the
  live registry, and persist a disconnected last-known peer status when the
  peer registry store is available.

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

`docs/OBSERVABILITY_SCOPE.md` maps the current structured-log events and records that a dedicated metrics backend is deferred until promoted lease/attempt-history dashboards, RabbitMQ outbox visibility, or promoted DLQ workflow metrics need operational visibility.
