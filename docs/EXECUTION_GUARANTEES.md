# Current Execution Behavior

This document records detailed current runtime behavior. The authoritative
guarantee contract, explicit non-goals, and current-versus-planned evidence
status live in [Guarantees and non-goals](GUARANTEES.md). A target invariant
marked partial or planned there is not promoted to a current guarantee by the
implementation notes below.

TaskFlow is a coordinator-mediated distributed task-execution framework. The
coordinator owns authoritative scheduling and state transitions; participant
nodes may enable the requester role, executor role, or both.

## Delivery and Execution Semantics

- **Task execution model:** at-least-once.
- **Implication:** a task may run more than once in failure or timeout scenarios.
- **Idempotency guard:** SQLite commits a successful task result only when task ID, `ASSIGNED` state, attempt number, assignment ID, and the reporting executor-participant ID match the current persisted assignment. Existing peer-ID fields carry that participant identity.

## Executor Assignment Deduplication

- Each executor runtime keeps a bounded, process-local cache keyed by the
  version-2 assignment ID. Running and completed entries share the same result
  future and no cache state is durable.
- A duplicate RabbitMQ delivery while the cached assignment is running is
  acknowledged immediately, does not invoke the processor again, and does not
  publish from the duplicate delivery. The original execution remains
  responsible for result publication.
- A duplicate delivery for a completed cached assignment republishes the exact
  cached `TASK_RESULT`, including its attempt number, assignment ID, timestamp,
  success disposition, and payload. That delivery is acknowledged only after
  the republish is confirmed; failure requeues it.
- `TASKFLOW_ASSIGNMENT_CACHE_MAX_ENTRIES` controls the maximum entry count
  (default `4096`). `TASKFLOW_ASSIGNMENT_CACHE_TTL_MS` controls expiry (default
  `900000` ms). Both values must be positive.
- Capacity eviction, TTL expiry, and executor restart may cause the assignment
  to execute again. This is permitted by the at-least-once contract: the cache
  is a duplicate-work optimization, while SQLite's persisted assignment tuple
  remains the authority for committing one result.
- The engine exposes current running/completed entry counts, duplicate-hit
  counts, and total/capacity/TTL eviction counts through its cache snapshot.
  Assignment-ID reuse for a different task identity while an entry is live is
  rejected as a permanent conflict.

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
- Invalid submissions return a failed terminal `JOB_RESULT` before scheduler startup persists tasks or assigns executor work when the requester can be routed. Invalid non-submit broker deliveries are rejected instead of requeued indefinitely.

## Participant Requester and Result Handling

- Participants may combine requester and executor roles depending on their runtime profile and transport; receiving and handling results belongs to the requester role.
- TCP, RabbitMQ, command-line participants, and JavaFX participants use explicit sanitized peer IDs as compatibility identifiers. `TASKFLOW_PEER_ID` provides a stable configured ID; otherwise the runtime generates a unique process-scoped fallback ID for local use.
- TCP coordinator registration uses the participant's declared peer ID from its heartbeat or first message, not the server-side socket address. Active duplicate TCP peer IDs are rejected without replacing the existing participant.
- Current RabbitMQ participant routes are keyed by peer ID; duplicate active participants with the same ID are an invalid deployment configuration because the broker cannot disambiguate ownership of the shared peer route.
- When SQLite persistence is available, the coordinator records durable last-known peer metadata for peer ID, runtime type, transport, capabilities, heartbeat/disconnect times, status, and scheduling metric snapshots. Live sockets, connection handles, broker consumers, and channels are not persisted.
- Submitter paths use `ClientJobPlugin.buildPayloads(...)` for local input handling. Conversion submitters inline Base64 by default and can use local-file payload references when `TASKFLOW_PAYLOAD_STORAGE_DIR` is configured.
- Successful final `JOB_RESULT` payloads are handled by the matching `ClientJobPlugin.handleResult(...)` in the JavaFX GUI and the RabbitMQ command-line submitter. The default handler calls `saveResults(...)` for list-based file-result plugins.
- The JavaFX GUI is the supported participant UI for TCP requester/executor behavior, and it has service-level RabbitMQ support for live submit, execute, result routing, and save flows.
- The RabbitMQ command-line `submit` path is the supported headless submit-and-save flow today.
- The legacy TCP command-line participant can execute assigned work and has a low-level signed submit helper, but it does not provide a supported final-result saving workflow.
- `docs/PEER_LIFECYCLE.md` records the current lifecycle, evidence, and shared-service candidates.
- `docs/PEER_IDENTITY.md` records the peer identity contract, sanitization, duplicate-ID behavior, and generated fallback limits.

## Runtime Direction

- RabbitMQ is the default runtime for the coordinator, command-line participants, and JavaFX GUI participants when `TASKFLOW_TRANSPORT` is unset or blank.
- TCP is deprecated as the legacy local compatibility/demo path and remains available only through explicit `TASKFLOW_TRANSPORT=tcp` until a later removal slice.
- RabbitMQ live broker tests remain opt-in for local runs, and RabbitMQ is still transitional until its remaining documented support-promotion gates are implemented and tested. GitHub Actions runs a dedicated RabbitMQ integration job for the focused live broker gates.
- `docs/RUNTIME_STRATEGY.md` records the support-promotion, TCP-deprecation, and TCP-removal gates.

## RabbitMQ Publication

- RabbitMQ transport channels enable publisher-confirm mode during startup.
- `publish` and `publishToPeer` return success only after RabbitMQ confirms the publish within `TASKFLOW_RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS`.
- A broker nack or publisher-confirm timeout is reported as a failed publish.
- Peer-targeted publishes also use RabbitMQ mandatory-return detection; an unroutable peer-targeted publish is reported as failed.
- With SQLite persistence available, SQLite owns RabbitMQ assignment creation: it conditionally reads a `PENDING` task, advances the persisted generation, creates the assignment UUID, and stores task state, attempt audit, and the exact serialized `TASK_ASSIGN` envelope in one transaction. Final `JOB_RESULT` publications are stored transactionally with the corresponding terminal job-state update.
- Pending outbox rows are replayed when the RabbitMQ coordinator starts and retried periodically while it runs. Confirmed publishes mark the outbox row sent; unconfirmed or unroutable peer-targeted publishes keep the row pending with a failed-attempt record.
- Replay can duplicate a task assignment or final job result if the coordinator crashes after RabbitMQ accepts a publish but before SQLite records the outbox row as sent. Scheduler task-result acceptance is fenced by the complete persisted assignment identity, and terminal job completion is persisted once.
- Participant-side `TASK_RESULT` publishes are not stored in the coordinator outbox. RabbitMQ executor roles defer acknowledgement of `TASK_ASSIGN` until the corresponding `TASK_RESULT` publish is confirmed, so publish failure requeues the original assignment.
- RabbitMQ remains a transitional adapter; `docs/RABBITMQ_SCOPE.md` records the gates required before calling it the primary supported runtime.

## RabbitMQ Connection Recovery

- RabbitMQ client automatic connection recovery is enabled for transport connections.
- Opt-in live transport coverage verifies that an existing transport can consume and publish again after the broker closes its connection through the RabbitMQ management API.
- SQLite-backed coordinator outbox replay covers coordinator-originated `TASK_ASSIGN` and final `JOB_RESULT` messages that were queued before a coordinator crash. Live broker coverage verifies seeded pending outbox rows, replay after a simulated publish-before-sent-marking crash window, and duplicate task-result rejection after replayed task assignments.
- This does not guarantee full broker outage recovery, participant-side durable `TASK_RESULT` replay, or redelivery of messages outside the coordinator outbox.

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

- The JavaFX GUI uses RabbitMQ by default and selects deprecated TCP only when `TASKFLOW_TRANSPORT=tcp` is set before launch.
- GUI job submission, live result reception, requester-token persistence, and GUI task execution are implemented behind `JobSubmissionClient`, `CoordinatorConnection`, result-routing, and the executor-specific `GuiWorkerRuntime` compatibility boundary for both TCP and RabbitMQ.
- RabbitMQ GUI task-assignment acknowledgements are deferred until the GUI participant publishes the corresponding `TASK_RESULT`; broker publish failure requeues the assignment.
- RabbitMQ GUI `JOB_RESULT` delivery is routed through the existing active-job router and plugin-backed save flow, but `JOB_RESULT_REQUEST` over RabbitMQ is not implemented because there is no broker route for that request yet.
- JavaFX RabbitMQ coverage includes service-level tests plus an automated desktop smoke helper for the text-analysis submit/execute/result/save path. Broader CI-grade JavaFX desktop automation remains deferred in `docs/GUI_AUTOMATION_SCOPE.md`.

## Scheduler Ingress and Backpressure

- Scheduler ingress uses a bounded mailbox controlled by `inboundQueueCapacity` / `TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY`, default `1000`.
- TCP `PeerHandler` compatibility components wait for bounded scheduler-mailbox capacity before admitting `JOB_SUBMIT` and `TASK_RESULT` messages, applying socket-level backpressure instead of dropping those messages.
- RabbitMQ job submissions and task results are requeued when the scheduler mailbox is full instead of being accepted into process memory.
- RabbitMQ transport channels apply `TASKFLOW_RABBITMQ_PREFETCH` with `basicQos`.
- Broker deliveries use manual acknowledgement.
- Deferred acknowledgements keep deliveries unacknowledged until the scheduler or participant explicitly settles them.
- Live broker coverage verifies `prefetch=1` prevents a second shared-route delivery while the first delivery remains unacknowledged.
- Adaptive backpressure across broker queue depth, executor capacity, and external autoscaling remains future work. `docs/BACKPRESSURE_SCOPE.md` records the current backpressure boundaries and the evidence required before adding adaptive throttling.

## Authoritative Successful-Result Commit

- **Preconditions:** the coordinator has validated a version-2 `TASK_RESULT`,
  found its active job/task, and not expired the task's current lease. The
  successful commit itself still depends exclusively on the SQLite predicate.
- **Rows examined:** the conditional `UPDATE` examines the task row. When it
  changes zero rows, a read of that task's status and persisted assignment
  tuple distinguishes unknown, duplicate-completed, and stale outcomes.
- **Rows written:** one qualifying `tasks` row receives `COMPLETED`, result
  payload, completion time, duration, and cleared lease fields. The exact
  matching `task_attempts` row moves from `RUNNING` to `SUCCEEDED` in the same
  transaction. No result-commit outbox row is written.
- **Conditional predicate:** `task_id`, `status='ASSIGNED'`, `attempt_number`,
  `assignment_id`, and `assigned_peer_id` must all match. The attempt-audit
  update repeats task ID, attempt number, assignment ID, participant ID, and
  `RUNNING` outcome checks.
- **Commit point:** `COMMITTED` is returned only after both row updates and the
  SQLite transaction commit. Only then does the scheduler update the task/job
  projection, participant capacity/success state, and success metrics.
- **Zero-row behavior:** an exact tuple already in `COMPLETED` is
  `DUPLICATE_ALREADY_COMPLETED`; any existing nonmatching or non-assigned task
  is `STALE_ASSIGNMENT`; a missing row is `UNKNOWN_TASK`. These outcomes do not
  mutate task/job/lease/success state and broker deliveries are acknowledged.
- **Failure and crash behavior:** SQL, serialization, or exact audit-update
  failure rolls back and returns `STORAGE_FAILURE`; the scheduler leaves memory
  assigned and requeues a broker delivery. A crash before commit leaves both
  rows unchanged. A crash after commit leaves the completed task/result
  recoverable from SQLite even if memory was not projected; complete
  last-task/job-finalization crash proof remains tracked separately.
- **Same-participant reassignment evidence:**
  `AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit` and
  `RabbitMqCoordinatorLiveIntegrationTest#sameWorkerAbaResultCannotCommitThroughLiveBroker`
  drive attempt 1 / assignment X through failure, reassign the same participant
  with attempt 2 / assignment Y, acknowledge X as stale without mutation, and
  prove that only Y commits.

## Task State Machine

The complete [task and job state machine](STATE_MACHINE.md) assigns a stable ID
to every current lifecycle mutation and records its trigger, preconditions,
durable writes, outbox effects, in-memory projection, replay behavior,
observability, and forbidden edges. Normal task execution uses
`PENDING -> ASSIGNED`, `ASSIGNED -> COMPLETED`, `ASSIGNED -> PENDING`, or
`ASSIGNED -> FAILED`. Job-wide terminalization also has an explicit
`PENDING -> FAILED` administrative path; recovery hydration and pending
normalization are distinguished from new execution transitions.

## Retry and Timeout Policy

- **Timeout per assigned task:** 60 seconds.
- **Lease per assigned task:** 120 seconds.
- **Maximum retries per task:** 20 attempts.
- During active scheduler operation, on timeout, lease expiry, or explicit executor failure:
  - the attempt is counted as failed,
  - the task is retried if attempts remain,
  - otherwise the task moves to terminal `FAILED`.
- When a retry is scheduled, the persisted task row is returned to `PENDING`, its previous assignment ID/timing/lease fields are cleared, its monotonic `attempt_number` is retained, and `retry_count` is incremented.
- During startup recovery, an expired lease or incomplete legacy assignment identity is released to `PENDING` without incrementing `retry_count`; the last known generation is retained from task state or the legacy attempt audit, and the next runtime assignment advances it.

## Capability-Aware Executor Assignment

- Executor-enabled participants advertise supported task types in heartbeat metadata.
- The scheduler assigns a task only to executor participants that advertise support for that task type.
- If no capable executor participant is available, the task remains pending instead of being assigned to an incompatible participant.

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
- Schema version 10 stores each task's latest assignment generation and current assignment UUID, and adds assignment UUID plus lease deadline to every new task-attempt audit row. Its migration from version 9 is transactional; legacy rows receive generation zero and no assignment UUID.
- Coordinator startup rebuilds resumable `RUNNING` jobs from persisted snapshots, restores completed task results when result payloads were persisted, preserves assigned tasks only when their complete persisted identity has an unexpired lease, releases expired assignments with a `lease_expired` attempt reason, and releases incomplete legacy assignments with an inspectable restart reason.
- Legacy or otherwise non-resumable `RUNNING` jobs are marked `FAILED` on startup.
- If startup recovery cannot safely reconcile persisted state, the coordinator closes that state store, disables persistence for the run, and logs `database_disabled` instead of writing against unreconciled history.
- After startup, non-outbox assignment must be persisted before dispatching work to an executor participant. For RabbitMQ outbox assignment, the scheduler installs the identity returned by the committed SQLite transaction and publishes its returned outbox row; repeated or concurrent creation calls for a no-longer-`PENDING` task create neither a new generation nor a second outbox row.
- If retry or task-failure persistence fails after the current legacy in-memory-first failure path changes state, the scheduler fails the job with a terminal `JOB_RESULT` and attempts to persist terminal task/job state. Successful completion is different: SQLite decides first, storage failure leaves the in-memory task assigned, and broker ingress requeues the result.
- For non-outbox outputs, final job-status persistence happens after final result delivery. For RabbitMQ with SQLite persistence, terminal job status and the final `JOB_RESULT` outbox row are committed together before immediate publish/replay. If a non-outbox terminal write fails after delivery, the scheduler removes the job from active memory and logs `job_terminal_persistence_degraded` with the failed operation and policy.
- `JOB_RESULT_REQUEST` can resend an in-memory pending terminal result or reconstruct a completed persisted `JOB_RESULT` when the requester token matches, any required requester identity signature is valid, and every task result snapshot exists. Reconstructed completed results include the schema-v6 semantic final payload when it was persisted, plus the compatibility ordered task-result list.
- Failed jobs and completed jobs with missing result snapshots are not reconstructed as successful persisted results.
- PostgreSQL/Flyway is not implemented. `docs/RECOVERY_SCOPE.md` records the SQLite lease behavior and keeps PostgreSQL/Flyway deferred until there is a concrete external database requirement.

## Heartbeat and Participant Liveness

- Coordinator sends periodic `PING` and expects `PONG`.
- TCP participants answer `PING` with their explicit peer ID and supported task types.
- Missing heartbeats beyond timeout mark the participant stale, remove it from the
  live registry, and persist a disconnected last-known participant status when the
  peer registry store is available.

## Core Scheduler Metrics

Scheduler emits structured event logs and periodic metrics snapshots including:

- `queue_depth`
- `active_jobs`
- `dispatch_latency_ms` (average from task becoming pending to assignment)
- `retry_count`
- `task_success_rate` (successful attempts / total attempts)
- `failure_count`
- `taskflow_task_results_committed_total`
- `taskflow_task_results_stale_total`
- `taskflow_task_results_duplicate_total`
- `taskflow_assignment_generations_total`

The four `taskflow_*_total` counters distinguish authoritative result commits,
obsolete-generation rejection, duplicate-completion suppression, and assignment
generation creation. The matching structured events are
`task_result_committed`, `task_result_stale_rejected`,
`task_result_duplicate_ignored`, and `task_assignment_created`. Each event
carries `job_id`, `task_id`, `attempt_number`, `assignment_id`, and `worker_id`,
the log-field spellings of the protocol correlation tuple. These metrics are
intended for immediate log-based operational visibility in Phase 1 and as
migration inputs to a dedicated metrics backend in later phases; no exporter or
high-cardinality metric exemplar is implemented.

`docs/OBSERVABILITY_SCOPE.md` maps the current structured-log events and records that a dedicated metrics backend is deferred until promoted lease/attempt-history dashboards, RabbitMQ outbox visibility, or promoted DLQ workflow metrics need operational visibility.
