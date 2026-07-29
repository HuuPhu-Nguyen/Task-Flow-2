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
  the republish is confirmed; transient failure schedules bounded broker retry.
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

## Plugin Retry Safety

- Every `PeerProcessorPlugin` and its paired coordinator-side `TaskPlugin`
  declare the same non-null `RetrySafety` value. The server declaration is the
  coordinator-visible admission authority; cross-role contract tests keep it
  aligned with the executor declaration.
- `PURE` and `IDEMPOTENT` allow the configured task retry policy.
  `REQUIRES_IDEMPOTENCY_KEY` also allows retries, but the plugin must document
  and supply the appropriate execution identity to its external system.
- `TaskAssignMessage.taskId` is stable across logical retry generations.
  `assignmentId` is stable across redelivery of one assignment generation and
  changes for a new generation. The former is normally the external key for a
  once-per-logical-task effect; the latter only deduplicates one generation.
- `UNSAFE_TO_RETRY` is rejected before plugin job construction, task creation,
  persistence, active-job projection, or dispatch when `maxTaskRetries > 0`.
  Current scheduler configuration requires a positive value, so an unsafe
  processor cannot currently accept a new job. Exact replay of a previously
  accepted job is still classified before this new-submission check.
- The example, text, image-conversion, and video-transcoding plugins declare
  `PURE`. Their framework-managed result creation/staging is not a separate
  plugin-owned external business effect.
- A declaration is an enforceable admission contract, not a distributed
  transaction. Coordinator fencing does not undo plugin work or make arbitrary
  external side effects exactly once.

## Job Submission Validation

- GUI and command-line submitters generate peer-scoped job IDs with the
  sanitized submitting peer ID, a timestamp, and a full UUID suffix.
- Protocol parsing validates required framework fields, safe peer/job/task
  identifiers, safe task-type names, task-count limits, inline job-payload
  size, and result-payload size through the shared `MessageValidator`.
- A submitted `jobId` is an idempotency key scoped to the requester owner tuple:
  the per-job token hash plus the verified public key when signed identity is
  present. The transport route/node ID is not the durable owner, but the outer
  route must match the submitted message node ID so a replay response cannot be
  redirected by changing only the envelope.
- SQLite atomically persists a versioned canonical request hash with the job
  and complete initial task set. The hash covers the owner tuple, normalized
  task type, null-to-empty parameter, and ordered per-payload canonical JSON
  digests; it excludes `jobId`, time, route, and signature.
- Same owner + same `jobId` + same hash creates no tasks and returns the current
  running status or persisted terminal result through the existing
  `JOB_RESULT` response path. A same-owner hash mismatch is an idempotency
  conflict. A different owner is rejected before request-hash comparison.
- Pre-schema-v12 rows have a blank request hash and are rejected as
  unverifiable legacy collisions. Task snapshots are not used to guess the
  original submission because plugins may transform submitted payloads while
  creating tasks.
- Coordinator-side `TaskPlugin` implementations validate submitted parameters and payload shapes during job startup.
- Built-in server plugins reject missing or unsupported task options, empty payload lists, malformed payload objects, unsupported conversion file extensions, invalid/oversized Base64 file data, malformed/oversized object references, and the removed local-filesystem reference shape.
- Invalid submissions return a failed terminal `JOB_RESULT` before scheduler startup persists tasks or assigns executor work when the requester can be routed. Invalid non-submit broker deliveries are rejected instead of requeued indefinitely.
- For an otherwise current successful `TASK_RESULT`, the server job parses and
  validates the plugin-owned result before `commitTaskResult(...)`. Built-in
  example/text results reject missing names and invalid counts; conversion
  results additionally require the requested extension, exactly one non-empty
  bounded inline body or portable object reference, and valid reference
  bounds. `IllegalArgumentException` remains the shared invalid-delivery
  classification, so malformed result data cannot become an authoritative
  task snapshot.

## Participant Requester and Result Handling

- Participants may combine requester and executor roles depending on their runtime profile; receiving and handling results belongs to the requester role.
- RabbitMQ command-line and JavaFX participants use explicit sanitized peer IDs as compatibility identifiers. `TASKFLOW_PEER_ID` provides a stable configured ID; otherwise the runtime generates a unique process-scoped fallback ID for local use.
- Current RabbitMQ participant routes are keyed by peer ID; duplicate active participants with the same ID are an invalid deployment configuration because the broker cannot disambiguate ownership of the shared peer route.
- When SQLite persistence is available, the coordinator records durable last-known peer metadata for peer ID, runtime type, transport, capabilities, heartbeat/disconnect times, status, and scheduling metric snapshots. Broker consumers, connections, and channels are not persisted.
- Submitter paths use `ClientJobPlugin.buildPayloads(...)` for local input
  handling. Conversion submitters inline only files smaller than
  `TASKFLOW_MAX_INLINE_PAYLOAD_BYTES`; at or above the exclusive limit they
  upload to object storage under an immutable `taskflow/inputs/<uuid>` key and
  submit `ObjectReference` metadata. Executors download by key through their
  own configured provider. No distributed payload contains a local filesystem
  location.
- Upload adapters verify the streamed bytes against the reference before
  returning success. Executor input readers and requester result readers verify
  exact length and SHA-256 before processing or file output. A mismatch becomes
  `PERMANENT_PAYLOAD_INTEGRITY`; after the exact assigned-attempt failure
  transaction commits, no logical task retry is created.
- Conversion outputs below the exclusive inline threshold remain Base64.
  Outputs at or above it are conditionally created at the exact immutable
  job/task/attempt/assignment key. `TASK_RESULT` carries that reference with
  the same assignment identity. Upload alone is non-authoritative:
  `tasks.result_payload_json` becomes the sole result pointer only when the
  exact SQLite task-result transaction commits. A later or mis-keyed attempt
  cannot replace it, and recovery reloads the committed reference.
- Successful final `JOB_RESULT` payloads are handled by the matching `ClientJobPlugin.handleResult(...)` in the JavaFX GUI and the RabbitMQ command-line submitter. The default handler calls `saveResults(...)` for list-based file-result plugins.
- The JavaFX GUI is the supported participant UI and has RabbitMQ service-level support for live submit, execute, result routing, and save flows.
- The RabbitMQ command-line `submit` path is the supported headless submit-and-save flow today.
- `docs/PEER_LIFECYCLE.md` records the current lifecycle, evidence, and shared-service candidates.
- `docs/PEER_IDENTITY.md` records the peer identity contract, sanitization, duplicate-ID behavior, and generated fallback limits.

## Runtime Direction

- RabbitMQ is the sole supported runtime transport for the coordinator, command-line participants, and JavaFX GUI participants. Entry points start it directly and do not read a transport selector.
- RabbitMQ live broker tests remain opt-in for local runs, and RabbitMQ is still transitional until its remaining documented support-promotion gates are implemented and tested. GitHub Actions runs focused live broker contracts in push-integration and managed broker/process faults in scheduled chaos; [`CI_EVIDENCE_TIERS.md`](CI_EVIDENCE_TIERS.md) records the exact split.
- `docs/RUNTIME_STRATEGY.md` records the sole-transport decision, migration boundary, and remaining production-readiness gates.

## RabbitMQ Publication

- RabbitMQ transport channels enable publisher-confirm mode during startup.
- Application envelopes use RabbitMQ persistent delivery mode
  (`deliveryMode=2`) even when a test or operator selects non-durable topology.
  `TASKFLOW_RABBITMQ_DURABLE` controls applicable shared, retry, dead-letter,
  and quarantine resources. Peer-specific queues remain non-durable,
  exclusive, and auto-delete by design.
- `publish` and `publishToPeer` return success only after RabbitMQ confirms the publish within `TASKFLOW_RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS`.
- A broker nack or publisher-confirm timeout is reported as a failed publish.
- Peer-targeted publishes also use RabbitMQ mandatory-return detection; an unroutable peer-targeted publish is reported as failed.
- Coordinator outbox publication requires a nonblank peer route and therefore
  always uses the mandatory peer-targeted path; it cannot fall back to an
  unroutable shared publish that lacks return detection.
- With SQLite persistence available, SQLite owns the conditional RabbitMQ assignment transaction: it reads a `PENDING` task, advances the persisted generation, validates the scheduler-supplied assignment UUID candidate, and stores task state, attempt audit, and the exact serialized `TASK_ASSIGN` envelope in one transaction. The last committed task result stores a replayable `FINALIZING` intent; final `JOB_RESULT` publications are then stored transactionally with the corresponding terminal job-state update.
- Pending outbox rows are replayed when the RabbitMQ coordinator starts and
  retried periodically while it runs. The order is persistent broker write,
  publisher confirmation, mandatory-return routing determination, then the
  conditional SQLite sent mark. Only success at all four boundaries removes the
  row from the pending set.
- A connection exception, broker nack, confirm timeout, mandatory return, or
  failed SQLite sent mark leaves the exact row pending. Failed/unconfirmed
  broker attempts record attempt metadata; a sent-mark failure is logged and
  deliberately leaves the pre-existing pending row unchanged.
- Replay can duplicate a task assignment or final job result if RabbitMQ
  accepts a publish but SQLite does not record the sent mark. Scheduler
  task-result acceptance is fenced by the complete persisted assignment
  identity, and terminal job completion is persisted once.
- Focused adapter evidence covers
  [persistent delivery mode and durable-versus-ephemeral topology](../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqTransportPublishConfirmTest.java),
  while the reusable
  [broker contract](../taskflow-spi/src/test/java/transport/BrokerTransportContractTest.java)
  runs unchanged through the
  [RabbitMQ/Testcontainers binding](../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqBrokerContractTest.java)
  for confirms, mandatory returns, manual settlement, redelivery, bounded
  retry/quarantine, duplicates, reconnect, and durable restart behavior,
  and the
  [coordinator live suite](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java)
  covers confirmed success, connection loss, mandatory return, failed sent
  marking, and identical duplicate replay against RabbitMQ and SQLite.
- Participant-side `TASK_RESULT` publishes are not stored in the coordinator outbox. RabbitMQ executor roles defer acknowledgement of `TASK_ASSIGN` until the corresponding `TASK_RESULT` publish is confirmed, so a transient publish failure schedules the assignment through the bounded broker retry topology.
- RabbitMQ remains transitional; `docs/RABBITMQ_SCOPE.md` records the gates required before calling it production-ready.

## RabbitMQ Delivery Disposition

- Every supported consumer settles through exactly one of `ACK_SUCCESS`,
  `ACK_DUPLICATE_OR_STALE`, `RETRY_TRANSIENT`, `REJECT_INVALID`, or
  `QUARANTINE_POISON`.
- Successful work is acknowledged. Duplicate, stale, or wrong-current-owner
  domain events are also acknowledged without changing authoritative state.
- Malformed, unsupported-version, unknown-type, or otherwise invalid protocol
  deliveries are rejected without requeue.
- Explicit transient infrastructure failures and bounded-mailbox pressure retry;
  otherwise-unclassified deterministic processing failures are poison and do
  not enter the generic requeue path.
- `RETRY_TRANSIENT` and `QUARANTINE_POISON` use explicit TTL retry queues with a
  configured attempt bound. Retry publications preserve original route and
  classified reason metadata, and exhaustion publishes once to final
  quarantine. `REJECT_INVALID` remains immediate reject-without-requeue into
  the configured ordinary dead-letter workflow.
- The finite retry/quarantine guarantee assumes that the original routing
  binding still exists when each TTL expires. A peer-specific auto-delete queue
  can disappear while its delivery waits; RabbitMQ dead-letter forwarding is
  not mandatory, so that return can be unroutable. Automatic topology recovery
  restores a continuously connected participant endpoint after a broker
  restart, but it cannot restore a route while that participant is offline.

## Coordinator Consumer Acknowledgement and Shutdown

- `JOB_SUBMIT` and `TASK_RESULT` callbacks defer the RabbitMQ delivery before
  attempting bounded scheduler-mailbox admission. Admission transfers only a
  process-local reference; it does not acknowledge the broker delivery.
- Scheduler processing chooses the typed disposition only after validation and
  the applicable durable store transition. A matching result is acknowledged
  after its SQLite commit and projection. Duplicate/stale results are
  acknowledged without mutation. Storage failure receives bounded transient
  retry instead of an acknowledgement.
- Closing the coordinator connection before settlement leaves RabbitMQ as the
  delivery owner. Live evidence observes the broker's redelivery flag both
  before any acknowledgement and after SQLite committed a result but before
  the acknowledgement frame. The post-commit delivery is classified
  `DUPLICATE_ALREADY_COMPLETED`, acknowledged, and leaves one succeeded attempt
  and one authoritative result.
- Graceful shutdown first closes the synchronized ingress gate and cancels
  consumers. It then stops the peer monitor and outbox replayer, requests a
  bounded scheduler drain, closes the RabbitMQ transport, and closes SQLite
  only after every database-using background component has stopped. Work
  admitted before the gate closed drains to a disposition. A callback that
  reaches the closed gate is deferred but deliberately unsettled, so transport
  close returns it to RabbitMQ.
- The default drain bound is 10 seconds. Timeout interrupts the scheduler and
  closes the broker channel so unacknowledged deliveries are recoverable. If
  the scheduler, an auxiliary database user, or the RabbitMQ transport does not
  stop, SQLite close is deliberately deferred to process exit rather than
  racing a live callback. The initial outbox replay also runs on the replayer
  executor, so the same bounded stop owns startup-time database work.
- The acknowledgement-window tests use intentional healthy connection/channel
  close. A separate managed Testcontainers/Toxiproxy test stops the RabbitMQ
  process, preserves durable coordinator state while it is offline, and proves
  topology, consumer, outbox, fencing, and active-job recovery after restart.

## RabbitMQ Connection Recovery

- Coordinator and command-line participant startup uses one connection owner
  that makes one attempt at a time. A TCP/AMQP attempt is bounded by
  `TASKFLOW_RABBITMQ_CONNECTION_TIMEOUT_MS` (`5000` ms by default). Transient
  failures retry with capped exponential delay configured by
  `TASKFLOW_RABBITMQ_RECOVERY_INITIAL_DELAY_MS`,
  `TASKFLOW_RABBITMQ_RECOVERY_MAX_DELAY_MS`, and
  `TASKFLOW_RABBITMQ_RECOVERY_BACKOFF_MULTIPLIER` (defaults `1000`, `30000`,
  and `2.0`). Invalid credentials, incompatible protocol, and invalid
  configuration fail; shutdown interrupts backoff.
- A coordinator process continues initial retry, but its liveness and readiness
  routes report `503/STARTING` until the scheduler loop exists. Its
  `coordinator_started` event occurs only after broker connection, topology
  declaration, consumer construction, and scheduler startup. Connection
  attempts and state changes emit
  `rabbitmq_initial_connection_retry_scheduled`,
  `rabbitmq_initial_connection_ready`, and
  `rabbitmq_initial_connection_stopped`.
- After first connection, RabbitMQ automatic connection and topology recovery
  uses the same capped delay policy. Recovery emits
  `rabbitmq_connection_interrupted`,
  `rabbitmq_connection_recovery_retry_scheduled`,
  `rabbitmq_connection_recovery_started`,
  `rabbitmq_topology_recovery_started`, and
  `rabbitmq_connection_recovery_completed`.
- SQLite-backed coordinator outbox rows remain pending when a broker publish
  fails. Periodic replay resumes after connection/topology recovery and uses
  the exact committed assignment identity; it does not create a recovery-only
  assignment generation.
- [`RabbitMqBrokerRecoveryIntegrationTest#unavailableStartupAndActiveBrokerRestartRecoverOutboxConsumersAndFencing`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqBrokerRecoveryIntegrationTest.java)
  uses managed RabbitMQ plus a stable Toxiproxy endpoint. It proves unavailable
  startup retry; a real broker stop during active work; offline durable
  replacement assignment/outbox state; restored shared and peer consumers;
  exact outbox replay; stale pre-outage result rejection; and eventual valid
  completion after the participant connection recovers.
- This is a single-broker, single-coordinator recovery guarantee. It does not
  provide RabbitMQ-cluster failover, zero downtime, multi-coordinator
  authority, participant-side durable `TASK_RESULT` replay, or exactly-once
  delivery/execution.

## RabbitMQ Dead Lettering

- RabbitMQ topology declaration configures an ordinary dead-letter workflow,
  explicit per-delay retry queues, and a final quarantine queue.
- Malformed or validation-failing broker deliveries use `REJECT_INVALID`, while
  deterministic handler failures use `QUARANTINE_POISON`. Invalid deliveries
  reject without requeue; deterministic poison retries only through the bounded
  TTL schedule.
- Rejected deliveries are routed by RabbitMQ to the configured dead-letter queue when dead-lettering is enabled.
- `peer.PeerNode dlq inspect` reads dead-letter metadata and body previews without acknowledging the DLQ entry.
- `peer.PeerNode dlq redrive` republishes only valid TaskFlow broker envelopes to the original routing key captured in RabbitMQ dead-letter metadata, increments `x-taskflow-redrive-count`, and acknowledges the DLQ entry only after RabbitMQ confirms the publish.
- Redrive refuses malformed or unknown-route poison messages and leaves them in the DLQ for an explicit quarantine or discard decision.
- `peer.PeerNode dlq quarantine` republishes a DLQ entry to the configured quarantine queue and acknowledges the original DLQ entry only after publish confirmation.
- `peer.PeerNode dlq discard` acknowledges and removes the DLQ entry without republishing it.
- `peer.PeerNode dlq inspect-quarantine` non-destructively exposes automatic
  quarantine entries, including delivery attempt and failure metadata.
- `peer.PeerNode dlq redrive-quarantine` republishes a valid quarantined
  envelope to its original route with a fresh bounded attempt budget and
  acknowledges the quarantine entry only after publish confirmation.

## JavaFX GUI Transport Scope

- The JavaFX GUI uses RabbitMQ as its only runtime transport.
- GUI job submission, live result reception, requester-token persistence, and GUI task execution are implemented behind `JobSubmissionClient`, `CoordinatorConnection`, result-routing, and the executor-specific `GuiWorkerRuntime` boundary.
- RabbitMQ GUI task-assignment acknowledgements are deferred until the GUI participant publishes the corresponding `TASK_RESULT`; a transient broker publish failure schedules the assignment through bounded delayed retry.
- RabbitMQ GUI `JOB_RESULT` delivery is routed through the existing active-job router and plugin-backed save flow, but `JOB_RESULT_REQUEST` over RabbitMQ is not implemented because there is no broker route for that request yet.
- JavaFX RabbitMQ coverage includes service-level tests plus an automated desktop smoke helper for the text-analysis submit/execute/result/save path. Broader CI-grade JavaFX desktop automation remains deferred in `docs/GUI_AUTOMATION_SCOPE.md`.

## Scheduler Ingress and Backpressure

- Scheduler ingress uses a bounded ordinary submission/control lane controlled
  by `inboundQueueCapacity` /
  `TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY`, default `1000`, plus one fixed
  task-result reserve. Total envelope capacity is therefore the configured
  capacity plus one.
- New-job admission allows resulting active jobs up to
  `maxActiveJobs` / `TASKFLOW_MAX_ACTIVE_JOBS`, default `1000`, and resulting
  retained active tasks up to `maxActiveTasks` /
  `TASKFLOW_MAX_ACTIVE_TASKS`, default `100000`.
- `TASKFLOW_MAX_TASKS_PER_JOB`, default `256`, is enforced on the submitted
  payload count and again on the plugin-produced task count.
  `TASKFLOW_MAX_JOB_PAYLOAD_BYTES`, default `67108864`, measures UTF-8 JSON
  bytes of the task payloads plus parameter. `TASKFLOW_MAX_INPUT_BYTES`,
  default `33554432`, is enforced per recursively discovered
  `ObjectReference.contentLength`. `TASKFLOW_MAX_INLINE_PAYLOAD_BYTES`, default
  `8388608`, is an exclusive raw-byte ceiling for recursively discovered
  `base64Data` in submissions, assignments, task results, and final results;
  `0` disables conversion-file inlining.
- A SQLite-backed coordinator rejects new work when the current unpublished
  broker-outbox count is at least `maxPendingOutboxRows` /
  `TASKFLOW_MAX_PENDING_OUTBOX_ROWS`, default `100000`. The count uses a SQL
  aggregate; read failure is storage failure, not zero.
- Submission validation and canonical request hashing precede capacity
  admission. Exact replay bypasses the capacity checks and creates no tasks.
  New work is checked before plugin construction when already impossible, then
  checked again against the plugin-produced task count before J0/T0.
- Limit rejection writes no job, task, attempt, lease, or outbox row and
  returns unsuccessful protocol-v2 `JOB_RESULT` with typed
  `admissionRejection`. The broker delivery is acknowledged only after that
  response is routed. Recovered accepted work is retained above a newly
  lowered bound and continues under the existing scheduling assumptions; only
  new work waits for cleanup.
- Each cycle processes at most the configured message, combined deadline,
  dispatch, and terminal/outbox budgets, in that order. The YAML fields are
  `schedulerMessageBatchSize`, `schedulerDeadlineBatchSize`,
  `schedulerDispatchBatchSize`, and `schedulerOutboxBatchSize`; every default
  is `100`.
- Cross-job dispatch uses a persistent round-robin pass whose cursor survives
  scheduler-cycle boundaries. `schedulerMaxAssignmentsPerJobPerRound` /
  `TASKFLOW_SCHEDULER_MAX_ASSIGNMENTS_PER_JOB_PER_ROUND` limits successful
  assignments per job turn, defaults to `1`, and cannot exceed the dispatch
  batch.
- Retry tasks lead the pending lanes only inside their own job. With one
  10,000-task job followed by ten one-task jobs and enough compatible capacity,
  all ten small jobs receive an assignment by the end of the first complete
  round, after exactly 11 dispatch units at the default quota.
- A stale timer pop still consumes one deadline unit. A no-capacity runnable-job
  probe still consumes one dispatch unit. This prevents unsuccessful discovery
  from escaping the cycle bound.
- A no-capacity probe removes that job from the runnable rotation and records it
  in a separate capacity-wait set. A later capacity-availability signal or the
  deterministic 500 ms recheck makes the prior waiting generation eligible;
  pending-task indexing alone cannot bypass the wait state. Capability-changing
  heartbeats wake an idle scheduler even though the registry update occurs
  outside its mailbox.
- Continuous messages cannot starve deadlines because the message stage ends at
  its limit. Continuous deadlines cannot starve task results because mailbox
  messages are always processed first. Within that stage, one queued task result
  is selected before ordinary FIFO work; it still consumes one message unit.
- When no stage leaves immediate work, the scheduler blocks until a mailbox
  message or the earliest assignment, terminal-retry, no-capacity-recheck, or
  metrics due time. Shutdown interrupts that wait before draining admitted
  envelopes.
- The independent SQLite broker-outbox replayer retains durable replay
  ownership and loads at most `schedulerOutboxBatchSize` rows per pass.
- RabbitMQ job submissions and task results receive bounded delayed retry when
  their bounded mailbox lane is full instead of being accepted into process
  memory.
- RabbitMQ transport channels normally apply `TASKFLOW_RABBITMQ_PREFETCH` with
  `basicQos`. Coordinator `JOB_SUBMIT` uses a dedicated channel with fixed
  route-local prefetch `1`; result and heartbeat intake retain the configured
  value.
- Broker deliveries use manual acknowledgement.
- Deferred acknowledgements keep deliveries unacknowledged until the scheduler or participant explicitly settles them.
- Live broker coverage verifies `prefetch=1` prevents a second shared-route delivery while the first delivery remains unacknowledged.
- Scheduler-mailbox admission and shutdown share one ingress gate: a full
  mailbox enters the bounded broker retry topology, while stopped intake leaves
  the delivery unsettled for channel-close redelivery. Neither path allocates
  an overflow queue.
- Intake remains continuously subscribed, so exact replay is still classified
  first, new work at a dynamic limit receives typed pre-J0/T0 rejection, and
  cleanup permits eligible new work without restart. Dynamic participant
  throttling, broker-management polling, and external autoscaling remain out
  of scope; `docs/BACKPRESSURE_SCOPE.md` records that boundary.

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
  transaction. When the expected task set is now complete and every result
  column is present, the parent job also moves from `RUNNING` to `FINALIZING`.
  No result-commit outbox row is written; the durable intermediate state is the
  replay instruction for semantic aggregation.
- **Conditional predicate:** `task_id`, `status='ASSIGNED'`, `attempt_number`,
  `assignment_id`, and `assigned_peer_id` must all match, and the parent job
  must remain `RUNNING`. The attempt-audit update repeats task ID, attempt
  number, assignment ID, participant ID, and `RUNNING` outcome checks. The
  finalization branch additionally checks positive expected cardinality, exact
  task cardinality, `COMPLETED` state, and a present result snapshot for every
  task.
- **Commit point:** `COMMITTED` is returned only after both row updates and the
  SQLite transaction commit. Only then does the scheduler update the task/job
  projection, participant capacity/success state, and success metrics.
- **Zero-row behavior:** an exact tuple already in `COMPLETED` is
  `DUPLICATE_ALREADY_COMPLETED`; any existing nonmatching or non-assigned task
  is `STALE_ASSIGNMENT`; a missing row is `UNKNOWN_TASK`. These outcomes do not
  mutate task/job/lease/success state and broker deliveries are acknowledged.
- **Failure and crash behavior:** SQL, serialization, exact audit-update, or
  finalization-intent failure rolls the transaction back and returns
  `STORAGE_FAILURE`; the scheduler leaves memory assigned and schedules bounded
  broker retry. A crash before commit leaves every row unchanged. A crash after a
  non-last result leaves that task recoverable; a crash after the last result
  leaves `FINALIZING` plus all ordered result snapshots recoverable. Startup
  deterministically rebuilds the plugin job from those inputs and retries J1.
- **Same-participant reassignment evidence:**
  `AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit` and
  `RabbitMqCoordinatorLiveIntegrationTest#sameWorkerAbaResultCannotCommitThroughLiveBroker`
  drive attempt 1 / assignment X through failure, reassign the same participant
  with attempt 2 / assignment Y, acknowledge X as stale without mutation, and
  prove that only Y commits.

## Durable-First Transition Projection

- Assignment creation, failed-attempt retry release, terminal task failure,
  job completion/failure, and final-result outbox creation return a typed
  `COMMITTED`, `ALREADY_APPLIED`, `STALE_STATE`, `UNKNOWN_ENTITY`, or
  `STORAGE_FAILURE` disposition. Successful task results use their equivalent
  specialized commit enum.
- Each path decides its desired transition, attempts the conditional SQLite
  transaction, and only then updates `TaskUnit`, active-job indexes,
  participant capacity, metrics, or outbound delivery. Exact
  `ALREADY_APPLIED` replay may install the matching projection; stale state is
  classified and preserved.
- Failed-attempt writes match task ID, `ASSIGNED`, attempt number, assignment
  UUID, and participant ID and atomically close the exact `RUNNING` audit row.
  A write failure leaves the task assigned and retains participant capacity.
- Job failure atomically terminalizes every remaining task, closes running
  attempts, and changes the job from `RUNNING` to `FAILED`. Projection then
  releases every affected participant slot exactly once.
- Successful finalization is a two-transaction protocol around plugin code:
  the last T2 atomically persists its task/attempt writes and `FINALIZING`, then
  J1 aggregates only the ordered committed task results and atomically stores
  `COMPLETED`, the semantic final payload, and (for RabbitMQ) one exact outbox
  envelope. Crash or duplicate invocation replays this protocol from SQLite.
- Non-outbox J1/J2 commits before final-result delivery. A terminal write
  failure retains the pending active job and suppresses delivery; after commit,
  bounded delivery exhaustion removes only the already-terminal projection.
- RabbitMQ J1/J2 additionally commits the final `JOB_RESULT` outbox row in the
  terminal transaction. Outbox insertion failure rolls back task, attempt, and
  job changes and leaves the active projection available for retry. An exact
  `ALREADY_APPLIED` replay reloads the original durable assignment/final-result
  outbox row instead of inventing a new envelope.
- Restart recovery hydrates `RUNNING` and `FINALIZING` jobs with task status,
  retry count, assignment generation and UUID, participant, lease, and
  committed result payload from SQLite. It does not use pre-crash scheduler
  memory as authority.

## Task State Machine

The complete [task and job state machine](STATE_MACHINE.md) assigns a stable ID
to every current lifecycle mutation and records its trigger, preconditions,
durable writes, outbox effects, in-memory projection, replay behavior,
observability, and forbidden edges. Normal task execution uses
`PENDING -> ASSIGNED`, `ASSIGNED -> COMPLETED`, `ASSIGNED -> PENDING`, or
`ASSIGNED -> FAILED`. Job-wide terminalization also has an explicit
`PENDING -> FAILED` administrative path; recovery hydration and pending
normalization are distinguished from new execution transitions.

The core module also contains a pure executable transition table:
`TaskStateMachine.decide(TaskState, SchedulerEvent)` returns a typed
classification, required durable transition, logical outbound intent,
resulting projection, and observability intents without importing a database,
transport, UI, plugin implementation, clock, random source, or executor. Its
parameterized tests cover every mandatory scheduler event, explicit invalid
edges, stale/duplicate/ignored distinctions, retry exhaustion, and exact event
  replay. Live submission, assignment, successful/failed result, lease-expiry,
  timeout, participant-unavailability, and recovery-hydration paths now consult
  that reducer through `TaskTransitionDecisions`. `AssignmentService`,
  `ResultCommitService`, `LeaseService`, `AttemptService`,
  `JobCompletionService`, and `RecoveryService` own distinct effects;
  `SchedulerLoop` only orders cycle stages. SQLite predicates remain the
  authoritative enforcement boundary whenever durable state is involved.

## Deterministic Transition Inputs

- `TaskFlowClock` supplies both `Instant now()` and epoch-millisecond time for
  coordinator lifecycle decisions, startup recovery, retry/pending timestamps,
  lease and timeout checks, persistence timestamps supplied by the scheduler,
  and scheduler-created protocol timestamps.
- `AssignmentIdGenerator` supplies the UUID candidate for each new assignment.
  SQLite still calculates and conditionally commits the monotonic attempt
  number; RabbitMQ assignment state, attempt audit, and the exact outbox
  envelope remain one transaction.
- Production coordinator entry points share `SystemTaskFlowClock` and
  `UuidAssignmentIdGenerator`. Plugin-created tasks are rebound to those ports
  after task initialization, and recovered jobs receive the same runtime ports
  before hydration.
- Focused tests use mutable/fixed clocks and exact UUID sequences. Evidence is
  [`TaskUnitDeterministicPortsTest#injectedClockAndIdsDriveEveryTaskTransitionExactly`](../taskflow-spi/src/test/java/server/job/TaskUnitDeterministicPortsTest.java),
  [`TaskSchedulerFailureTest#expiredLeaseReassignsTaskAndRejectsLateResultFromOldPeer`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java),
  [`TaskSchedulerPersistenceTest#brokerOutboxAssignmentStaysPendingWhenPublishFails`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java),
  and [`CoordinatorStartupRecoveryTest#resumesRunningJobsWithPersistedPendingPayloads`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java).
- No schema, protocol field, lease calculation, retry limit, or transport
  acknowledgement behavior changes at this boundary. Participant liveness and
  compatibility peer/job-ID generation remain separate infrastructure concerns.

## Retry and Timeout Policy

- **Timeout per assigned task:** 60 seconds.
- **Lease per assigned task:** 120 seconds.
- **Maximum retries per task:** 20 attempts.
- The policy applies only after the plugin's retry-safety declaration permits
  retry-capable admission; see **Plugin Retry Safety** above.
- During active scheduler operation, on timeout, lease expiry, or explicit executor failure:
  - the attempt is counted as failed,
  - an ordinary retryable failure is retried if attempts remain,
  - `PERMANENT_PAYLOAD_INTEGRITY` closes the first accepted exact assignment
    as terminal regardless of the remaining retry budget,
  - otherwise the task moves to terminal `FAILED`.
- When a retry is scheduled, the persisted task row is returned to `PENDING`, its previous assignment ID/timing/lease fields are cleared, its monotonic `attempt_number` is retained, and `retry_count` is incremented.
- During startup recovery, an expired lease or incomplete legacy assignment identity is released to `PENDING` without incrementing `retry_count`; the last known generation is retained from task state or the legacy attempt audit, and the next runtime assignment advances it.

## Capability-Aware Executor Assignment

- Executor-enabled participants advertise supported task types in heartbeat metadata.
- The scheduler assigns a task only to executor participants that advertise support for that task type.
- If no capable executor participant is available, the task remains pending instead of being assigned to an incompatible participant.

## Job Completion/Failure

- A job is **successful** only when the exact expected task set completes with
  durable result snapshots. The last result commit persists `FINALIZING`;
  deterministic aggregation from those snapshots then commits `COMPLETED`.
- A job is **failed** when any task reaches terminal `FAILED`.
- On job failure, non-terminal remaining tasks and the job terminal edge commit
  atomically before their in-memory failure/capacity projection.
- For non-outbox outputs, final `JOB_RESULT` delivery is retried when the requester cannot be reached or the output transport reports failure.
- Non-outbox final result delivery is bounded by `jobResultMaxDeliveryAttempts` / `TASKFLOW_JOB_RESULT_MAX_DELIVERY_ATTEMPTS`.
- If non-outbox final result delivery is exhausted, the scheduler removes the already-terminal job from active memory and logs `job_result_delivery_abandoned`; it does not rewrite a completed job as failed.
- RabbitMQ with SQLite persistence commits the terminal job state and final `JOB_RESULT` outbox row together; failed publishes remain pending for replay instead of using the bounded non-outbox delivery counter.
- RabbitMQ delivery remains at least once: a publish accepted before the
  outbox sent mark may be replayed, but duplicate or concurrent finalization
  creates neither a second terminal transition nor a second logical outbox
  record.

## Result Ownership

- Job submissions carry a per-job requester token.
- Job submissions may also carry a requester public key and Ed25519 signature. The JavaFX submitter and command-line submit paths sign submissions.
- The coordinator persists only the token hash and requester public key, not the raw token or private key.
- Submission replay compares both stored ownership fields. A reconnect may use
  a different routing peer ID, but it must reuse the original per-job token and
  optional signing key. The JavaFX token store issues a token once per job ID
  and retains it after an uncertain send or publish-confirm failure.
- `JOB_RESULT_REQUEST` must include the matching requester token before the coordinator resends a durably terminal in-memory pending result, reports an active or terminal-write-deferred job as still running, or reconstructs a completed persisted result.
- Jobs submitted with a requester public key are identity-bound: result requests must include the same public key and a valid signature over the request fields.
- Requests with a missing or wrong token, missing identity signature, mismatched public key, or invalid signature return a failed `JOB_RESULT` instead of task results.
- Running jobs without a persisted requester token hash are treated as non-resumable at startup because ownership cannot be verified after restart.
- JavaFX submissions persist raw requester tokens and a requester identity keypair in a local user-profile token file so exact duplicate submissions can reuse ownership after uncertain publication. RabbitMQ GUI `JOB_RESULT_REQUEST` replay is not implemented. The default path is `<user-home>/.taskflow/gui-requester-tokens.properties`, overrideable with `TASKFLOW_GUI_REQUESTER_TOKEN_STORE`.
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
- Schema version 11 defines durable `FINALIZING` intent. Its migration changes
  a version-10 `RUNNING` job only when its positive expected count exactly
  matches a fully `COMPLETED`, result-bearing task set; other legacy rows remain
  subject to normal recovery validation.
- Schema version 12 adds the non-null `jobs.request_hash` column. New scheduler
  submissions store a `v1:` SHA-256 fingerprint in the same transaction as the
  job/tasks. Migrated rows retain the empty default and therefore cannot claim
  exact-submission replay.
- Schema version 13 adds `orphan_output_gc_failures`. Each exact object key
  retains first/last deletion-failure time, a saturated attempt count, and a
  bounded last error until idempotent deletion succeeds or SQLite later
  classifies the key as active/authoritative. Migration from v12 creates an
  empty retry set without changing task/result authority.
- Schema version 14 adds indexes for job-scoped task and attempt reads and for
  pending broker-outbox replay. Migration from v13 creates only indexes; it
  neither rewrites durable rows nor changes transition or recovery semantics.
- Coordinator startup rebuilds resumable `RUNNING` and `FINALIZING` jobs from persisted snapshots, restores completed task results when result payloads were persisted, preserves assigned tasks only when their complete persisted identity has an unexpired lease, releases expired assignments with a `lease_expired` attempt reason, and releases incomplete legacy assignments with an inspectable restart reason. Recovered task results are supplied to plugins in canonical task order for deterministic aggregation.
- Legacy or otherwise non-resumable `RUNNING` or `FINALIZING` jobs are marked `FAILED` on startup.
- If startup recovery cannot safely reconcile persisted state, the coordinator closes that state store, disables persistence for the run, and logs `database_disabled` instead of writing against unreconciled history.
- After startup, non-outbox assignment commits before task/capacity projection and dispatch. For RabbitMQ outbox assignment, the scheduler installs the identity returned by the committed SQLite transaction and publishes its returned outbox row; repeated or concurrent creation calls for a no-longer-`PENDING` task create neither a new generation nor a second outbox row.
- Retry release and terminal task failure commit the exact current assignment generation and its attempt-audit outcome before changing task state, participant capacity, retry/failure metrics, or redispatch eligibility. Storage failure preserves the assigned projection; an ensuing job-failure decision must itself commit before terminal projection.
- Executor PONG protocol v3 advertises a process-instance UUID, monotonic
  snapshot sequence, scalar total/available units, and per-task-type maximum
  concurrency while the RabbitMQ envelope and all non-PONG messages remain
  protocol v2. Legacy PONGs remain live for inspection but are scheduling
  ineligible.
- Coordinator placement first applies hard weighted-unit and type-slot
  eligibility, then scores only eligible peers. Capacity is reserved after the
  durable assignment commits and released only after an exact current
  assignment transition commits; stale, duplicate, unknown, disconnect-only,
  and storage-failure events do not release it.
- The last successful task transaction commits its task result, attempt outcome,
  and `FINALIZING` intent together. For non-outbox outputs, the subsequent
  terminal job state commits before final-result delivery. For RabbitMQ with
  SQLite persistence, that terminal job state, semantic aggregate, and final
  `JOB_RESULT` outbox row commit together before immediate publish/replay. Any
  finalization or terminal/outbox write failure keeps a replayable durable or
  active pending-completion projection and sends no final result until retry
  commits.
- `JOB_RESULT_REQUEST` can resend a durably terminal in-memory pending result or reconstruct a completed persisted `JOB_RESULT` when the requester token matches, any required requester identity signature is valid, and every task result snapshot exists. A pending completion whose terminal write has not committed is reported as still running. Reconstructed completed results include the schema-v6 semantic final payload when it was persisted, plus the compatibility ordered task-result list.
- Failed jobs and completed jobs with missing result snapshots are not reconstructed as successful persisted results.
- PostgreSQL/Flyway is not implemented. `docs/RECOVERY_SCOPE.md` records the SQLite lease behavior and keeps PostgreSQL/Flyway deferred until there is a concrete external database requirement.

## Heartbeat and Participant Liveness

- RabbitMQ participants publish initial, periodic, and coalesced
  capacity-change `PONG` heartbeats with their explicit peer ID, supported task
  types, scalar units, and per-type concurrency limits.
- The coordinator accepts a heartbeat only when the broker envelope sender and
  inner message identity agree.
- Missing heartbeats beyond timeout mark the participant stale, remove it from the
  live registry, and persist a disconnected last-known participant status when the
  peer registry store is available.

## Core Scheduler Metrics

Scheduler emits structured event logs and periodic metrics snapshots including:

- `queue_depth`
- `active_jobs`
- `overloaded`
- `overload_primary_reason`
- `overload_configured_maximum`
- `overload_observed_value`
- `overload_reasons`
- `job_submit_prefetch`
- `pending_outbox_observation_healthy`
- `pending_tasks_indexed`
- `runnable_jobs_indexed`
- `live_assignments_indexed`
- `deadline_entries_indexed`
- `deadline_head_checks_total`
- `deadline_entries_popped_total`
- `deadline_entries_validated_total`
- `deadline_stale_rejected_total`
- `deadline_reschedules_total`
- `dispatch_latency_ms` (average from task becoming pending to assignment)
- `retry_count`
- `task_success_rate` (successful attempts / total attempts)
- `failure_count`
- `taskflow_task_results_committed_total`
- `taskflow_task_results_stale_total`
- `taskflow_task_results_duplicate_total`
- `taskflow_assignment_generations_total`
- `taskflow_payload_integrity_failures_total`

The five `taskflow_*_total` counters distinguish authoritative result commits,
obsolete-generation rejection, duplicate-completion suppression, and assignment
generation creation, plus durably committed immutable-payload corruption. The
matching structured events are
`task_result_committed`, `task_result_stale_rejected`,
`task_result_duplicate_ignored`, `task_assignment_created`, and
`payload_integrity_failure_committed`. Each assignment-scoped event
carries `job_id`, `task_id`, `attempt_number`, `assignment_id`, and `worker_id`,
the log-field spellings of the protocol correlation tuple. These metrics are
intended for immediate log-based operational visibility in Phase 1 and as
migration inputs to a dedicated metrics backend in later phases; no exporter or
high-cardinality metric exemplar is implemented.

The workload-index counters and their steady-state complexity are defined in
[`SCHEDULER.md`](SCHEDULER.md).

`docs/OBSERVABILITY_SCOPE.md` maps the current structured-log events and records that a dedicated metrics backend is deferred until promoted lease/attempt-history dashboards, RabbitMQ outbox visibility, or promoted DLQ workflow metrics need operational visibility.
