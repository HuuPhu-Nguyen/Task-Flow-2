# Participant Requester And Result Lifecycle

This document records how requester, executor, and result-handling behavior fits
TaskFlow's coordinator-mediated distributed task-execution architecture. It does
not add transport guarantees beyond `docs/EXECUTION_GUARANTEES.md`.

`taskflow-peer`, `PeerNode`, `RabbitMqPeerNode`, peer IDs, and peer-routed queue
names are retained compatibility names. They identify participant runtimes and
routes; they do not imply that participants share the coordinator's scheduling
or authoritative result-commit authority.

## Decision

A TaskFlow participant can enable either or both runtime roles:

- **Requester role:** submit jobs through a `ClientJobPlugin`, receive terminal
  `JOB_RESULT` messages for those jobs, and handle successful final results
  through the matching `ClientJobPlugin`.
- **Executor role:** advertise task capabilities and execute coordinator-assigned
  work through `PeerProcessorPlugin` implementations.

The JavaFX GUI is the participant-facing UI for the default RabbitMQ path and
the explicit legacy TCP path, not a separate client/server application. The
`taskflow-peer` artifact is the headless command-line participant runtime.
Both are expected to use the same plugin ownership rules when they submit jobs
or handle successful final results.

RabbitMQ is the default runtime for these participant roles. The GUI has RabbitMQ
service adapters for live broker-backed submit, execute, result routing, and
result handling, while deprecated TCP remains available only when selected with
`TASKFLOW_TRANSPORT=tcp`. `docs/RUNTIME_STRATEGY.md` records the runtime
direction and the gates before TCP can be removed.

## Lifecycle

1. A requester-enabled participant loads `ClientJobPlugin` providers on its runtime
   classpath.
2. The selected client plugin validates local options, reads local inputs, and
   builds JSON-serializable job payloads.
3. The participant sends `JOB_SUBMIT` with a peer-scoped compatibility job id, requester token, and
   requester identity signature when supported by that path.
4. Participants use the explicit peer ID compatibility contract in `docs/PEER_IDENTITY.md`; set
   `TASKFLOW_PEER_ID` for stable identity across restarts.
5. Executor-enabled participants advertise supported task types through heartbeat
   metadata and process `TASK_ASSIGN` messages with peer processor plugins.
6. The coordinator validates submissions and aggregates task results through
   server-side task plugins.
7. The coordinator returns a terminal `JOB_RESULT` to the requester role of the
   submitting participant.
8. The submitter-side result handler checks whether the job id is one it is
   tracking, then routes successful final results by task type to
   `ClientJobPlugin.handleResult(...)`.

Failed `JOB_RESULT` messages are terminal user-visible outcomes, not payloads
for client plugins to save.

## Current Implementations

The JavaFX GUI path is implemented through GUI-facing services:

- `GuiJobSubmissionService` calls `ClientJobPlugin.buildPayloads(...)`.
- `GuiJobSubmitter` reserves the job id before sending so fast terminal results
  cannot be lost.
- `TcpCoordinatorConnection` handles `TASK_ASSIGN` through
  `PeerExecutionEngine` and forwards `JOB_RESULT` messages to the GUI listener.
- `RabbitMqCoordinatorConnection` sends heartbeats, consumes peer-specific
  `TASK_ASSIGN` and `JOB_RESULT` queues, publishes `TASK_RESULT`, and forwards
  live `JOB_RESULT` messages to the same GUI listener.
- `TcpJobSubmissionClient` and `RabbitMqJobSubmissionClient` both create signed
  `JOB_SUBMIT` messages with the GUI requester-token store. TCP still supports
  `JOB_RESULT_REQUEST`; RabbitMQ GUI result-request replay is not implemented.
- `GuiJobResultRouter` ignores results for untracked job ids and routes active
  failed or successful results.
- `GuiResultSaveService` handles successful final results with the matching
  `ClientJobPlugin`.

The RabbitMQ command-line participant path is implemented in `RabbitMqPeerNode`:

- `submit` mode loads client plugins, builds payloads, publishes `JOB_SUBMIT`,
  and waits on the submitting participant's `JOB_RESULT` route.
- While using the default `combined-runtime` profile, the same process also
  remains available for assigned task execution.
- Successful results are handled under `target/rabbitmq-results/<jobId>` by
  calling `ClientJobPlugin.handleResult(...)`.
- A `JOB_RESULT` for another job id is acknowledged and ignored while the
  submitter waits for its own job.

The legacy TCP command-line `PeerNode` supports the executor role and has a
low-level signed `submitJob(...)` helper, but it does not provide a supported
submit-and-save result workflow. Use the JavaFX GUI for an interactive participant UI,
or use the RabbitMQ command-line submit path for a headless submit-and-save
flow.

## Shared-Service Candidates

The audit found two pieces of intentional duplication that should not be solved
by a broad refactor in isolation:

- GUI and RabbitMQ command-line result handlers both resolve a
  `ClientJobPlugin` by final result task type and call `handleResult(...)`.
  A future shared participant-facing result service should accept transport/UI policy
  for output location, failed-handler behavior, and acknowledgement timing.
- GUI and command-line participants share `protocol.JobIds` for peer-scoped,
  collision-resistant job IDs. A future shared submitter service should avoid
  duplicating the surrounding requester-token, requester-signature, and send
  failure cleanup policy.

Keep these candidates separate from semantic final-result payload work.
`JobResultMessage.resultPayload` is now the semantic final payload, while
`resultsByTaskId` remains a compatibility list for existing list-based plugins.

## Evidence

Current focused coverage includes:

- `GuiJobSubmissionServiceTest` for GUI payload building and submit reservation.
- `GuiJobSubmitterTest` for fast terminal-result routing during send and TCP
  result-request construction.
- `GuiJobResultRouterTest` for active, failed, successful, and foreign
  `JOB_RESULT` routing.
- `GuiResultSaverTest` and `GuiResultSaveServiceTest` for GUI final-result handling
  through `ClientJobPlugin`.
- `RabbitMqJobSubmissionClientTest` for RabbitMQ GUI publish-confirm behavior,
  requester-token cleanup, and signed job submission.
- `RabbitMqCoordinatorConnectionTest` for RabbitMQ GUI heartbeat startup,
  peer-route subscription, task assignment execution, task-result publish
  acknowledgement/requeue behavior, job-result routing, malformed-result
  rejection, and startup failure handling.
- `PeerIdentityTest`, `PingHandlerTest`, and `PeerHandlerTest` for shared peer
  ID normalization, TCP heartbeat identity, and duplicate TCP peer rejection.
- `InMemoryPeerRegistryTest` and `DatabaseManagerTest` for peer metadata
  persistence hooks, SQLite restart reload, heartbeat capability updates,
  disconnect status updates, duplicate peer ID handling, and task retry state
  coexistence.
- `JobIdsTest`, `PeerNodeTest`, `TcpJobSubmissionClientTest`, and
  `RabbitMqJobSubmissionClientTest` for shared peer-scoped job ID generation
  across command-line and GUI submit paths.
- `PeerNodeTest` for RabbitMQ command-line payload creation, publish-confirm
  failure, and result handling through `ClientJobPlugin`.
