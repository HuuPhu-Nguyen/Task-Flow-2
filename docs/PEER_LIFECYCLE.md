# Participant Requester and Result Lifecycle

This document records how requester, executor, and result handling fit
TaskFlow's coordinator-mediated RabbitMQ runtime. It does not add guarantees
beyond `docs/EXECUTION_GUARANTEES.md`.

`taskflow-peer`, `PeerNode`, `RabbitMqPeerNode`, peer IDs, and peer-routed queue
names are retained compatibility names. Participants never share the
coordinator's scheduling or authoritative result-commit authority.

## Roles

A participant can enable either or both roles:

- **Requester:** build and submit jobs through a `ClientJobPlugin`, receive
  terminal `JOB_RESULT`, and handle successful results through that plugin.
- **Executor:** advertise capabilities and capacity, then execute
  coordinator-assigned work through `PeerProcessorPlugin` implementations.

The JavaFX GUI is the interactive participant runtime. `taskflow-peer` is the
headless command-line runtime. Both use RabbitMQ and the same plugin ownership
rules. Maven runtime profiles select requester-only, executor-only, or combined
plugin classpaths.

## Lifecycle

1. A requester loads `ClientJobPlugin` providers from its runtime classpath.
2. The selected plugin validates local options, reads inputs, and builds
   JSON-serializable payloads.
3. The participant reserves a peer-scoped job ID and sends signed `JOB_SUBMIT`
   with its requester token.
4. The broker envelope sender and inner `nodeId` identify the reply route; the
   requester token hash plus optional public key define durable ownership.
5. An explicit retry must reuse the job ID, token/key, and canonical request.
6. Executor-enabled participants advertise supported task types, scalar free
   capacity, and per-type concurrency limits in v3 heartbeat metadata, then
   receive participant-specific `TASK_ASSIGN` messages.
7. The executor processor declares retry safety. `taskId` is stable across
   logical retries; `assignmentId` identifies one redeliverable generation.
8. The participant publishes `TASK_RESULT` and acknowledges the assignment only
   after broker confirmation.
9. The coordinator validates and commits fenced results, aggregates through the
   server plugin, and publishes terminal `JOB_RESULT`.
10. The requester acknowledges its final-result delivery after accepting it for
    its local handling policy. Successful results flow to
    `ClientJobPlugin.handleResult(...)`; failed results remain user-visible
    terminal outcomes and are not passed to save plugins.

## JavaFX Implementation

- `GuiJobSubmissionService` calls `ClientJobPlugin.buildPayloads(...)`.
- `GuiJobSubmitter` reserves the job ID before publishing so an immediate result
  cannot race ahead of local tracking.
- `RabbitMqJobSubmissionClient` creates signed submissions and keeps requester
  credentials after uncertain publisher-confirm failure for exact replay.
- `RabbitMqCoordinatorConnection` sends heartbeats, consumes peer-specific task
  assignments and final results, executes work through `GuiWorkerRuntime`, and
  publishes task results.
- `GuiJobResultRouter` ignores foreign job IDs and routes tracked failed or
  successful results.
- `GuiResultSaveService` invokes the matching client plugin for successful final
  payloads.

The GUI persists requester tokens and its local signing key, but there is no
RabbitMQ `JOB_RESULT_REQUEST` route. Post-restart lookup through that message is
therefore unsupported. Exact duplicate submission replay and live final-result
delivery remain available.

## Command-Line Implementation

`RabbitMqPeerNode` provides the headless path:

- `submit` loads a client plugin, builds payloads, publishes `JOB_SUBMIT`, and
  waits on the submitting participant's final-result route;
- the default combined profile remains available for assigned task execution
  while waiting;
- successful results are handled under
  `target/rabbitmq-results/<jobId>` through
  `ClientJobPlugin.handleResult(...)`;
- a result for another job ID is acknowledged and ignored by the waiting
  submitter;
- executor-only mode advertises its processor capabilities and consumes
  assignments without requester plugins.

Both executor runtimes send an initial v3 capacity snapshot, periodic refreshes,
and coalesced updates after local queued/running reservations change. Total
capacity defaults to the available processor count and can be set with
`TASKFLOW_EXECUTOR_TOTAL_CAPACITY_UNITS`; optional per-type limits use
`TASKFLOW_EXECUTOR_TYPE_CONCURRENCY_LIMITS`. Invalid values fail startup.
Legacy v0-v2 `PONG` messages still refresh liveness but are scheduling-ineligible.

## Shared-Service Candidates

Two narrow duplications remain intentional:

- GUI and command-line result handlers both resolve a client plugin and call
  `handleResult(...)`, but their output location, acknowledgement timing, and
  user-error policy differ.
- GUI and command-line submitters share `protocol.JobIds`, while surrounding
  token, signature, and publish-failure policy remains runtime-specific.

Do not merge these paths unless a focused task defines those policies. Semantic
`JobResultMessage.resultPayload` is the current aggregate; `resultsByTaskId`
remains the compatibility list for list-based plugins.

## Evidence

Focused coverage includes:

- `GuiJobSubmissionServiceTest` and `GuiJobSubmitterTest` for payload building,
  reservation, fast results, connection changes, and publish failure cleanup;
- `GuiJobResultRouterTest`, `GuiResultSaverTest`, and
  `GuiResultSaveServiceTest` for final-result routing and handling;
- `RabbitMqJobSubmissionClientTest` for signed publication, requester-token
  retention, and publisher-confirm failure;
- `RabbitMqCoordinatorConnectionTest` for heartbeat startup, peer routes,
  assignment execution, result publication, acknowledgement/bounded retry, final
  result routing, invalid-message rejection, and startup failure;
- `PeerNodeTest` and `WorkerAssignmentDeduplicationIntegrationTest` for
  command-line submit/result behavior and executor redelivery handling;
- `InMemoryPeerRegistryTest` and `DatabaseManagerTest` for participant metadata
  lifecycle;
- `JobIdsTest` and `PeerIdentityTest` for shared identifier behavior.
