# Peer Submitter And Result Lifecycle

This document records how submitter and result-handling behavior fits the
coordinated peer-to-peer architecture. It does not add transport guarantees
beyond `docs/EXECUTION_GUARANTEES.md`.

## Decision

A TaskFlow peer can combine three capabilities:

- submit jobs through a `ClientJobPlugin`;
- advertise task capabilities and execute assigned work through
  `PeerProcessorPlugin` implementations;
- receive terminal `JOB_RESULT` messages for jobs it submitted and handle
  successful final results through the matching `ClientJobPlugin`.

The JavaFX GUI is the peer-facing UI for the default TCP path and the selectable
RabbitMQ path, not a separate client/server application. The command-line peer
is the headless peer runtime.
Both are expected to use the same plugin ownership rules when they submit jobs
or handle successful final results.

RabbitMQ is the planned primary runtime for these peer roles. The GUI now has
RabbitMQ service adapters for live broker-backed submit, execute, result
routing, and result saving, while TCP remains the default. `docs/RUNTIME_STRATEGY.md`
records the runtime direction and the gates before TCP can be deprecated or
removed.

## Lifecycle

1. A submit-capable peer loads `ClientJobPlugin` providers on its runtime
   classpath.
2. The selected client plugin validates local options, reads local inputs, and
   builds JSON-serializable job payloads.
3. The peer sends `JOB_SUBMIT` with a job id, requester token, and requester
   identity signature when supported by that path.
4. Execute-capable peers advertise supported task types through heartbeat
   metadata and process `TASK_ASSIGN` messages with peer processor plugins.
5. The coordinator validates submissions and aggregates task results through
   server-side task plugins.
6. The coordinator returns a terminal `JOB_RESULT` to the submitting peer.
7. The submitter-side result handler checks whether the job id is one it is
   tracking, then routes successful final results by task type to
   `ClientJobPlugin.saveResults(...)`.

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
- `GuiResultSaveService` saves successful results with the matching
  `ClientJobPlugin`.

The RabbitMQ command-line peer path is implemented in `RabbitMqPeerNode`:

- `submit` mode loads client plugins, builds payloads, publishes `JOB_SUBMIT`,
  and waits on the submitting peer's `JOB_RESULT` route.
- While using the default `combined-runtime` profile, the same process also
  remains available for assigned task execution.
- Successful results are written under `target/rabbitmq-results/<jobId>` by
  calling `ClientJobPlugin.saveResults(...)`.
- A `JOB_RESULT` for another job id is acknowledged and ignored while the
  submitter waits for its own job.

The legacy TCP command-line `PeerNode` supports task execution and has a
low-level signed `submitJob(...)` helper, but it does not provide a supported
submit-and-save result workflow. Use the JavaFX GUI for an interactive peer UI,
or use the RabbitMQ command-line submit path for a headless submit-and-save
flow.

## Shared-Service Candidates

The audit found two pieces of intentional duplication that should not be solved
by a broad refactor in isolation:

- GUI and RabbitMQ command-line result handlers both resolve a
  `ClientJobPlugin` by final result task type and call `saveResults(...)`.
  A future shared peer-facing result service should accept transport/UI policy
  for output location, failed-save behavior, and acknowledgement timing.
- GUI and command-line peers both generate job ids locally. Peer-scoped,
  collision-resistant job ids should wait for the explicit peer identity slice
  so new ids can include a canonical peer id.

Keep these candidates separate from semantic final-result payload work. The
later result-contract slice should decide whether `resultsByTaskId` remains a
compatibility shape or becomes a plugin-defined final payload.

## Evidence

Current focused coverage includes:

- `GuiJobSubmissionServiceTest` for GUI payload building and submit reservation.
- `GuiJobSubmitterTest` for fast terminal-result routing during send and TCP
  result-request construction.
- `GuiJobResultRouterTest` for active, failed, successful, and foreign
  `JOB_RESULT` routing.
- `GuiResultSaverTest` and `GuiResultSaveServiceTest` for GUI result saving
  through `ClientJobPlugin`.
- `RabbitMqJobSubmissionClientTest` for RabbitMQ GUI publish-confirm behavior,
  requester-token cleanup, and signed job submission.
- `RabbitMqCoordinatorConnectionTest` for RabbitMQ GUI heartbeat startup,
  peer-route subscription, task assignment execution, task-result publish
  acknowledgement/requeue behavior, job-result routing, malformed-result
  rejection, and startup failure handling.
- `PeerNodeTest` for RabbitMQ command-line payload creation, publish-confirm
  failure, and result saving through `ClientJobPlugin`.
