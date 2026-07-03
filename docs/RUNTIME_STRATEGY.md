# Runtime Strategy

This document records TaskFlow's transport direction. It does not change the
current runtime defaults or add guarantees beyond `docs/EXECUTION_GUARANTEES.md`.

## Decision

RabbitMQ is the planned primary runtime for the coordinator, command-line peers,
and JavaFX GUI peers.

TCP remains the current default local runtime and compatibility/demo path until
RabbitMQ reaches the replacement gates below. TCP must keep working while the
RabbitMQ path is completed, documented, and verified.

This is a direction decision, not a support-status promotion. RabbitMQ is still
documented as transitional in `docs/RABBITMQ_SCOPE.md` because broader
broker-failure coverage and desktop GUI evidence gates are not
complete.

## Rationale

The broker-backed runtime is the better long-term fit for the framework:

- peers have explicit broker identities and peer-targeted assignment/result
  routes;
- coordinator and peer processes can communicate without one socket per peer;
- broker queues expose natural integration points for prefetch, acknowledgement,
  dead-lettering, and later operator workflows;
- the Docker Compose demo already exercises a distributed broker-backed path.

TCP remains useful for simple local demos, manual JavaFX smoke runs, and
compatibility while the identity, recovery, and operational gaps are closed.

## Current State

- TCP is still the default when `TASKFLOW_TRANSPORT` is unset.
- The JavaFX GUI submits jobs, executes assigned work, and receives live results
  through TCP by default or RabbitMQ when `TASKFLOW_TRANSPORT=rabbitmq`.
- RabbitMQ is selected explicitly with `TASKFLOW_TRANSPORT=rabbitmq` for the
  coordinator, command-line peer, and JavaFX GUI.
- TCP and RabbitMQ peers share the explicit peer identity contract in
  `docs/PEER_IDENTITY.md`: `TASKFLOW_PEER_ID` provides stable configured IDs,
  generated fallback IDs are runtime-scoped, and TCP no longer uses remote
  socket addresses as peer IDs.
- The coordinator persists durable peer registry metadata in SQLite when
  persistence is available: peer ID, runtime type, transport, capabilities,
  heartbeat/disconnect times, status, and scheduling metric snapshots. Live
  connection objects and broker consumers remain in memory only.
- GUI and command-line submitters generate peer-scoped, collision-resistant job
  IDs with the sanitized peer ID, a timestamp, and a full UUID. The scheduler
  rejects duplicate job IDs that are already active or present in persisted job
  history when persistence is enabled.
- RabbitMQ command-line peers can register with peer IDs, send heartbeats,
  execute assigned work, submit jobs, receive `JOB_RESULT`, and handle
  successful final results through `ClientJobPlugin.handleResult(...)`.
- RabbitMQ JavaFX peers use GUI service adapters for signed job submission,
  peer-specific task assignment, task-result publication, live `JOB_RESULT`
  routing, and plugin-backed result handling. RabbitMQ GUI result-request replay
  after restart is not implemented.
- SQLite-backed RabbitMQ coordinator runs persist outbound `TASK_ASSIGN` and
  final `JOB_RESULT` messages in a broker outbox transactionally with scheduler
  state changes, replay pending rows on startup, and retry unsent rows while the
  coordinator runs.
- RabbitMQ dead-letter entries can be inspected through the command-line peer
  artifact, redriven to their original routing key when they contain valid
  TaskFlow broker envelopes, quarantined, or discarded.
- RabbitMQ has focused live broker coverage, including coordinator outbox
  crash-window scenarios, a dedicated GitHub Actions broker integration job,
  and a Docker Compose demo. Live broker tests remain opt-in for local runs.

## RabbitMQ Primary Runtime Gates

Do not flip the default transport, describe RabbitMQ as the primary supported
runtime, or start TCP deprecation until all of these are complete:

- JavaFX RabbitMQ desktop smoke or automation evidence for job submission,
  assigned-task execution, `JOB_RESULT` reception, result handling, and
  disconnect/broker-failure handling.
- A clear RabbitMQ GUI result-request decision: implement broker-backed
  `JOB_RESULT_REQUEST` replay or keep it explicitly unsupported while live
  `JOB_RESULT` delivery is the supported GUI RabbitMQ path.
- Broader broker-failure integration tests for coordinator, command-line peer,
  GUI service adapters, publisher confirms, requeue/reject/DLQ behavior, and
  result routing.
- The RabbitMQ-backed CI integration profile remains reliable for the focused
  live broker gates.
- README and docs updated so quick-start, demos, execution guarantees, and
  limitations all describe the same runtime behavior.

## TCP Deprecation And Removal Gates

TCP removal must happen only after the RabbitMQ primary runtime gates pass and
in two separate steps.

First, deprecate TCP:

- keep the TCP runtime working;
- mark TCP as legacy in docs and runtime logs;
- provide a migration note from TCP commands to RabbitMQ commands;
- make RabbitMQ the recommended quick-start/demo path;
- verify fresh clone, CI, Docker Compose demo, JavaFX RabbitMQ service tests,
  and manual or automated GUI smoke evidence.

Only in a later cleanup, remove TCP:

- remove TCP-only coordinator, peer, and GUI transport code with no unrelated
  feature work;
- remove or rewrite TCP-only docs, scripts, and smoke paths;
- keep plugin contracts and peer lifecycle semantics unchanged;
- run full Maven, dependency-tree, demo, fresh-clone, and CI gates.

## Public Claim Rule

Use wording such as planned primary runtime, target broker runtime, current
TCP default, and transitional RabbitMQ support.

Avoid saying RabbitMQ is the default runtime, production-ready runtime, fully
supported runtime, durable broker runtime, or TCP replacement until the gates in
this document and `docs/RABBITMQ_SCOPE.md` are complete and tested.
