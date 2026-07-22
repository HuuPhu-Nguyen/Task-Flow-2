# Runtime Strategy

This document records TaskFlow's transport direction. It does not add
guarantees beyond `docs/EXECUTION_GUARANTEES.md`.

## Decision

RabbitMQ is the default runtime for the coordinator, command-line participants
(the existing `taskflow-peer` artifact), and JavaFX GUI participants when
`TASKFLOW_TRANSPORT` is unset or blank.

TCP is deprecated as the legacy local compatibility/demo path. It remains
available only when selected explicitly with `TASKFLOW_TRANSPORT=tcp`, and TCP
must keep working until a later removal slice closes the removal gates below.

This is a default-runtime decision, not a support-status promotion. RabbitMQ is
still documented as transitional in `docs/RABBITMQ_SCOPE.md` because full
broker outage/restart recovery remains outside the current support claim.

## Rationale

The broker-backed runtime is the better long-term fit for the framework:

- participants have explicit broker identities and peer-targeted compatibility
  routes;
- coordinator and participant processes can communicate without one socket per
  participant;
- broker queues expose natural integration points for prefetch, acknowledgement,
  dead-lettering, and later operator workflows;
- the Docker Compose demo already exercises a distributed broker-backed path.

TCP remains available for compatibility while the remaining recovery and
operational gaps are closed.

## Current State

- RabbitMQ is the default when `TASKFLOW_TRANSPORT` is unset or blank.
- The JavaFX GUI submits jobs, executes assigned work, and receives live results
  through RabbitMQ by default or TCP when `TASKFLOW_TRANSPORT=tcp`.
- TCP is selected explicitly with `TASKFLOW_TRANSPORT=tcp` for the coordinator,
  command-line participant, and JavaFX participant.
- TCP and RabbitMQ participants share the explicit peer identity contract in
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
- RabbitMQ command-line participants can register with peer IDs, send heartbeats,
  execute assigned work in the executor role, submit jobs in the requester role,
  receive `JOB_RESULT`, and handle successful final results through
  `ClientJobPlugin.handleResult(...)`.
- RabbitMQ JavaFX participants use GUI service adapters for signed job submission,
  peer-specific task assignment, task-result publication, live `JOB_RESULT`
  routing, and plugin-backed result handling. RabbitMQ GUI result-request replay
  after restart is not implemented.
- Automated JavaFX RabbitMQ desktop smoke evidence covers GUI startup,
  broker-backed text-analysis submission, assigned-task execution, live
  `JOB_RESULT` routing, plugin-backed result saving, and broker-failure
  heartbeat handling.
- SQLite-backed RabbitMQ coordinator runs persist outbound `TASK_ASSIGN` and
  final `JOB_RESULT` messages in a broker outbox transactionally with scheduler
  state changes, replay pending rows on startup, and retry unsent rows while the
  coordinator runs.
- RabbitMQ dead-letter entries can be inspected through the compatibility
  `taskflow-peer` artifact, redriven to their original routing key when they
  contain valid TaskFlow broker envelopes, quarantined, or discarded.
- RabbitMQ has focused live broker coverage, including coordinator outbox
  crash-window scenarios, a dedicated GitHub Actions broker integration job,
  and a Docker Compose demo. Live broker tests remain opt-in for local runs.
- Focused RabbitMQ failure-path tests cover command-line requester publish
  exceptions, JavaFX GUI heartbeat publish failure, task-result publish
  failure, task-execution failure requeue, publisher confirms,
  requeue/reject/DLQ behavior, and result routing.

## RabbitMQ Support Promotion Gates

Do not describe RabbitMQ as the primary supported production runtime until the
remaining gates are complete. The default transport has flipped to RabbitMQ,
TCP deprecation has started, and TCP removal remains separate.

- Keep JavaFX RabbitMQ desktop smoke or automation evidence passing for job
  submission, assigned-task execution, `JOB_RESULT` reception, result handling,
  and disconnect/broker-failure handling.
- A clear RabbitMQ GUI result-request decision: implement broker-backed
  `JOB_RESULT_REQUEST` replay or keep it explicitly unsupported while live
  `JOB_RESULT` delivery is the supported GUI RabbitMQ path.
- Keep broader broker-failure integration tests passing for coordinator,
  command-line participant, GUI service adapters, publisher confirms,
  requeue/reject/DLQ behavior, and result routing.
- The RabbitMQ-backed CI integration profile remains reliable for the focused
  live broker gates.
- README and docs stay updated so quick-start, demos, execution guarantees, and
  limitations all describe the same runtime behavior.

## TCP Deprecation And Removal Gates

TCP removal must happen only after the RabbitMQ support-promotion gates pass
and after a compatibility window.

`docs/TCP_DEPRECATION_GATES.md` tracks the deprecation evidence and the
remaining limits before TCP can be removed.

TCP deprecation keeps the runtime working:

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

Use wording such as default RabbitMQ runtime, deprecated TCP compatibility path,
and transitional RabbitMQ support.

Avoid saying RabbitMQ is a production-ready runtime, fully supported runtime,
durable broker runtime, or complete TCP replacement until the gates in this
document and `docs/RABBITMQ_SCOPE.md` are complete and tested.
