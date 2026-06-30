# Runtime Strategy

This document records TaskFlow's transport direction. It does not change the
current runtime defaults or add guarantees beyond `docs/EXECUTION_GUARANTEES.md`.

## Decision

RabbitMQ is the planned primary runtime for the coordinator, command-line peers,
and future JavaFX GUI peers.

TCP remains the current default local runtime and compatibility/demo path until
RabbitMQ reaches the replacement gates below. TCP must keep working while the
RabbitMQ path is completed, documented, and verified.

This is a direction decision, not a support-status promotion. RabbitMQ is still
documented as transitional in `docs/RABBITMQ_SCOPE.md` because durable
outbox/replay, TaskFlow DLQ workflow, JavaFX RabbitMQ support, and several
identity and evidence gates are not complete.

## Rationale

The broker-backed runtime is the better long-term fit for the framework:

- peers have explicit broker identities and peer-targeted assignment/result
  routes;
- coordinator and peer processes can communicate without one socket per peer;
- broker queues expose natural integration points for prefetch, acknowledgement,
  dead-lettering, and later operator workflows;
- the Docker Compose demo already exercises a distributed broker-backed path.

TCP remains useful for simple local demos, manual JavaFX smoke runs, and
compatibility while the GUI and recovery gaps are closed.

## Current State

- TCP is still the default when `TASKFLOW_TRANSPORT` is unset.
- The JavaFX GUI currently submits jobs, executes assigned work, and receives
  results only through TCP.
- RabbitMQ is selected explicitly with `TASKFLOW_TRANSPORT=rabbitmq` for the
  coordinator and command-line peer.
- RabbitMQ command-line peers can register with peer IDs, send heartbeats,
  execute assigned work, submit jobs, receive `JOB_RESULT`, and save successful
  final results through `ClientJobPlugin`.
- RabbitMQ has focused live broker coverage and a Docker Compose demo, but live
  broker tests are still opt-in.

## RabbitMQ Primary Runtime Gates

Do not flip the default transport, describe RabbitMQ as the primary supported
runtime, or start TCP deprecation until all of these are complete:

- JavaFX RabbitMQ support for job submission, assigned-task execution,
  `JOB_RESULT` reception, result saving, disconnect/broker-failure handling,
  and result-request behavior where applicable.
- Stable peer identity shared by GUI and command-line peers, including
  configured or locally generated peer IDs and duplicate-ID behavior.
- Peer-scoped, collision-resistant job IDs used by GUI, command-line submitters,
  and RabbitMQ submitters.
- Durable RabbitMQ outbox/replay for coordinator publications, with idempotent
  replay of task assignments and final job results.
- TaskFlow DLQ inspection, quarantine/discard decisions, and redrive back to the
  correct normal route.
- Broker-failure integration tests for coordinator, command-line peer, GUI
  service adapters, publisher confirms, requeue/reject/DLQ behavior, and result
  routing.
- A RabbitMQ-backed CI integration profile that starts a broker and runs the
  focused live broker gates reliably.
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
