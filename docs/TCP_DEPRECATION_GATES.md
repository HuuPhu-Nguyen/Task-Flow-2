# TCP Deprecation Gates

This checklist records why TCP is now deprecated and what must stay true before
TaskFlow can remove it. It is a gate document, not a support promotion for
RabbitMQ and not a plan to remove TCP immediately.

## Current Decision

TCP is deprecated as the legacy local compatibility/demo path.

RabbitMQ is the default path for new demos and feature work when
`TASKFLOW_TRANSPORT` is unset or blank. TCP remains available only when selected
explicitly with `TASKFLOW_TRANSPORT=tcp` and must keep working until a later
removal slice closes the removal gates below.

## Gate Status

| Gate | Status | Evidence | Remaining Work |
| --- | --- | --- | --- |
| JavaFX RabbitMQ submit, execute, live result routing, and result handling exists behind GUI services. | Complete. | `RabbitMqJobSubmissionClientTest`, `RabbitMqCoordinatorConnectionTest`, `GuiTransportModeTest`, `docs/EXECUTION_GUARANTEES.md`, and automated desktop smoke evidence. | No TCP deprecation blocker. |
| JavaFX RabbitMQ desktop smoke or automation evidence covers submit, assigned-task execution, `JOB_RESULT` reception, result handling, disconnect, and broker-failure handling. | Complete. | `scripts/smoke-rabbitmq-gui.ps1 -AutoRun` recorded run `gui-smoke-20260705013655` on 2026-07-05 with result file, GUI submit/save logs, coordinator completion log, and GUI broker-failure heartbeat log all present. | Keep the helper working; broaden only if JavaFX CI automation becomes available. |
| RabbitMQ GUI result-request behavior is decided. | Complete as a scoped limitation. | README, `docs/EXECUTION_GUARANTEES.md`, `docs/RUNTIME_STRATEGY.md`, and `docs/RABBITMQ_SCOPE.md` state that RabbitMQ GUI `JOB_RESULT_REQUEST` replay is not implemented and live `JOB_RESULT` delivery is the supported RabbitMQ GUI result path. | Revisit only if post-restart RabbitMQ GUI result replay becomes required before default flip. |
| Shared peer identity and peer-scoped job IDs work across TCP, RabbitMQ, CLI, and GUI paths. | Complete. | `docs/PEER_IDENTITY.md`, `protocol.PeerIdentity`, `protocol.JobIds`, identity and submitter tests. | No TCP deprecation blocker. |
| RabbitMQ coordinator outbox replay and DLQ workflow have live broker evidence. | Complete for current scope. | `RabbitMqCoordinatorLiveIntegrationTest`, `RabbitMqTransportLiveTest`, `docs/RABBITMQ_SCOPE.md`. | Keep broker-backed CI passing. |
| Broader broker-failure integration coverage exists for coordinator, command-line peer, GUI service adapters, publisher confirms, requeue/reject/DLQ behavior, and result routing. | Complete for focused failure-path coverage. | Live tests cover focused transport delivery, publisher confirms, connection-close recovery, requeue/reject/DLQ, coordinator completion, and coordinator outbox replay. Unit tests cover command-line peer submit publish exceptions plus JavaFX RabbitMQ startup heartbeat publish failure, task-result publish failure, task-execution failure requeue, and result routing acknowledgement. | Keep these tests in the broker-backed CI selector; full broker outage/restart recovery remains outside the current support claim. |
| RabbitMQ-backed CI remains reliable. | Complete for focused gates. | GitHub Actions `RabbitMQ Integration Tests` job. | Keep the job green after each RabbitMQ behavior change. |
| README, demos, execution guarantees, limitations, and runtime docs align with the same default and support status. | Complete for RabbitMQ default. | README, `docs/RUNTIME_STRATEGY.md`, `docs/RABBITMQ_SCOPE.md`, `docs/EXECUTION_GUARANTEES.md`. | Update them again when RabbitMQ support is promoted or TCP removal starts. |

## Deprecation Start Rule

TCP deprecation has started. This slice keeps TCP working and includes:

- migration notes from TCP commands to RabbitMQ commands;
- RabbitMQ as the default quick-start/demo path;
- JavaFX RabbitMQ desktop smoke or automation evidence;
- no TCP code removal.

## Removal Rule

TCP removal is a later cleanup after deprecation. It must remove TCP-only
coordinator, peer, GUI, docs, scripts, and smoke paths without changing plugin
contracts or RabbitMQ behavior in the same slice.
