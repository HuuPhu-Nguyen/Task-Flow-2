# TCP Deprecation Gates

This checklist records why TCP is not deprecated yet and what must be true
before TaskFlow can mark TCP as legacy. It is a gate document, not a support
promotion for RabbitMQ and not a plan to remove TCP immediately.

## Current Decision

TCP remains the default local runtime and compatibility/demo path.

RabbitMQ is the planned primary runtime, but TaskFlow must keep TCP working
until the replacement gates in `docs/RUNTIME_STRATEGY.md` and the evidence
below are complete.

## Gate Status

| Gate | Status | Evidence | Remaining Work |
| --- | --- | --- | --- |
| JavaFX RabbitMQ submit, execute, live result routing, and result handling exists behind GUI services. | Complete for headless service coverage. | `RabbitMqJobSubmissionClientTest`, `RabbitMqCoordinatorConnectionTest`, `GuiTransportModeTest`, `docs/EXECUTION_GUARANTEES.md`. | Add desktop smoke or automation evidence before deprecation. |
| JavaFX RabbitMQ desktop smoke or automation evidence covers submit, assigned-task execution, `JOB_RESULT` reception, result handling, disconnect, and broker-failure handling. | Open. | `docs/GUI_MANUAL_SMOKE.md` currently covers the default TCP desktop path and describes RabbitMQ desktop smoke as future work. | Add a repeatable RabbitMQ desktop smoke procedure and record one successful run, or add stable JavaFX automation with a display backend. |
| RabbitMQ GUI result-request behavior is decided. | Complete as a scoped limitation. | README, `docs/EXECUTION_GUARANTEES.md`, `docs/RUNTIME_STRATEGY.md`, and `docs/RABBITMQ_SCOPE.md` state that RabbitMQ GUI `JOB_RESULT_REQUEST` replay is not implemented and live `JOB_RESULT` delivery is the supported RabbitMQ GUI result path. | Revisit only if post-restart RabbitMQ GUI result replay becomes required before default flip. |
| Shared peer identity and peer-scoped job IDs work across TCP, RabbitMQ, CLI, and GUI paths. | Complete. | `docs/PEER_IDENTITY.md`, `protocol.PeerIdentity`, `protocol.JobIds`, identity and submitter tests. | No TCP deprecation blocker. |
| RabbitMQ coordinator outbox replay and DLQ workflow have live broker evidence. | Complete for current scope. | `RabbitMqCoordinatorLiveIntegrationTest`, `RabbitMqTransportLiveTest`, `docs/RABBITMQ_SCOPE.md`. | Keep broker-backed CI passing. |
| Broader broker-failure integration coverage exists for coordinator, command-line peer, GUI service adapters, publisher confirms, requeue/reject/DLQ behavior, and result routing. | Partial. | Current live tests cover focused transport delivery, publisher confirms, connection-close recovery, requeue/reject/DLQ, coordinator completion, and coordinator outbox replay. | Add explicit outage/restart scenarios or document why existing focused cases are enough before deprecation. |
| RabbitMQ-backed CI remains reliable. | Complete for focused gates. | GitHub Actions `RabbitMQ Integration Tests` job. | Keep the job green after each RabbitMQ behavior change. |
| README, demos, execution guarantees, limitations, and runtime docs align with the same default and support status. | Complete for current transitional status. | README, `docs/RUNTIME_STRATEGY.md`, `docs/RABBITMQ_SCOPE.md`, `docs/EXECUTION_GUARANTEES.md`. | Update them again only when TCP is actually marked legacy or RabbitMQ becomes the default. |

## Deprecation Start Rule

Do not mark TCP as legacy until the open and partial gates above are closed.
When they are closed, TCP deprecation must be a documentation/runtime-warning
slice that keeps TCP working and includes:

- migration notes from TCP commands to RabbitMQ commands;
- RabbitMQ as the recommended quick-start/demo path;
- fresh clone and CI evidence;
- Docker Compose demo evidence;
- JavaFX RabbitMQ desktop smoke or automation evidence;
- no TCP code removal.

## Removal Rule

TCP removal is a later cleanup after deprecation. It must remove TCP-only
coordinator, peer, GUI, docs, scripts, and smoke paths without changing plugin
contracts or RabbitMQ behavior in the same slice.
