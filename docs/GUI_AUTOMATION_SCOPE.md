# GUI Automation Scope

This document records the current JavaFX GUI automation decision.

## Current Automated Coverage

The main unit-test job runs without a configured desktop session, Xvfb, TestFX,
Monocle, or another JavaFX window-driving harness. Stable CI coverage is
therefore headless and service-level:

- broker connection lifecycle and startup failure;
- job submission state and fast-result routing;
- failed and successful result routing;
- plugin-owned result saving;
- input staging and download-save behavior;
- background task execution;
- local requester-token and requester-identity persistence;
- job-history formatting and refresh behavior;
- RabbitMQ submission and publisher-confirm failure;
- RabbitMQ assignment execution, task-result publication and acknowledgement;
- live final-result routing and malformed-delivery rejection.

The broker-backed CI job runs the focused RabbitMQ integration gates. The local
desktop helper can also launch the JavaFX GUI and drive one text-analysis
submit/execute/result/save path:

```powershell
.\scripts\smoke-rabbitmq-gui.ps1 -AutoRun
```

That helper records desktop evidence but is not a CI window-driving gate.

## Required Desktop Gate

Use `docs/GUI_RABBITMQ_DESKTOP_SMOKE.md` before making GUI support-promotion
claims. It covers GUI startup, broker connection, submission, assigned-task
execution, live `JOB_RESULT` reception, plugin-backed save, and broker-failure
handling.

## Deferred CI End-to-End UI Smoke

Automated JavaFX window driving remains deferred because the CI environment does
not provide the prerequisites needed to run the actual UI reliably.

Do not make it a required CI gate until the workflow includes:

- a stable display backend such as Xvfb or a supported headless JavaFX stack;
- a UI automation library and lifecycle rule for JavaFX startup and shutdown;
- deterministic file chooser and save-dialog handling;
- timeouts and cleanup for broker, coordinator, participant, and GUI processes;
- a RabbitMQ-backed submit-to-save scenario that passes repeatedly in CI.

Until then, keep CI focused on GUI services and broker contracts behind the
JavaFX presentation layer.
