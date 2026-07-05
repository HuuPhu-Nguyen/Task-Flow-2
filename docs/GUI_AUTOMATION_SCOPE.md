# GUI Automation Scope

This document records the current JavaFX GUI automation decision.

## Current Automated Coverage

The CI workflow runs `bash ./mvnw --batch-mode --no-transfer-progress test` on
`ubuntu-latest` without a configured desktop session, Xvfb, TestFX, Monocle, or
another JavaFX UI automation harness.

The stable automated GUI coverage is therefore service-level and headless:

- TCP coordinator connection behavior;
- job submission state handling;
- fast result routing;
- failed result routing;
- result saving;
- input staging;
- download-save controller behavior;
- background task runner behavior;
- local requester-token and requester-identity persistence;
- job-history formatting and refresh behavior;
- RabbitMQ GUI job submission publish behavior;
- RabbitMQ GUI task assignment execution, task-result publish acknowledgement,
  result routing, and startup failure behavior.

The local RabbitMQ desktop smoke helper can also launch the JavaFX GUI and drive
one text-analysis submit/execute/result/save path with
`scripts/smoke-rabbitmq-gui.ps1 -AutoRun`. That helper records desktop smoke
evidence, but it is not yet a CI gate because the CI environment still lacks a
stable JavaFX display backend.

## Required Manual Gate

`docs/GUI_MANUAL_SMOKE.md` remains the required desktop smoke gate for the
deprecated explicit TCP JavaFX path. It covers:

- connection refusal;
- successful TCP job submit, execute, result receive, and save;
- job history refresh;
- coordinator disconnect alert.

RabbitMQ GUI behavior has headless service coverage and a local automated
desktop smoke helper. Use `docs/GUI_RABBITMQ_DESKTOP_SMOKE.md` for the
RabbitMQ desktop smoke procedure before promoting RabbitMQ as the primary
supported GUI runtime.

## Deferred CI End-To-End UI Smoke

Automated JavaFX end-to-end smoke is deferred because the current CI environment
does not provide the desktop automation prerequisites needed to run the actual
JavaFX window reliably.

Do not make a JavaFX window-driving test a required CI gate until the workflow
includes:

- a stable display backend such as Xvfb or a supported headless JavaFX stack;
- a UI automation library and lifecycle rule for JavaFX application startup and
  shutdown;
- deterministic file chooser or save-dialog handling;
- a timeout and cleanup strategy for coordinator and GUI processes;
- a local deprecated-TCP submit-to-save scenario that passes repeatedly in CI;
- a RabbitMQ-backed GUI submit-to-save scenario based on the current local
  smoke helper.

Until those prerequisites exist, keep JavaFX end-to-end smoke out of CI and keep
CI coverage focused on the GUI services behind the JavaFX presentation layer.
