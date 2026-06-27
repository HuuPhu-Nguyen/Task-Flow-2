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
- job-history formatting and refresh behavior.

## Required Manual Gate

`docs/GUI_MANUAL_SMOKE.md` remains the required desktop smoke gate for the
user-facing JavaFX path. It covers:

- connection refusal;
- successful TCP job submit, execute, result receive, and save;
- job history refresh;
- coordinator disconnect alert.

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
- a local TCP-only submit-to-save scenario that passes repeatedly in CI.

Until those prerequisites exist, keep JavaFX end-to-end smoke manual and keep
CI coverage focused on the GUI services behind the JavaFX presentation layer.
