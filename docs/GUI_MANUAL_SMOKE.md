# GUI Manual Smoke Run

Last updated: 2026-06-30 23:37 Asia/Bangkok

This is the repeatable desktop smoke check for the default TCP JavaFX GUI path. It complements the headless GUI service tests, including RabbitMQ GUI adapter tests; it does not claim automated JavaFX end-to-end coverage. The RabbitMQ JavaFX desktop smoke gate is documented separately in `docs/GUI_RABBITMQ_DESKTOP_SMOKE.md`.

JavaFX end-to-end UI smoke remains manual until CI has a stable desktop automation harness. See `docs/GUI_AUTOMATION_SCOPE.md` for the automation decision and prerequisites.

## Scope

- TCP coordinator connection from `PeerApp`
- Initial connection refusal handling
- GUI job submission with the GUI also executing assigned work
- Plugin-owned result saving
- Job history refresh
- Coordinator disconnect alert

RabbitMQ GUI behavior is covered by headless service tests for broker publish,
task assignment execution, result routing, and failure handling. Use
`docs/GUI_RABBITMQ_DESKTOP_SMOKE.md` for the desktop RabbitMQ smoke procedure.

## Preflight

Run from the repository root on a machine with a desktop environment:

```powershell
.\mvnw.cmd -pl taskflow-gui -am test
New-Item -ItemType Directory -Force target\gui-smoke\input, target\gui-smoke\output | Out-Null
Set-Content -Path target\gui-smoke\input\sample.txt -Encoding UTF8 -Value @(
    "TaskFlow GUI smoke test",
    "alpha beta beta",
    "distributed JavaFX peer"
)
```

Close old TaskFlow coordinator, peer, and GUI processes before starting the smoke run.

## Connection Failure Check

1. Make sure no coordinator is listening on port `6789`.
2. Start the GUI:

```powershell
.\mvnw.cmd -pl taskflow-gui javafx:run
```

3. Leave host as `localhost` and port as `6789`.
4. Click `Connect to Coordinator`.

Expected result: an error alert says the GUI could not connect to the coordinator, and the Connect button becomes usable again.

Close the GUI before continuing.

## Successful TCP Job

Start the coordinator in terminal 1:

```powershell
.\mvnw.cmd -pl taskflow-coordinator exec:java
```

Expected coordinator log:

```text
event=coordinator_started transport=tcp port=6789
```

Start the GUI in terminal 2:

```powershell
.\mvnw.cmd -pl taskflow-gui javafx:run
```

In the GUI:

1. Enter host `localhost` and port `6789`.
2. Click `Connect to Coordinator`.
3. Keep or select job type `Text Analysis`.
4. Keep target `csv`.
5. Click `Upload Files` and choose `target\gui-smoke\input\sample.txt`.
6. Click `Start Job`.
7. Confirm the started-job alert if it appears.
8. When the download window appears, click `Choose Folder & Save` and select `target\gui-smoke\output`.

Expected results:

- The GUI shows `Files saved successfully!`.
- `target\gui-smoke\output\text-analysis-results.csv` exists.
- The CSV contains a row for `sample.txt`.
- Logs include `event=gui_job_submitted`, `event=job_completed`, and `event=gui_results_saved`.

## History Check

1. Open the `Job History` tab.
2. Click `Refresh`.
3. Select the newest text-analysis job.

Expected results:

- The job table shows a completed `TEXT_ANALYSIS` job.
- The task table shows at least one completed task for the selected job.

## Disconnect Check

1. Leave the GUI open after the successful job.
2. Stop the coordinator with `Ctrl+C`.

Expected result: the GUI displays a warning alert reporting that the coordinator connection closed or was lost.

## Cleanup

Close the GUI, stop the coordinator if it is still running, and leave `target\gui-smoke` as disposable local output.
