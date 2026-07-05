# RabbitMQ JavaFX Desktop Smoke

This is the repeatable desktop smoke gate for the JavaFX GUI in RabbitMQ mode.
It complements the headless GUI service tests and the focused live RabbitMQ CI
profile. It is intentionally manual until JavaFX window automation is added.

## Scope

This smoke checks the visible JavaFX path for:

- RabbitMQ broker connection from the GUI;
- JavaFX job submission through RabbitMQ;
- assigned-task execution by the GUI peer;
- live `JOB_RESULT` reception;
- plugin-owned result handling and save;
- broker-failure observation after a successful job.

It does not add RabbitMQ `JOB_RESULT_REQUEST` replay. Live `JOB_RESULT`
delivery remains the supported RabbitMQ GUI result path.

## Helper Script

On a Windows desktop with Docker Desktop available, run:

```powershell
.\scripts\smoke-rabbitmq-gui.ps1
```

For an automated desktop smoke that launches JavaFX and drives the RabbitMQ
text-analysis path through GUI services without manual clicks, run:

```powershell
.\scripts\smoke-rabbitmq-gui.ps1 -AutoRun
```

The helper:

- builds the coordinator and GUI modules with tests skipped;
- starts the Compose RabbitMQ service;
- starts a RabbitMQ coordinator with an isolated exchange and queue prefix;
- prepares `target\gui-rabbitmq-smoke\input\sample.txt`;
- launches the JavaFX GUI in RabbitMQ mode with `Text Analysis` preselected;
- opens GUI file/save choosers in the smoke input and output directories;
- prompts you through the manual GUI steps, unless `-AutoRun` is used;
- in `-AutoRun` mode, submits the prepared text input after the JavaFX GUI
  connects, saves the live result through the normal client plugin, and waits
  for evidence logs and the result file;
- stops RabbitMQ to exercise broker-failure observation;
- writes `target\gui-rabbitmq-smoke\evidence.md`.

Use `-SkipDocker` when a broker is already running on `localhost:5672`.
Use `-NoLaunchGui` to prepare the broker, coordinator, and input while launching
the GUI yourself. In that mode, copy the environment-variable commands printed
by the helper so the GUI uses the same isolated exchange and queue prefix as the
coordinator. `-AutoRun` cannot be combined with `-NoLaunchGui`.

## Manual Steps

In the JavaFX window:

1. Connect to broker host `localhost` and port `5672`.
2. Confirm `Text Analysis` is selected.
3. Confirm target `csv`.
4. Click `Upload Files` and choose
   `target\gui-rabbitmq-smoke\input\sample.txt`.
5. Click `Start Job`.
6. When the download window appears, click `Choose Folder & Save` and select
   `target\gui-rabbitmq-smoke\output`.
7. Confirm `target\gui-rabbitmq-smoke\output\text-analysis-results.csv`
   exists and contains a row for `sample.txt`.
8. Return to the helper prompt and press Enter.
9. After the helper stops RabbitMQ, observe that the GUI remains open and review
   `target\gui-rabbitmq-smoke\logs\gui.out.log` for
   `event=gui_rabbitmq_heartbeat_failed`.

## Evidence Criteria

Record this gate as passed only when `evidence.md` shows:

- `Result file exists: True`;
- `GUI submitted job log found: True`;
- `GUI saved result log found: True`;
- `Coordinator completed job log found: True`.

Broker-failure observation should also be recorded. With the current service
behavior, the expected evidence is the GUI staying open and the heartbeat
failure event being logged after RabbitMQ is stopped.

If any evidence line is false, do not use that run as promotion evidence; inspect
the logs under `target\gui-rabbitmq-smoke\logs`.
