# TaskFlow Working Rules

Last updated: 2026-06-22 14:10 Asia/Bangkok

Durable conventions and rules for working on this project. `PLAN.md` is for active flaws and next slices. `LOG.md` is for completed work, verification evidence, historical notes, and handoff records.

## Session Start

- Start with `git status --short --branch`.
- Review `PLAN.md` before choosing work.
- Review the latest relevant `LOG.md` entries before trusting current behavior.
- Assume the worktree may contain user or prior-agent changes; do not revert unrelated changes.
- Scan the touched area for correctness risks before adding demos, benchmarks, polish, or resume wording.

## Priority Rules

- Correctness, failure handling, and evidence come before demos, benchmark polish, and presentation media.
- Keep RabbitMQ and persistence claims limited until durable outbox/replay, restart resume, and broader broker failure behavior are implemented and tested.
- Keep plugin/extensibility claims limited to independent task families unless broader workflow behavior is implemented and tested.
- If a flaw is intentionally deferred, record why it is deferred and what evidence is still missing.

## Planning And Logging

- Keep `PLAN.md` to active goals, active flaws, priority order, mitigation slices, and current gates.
- Keep slice status current; leave concise completion markers when they change the next step.
- Move resolved items and test evidence to `LOG.md`.
- Keep `LOG.md` historical. Do not use it as the active plan.
- Add newly discovered flaws to `PLAN.md` with a concrete mitigation path.

## Verification Gates

Default gate:

```powershell
git status --short --branch
.\mvnw.cmd test
git diff --check
```

Boundary checks when module ownership or classpaths change:

```powershell
.\mvnw.cmd -pl taskflow-coordinator -am dependency:tree
.\mvnw.cmd -pl taskflow-gui -am dependency:tree
```

RabbitMQ live tests when broker behavior changes:

```powershell
.\mvnw.cmd -pl taskflow-transport-rabbitmq -am "-Dtaskflow.rabbitmq.live=true" "-Dtest=RabbitMqTransportLiveTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
.\mvnw.cmd -pl taskflow-coordinator -am "-Dtaskflow.rabbitmq.live=true" "-Dtest=RabbitMqCoordinatorLiveIntegrationTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

PowerShell command rule:

- Quote Maven `-D...` properties, especially dotted properties such as `"-Dtaskflow.rabbitmq.live=true"`.

## Architecture Conventions

- Use the wording "coordinated peer-to-peer" for the main architecture.
- Do not describe the TCP/GUI runtime as producer/worker.
- Keep framework/runtime modules at the repository root.
- Keep task plugins under `plugins/<domain>`.
- Keep role-split plugin artifacts where dependencies differ: `model`, `server`, `client`, and `peer`.
- Payload creation and result saving belong to client plugins, not hardcoded GUI or RabbitMQ CLI branches.
- Peer execution processors belong to peer plugins, not coordinator/client modules.
- Do not move `taskflow-transport-rabbitmq` under a new directory in the same change as plugin/module ownership changes.

## Claim Discipline

- README claims must be no stronger than the module graph, tests, and live runs prove.
- RabbitMQ mode is functional but transitional until durable outbox/replay, broker outage behavior, higher-level backpressure, and durable recovery are implemented and tested.
- SQLite is the current state store; PostgreSQL/Flyway and transactional restart resume must not be claimed until implemented.
- JavaFX GUI is TCP-only until RabbitMQ GUI support is implemented.
- Focused headless GUI tests do not equal full JavaFX end-to-end UI integration coverage.

## Repo Hygiene

- Keep runtime artifacts out of Git: `taskflow.db*`, `java/in_PEER_*`, `java/out_PEER_*`, `target/`, IDE files, and local env files.
- `config/taskflow.yml` is local runtime configuration; keep `config/taskflow.example.yml` as the committed template.
- Do not run destructive cleanup commands without explicit user approval.
- If `.\mvnw.cmd clean test` fails while deleting `target`, check for a local Windows file lock before treating it as a source failure.
- If Docker reports a missing `dockerDesktopLinuxEngine` pipe, restart Docker Desktop before retrying Compose or live RabbitMQ work.
- After a profiled Compose run, use `docker compose --profile full-demo down --remove-orphans`; plain `docker compose down` may leave the profiled service container behind.

## Resume Exit Criteria

- `git status --short` is clean after commit.
- Fresh clone runs `.\mvnw.cmd clean test`.
- Fresh clone runs the documented demo.
- README has no stale claims, broken commands, or encoding defects.
- No tracked runtime artifacts, IDE files, generated Maven outputs, or user-specific absolute paths exist.
- Failure paths return terminal results or documented abandoned states.
- GUI handles success, failure, disconnect, duplicate filenames, and result-saving errors safely.
- RabbitMQ claims match tested behavior.
- Persistence claims match schema, tests, and restart behavior.
- At least one end-to-end distributed run is documented.
- At least one failure-recovery scenario is documented.
