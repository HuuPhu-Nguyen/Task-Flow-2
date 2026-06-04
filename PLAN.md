# TaskFlow Framework Migration Plan

Last updated: 2026-06-04 09:24 Asia/Bangkok

## Showcase Objective

TaskFlow should be developed and presented as a resume-grade distributed systems project, not only as a course demo.

The target positioning is:

> TaskFlow is a Java 21 coordinated peer-to-peer distributed task execution platform with pluggable job processors, TCP and RabbitMQ transports, fault-tolerant scheduling, persisted job history, and documented execution guarantees.

Professional standard for this project means:

- A clean clone builds and tests without relying on local untracked files.
- The README explains the system quickly, accurately, and without encoding or formatting defects.
- The architecture is visible through diagrams, module boundaries, and short design notes.
- Reliability claims are backed by tests, fault-injection demos, and documented limitations.
- Runtime behavior is observable through structured logs, metrics, history, or a simple dashboard.
- Demo setup is simple enough for a reviewer to run or understand in under five minutes.
- Code comments explain invariants, tradeoffs, and non-obvious behavior; they should not narrate obvious statements.
- Security and filesystem behavior are safe by default.

The project should compete on evidence: clean repository, tests, demo, metrics, failure handling, and architecture clarity.

## Current State

TaskFlow has moved from a single Maven demo project into a modular framework foundation for coordinated peer-to-peer distributed task processing.

Completed so far:

- Converted the project into a Maven reactor with separate SPI, core, plugin, transport, coordinator, worker, and GUI modules.
- Added plugin contracts for coordinator-side job creation and peer-side task processing.
- Moved image and video conversion into a concrete conversion plugin module loaded through Java ServiceLoader.
- Removed hardcoded image/video job branching from core job creation.
- Added a peer processor discovery path through ServiceLoader.
- Added a TransportConnection abstraction so core peer tracking no longer depends directly on java.net.Socket.
- Added a SchedulerOutput abstraction so the scheduler can dispatch through TCP or a broker transport.
- Added a RabbitMQ transport module with topology declaration, JSON message envelopes, publish/subscribe, manual ack, requeue, reject, and prefetch configuration.
- Added RabbitMQ coordinator and command-line peer runtime entry points.
- Added TASKFLOW_TRANSPORT mode selection while keeping TCP as the default runtime.
- Updated README runtime notes for the current transport state.

Current runtime status:

- TCP is still the default coordinated peer runtime.
- The JavaFX GUI is a peer: it can submit jobs and execute assigned tasks in the same runtime.
- The command-line peer executes assigned tasks. In RabbitMQ mode it also has a basic CLI submit command.
- RabbitMQ can be selected with TASKFLOW_TRANSPORT=rabbitmq.
- RabbitMQ coordinator/peer runtime now models first-class broker peers with peer IDs, heartbeat registration, peer-specific task assignment, peer-specific job-result routing, and a basic broker-aware peer submit path.
- RabbitMQ can support the coordinated peer-to-peer objective, but the current implementation still needs a live broker integration test, broker backpressure, dead-letter handling, and stronger restart/recovery semantics.
- Local RabbitMQ was not running in the latest session: localhost:5672 was not reachable on 2026-06-03 at 23:38 Asia/Bangkok.
- Runtime state store is still SQLite through DatabaseManager.
- PostgreSQL/Flyway migration has not started.
- GUI is still a JavaFX TCP peer. It is not yet a RabbitMQ peer.

Showcase readiness status:

- The core architectural direction is strong: modular Maven reactor, SPI contracts, plugin discovery, scheduler output abstraction, and transport abstraction.
- The modular refactor has been committed and pushed to the new Task-Flow-2 repository through commit `ae1f564`.
- Local `main` tracks `task-flow-2/main` and is intentionally ahead with local-only commits until the user pushes manually.
- `origin` still points to the older TaskFlow repository. Use the `task-flow-2` remote for the new showcase repo unless intentionally updating the old repo.
- Runtime DB artifacts were removed from Git tracking and should remain ignored.
- README encoding and architecture wording have been corrected.
- The current tests pass, but coverage is still concentrated on unit-level state/codec behavior; end-to-end transport flows need stronger proof.
- RabbitMQ mode is improved but should still be presented as experimental until live broker integration, acknowledgement timing, backpressure, and recovery behavior are tested.

## Next-Day Handoff

Start here before writing new feature code:

- Run `git status --short --branch`.
- Expect `main` to stay ahead of `task-flow-2/main` because the user asked to keep changes local and push manually.
- Use the wording "coordinated peer-to-peer" for the main architecture. Do not describe the TCP/GUI runtime as producer/worker.
- Be precise about RabbitMQ: RabbitMQ now uses broker-backed peer IDs and peer-specific routing, but it still needs live broker integration proof before being presented as production-grade.
- Do not present RabbitMQ as resume-grade production infrastructure until live integration, backpressure, dead-letter handling, and recovery behavior are implemented and tested.
- If `.\mvnw.cmd clean test` fails while deleting `target`, check for a local Windows file lock before treating it as a source failure.

## Current Maven Modules

Root pom.xml is packaging=pom and includes:

- taskflow-spi
- taskflow-core
- taskflow-plugin-conversion
- taskflow-transport-rabbitmq
- taskflow-coordinator
- taskflow-worker
- taskflow-gui

## Important Files And Responsibilities

### Root

- pom.xml
  Maven reactor parent and dependency management.

- README.md
  Current project overview and runtime commands.

- PLAN.md
  This handoff plan.

### SPI

- taskflow-spi/src/main/java/server/job/TaskPlugin.java
  Coordinator-side plugin contract for creating jobs from JobSubmitMessage.

- taskflow-spi/src/main/java/peer/engine/WorkerPlugin.java
  Historical peer-side plugin contract for creating task processors.

- taskflow-spi/src/main/java/peer/engine/TaskProcessor.java
  Peer task execution contract.

- taskflow-spi/src/main/java/server/job/EmbarrassinglyParallelJob.java
  Base abstraction for jobs that split into independent task units.

- taskflow-spi/src/main/java/server/job/TaskUnit.java
  Task lifecycle state, retries, timing, and assignment ownership.

- taskflow-spi/src/main/java/protocol/*.java
  Protocol messages shared by TCP and RabbitMQ runtimes.

- taskflow-spi/src/main/java/transport/TransportConnection.java
  Connection abstraction used by PeerInfo for direct peer communication.

- taskflow-spi/src/main/java/transport/BrokerTransport.java
  Broker-style transport abstraction for publish/subscribe flows.

- taskflow-spi/src/main/java/transport/TransportRoute.java
  Logical broker routes: JOB_SUBMIT, TASK_ASSIGN, TASK_RESULT, JOB_RESULT, HEARTBEAT.

- taskflow-spi/src/main/java/transport/OutboundTransportMessage.java
  Broker outbound envelope.

- taskflow-spi/src/main/java/transport/InboundTransportMessage.java
  Broker inbound envelope.

- taskflow-spi/src/main/java/transport/TransportAcknowledgement.java
  Ack, requeue, and reject contract for broker deliveries.

- taskflow-spi/src/main/java/transport/TransportMessageHandler.java
  Broker delivery handler contract.

### Core

- taskflow-core/src/main/java/server/job/JobFactory.java
  Uses ServiceLoader<TaskPlugin> instead of hardcoded task-type branching.

- taskflow-core/src/main/java/server/scheduler/TaskScheduler.java
  Scheduler loop, message handling, task dispatch, timeout handling, retry handling, result aggregation, and metrics.

- taskflow-core/src/main/java/server/scheduler/SchedulerOutput.java
  Output abstraction used by TaskScheduler to send task assignments and final job results.

- taskflow-core/src/main/java/server/scheduler/PeerRegistrySchedulerOutput.java
  TCP/default SchedulerOutput implementation that sends through PeerInfo and PeerRegistry.

- taskflow-core/src/main/java/server/scheduler/SchedulerMetrics.java
  Scheduler metrics snapshot and counters.

- taskflow-core/src/main/java/server/registry/PeerInfo.java
  Peer state and load tracking through TransportConnection.

- taskflow-core/src/main/java/server/registry/InMemoryPeerRegistry.java
  Current in-memory peer registry.

- taskflow-core/src/main/java/server/monitor/PeerLivenessMonitor.java
  TCP peer heartbeat/liveness monitor.

- taskflow-core/src/main/java/server/db/DatabaseManager.java
  SQLite-backed runtime history. This should eventually move behind a state-store SPI.

### Conversion Plugin

- taskflow-plugin-conversion/src/main/java/server/concreteJobs/conversion/ImageConversionTaskPlugin.java
  Registers image conversion as both TaskPlugin and WorkerPlugin.

- taskflow-plugin-conversion/src/main/java/server/concreteJobs/conversion/VideoTranscodingTaskPlugin.java
  Registers video transcoding as both TaskPlugin and WorkerPlugin.

- taskflow-plugin-conversion/src/main/resources/META-INF/services/server.job.TaskPlugin
  ServiceLoader provider registration for coordinator-side job plugins.

- taskflow-plugin-conversion/src/main/resources/META-INF/services/peer.engine.WorkerPlugin
  ServiceLoader provider registration for peer-side processor plugins.

### RabbitMQ Transport

- taskflow-transport-rabbitmq/src/main/java/transport/rabbitmq/RabbitMqTransport.java
  RabbitMQ implementation of BrokerTransport.

- taskflow-transport-rabbitmq/src/main/java/transport/rabbitmq/RabbitMqTransportConfig.java
  RabbitMQ configuration and environment-variable parsing.

- taskflow-transport-rabbitmq/src/main/java/transport/rabbitmq/RabbitMqTopology.java
  Exchange, queue, and route mapping.

- taskflow-transport-rabbitmq/src/main/java/transport/rabbitmq/RabbitMqMessageCodec.java
  JSON envelope codec for protocol messages.

- taskflow-transport-rabbitmq/src/main/java/transport/rabbitmq/RabbitMqAcknowledgement.java
  Manual ack, requeue, and reject wrapper.

- taskflow-transport-rabbitmq/src/main/java/transport/rabbitmq/RabbitMqRuntimeDefaults.java
  Shared RabbitMQ runtime node IDs.

### Coordinator Runtime

- taskflow-coordinator/src/main/java/server/TaskCoordinatorServer.java
  Main coordinator entry point. Defaults to TCP and delegates to RabbitMqTaskCoordinatorServer when TASKFLOW_TRANSPORT=rabbitmq.

- taskflow-coordinator/src/main/java/server/RabbitMqTaskCoordinatorServer.java
  RabbitMQ coordinator runtime. Subscribes to JOB_SUBMIT and TASK_RESULT, feeds the scheduler mailbox, and uses RabbitMqSchedulerOutput.

- taskflow-coordinator/src/main/java/server/rabbitmq/RabbitMqSchedulerOutput.java
  SchedulerOutput implementation that publishes TASK_ASSIGN and JOB_RESULT through BrokerTransport.

- taskflow-coordinator/src/main/java/server/handler/PeerHandler.java
  TCP peer handler. Wraps socket output through TcpPeerConnection.

- taskflow-coordinator/src/main/java/server/transport/TcpPeerConnection.java
  TCP implementation of TransportConnection.

### Command-Line Peer Runtime

- taskflow-worker/src/main/java/peer/PeerNode.java
  Main command-line peer entry point. Defaults to TCP and delegates to RabbitMqPeerNode when TASKFLOW_TRANSPORT=rabbitmq.

- taskflow-worker/src/main/java/peer/RabbitMqPeerNode.java
  RabbitMQ peer execution runtime. Subscribes to TASK_ASSIGN, executes tasks, publishes TASK_RESULT, and relies on transport auto-ack after successful handler completion.

- taskflow-worker/src/main/java/peer/engine/PeerExecutionEngine.java
  Peer execution engine. Discovers WorkerPlugin implementations through ServiceLoader and exposes executeTask for broker runtime plus submitTask for TCP runtime.

### GUI Runtime

- taskflow-gui/src/main/java/GUI/PeerApp.java
  JavaFX peer. Submits jobs and executes assigned tasks over TCP.

- taskflow-gui/src/main/java/GUI/FileUtils.java
  GUI helper utilities.

## RabbitMQ Runtime Design

Current RabbitMQ routes:

- jobs.submit maps to taskflow.jobs
- tasks.assign maps to taskflow.tasks
- tasks.result maps to taskflow.task-results
- jobs.result maps to taskflow.job-results
- heartbeats maps to taskflow.heartbeats

RabbitMQ configuration defaults:

- TASKFLOW_RABBITMQ_HOST defaults to localhost
- TASKFLOW_RABBITMQ_PORT defaults to 5672
- TASKFLOW_RABBITMQ_USERNAME defaults to guest
- TASKFLOW_RABBITMQ_PASSWORD defaults to guest
- TASKFLOW_RABBITMQ_VHOST defaults to /
- TASKFLOW_RABBITMQ_EXCHANGE defaults to taskflow.exchange
- TASKFLOW_RABBITMQ_QUEUE_PREFIX defaults to taskflow
- TASKFLOW_RABBITMQ_DURABLE defaults to true
- TASKFLOW_RABBITMQ_PREFETCH defaults to 3

Runtime selector:

- TASKFLOW_TRANSPORT=tcp uses the existing TCP runtime.
- TASKFLOW_TRANSPORT=rabbitmq uses RabbitMQ coordinator/peer runtime.
- If TASKFLOW_TRANSPORT is unset, TCP is used.

Implemented coordinated peer-to-peer RabbitMQ behavior:

- Each RabbitMQ peer starts with `TASKFLOW_PEER_ID` or generates a unique runtime peer ID.
- Each RabbitMQ peer can publish JOB_SUBMIT and consume TASK_ASSIGN.
- The coordinator tracks RabbitMQ peers individually through heartbeat messages.
- Task assignment routes to the selected peer through a peer-specific queue.
- Each peer publishes TASK_RESULT with its own peer ID so scheduler ownership, metrics, and retries remain accurate.
- JOB_RESULT routes back to the submitting peer through a peer-specific result queue.

Remaining RabbitMQ productionization work:

- Broker acknowledgement should happen only after the message is safely accepted by the scheduler/state path.
- A live RabbitMQ integration profile should verify topology declaration, publish, consume, ack, requeue, reject, and end-to-end job completion.
- Dead-letter exchange/queue behavior is not implemented.
- Broker-aware backpressure and restart recovery are not implemented.

## Why RabbitMQ First, Not Kafka

RabbitMQ still fits the coordinated TaskFlow model because it provides message routing, per-message acknowledgement, backpressure, retry, and dead-letter semantics without forcing the project into retained event-stream architecture.

In the target design, the broker is transport infrastructure, not the architecture. Peers still remain peers: each can submit work, receive assigned work, execute tasks, and receive final job results.

Kafka is better for retained event streams, replay, and event pipelines. It can support task processing, but task leasing, per-task acknowledgement, retry, and dead-letter handling require more framework code.

Decision:

- Use RabbitMQ as the first broker transport.
- Keep the broker SPI broad enough that Kafka can be added later as a separate adapter.

## Verification Results

Latest verified commands:

```powershell
.\mvnw.cmd test
```

Result on 2026-06-03 at 23:05 Asia/Bangkok:

- Build success.
- 15 tests passed.
- 0 failures.

Clean verification attempt:

```powershell
.\mvnw.cmd clean test
```

Result on 2026-06-03 at 23:05 Asia/Bangkok:

- Build failed during Maven clean before source compilation.
- Windows refused deletion of generated target directories, for example target/test-classes and taskflow-spi/target/test-classes.
- This appears to be a local file-lock or stale workspace artifact problem, not a source compile failure.
- Before showcasing, confirm a clean clone can run `.\mvnw.cmd clean test` successfully.

```powershell
.\mvnw.cmd package -DskipTests
```

Result on 2026-06-02 at 17:50 Asia/Bangkok:

- Build success.
- All 8 modules packaged.
- Coordinator jar-with-dependencies builds.

Known warnings:

- JavaFX dependency model warnings.
- GUI deprecated/unchecked warnings.
- Maven/Jansi/Guava future JDK native-access warnings.

These are warnings only, not current blockers.

## How To Run Current Runtime

Run TCP coordinator, default mode:

```powershell
.\mvnw.cmd -pl taskflow-coordinator exec:java
```

Run TCP command-line peer:

```powershell
.\mvnw.cmd -pl taskflow-worker exec:java -Dexec.args="localhost 6789"
```

Run GUI TCP peer:

```powershell
.\mvnw.cmd -pl taskflow-gui javafx:run
```

Run RabbitMQ coordinator after RabbitMQ is available at localhost:5672:

```powershell
$env:TASKFLOW_TRANSPORT = "rabbitmq"
.\mvnw.cmd -pl taskflow-coordinator exec:java
```

Run RabbitMQ command-line peer after RabbitMQ is available at localhost:5672:

```powershell
$env:TASKFLOW_TRANSPORT = "rabbitmq"
.\mvnw.cmd -pl taskflow-worker exec:java
```

Reset to TCP in the same PowerShell session:

```powershell
$env:TASKFLOW_TRANSPORT = "tcp"
```

## Known Gaps

Review-critical showcase gaps:

- Video transcoding currently records image frames only and drops audio.
- Runtime paths still use System.out, System.err, and printStackTrace instead of a logging framework.
- Several comments explain obvious mechanics instead of documenting invariants, tradeoffs, or non-obvious behavior.
- No one-command demo exists yet.
- README architecture diagram exists, but it must stay aligned with the coordinated peer-to-peer model.
- No demo video exists yet.
- No CI pipeline exists yet.
- No benchmark or throughput comparison exists yet.
- No fault-injection demo proves retry and recovery behavior yet.
- No clean-clone validation from the Task-Flow-2 remote has been recorded yet.

RabbitMQ runtime gaps:

- No live RabbitMQ integration test yet.
- RabbitMQ peer submitter exists for command-line mode, but not for the JavaFX GUI.
- Job submit messages are acked after enqueueing into the scheduler mailbox, not after durable persistence.
- RabbitMQ end-to-end behavior has not been validated against a live broker.
- Broker backpressure and restart recovery are not implemented.
- No dead-letter exchange configuration yet.

Framework gaps:

- DatabaseManager is still SQLite-specific and lives in core.
- There is no StateStore SPI yet.
- PostgreSQL and Flyway are not implemented.
- No restart recovery workflow exists yet.
- GUI is not separated into a pure client module.

Repo hygiene notes:

- Local `main` currently tracks `task-flow-2/main`.
- Local `main` is ahead by one commit: `7218b42 Align GUI peer terminology`.
- `origin` still points to the older TaskFlow repository.
- `taskflow.db` may exist locally as an ignored runtime artifact; do not add it back to Git.
- Do not run destructive cleanup commands without explicit user approval.

Current .gitignore now ignores:

- taskflow.db
- taskflow.db-shm
- taskflow.db-wal
- java/in_PEER_*/
- java/out_PEER_*/

Runtime DB files have already been removed from Git tracking in the modular refactor commit. If they ever reappear as tracked files, remove them from the index rather than deleting local runtime data:

```powershell
git rm --cached taskflow.db taskflow.db-shm taskflow.db-wal
```

## Next Implementation Slices

Recommended next goal: make the repository showcase-ready before adding more feature surface.

### Slice 1: Repository And Presentation Hardening

Objective: a reviewer can clone the repository, read the README, and immediately understand that this is a serious distributed systems project.

- Push or intentionally hold local commit `7218b42 Align GUI peer terminology`.
- Validate a fresh clone from `https://github.com/HuuPhu-Nguyen/Task-Flow-2.git`.
- Verify `.gitignore` excludes generated files, IDE files, runtime DBs, temporary peer folders, and build outputs.
- Keep README architecture wording focused on coordinated peer-to-peer, not producer/worker.
- Add demo media, benchmark numbers, and clean-clone proof.
- Confirm `.\mvnw.cmd clean test` succeeds from a clean clone.

Acceptance:

- `git status --short` contains only intentional source/doc changes before commit.
- No runtime DB, target output, IDE workspace state, or temporary conversion folders are tracked.
- README renders cleanly on GitHub.
- A new reader can identify the coordinator, scheduler, peer runtime, plugin, transport, and persistence boundaries within two minutes.

### Slice 2: Correctness And Safety Fixes

Objective: remove behavior that can hang users, lose results silently, or write unsafe files.

- Send failed JOB_RESULT responses when job creation fails.
- Reject empty payload jobs or complete them deterministically with a failed result.
- Teach the GUI to display failed JOB_RESULT messages instead of opening the download flow.
- Sanitize output filenames before writing results to disk.
- Preserve deterministic result ordering or return an explicit taskId-to-result structure.
- Document or implement audio preservation for video transcoding.
- Add tests for invalid job type, empty payload, failed task terminal state, duplicate/stale result rejection, and GUI-safe filename handling.

Acceptance:

- No supported job submission path can leave a client waiting forever without a terminal result.
- Failed jobs are visible to the user and persisted as failed.
- Result saving cannot escape the selected directory.
- Unit tests cover the major failure paths.

### Slice 3: Observability And Professional Runtime Behavior

Objective: make runtime behavior easy to inspect during demos and credible in code review.

- Replace direct System.out/System.err/printStackTrace usage in runtime paths with SLF4J and Logback.
- Keep scheduler event logs structured.
- Add clear metrics for queue depth, active jobs, dispatch latency, retries, success/failure counts, and peer utilization.
- Consider a simple HTTP metrics endpoint or a metrics CLI output mode.
- Remove stale or mechanical comments and keep only comments that explain invariants or tradeoffs.

Acceptance:

- Logs are consistent, levelled, and suitable for debugging.
- Demo output clearly shows task assignment, retries, completion, and failures.
- Comments improve maintainability rather than repeating the code.

### Slice 4: One-Command Demo And Fault Injection

Objective: demonstrate the distributed-system value instead of only describing it.

- Add Docker Compose for RabbitMQ, coordinator, and multiple peers.
- Add a sample job input set small enough to run quickly.
- Add a CLI demo path that can submit a job and receive a result without the GUI.
- Add a fault-injection script that kills one peer during execution and shows retry/recovery.
- Add a short demo video or GIF for the README.
- Add a benchmark section comparing 1 peer vs 3 peers vs 5 peers.

Acceptance:

- A reviewer can run the demo with one command or follow a short script.
- The demo visibly distributes work across peers.
- The fault-injection demo proves timeout/retry behavior.
- README includes concrete benchmark numbers.

### Slice 5: RabbitMQ Productionization

Objective: make RabbitMQ mode honest, tested, and meaningfully distributed.

- Add a live RabbitMQ integration profile.
- Keep the integration profile disabled by default.
- Test topology declaration, publish, consume, ack, requeue, and reject against a real broker.
- Harden the broker-aware peer submitter with live broker coverage.
- Add explicit broker backpressure and peer capacity behavior.
- Revisit acknowledgement timing so submitted jobs are not acked before durable scheduler/state-store acceptance.
- Add dead-letter exchange and dead-letter queue support.

Acceptance:

- RabbitMQ mode has an end-to-end test or reproducible demo.
- Multiple RabbitMQ peers increase actual parallel throughput.
- Broker delivery semantics are documented honestly and match the implementation.

### Slice 6: Durable State And Recovery

Objective: move from demo persistence to credible task orchestration persistence.

- Add a StateStore SPI.
- Move SQLite DatabaseManager behind a SQLite state-store implementation.
- Add PostgreSQL and Flyway migrations.
- Model jobs, tasks, attempts, leases, retries, and terminal states.
- Add restart recovery tests.

Acceptance:

- Coordinator restart can recover unfinished jobs or mark them safely according to documented semantics.
- State schema is versioned.
- Runtime history remains queryable.

## Standout Deliverables

These are the deliverables that should make TaskFlow stand out among personal projects:

- One-command distributed demo with coordinator, broker, and multiple peers.
- Architecture diagram in README.
- Fault-injection demo showing peer failure, retry, and eventual job completion.
- Benchmark table showing throughput improvement as peer count increases.
- Execution guarantees document with implemented guarantees, tradeoffs, and limitations.
- Plugin example beyond conversion, such as word count or checksum, to prove the framework is not hardcoded to media conversion.
- CI pipeline with build, test, and formatting checks.
- Structured logs or metrics output that make scheduling behavior visible.
- Short demo video or GIF linked from README.
- Clean release/tag suitable for resume links.

## Resume-Grade Quality Gates

Before this project is linked on a resume:

- `git status --short` is clean after commit.
- Fresh clone can run `.\mvnw.cmd clean test`.
- Fresh clone can run the documented demo.
- README has no encoding corruption, broken commands, or stale claims.
- README explains what makes the system technically interesting in the first screen.
- No tracked runtime artifacts exist.
- No IDE workspace files are tracked.
- No generated Maven target files are tracked.
- No user-specific absolute paths are required.
- Failure paths return terminal results instead of hanging.
- GUI clearly handles success and failure.
- File output paths are sanitized.
- RabbitMQ claims match tested behavior.
- At least one end-to-end distributed run is documented.
- At least one failure-recovery scenario is documented.
- Tests cover core scheduling and failure semantics.

## Acceptance Criteria

Completed since latest review:

- Modular Maven layout with clear framework boundaries.
- Plugin loading through ServiceLoader.
- Core job creation no longer hardcodes concrete job classes.
- Peer execution no longer hardcodes concrete processors.
- Core peer tracking no longer depends directly on socket APIs.
- Scheduler dispatch is abstracted behind SchedulerOutput.
- RabbitMQ broker transport module exists and has no-broker tests.
- RabbitMQ coordinator and command-line peer entry points exist.
- TASKFLOW_TRANSPORT selects tcp or rabbitmq, with tcp as default.
- RabbitMQ command-line peers now use real peer IDs with heartbeat registration.
- RabbitMQ task assignment and job-result delivery now use peer-specific routes.
- RabbitMQ command-line peers can submit jobs through the broker.
- Existing tests pass.
- All modules package.

Completed:

- README encoding cleanup for the known mojibake findings.
- Failed job startup now returns a failed JOB_RESULT instead of only logging.
- Zero-task jobs are rejected during scheduler startup handling.
- GUI now displays failed JOB_RESULT messages instead of opening the download flow.
- GUI output filenames are sanitized before writing to disk.
- Conversion result aggregation is deterministic by task index.
- Tracked runtime DB artifacts were removed from the Git index.
- Scheduler regression test covers unsupported job startup failure.
- Refactor has been committed as a clean modular Maven migration.
- README now includes stronger project positioning, technical differentiators, an architecture diagram, and known limitations.
- Repository now has `.gitattributes` for consistent text and binary handling.

Pending:

- Push or intentionally hold local commit `7218b42 Align GUI peer terminology`.
- README demo media and benchmark polish.
- One-command demo.
- Demo video or GIF.
- CI pipeline.
- Benchmark section.
- Fault-injection demo.
- More scheduler failure-path tests beyond unsupported job startup.
- Logging framework adoption.
- Live RabbitMQ integration test.
- Harden RabbitMQ peer submitter with live broker tests.
- Better broker-mode peer capacity and backpressure model.
- Dead-letter queue support.
- PostgreSQL and Flyway state-store implementation.
- Restart recovery from durable state.

## Commands To Resume

Check status:

```powershell
git status --short --branch
```

Run tests:

```powershell
.\mvnw.cmd test
```

Package all modules:

```powershell
.\mvnw.cmd package -DskipTests
```

Run TCP coordinator:

```powershell
.\mvnw.cmd -pl taskflow-coordinator exec:java
```

Run TCP command-line peer:

```powershell
.\mvnw.cmd -pl taskflow-worker exec:java -Dexec.args="localhost 6789"
```

Run current GUI:

```powershell
.\mvnw.cmd -pl taskflow-gui javafx:run
```

Run RabbitMQ coordinator:

```powershell
$env:TASKFLOW_TRANSPORT = "rabbitmq"
.\mvnw.cmd -pl taskflow-coordinator exec:java
```

Run RabbitMQ command-line peer:

```powershell
$env:TASKFLOW_TRANSPORT = "rabbitmq"
.\mvnw.cmd -pl taskflow-worker exec:java
```
