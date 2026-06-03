# TaskFlow Framework Migration Plan

Last updated: 2026-06-03 21:50 Asia/Bangkok

## Showcase Objective

TaskFlow should be developed and presented as a resume-grade distributed systems project, not only as a course demo.

The target positioning is:

> TaskFlow is a Java 21 distributed task execution platform with pluggable job processors, TCP and RabbitMQ transports, fault-tolerant scheduling, persisted job history, and documented execution guarantees.

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

TaskFlow has moved from a single Maven demo project into a modular framework foundation for distributed task processing.

Completed so far:

- Converted the project into a Maven reactor with separate SPI, core, plugin, transport, coordinator, worker, and GUI modules.
- Added plugin contracts for coordinator-side job creation and worker-side task processing.
- Moved image and video conversion into a concrete conversion plugin module loaded through Java ServiceLoader.
- Removed hardcoded image/video job branching from core job creation.
- Added a worker processor discovery path through ServiceLoader.
- Added a TransportConnection abstraction so core peer tracking no longer depends directly on java.net.Socket.
- Added a SchedulerOutput abstraction so the scheduler can dispatch through TCP or a broker transport.
- Added a RabbitMQ transport module with topology declaration, JSON message envelopes, publish/subscribe, manual ack, requeue, reject, and prefetch configuration.
- Added RabbitMQ coordinator and worker runtime entry points.
- Added TASKFLOW_TRANSPORT mode selection while keeping TCP as the default runtime.
- Updated README runtime notes for the current transport state.

Current runtime status:

- TCP is still the default coordinator/worker transport.
- RabbitMQ can be selected with TASKFLOW_TRANSPORT=rabbitmq.
- RabbitMQ coordinator/worker runtime exists, but still needs a live broker integration test and a broker-aware client submitter.
- Runtime state store is still SQLite through DatabaseManager.
- PostgreSQL/Flyway migration has not started.
- GUI is still a JavaFX TCP demo client/worker. It is not yet a RabbitMQ client.

Showcase readiness status:

- The core architectural direction is strong: modular Maven reactor, SPI contracts, plugin discovery, scheduler output abstraction, and transport abstraction.
- The repository is not ready to publish yet because the refactor is still represented as untracked modules plus deleted legacy files in Git.
- Runtime DB artifacts were previously tracked and must be removed from Git tracking before publishing.
- README currently contains visible encoding corruption and must be fixed before being shown to recruiters or reviewers.
- The current tests pass, but coverage is concentrated on unit-level state/codec behavior; scheduler failure paths and end-to-end transport flows need stronger proof.
- RabbitMQ mode is transitional and should be presented as experimental until worker identity, acknowledgement timing, and integration tests are fixed.

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
  Worker-side plugin contract for creating task processors.

- taskflow-spi/src/main/java/peer/engine/TaskProcessor.java
  Worker task execution contract.

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
  ServiceLoader provider registration for worker-side processor plugins.

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

### Worker Runtime

- taskflow-worker/src/main/java/peer/PeerNode.java
  Main worker entry point. Defaults to TCP and delegates to RabbitMqPeerNode when TASKFLOW_TRANSPORT=rabbitmq.

- taskflow-worker/src/main/java/peer/RabbitMqPeerNode.java
  RabbitMQ worker runtime. Subscribes to TASK_ASSIGN, executes tasks, publishes TASK_RESULT, and relies on transport auto-ack after successful handler completion.

- taskflow-worker/src/main/java/peer/engine/PeerExecutionEngine.java
  Worker execution engine. Discovers WorkerPlugin implementations through ServiceLoader and exposes executeTask for broker runtime plus submitTask for TCP runtime.

### GUI Runtime

- taskflow-gui/src/main/java/GUI/PeerApp.java
  JavaFX demo client/worker. Still uses TCP.

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
- TASKFLOW_TRANSPORT=rabbitmq uses RabbitMQ coordinator/worker runtime.
- If TASKFLOW_TRANSPORT is unset, TCP is used.

Transitional broker-mode compromise:

- RabbitMQ runtime registers one synthetic peer: RABBITMQ_WORKER_POOL.
- The scheduler still owns task assignment and result ownership checks.
- RabbitMqSchedulerOutput rewrites assigned TASK_ASSIGN node IDs to the selected synthetic peer.
- RabbitMqTaskCoordinatorServer normalizes TASK_RESULT sender IDs to RABBITMQ_WORKER_POOL.
- This keeps existing scheduler semantics intact for the first broker runtime.
- Limitation: per-worker metrics are not accurate in broker mode yet.
- Limitation: broker concurrency is effectively capped by MAX_TASKS_PER_PEER for the synthetic peer, currently 3.

## Why RabbitMQ First, Not Kafka

RabbitMQ fits the current TaskFlow model because TaskFlow is a work-queue system:

- a coordinator produces units of work
- workers compete for tasks
- task attempts need acknowledgement
- failed or unacked deliveries should be retried or rejected

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

Run TCP command-line worker:

```powershell
.\mvnw.cmd -pl taskflow-worker exec:java -Dexec.args="localhost 6789"
```

Run GUI TCP client/worker:

```powershell
.\mvnw.cmd -pl taskflow-gui javafx:run
```

Run RabbitMQ coordinator after RabbitMQ is available at localhost:5672:

```powershell
$env:TASKFLOW_TRANSPORT = "rabbitmq"
.\mvnw.cmd -pl taskflow-coordinator exec:java
```

Run RabbitMQ worker after RabbitMQ is available at localhost:5672:

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
- No architecture diagram or demo video exists yet.
- No CI pipeline exists yet.
- No benchmark or throughput comparison exists yet.
- No fault-injection demo proves retry and recovery behavior yet.
- No clean-clone validation has been recorded yet.

RabbitMQ runtime gaps:

- No live RabbitMQ integration test yet.
- No RabbitMQ client submitter yet, so the GUI still submits jobs only through TCP.
- Broker mode uses one synthetic peer instead of tracking each worker separately.
- Broker mode concurrency is currently limited by the synthetic peer and MAX_TASKS_PER_PEER.
- Job submit messages are acked after enqueueing into the scheduler mailbox, not after durable persistence.
- No dead-letter exchange configuration yet.

Framework gaps:

- DatabaseManager is still SQLite-specific and lives in core.
- There is no StateStore SPI yet.
- PostgreSQL and Flyway are not implemented.
- No restart recovery workflow exists yet.
- GUI is not separated into a pure client module.

Repo hygiene gaps:

- The worktree contains many deletes under src/main/java because code was moved into modules.
- PLAN.md and module directories are currently untracked until committed.
- taskflow.db and taskflow.db-shm/taskflow.db-wal were tracked runtime artifacts before .gitignore cleanup.
- Do not run destructive cleanup commands without explicit user approval.

Current .gitignore now ignores:

- taskflow.db
- taskflow.db-shm
- taskflow.db-wal
- java/in_PEER_*/
- java/out_PEER_*/

Suggested safe cleanup later, only if the user wants runtime DB files removed from Git tracking:

```powershell
git rm --cached taskflow.db taskflow.db-shm taskflow.db-wal
```

## Next Implementation Slices

Recommended next goal: make the repository showcase-ready before adding more feature surface.

### Slice 1: Repository And Presentation Hardening

Objective: a reviewer can clone the repository, read the README, and immediately understand that this is a serious distributed systems project.

- Commit or stage the modular refactor cleanly.
- Remove tracked runtime artifacts from Git tracking.
- Verify `.gitignore` excludes generated files, IDE files, runtime DBs, temporary peer folders, and build outputs.
- Fix README encoding corruption.
- Rewrite README opening around the stronger platform positioning.
- Add a high-level architecture diagram to README.
- Add a short module responsibility table.
- Add a "Why this is technically interesting" section.
- Add a "Known limitations" section that is accurate and professional.
- Confirm `.\mvnw.cmd clean test` succeeds from a clean clone.

Acceptance:

- `git status --short` contains only intentional source/doc changes before commit.
- No runtime DB, target output, IDE workspace state, or temporary conversion folders are tracked.
- README renders cleanly on GitHub.
- A new reader can identify the coordinator, scheduler, worker, plugin, transport, and persistence boundaries within two minutes.

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
- Add clear metrics for queue depth, active jobs, dispatch latency, retries, success/failure counts, and worker utilization.
- Consider a simple HTTP metrics endpoint or a metrics CLI output mode.
- Remove stale or mechanical comments and keep only comments that explain invariants or tradeoffs.

Acceptance:

- Logs are consistent, levelled, and suitable for debugging.
- Demo output clearly shows task assignment, retries, completion, and failures.
- Comments improve maintainability rather than repeating the code.

### Slice 4: One-Command Demo And Fault Injection

Objective: demonstrate the distributed-system value instead of only describing it.

- Add Docker Compose for RabbitMQ, coordinator, and multiple workers.
- Add a sample job input set small enough to run quickly.
- Add a CLI demo path that can submit a job and receive a result without the GUI.
- Add a fault-injection script that kills one worker during execution and shows retry/recovery.
- Add a short demo video or GIF for the README.
- Add a benchmark section comparing 1 worker vs 3 workers vs 5 workers.

Acceptance:

- A reviewer can run the demo with one command or follow a short script.
- The demo visibly distributes work across workers.
- The fault-injection demo proves timeout/retry behavior.
- README includes concrete benchmark numbers.

### Slice 5: RabbitMQ Productionization

Objective: make RabbitMQ mode honest, tested, and meaningfully distributed.

- Add a live RabbitMQ integration profile.
- Keep the integration profile disabled by default.
- Test topology declaration, publish, consume, ack, requeue, and reject against a real broker.
- Add a broker-aware client submitter.
- Replace the single synthetic peer with broker-aware worker identity, worker slots, or explicit broker backpressure.
- Revisit acknowledgement timing so submitted jobs are not acked before durable scheduler/state-store acceptance.
- Add dead-letter exchange and dead-letter queue support.

Acceptance:

- RabbitMQ mode has an end-to-end test or reproducible demo.
- Multiple RabbitMQ workers increase actual parallel throughput.
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

- One-command distributed demo with coordinator, broker, and multiple workers.
- Architecture diagram in README.
- Fault-injection demo showing worker failure, retry, and eventual job completion.
- Benchmark table showing throughput improvement as worker count increases.
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
- Worker execution no longer hardcodes concrete processors.
- Core peer tracking no longer depends directly on socket APIs.
- Scheduler dispatch is abstracted behind SchedulerOutput.
- RabbitMQ broker transport module exists and has no-broker tests.
- RabbitMQ coordinator and worker entry points exist.
- TASKFLOW_TRANSPORT selects tcp or rabbitmq, with tcp as default.
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
- Refactor has been staged as a modular Maven migration rather than untracked replacement files.
- README now includes stronger project positioning, technical differentiators, an architecture diagram, and known limitations.
- Repository now has `.gitattributes` for consistent text and binary handling.

Pending:

- Commit the staged refactor after final review.
- README demo media and benchmark polish.
- One-command demo.
- Demo video or GIF.
- CI pipeline.
- Benchmark section.
- Fault-injection demo.
- More scheduler failure-path tests beyond unsupported job startup.
- Logging framework adoption.
- Live RabbitMQ integration test.
- RabbitMQ client submitter.
- Better broker-mode worker/backpressure model.
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

Run TCP worker:

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

Run RabbitMQ worker:

```powershell
$env:TASKFLOW_TRANSPORT = "rabbitmq"
.\mvnw.cmd -pl taskflow-worker exec:java
```
