# TaskFlow Reproducible Baseline

This report freezes the Phase 0 baseline measured on 2026-07-22. The measured
source revision is `ebd273a96b879ca45c056d16de691223e25a77cb`, and both
verification entry points reported a clean worktree at that revision.

This is a lightweight scheduler ceiling, not a production-capacity claim. It
uses the real `TaskScheduler`, mailbox, registry, task model, and result
handling, but uses in-process synthetic executor participants with no SQLite,
RabbitMQ, network serialization, or object storage in the measured path.

## Reproduce the baseline

Prerequisites:

- Git;
- a JDK version 21 or newer on `PATH`;
- network access for the Maven wrapper and dependencies if they are not already
  cached;
- PowerShell 5.1 or newer on Windows, or Bash on a POSIX environment.

From a clean checkout, run exactly one platform entry point:

```powershell
git checkout ebd273a96b879ca45c056d16de691223e25a77cb
git status --short
.\scripts\verify-baseline.ps1
```

```bash
git checkout ebd273a96b879ca45c056d16de691223e25a77cb
git status --short
./scripts/verify-baseline.sh
```

The versioned entry points are the
[PowerShell verifier](../../scripts/verify-baseline.ps1) and the
[Bash verifier](../../scripts/verify-baseline.sh). They reject a dirty
worktree, require Java 21 or newer, run the complete test gate, inventory the
source and Surefire reports, and then invoke the opt-in
[scheduler experiment](../../taskflow-core/src/test/java/server/scheduler/BaselineSchedulerExperiment.java)
for one and four executor participants.

The complete test commands run by the scripts are:

```powershell
.\mvnw.cmd --batch-mode --no-transfer-progress clean test
```

```bash
./mvnw --batch-mode --no-transfer-progress clean test
```

Generated evidence is intentionally untracked under `target/baseline/`:

- `environment.txt` and `inventory.properties`;
- `modules.txt` and `test-counts.properties`;
- `maven-test.log`;
- `workers-1.properties`, `workers-1.log`, `workers-4.properties`, and
  `workers-4.log`;
- `summary.md`.

Both scripts preserve Maven's nonzero exit status. This was checked by setting
`MAVEN_ARGS=-Dtest=DefinitelyMissingBaselineTest`: Surefire reported that no
matching test ran, and each verifier returned exit code `1` without starting
the experiments. The development-only dirty-worktree overrides (`-AllowDirty`
and `--allow-dirty`) must not be used for a published measurement.

## Measurement environment

| Item | Recorded value |
|---|---|
| Commit | `ebd273a96b879ca45c056d16de691223e25a77cb` |
| Time zone and date | Asia/Bangkok, 2026-07-22 |
| OS | Windows 11, Maven-reported version `10.0`, JVM-reported `10.0.26200.0`, `amd64`/`X64` |
| CPU | 13th Gen Intel Core i5-13500HX, 20 logical processors |
| Physical memory | 68,425,736,192 bytes (63.73 GiB) |
| Java | Oracle Java `25.0.2`, HotSpot 64-bit Server VM, build `25.0.2+10-LTS-69` |
| Compiler target | Maven `maven.compiler.release=21` |
| Maven | Apache Maven `3.9.9`, wrapper build `8e8579a9e76f7d015ee5ec7bfcdc97d260186937` |
| Experiment heap | `-Xms64m -Xmx512m` |

Java 25.0.2 was the installed runtime on the measurement host. The project is
compiled for Java 21, and the verifier accepts any Java 21-or-newer runtime,
but throughput from a different JDK is not directly comparable to the numbers
below.

## Reactor inventory

The reactor contained 24 POM projects:

- Framework/runtime (8): `taskflow-parent`, `taskflow-spi`, `taskflow-core`,
  `taskflow-persistence-sqlite`, `taskflow-transport-rabbitmq`,
  `taskflow-coordinator`, `taskflow-peer`, and `taskflow-gui`.
- Example plugin family (6): `taskflow-plugin-example-parent`,
  `taskflow-plugin-example-model`, `taskflow-plugin-example-server`,
  `taskflow-plugin-example-client`, `taskflow-plugin-example-peer`, and
  `taskflow-plugin-example-harness`.
- Conversion plugin family (5): `taskflow-plugin-conversion-parent`,
  `taskflow-plugin-conversion-model`, `taskflow-plugin-conversion-server`,
  `taskflow-plugin-conversion-client`, and
  `taskflow-plugin-conversion-peer`.
- Text plugin family (5): `taskflow-plugin-text-parent`,
  `taskflow-plugin-text-model`, `taskflow-plugin-text-server`,
  `taskflow-plugin-text-client`, and `taskflow-plugin-text-peer`.

## Test baseline

The scripts classify a suite as integration/live when its class name ends in
`IntegrationTest` or `LiveTest`; all other default Surefire suites are counted
as unit/component. The explicitly selected baseline experiment is excluded
from these counts.

| Classification | Suites | Discovered tests | Executed | Skipped | Failures | Errors |
|---|---:|---:|---:|---:|---:|---:|
| Unit/component | 70 | 399 | 398 | 1 | 0 | 0 |
| Integration/live | 2 | 10 | 0 | 10 | 0 | 0 |
| Total | 72 | 409 | 398 | 11 | 0 | 0 |

The two integration/live suites are
`server.rabbitmq.RabbitMqCoordinatorLiveIntegrationTest` (3 tests) and
`transport.rabbitmq.RabbitMqTransportLiveTest` (7 tests). They require a live
broker and explicit `-Dtaskflow.rabbitmq.live=true` or
`TASKFLOW_RABBITMQ_LIVE_TEST=true`, so the default clean test gate discovers
but skips them.

## Source baseline

| Fact | Value | Source |
|---|---:|---|
| `TaskScheduler.java` physical lines | 1,531 | [`TaskScheduler.java`](../../taskflow-core/src/main/java/server/scheduler/TaskScheduler.java) |
| Current protocol version | 1 | [`ProtocolVersions.java`](../../taskflow-spi/src/main/java/protocol/ProtocolVersions.java) |
| Current SQLite schema version | 9 | [`DatabaseManager.java`](../../taskflow-persistence-sqlite/src/main/java/server/db/DatabaseManager.java) |

The line count is a physical `Get-Content`/`wc -l` count, including comments
and blank lines. It is a change baseline, not a complexity score.

## Lightweight throughput and memory

Each measurement runs in a fresh Surefire JVM after a separate 1,000-task
warm-up job. The measured job contains exactly 10,000 tasks. Every task applies
64 deterministic integer-mixing iterations to a short string payload. Each
synthetic executor participant advertises the test task type and has a bounded
maximum concurrency of 3, giving aggregate advertised capacities of 3 and 12
for the one- and four-participant cases.

Timing starts before construction/submission of the measured job payload list
and stops when the scheduler emits its successful final job result. It excludes
Maven startup, JVM startup, compilation, and the warm-up job. Heap usage is
sampled through `MemoryMXBean` approximately every millisecond from the end of
warm-up through measured completion; no forced garbage collection is used.

Primary Windows verifier results:

| Executor participants | Duration (ns) | Throughput (tasks/s) | Heap before (bytes) | Peak used heap (bytes) | Peak used heap (MiB) | Heap increase (bytes) |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9,587,403,800 | 1,043.035 | 11,228,216 | 46,566,032 | 44.41 | 35,337,816 |
| 4 | 9,531,854,600 | 1,049.114 | 12,270,232 | 46,546,552 | 44.39 | 34,276,320 |

The four-participant run distributed assignments as `2502, 2499, 2499,
2500`; the one-participant run assigned all 10,000 tasks to its only
participant. Four participants improved the single observed throughput by only
0.58%. This synthetic task body is deliberately tiny, so the result indicates
that sequential scheduler/mailbox work dominates this workload. One sample per
configuration is insufficient for a statistical performance claim.

The memory values describe the coordinator-harness JVM. That JVM contains the
real scheduler plus the in-process synthetic executor pools and their result
messages, so 46.6 million bytes is not an isolated coordinator-process RSS and
must not be presented as one. It is the reproducible 10,000-task heap baseline
for comparing later scheduler changes under this same harness.

As a platform-entry-point parity check, the Bash verifier was then run from the
same clean commit on the same Windows host through Git Bash. It passed the full
reactor and recorded 1,043.483 tasks/s with one participant and 1,030.492
tasks/s with four; peak used heap was 46,450,240 and 46,530,928 bytes,
respectively. The small run-to-run movement reinforces that the primary table
is a baseline observation rather than a throughput guarantee.

## Known failures, flakes, skips, and warnings

- Known failing tests in the clean default gate: none. The clean Windows and
  Git Bash runs both completed with zero failures and zero errors.
- Observed timing-sensitive test: an earlier unchanged-code Phase 0 run failed
  once in
  [`TaskSchedulerPersistenceTest.brokerOutboxAssignmentStaysPendingWhenPublishFails`](../../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java).
  Its publish-attempt latch won a race with asynchronous failure recording. The
  exact isolated test passed immediately, and every subsequent full reactor run
  used for this baseline passed without a scheduler change. Treat this as a
  known flaky-test risk until its synchronization is made deterministic.
- Expected platform skip: one
  [`FileGuiRequesterTokenStoreTest`](../../taskflow-gui/src/test/java/gui/FileGuiRequesterTokenStoreTest.java)
  POSIX-permission assertion is skipped on Windows because the filesystem does
  not expose `PosixFileAttributeView`.
- Expected environment skips: all 10 live RabbitMQ tests are skipped unless the
  live-test flag and broker prerequisites are present. Therefore the default
  baseline does not establish live-broker health.
- Non-failing runtime warnings: JDK 25 reports restricted native access for
  Maven Jansi and SQLite JDBC, and deprecation warnings for Maven's Guava use of
  `sun.misc.Unsafe`. These did not change the zero-failure test result.

## Comparison boundary

Future reports should use the same scripts, JDK, heap bounds, 10,000 measured
tasks, 1,000 warm-up tasks, and 64 work units before comparing throughput or
heap. Changes to those inputs, the host power/thermal state, persistence,
transport, payload serialization, or executor implementation define a new
benchmark rather than a direct continuation of this baseline.
