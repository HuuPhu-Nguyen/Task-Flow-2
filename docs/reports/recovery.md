# TF-0708 Recovery Experiment

The report-grade run passed on 2026-07-29. On this host, the production
liveness monitor detected stopped executor-role heartbeats in 90.013 seconds,
the scheduler reassigned an expired lease in 22 ms, and coordinator startup
reconstructed 1,000, 10,000, and 100,000 persisted tasks in 78.814 ms,
345.555 ms, and 2.657 seconds. A real RabbitMQ restart followed by delivery
and durable publication of 100 pending rows took 34.736 seconds. Separate
steady-state outbox replay delivered and marked 500 rows at 3.821 rows/s, and
the production orphan collector deleted 1,000 MinIO objects at
374.977 objects/s.

These are one-host observations with explicitly bounded fixtures, not
recovery SLOs or production-capacity claims.

## Reproduce

Prerequisites are Git, Docker Engine, PowerShell 5.1 or newer, and a JDK
version 21 or newer on `PATH`. From a clean checkout of the measured harness:

```powershell
git checkout 9b313fd2794039a23cf2623d21d1c67a4a058908
git status --short
.\scripts\verify-recovery.ps1
```

The [PowerShell verifier](../../scripts/verify-recovery.ps1) rejects a dirty
report-grade checkout, changed workload constants, invalid bounded inputs, or
an existing output directory. It invokes the opt-in
[recovery experiment](../../taskflow-coordinator/src/test/java/server/RecoveryExperiment.java),
validates every required property and exact artifact cardinality, checks the
SQLite integrity results and bounded batch evidence, and creates and
re-verifies a SHA-256 manifest.

`-Calibration` and its optional `-AllowDirty` switch exist only for harness
development. Calibration values are not report-grade evidence.

Generated raw evidence is intentionally ignored under
`target/recovery/report/`; rerunning the command regenerates these links:

- [generated summary](../../target/recovery/report/summary.md);
- [run identity and fixed inputs](../../target/recovery/report/run.properties);
- [machine, JVM, Maven, Docker, and image profile](../../target/recovery/report/environment.txt);
- [metrics](../../target/recovery/report/run/metrics.properties) and
  [durable audit](../../target/recovery/report/run/audit.properties);
- [experiment configuration](../../target/recovery/report/run/configuration.properties);
- [lease assignment timeline](../../target/recovery/report/run/lease-assignments.csv);
- [coordinator-restart](../../target/recovery/report/run/coordinator-restart.properties),
  [10,000-task](../../target/recovery/report/run/persisted-10000.properties),
  and
  [100,000-task](../../target/recovery/report/run/persisted-100000.properties)
  recovery sidecars and their adjacent SQLite databases;
- [outbox delivery identities](../../target/recovery/report/run/outbox-deliveries.txt)
  and the `outbox-replay.db` and `rabbitmq-restart.db` durable records;
- [orphan object identities](../../target/recovery/report/run/orphan-object-keys.txt)
  and the `orphan-cleanup.db` durable retry record;
- [complete Maven output](../../target/recovery/report/recovery.maven.log);
- [raw-file SHA-256 manifest](../../target/recovery/report/checksums.sha256).

The report bundle contained 21 checksummed files. Independent recalculation
matched every entry. The manifest file itself had SHA-256
`5bf8b57d3375fe469e51f54a524edf23b39c069492b0a5fe493d9e51cacd04ac`.

## Tested revision and environment

| Item | Recorded value |
|---|---|
| Harness commit | `9b313fd2794039a23cf2623d21d1c67a4a058908` |
| Worktree / report grade | Clean / `true` |
| Start time | 2026-07-29 13:22:08 UTC+07:00 |
| Time zone | SE Asia Standard Time |
| OS | Windows 11, JVM-reported `10.0.26200.0`, X64 |
| CPU | 13th Gen Intel Core i5-13500HX, 20 logical processors |
| Physical memory | 68,425,736,192 bytes (63.73 GiB) |
| Java | Oracle Java `25.0.2` LTS, HotSpot 64-bit Server VM |
| Compiler target | Java release 21 |
| Maven | Wrapper-managed Apache Maven `3.9.9` |
| Docker Engine | `29.6.1` |
| Harness JVM | `-Xms256m -Xmx2g` |
| RabbitMQ | `rabbitmq:3.13-management` |
| Stable broker endpoint | `ghcr.io/shopify/toxiproxy:2.12.0` |
| Object store | `minio/minio:RELEASE.2025-04-22T22-12-26Z` |

The complete wrapper took 296.191 seconds, including Maven startup, all
fixtures, container lifecycle, validation, and checksum generation. Wrapper
time is not used to calculate any recovery or throughput metric.

## Workload and measurement definitions

| Input | Value |
|---|---:|
| Coordinator-restart fixture | 1,000 tasks in 4 jobs |
| Persisted recovery fixtures | 10,000 / 100,000 tasks in 40 / 400 jobs |
| Tasks per job | 250 |
| Worker heartbeat timeout | 90,000 ms |
| Task lease | 1,000 ms |
| Steady outbox fixture | 500 rows |
| Broker-restart outbox fixture | 100 rows |
| Orphan fixture | 1,000 objects |
| Replay / cleanup batch bound | 100 |
| Completion timeout | 900 seconds |
| Warm-up | None |
| Repetitions | One report-grade run per scenario |

All SQLite fixtures are created through the production schema-v14
`DatabaseManager`. Job fixtures are accepted through
`commitJobSubmission`; outbox rows are inserted through the coordinator's
durable outbox API. RabbitMQ publication uses the production transport,
mandatory routing, persistent messages, publisher confirms, subscriber
settlement, and publication marking. Orphan cleanup uses the ServiceLoader
selected production MinIO adapter, database-backed ownership classification,
and production bounded collector.

The reported measurements mean:

- **Worker failure detection** starts immediately after the last synthetic
  executor-role heartbeat and ends when `PeerLivenessMonitor` removes that
  executor and invokes its timeout callback. It includes the configured
  90-second timeout and monitor scan scheduling. Heartbeats then cease; no
  executor operating-system process is killed.
- **Lease expiry to reassignment** starts at generation 1's durable lease
  deadline and ends when the production scheduler emits the already-persisted
  generation 2 assignment. The scheduler then accepts generation 2's result,
  and the database must show generation 1 as `RETRY_SCHEDULED` with
  `lease_expired` and generation 2 as `SUCCEEDED`.
- **Coordinator restart recovery** starts before reopening the closed SQLite
  database and ends after `CoordinatorStartupRecovery` returns the exact
  reconstructed active-job snapshots. It includes database construction and
  schema validation. The coordinator components are reconstructed in one
  Surefire JVM; this is not JVM process startup time.
- **10,000- and 100,000-task recovery** uses the same boundary and task shape
  as coordinator restart. Seeding is measured separately and excluded from
  recovery time.
- **RabbitMQ restart recovery** starts before restarting the stopped real
  broker container, after the production transport has observed the outage.
  It ends only after all 100 precommitted rows have been confirmed,
  delivered and acknowledged, and durably marked published.
- **Outbox replay throughput** starts before constructing the production
  replayer and ends after all 500 precommitted rows are confirmed, delivered
  and acknowledged, and durably marked published. It is rows divided by that
  duration, not enqueue throughput.
- **Object-orphan cleanup rate** starts before constructing the production
  collector and ends after all 1,000 pre-created 15-byte attempt-output
  objects for unknown tasks are deleted. Object creation time is excluded.

## Results

| Required measurement | Fixture | Result |
|---|---:|---:|
| Worker failure detection | 90,000 ms timeout | 90,012.642 ms |
| Lease expiry to reassignment | generation 1 to 2 | 22.000 ms |
| Coordinator restart recovery | 1,000 tasks | 78.814 ms |
| Persisted-task recovery | 10,000 tasks | 345.555 ms |
| Persisted-task recovery | 100,000 tasks | 2,656.545 ms |
| RabbitMQ restart recovery | restart + 100-row drain | 34,736.486 ms |
| Outbox replay | 500 rows | 130,868.936 ms / 3.821 rows/s |
| Object-orphan cleanup | 1,000 objects | 2,666.832 ms / 374.977 objects/s |

Persisted fixture setup and recovery remained separate:

| Fixture | Seed time | Recovery time | Recovered jobs / tasks |
|---|---:|---:|---:|
| Coordinator restart | 51.668 ms | 78.814 ms | 4 / 1,000 |
| Persisted 10,000 | 192.389 ms | 345.555 ms | 40 / 10,000 |
| Persisted 100,000 | 1,515.480 ms | 2,656.545 ms | 400 / 100,000 |

The RabbitMQ value is dominated by real broker stop/start, readiness, client
recovery, and confirmed replay. The separate 3.821 rows/s value exposes the
current production publisher's per-row mandatory-return/confirm path on this
local broker; it must not be interpreted as SQLite insert capacity or a
batched broker-publish limit.

## Integrity audit

The wrapper, experiment assertions, and independent read-only `sqlite3`
queries agreed:

| Audit | Observed result |
|---|---:|
| Coordinator restart | schema 14; 4 `RUNNING` jobs; 1,000 `PENDING` tasks |
| Persisted 10,000 | schema 14; 40 `RUNNING` jobs; 10,000 `PENDING` tasks |
| Persisted 100,000 | schema 14; 400 `RUNNING` jobs; 100,000 `PENDING` tasks |
| Lease attempts | 2; generation 1 `RETRY_SCHEDULED`, generation 2 `SUCCEEDED` |
| Lease authority after completion | generation 2 assignment ID |
| Steady outbox | 500 published; 0 pending |
| Restart outbox | 100 published; 0 pending |
| Broker deliveries | 600 raw / 600 unique |
| Orphan collector | 10 batches; maximum 100 examined; 0 retry rows |
| Remaining orphan objects | 0 |
| SQLite integrity | `ok` for all seven databases |

The delivery artifact contained exactly 600 identities, the orphan artifact
exactly 1,000 keys, and the lease CSV one header plus two assignment rows.
Generation 1 expired at epoch millisecond `1785306227555`; generation 2 was
captured at `1785306227577`, matching the reported 22 ms delay. All
Testcontainers resources exited after the run, and the verifier driver
recorded no stderr.

## Scope and limitations

- This is one run per scenario on one host, with no warm-up, repeated trials,
  confidence intervals, controlled thermal state, or multi-host network
  effects. Millisecond differences should not be generalized beyond this
  environment.
- Worker failure is heartbeat cessation observed by the production liveness
  monitor, not an executor process kill. The result measures coordinator
  detection policy and scheduling, not plugin interruption or recovery of
  executor-local work.
- Coordinator recovery closes and reopens SQLite and reconstructs production
  coordinator state in one test JVM. It excludes operating-system process
  launch, dependency injection, configuration parsing, and service
  supervision. The separate
  [process crash-window matrix](../CRASH_WINDOW_MATRIX.md) covers selected
  process-termination windows.
- Persisted fixtures contain lightweight pending `RABBITMQ_TEST_TASK`
  records. They measure durable state loading and reconstruction, not task
  execution, large payload transfer, or native plugin initialization.
- RabbitMQ is one real container behind a stable Toxiproxy endpoint and is
  restarted with its container filesystem intact. Clustering, mirrored
  queues, failover, durable-volume loss, and remote-broker latency are not
  tested.
- Outbox throughput is 500 sequential production publications to a local
  broker. It includes mandatory routing observation, confirms, consumer
  acknowledgement, and SQLite publication marking, and is not a maximum
  broker or database throughput claim.
- Orphan objects are 15 bytes each and all represent old attempt outputs for
  unknown tasks. Upload time, large-object deletion, mixed live/orphan
  listings, MinIO clustering, and object-store outage recovery are outside
  this rate measurement and remain covered only by separate contract and
  failure tests.
- The supported authority model remains one coordinator and one SQLite
  database. Multiple coordinators, power-loss filesystem semantics, and
  exactly-once arbitrary plugin side effects remain out of scope.
