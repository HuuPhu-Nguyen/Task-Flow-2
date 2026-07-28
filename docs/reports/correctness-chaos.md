# TF-0706 Correctness Chaos Experiment

The report-grade run passed on 2026-07-28. It accepted and completed 100,000
lightweight tasks while injecting duplicate publication, beyond-lease results,
executor transport termination, a real RabbitMQ container restart, and a
coordinator runtime restart. Every configured job reached `COMPLETED`, and the
post-run SQLite audit found no duplicate authoritative success, stale success,
lost accepted task, running attempt, or pending outbox row.

## Reproduce

Prerequisites are Git, Docker Engine, PowerShell 5.1 or newer, and a JDK version
21 or newer on `PATH`. The measured host used Java 25 while Maven compiled the
project for Java 21.

From a clean checkout of the tested commit:

```powershell
git checkout e03dc91a9c1015be3086bdb81c6034b03661f2c3
git status --short
.\scripts\verify-correctness-chaos.ps1
```

The
[PowerShell verifier](../../scripts/verify-correctness-chaos.ps1)
rejects a dirty report-grade checkout, an existing evidence directory, fewer
than 100,000 tasks, and any duplicate-publication ratio other than exactly 5%.
It runs the opt-in
[correctness experiment](../../taskflow-coordinator/src/test/java/server/CorrectnessChaosExperiment.java)
outside the normal unit-test job, validates the final audit and required event
markers, and records SHA-256 checksums. Development-only `-Calibration` and
`-AllowDirty` runs are not report-grade evidence.

Generated raw evidence is intentionally ignored under
`target/correctness-chaos/`; rerunning the command regenerates these links:

- [run identity and wrapper inputs](../../target/correctness-chaos/run.properties);
- [experiment configuration](../../target/correctness-chaos/configuration.properties);
- [final invariant audit](../../target/correctness-chaos/audit.properties);
- [structured fault/event log](../../target/correctness-chaos/events.jsonl);
- [audited SQLite database](../../target/correctness-chaos/correctness-chaos.db);
- [machine, JVM, Maven, and Docker profile](../../target/correctness-chaos/environment.txt);
- [complete Maven output](../../target/correctness-chaos/maven.log);
- [generated summary](../../target/correctness-chaos/summary.md);
- [raw-file SHA-256 manifest](../../target/correctness-chaos/checksums.sha256).

## Tested revision and environment

| Item | Recorded value |
|---|---|
| Tested commit | `e03dc91a9c1015be3086bdb81c6034b03661f2c3` |
| Worktree / report grade | Clean / `true` |
| Date and time zone | 2026-07-28, SE Asia Standard Time (UTC+07:00) |
| OS | Windows 11, JVM-reported `10.0.26200.0`, X64 |
| CPU | 13th Gen Intel Core i5-13500HX, 20 logical processors |
| Physical memory | 68,425,736,192 bytes |
| Java | Oracle Java `25.0.2`, HotSpot 64-bit Server VM |
| Maven | Wrapper-managed Apache Maven `3.9.9` |
| Docker Engine | `29.6.1` |
| Experiment JVM | `-Xms256m -Xmx2g` |
| RabbitMQ / Toxiproxy images | `rabbitmq:3.13-management`, `ghcr.io/shopify/toxiproxy:2.12.0` |

## Configuration

| Input | Value |
|---|---:|
| Seed | `55707398` |
| Tasks / jobs | 100,000 / 400 |
| Tasks per job | 250 |
| Executor fixtures | 4 |
| Advertised capacity per executor | 8 |
| Duplicate assignment publication | 5,000 (exactly 5%) |
| Duplicate result publication | 5,000 (exactly 5%) |
| Beyond-lease delayed results | 1,000 |
| Executor transport terminations | 10 |
| Broker restart threshold | 33,333 completed tasks |
| Coordinator restart threshold | 66,667 completed tasks |
| Lease / selected result delay | 2,000 ms / 2,500 ms |
| Convergence timeout | 1,800 seconds |

The seed deterministically selects duplicate, delayed, poison, and termination
ordinals. Assignment and result fixtures publish the production JSON protocol
as persistent messages over predeclared RabbitMQ routes and wait for publisher
confirms. Production RabbitMQ consumers, retry/quarantine topology,
`TaskScheduler`, SQLite `DatabaseManager`, transactional outbox, fencing,
startup recovery, and capacity ledger remain in the exercised path.

The run injects one deterministic poison assignment until RabbitMQ exhausts
its bounded delivery policy and quarantines it. It closes and recreates ten
executor transports, stops and starts the real RabbitMQ Testcontainers
container at its stable Toxiproxy endpoint, and later closes and recreates the
coordinator transport, scheduler, outbox replayer, registry projection, and
SQLite handle against the same database. Failures stop after the coordinator
restart and the remaining seed-selected injections, after which the harness
waits for durable convergence.

## Results

| Required outcome or observation | Result |
|---|---:|
| Accepted / completed tasks | 100,000 / 100,000 |
| Terminal / non-completed jobs | 400 / 0 |
| Completed tasks without result payload | 0 |
| Tasks without exactly one `SUCCEEDED` attempt | 0 |
| Successes whose attempt/assignment differs from the task authority | 0 |
| Remaining `RUNNING` attempts | 0 |
| Duplicate assignment / result publications | 5,000 / 5,000 |
| Scheduler duplicate / stale-result classifications | 4,945 / 1,373 |
| Delayed results published | 1,000 |
| Executor / broker / coordinator restarts | 10 / 1 / 1 |
| Pending outbox rows observed during broker outage | 23 |
| Pending outbox rows after recovery | 0 |
| Poison deliveries / quarantined messages | 3 / 1 |
| Minimum sampled active-task count | 0 |
| Capacity projection validity | Valid throughout sampled run |
| Harness / Surefire / wrapper wall time | 931.768 s / 935.5 s / 945.6 s |

An independent read-only JDBC query after the wrapper completed found schema
version 14, 400 completed jobs, 100,000 completed tasks, 100,000 succeeded
attempts, 1,340 `RETRY_SCHEDULED` attempts, 101,340 assignment-outbox rows, 400
final-result-outbox rows, and all three schema-v14 query indexes. The same query
returned zero non-completed jobs/tasks, missing task results, tasks with a
non-singleton success set, stale authoritative successes, running attempts,
and pending outbox rows.

All required fault markers were present: one broker restart start/completion,
one coordinator restart start/completion, ten executor termination
start/completions, 1,000 delayed-result publications, and one experiment
completion. All eight checksums in `checksums.sha256` were recalculated after
the run and matched.

## Scaling defect found while producing this evidence

The first clean 100,000-task attempt at commit
`39cb1c6e88112b511bd000854f02c9cc25beb3b7` timed out after 1,800 seconds with
only 34,462 completed tasks. Per-result finalization queried `tasks.job_id`
without an index, repeatedly scanning the entire accepted task table; the
experiment also sampled a full-table progress count every 25 ms.

The tested commit adds SQLite schema v14 query indexes for job-scoped task and
attempt reads and pending-outbox replay, with clean-schema and v13 migration
coverage. It also changes only the experiment's progress sampling interval to
one second. A 10,000-task calibration fell from 137.353 seconds to 111.011
seconds, and the report-grade workload then completed in 931.768 seconds.
These measurements explain and validate the blocking fix; they are not a
general throughput or scaling claim.

## Scope and limitations

- The supported authority model remains one coordinator and SQLite database.
  Multi-coordinator writes, RabbitMQ clustering, and broker failover are not
  tested.
- RabbitMQ is a real restarted container. Coordinator and executor failures
  are component/transport restarts inside one test JVM, not operating-system
  process kills. The separate
  [process crash-window matrix](../CRASH_WINDOW_MATRIX.md)
  covers selected OS-process termination windows.
- Executors are synchronous, test-only lightweight fixtures. They exercise
  real broker delivery, protocol encoding, confirms, scheduler persistence,
  leases, fencing, and recovery, but not plugin CPU/native-library behavior.
- Payloads remain broker-sized and do not exercise MinIO or object-orphan
  collection. Payload-integrity and object-lifecycle claims rely on their
  separate contract and crash-window evidence.
- This report records one deterministic seed on one machine. It is a bounded
  correctness experiment, not exhaustive state-space proof or a scaling,
  latency, CPU, heap, or utilization benchmark.
- At-least-once execution and publication remain intentional. The result
  proves one authoritative task completion under the measured faults; it does
  not make arbitrary external plugin side effects exactly once.
