# TF-0707 Scaling Experiment

The report-grade matrix passed on 2026-07-29. On this workload and host,
completion throughput rose from 101.603 tasks/s with one executor worker to a
maximum observed 123.602 tasks/s with two workers, then remained between
116.320 and 122.131 tasks/s at four and eight workers. Eight-worker parallel
efficiency was 15.026%, so this result is a measured coordinator-side scaling
plateau, not a linear-scaling or production-capacity claim.

## Reproduce

Prerequisites are Git, Docker Engine, PowerShell 5.1 or newer, and a JDK
version 21 or newer on `PATH`. From a clean checkout of the measured revision:

```powershell
git checkout 719717a6672a30f3b39a3f8f7858b93d6d0edd9d
git status --short
.\scripts\verify-scaling.ps1
```

The [PowerShell verifier](../../scripts/verify-scaling.ps1) fixes the matrix at
1, 2, 4, and 8 workers. It rejects a dirty report-grade checkout, fewer than
10,000 measured tasks, invalid bounded inputs, or an existing output
directory. Each point runs in a fresh Maven/Surefire JVM, starts a fresh
RabbitMQ Testcontainers container, creates a fresh schema-v14 SQLite database,
and starts the requested executor workers as separate child JVMs. The wrapper
then validates configuration, terminal and authority audits, CSV row counts,
worker startup and clean exit evidence, outbox and broker drain, and raw-file
checksums.

`-Calibration` and its optional `-AllowDirty` switch exist only for developing
the harness. Their results are not report-grade evidence.

Generated raw evidence is intentionally ignored under `target/scaling/`;
rerunning the command regenerates these links:

- [matrix and calculated efficiency](../../target/scaling/matrix.csv);
- [generated run summary](../../target/scaling/summary.md);
- [run identity and wrapper inputs](../../target/scaling/run.properties);
- [machine, JVM, Maven, and Docker profile](../../target/scaling/environment.txt);
- [raw-file SHA-256 manifest](../../target/scaling/checksums.sha256);
- one-, two-, four-, and eight-worker
  [point directories](../../target/scaling/), each containing configuration
  and metrics properties, 10,000 task-latency rows, SQLite write samples,
  resource samples, worker metrics and lifecycle evidence, and the audited
  SQLite database;
- Maven output in `target/scaling/workers-N.maven.log`.

The report-grade bundle contained 96 checksummed files. Independent
recalculation matched every entry in `checksums.sha256`; the manifest file
itself had SHA-256
`8270d55db9e1e956017e257cb1675759df7a3893bce49ad5952cb6feaa8e24c5`.
The matrix checksum recorded inside the manifest was
`0085661344d8f45cfd87a7a4b90d84147b2c723e38aae745ff77ccd33b7841af`.

## Tested revision and environment

| Item | Recorded value |
|---|---|
| Harness commit | `719717a6672a30f3b39a3f8f7858b93d6d0edd9d` |
| Worktree / report grade | Clean / `true` |
| Start time | 2026-07-29 00:56:59 UTC+07:00 |
| Time zone | SE Asia Standard Time |
| OS | Windows 11, JVM-reported `10.0.26200.0`, X64 |
| CPU | 13th Gen Intel Core i5-13500HX, 20 logical processors |
| Physical memory | 68,425,736,192 bytes (63.73 GiB) |
| Java | Oracle Java `25.0.2` LTS, HotSpot 64-bit Server VM |
| Compiler target | Java release 21 |
| Maven | Wrapper-managed Apache Maven `3.9.9` |
| Docker Engine | `29.6.1` |
| RabbitMQ | `rabbitmq:3.13-management` |
| Coordinator-harness JVM | `-Xms256m -Xmx1g` |
| Each worker JVM | `-Xms32m -Xmx128m` |

## Workload and measurement definitions

Every matrix point used the same inputs:

| Input | Value |
|---|---:|
| Warm-up | 1,000 tasks in 4 jobs |
| Measured work | 10,000 tasks in 40 jobs |
| Tasks per job | 250 |
| Executor capacity per worker | 1 |
| Task body | 300,000 deterministic integer-mix iterations |
| Payload | 128 UTF-8-compatible characters, carried in the broker message |
| Resource sample interval | 100 ms |
| Completion timeout | 900 seconds per point |
| Run order | 1, 2, 4, then 8 workers |
| Repetitions | One report-grade run per point |

The harness uses the production `TaskScheduler`, schema-v14
`DatabaseManager`, transactional coordinator outbox, protocol-v2 messages, and
a real RabbitMQ broker. The requester and bounded resource/queue samplers run
in the coordinator harness process; each executor worker runs in a separate
JVM.

The measurements mean:

- Completion duration starts immediately before measured job publication and
  ends after all measured tasks and jobs are durably `COMPLETED`. Throughput is
  10,000 divided by that duration.
- Assignment latency starts at the requester timestamp immediately before its
  confirmed job publication and ends at `task_attempts.started_at` for the
  authoritative successful attempt. End-to-end latency uses the same start and
  the durable `tasks.completed_at` timestamp. These persisted timestamps have
  millisecond resolution. Percentiles use nearest rank across exactly 10,000
  measured tasks.
- Coordinator CPU is the process CPU-time delta divided by measured drain
  duration. Core percent treats one fully busy logical core as 100%; host
  percent divides that value by the 20 logical processors. This is the
  coordinator-requester-sampler JVM, not an isolated scheduler metric.
- Heap is `MemoryMXBean` used heap in that same JVM. No forced garbage
  collection is used.
- Worker utilization is the sum of time spent in the deterministic task body
  divided by measured drain duration and worker count. It excludes broker
  delivery and confirmed result publication, so it is compute utilization,
  not whole-process CPU.
- RabbitMQ depth is the management API `messages` total (ready plus unacked)
  across all queues in the run namespace, sampled approximately every 100 ms.
- SQLite latency times existing top-level mutation calls while the measured
  workload runs. It is not disk-device latency or a single SQL-statement
  profile.
- Parallel efficiency is measured throughput divided by one-worker throughput
  and worker count.

Assignments use the same test-only fast publisher seam as TF-0706:
predeclared per-participant routes, persistent messages, publisher confirms,
production protocol encoding, and durable outbox intent remain in the path,
while the production adapter's fixed mandatory-return observation delay is
not paid for every known-routable assignment.

## Results

### Completion throughput and scaling

| Workers | Completion duration (s) | Throughput (tasks/s) | Speedup vs. 1 worker | Parallel efficiency |
|---:|---:|---:|---:|---:|
| 1 | 98.423 | 101.603 | 1.000x | 100.000% |
| 2 | 80.905 | 123.602 | 1.217x | 60.826% |
| 4 | 85.970 | 116.320 | 1.145x | 28.621% |
| 8 | 81.879 | 122.131 | 1.202x | 15.026% |

The complete four-point wrapper took 461.711 seconds, including four Maven
invocations, JVM and container startup, warm-up, validation, shutdown, and
checksum generation. That wrapper time is not used to calculate throughput.

### Task latency

All values are milliseconds.

| Workers | Assignment p50 | Assignment p95 | Assignment p99 | Assignment max | End-to-end p50 | End-to-end p95 | End-to-end p99 | End-to-end max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 53,654 | 93,514 | 97,127 | 97,842 | 53,658 | 93,518 | 97,130 | 97,847 |
| 2 | 40,030 | 76,485 | 79,792 | 80,496 | 40,040 | 76,496 | 79,806 | 80,502 |
| 4 | 42,748 | 81,143 | 84,701 | 85,377 | 42,775 | 81,169 | 84,726 | 85,392 |
| 8 | 40,443 | 77,134 | 80,405 | 81,167 | 40,495 | 77,181 | 80,453 | 81,195 |

### Coordinator and worker observations

| Workers | Coordinator CPU, core % | Coordinator CPU, host % | Heap before (bytes) | Peak heap (bytes) | Worker compute utilization | Worker executions | Extra executions |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 35.147% | 1.757% | 57,184,728 | 174,190,456 | 8.260% | 10,022 | 22 |
| 2 | 38.451% | 1.923% | 57,494,944 | 174,139,200 | 4.739% | 10,029 | 29 |
| 4 | 35.276% | 1.764% | 59,805,248 | 174,247,016 | 2.259% | 10,033 | 33 |
| 8 | 37.363% | 1.868% | 59,384,024 | 174,131,528 | 1.215% | 10,030 | 30 |

Executor assignment was balanced: the two-worker authoritative distribution
was 5,010/4,990; the four-worker range was 2,498-2,503; and the eight-worker
range was 1,249-1,251. The 22-33 extra executions are expected evidence of
at-least-once delivery/publication behavior and are reported separately from
authoritative completion. The durable audit found exactly one matching
successful authoritative attempt per measured task in every point.

### RabbitMQ and SQLite

All SQLite values are milliseconds.

| Workers | RabbitMQ depth p50/p95/p99/max | SQLite writes | SQLite p50 | SQLite p95 | SQLite p99 | SQLite max |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 / 0 / 0 / 0 | 40,182 | 1.572 | 2.114 | 2.897 | 37.601 |
| 2 | 0 / 0 / 0 / 0 | 40,181 | 1.380 | 2.109 | 2.797 | 14.199 |
| 4 | 0 / 0 / 0 / 0 | 40,194 | 1.402 | 2.127 | 2.819 | 18.118 |
| 8 | 0 / 0 / 0 / 0 | 40,182 | 1.385 | 2.091 | 2.878 | 26.325 |

The write samples cover measured job acceptance, peer-capacity updates, task
assignment plus outbox intent, task-result commits including extra deliveries,
job completion plus final-result outbox intent, and publication marking.
Resource sample counts were 919, 755, 802, and 764 for the 1/2/4/8 points.

Zero sampled broker depth does not mean that no work was waiting. Capacity
gating publishes assignments only when an executor advertises availability,
so most waiting work remains durable coordinator/task state rather than a
RabbitMQ backlog. The 100 ms management sample can also miss shorter queue
occupancy.

## Integrity audit

The wrapper and an independent read-only SQLite query agreed at every point:

| Audit field | 1 worker | 2 workers | 4 workers | 8 workers |
|---|---:|---:|---:|---:|
| Schema version | 14 | 14 | 14 | 14 |
| Measured jobs / completed | 40 / 40 | 40 / 40 | 40 / 40 | 40 / 40 |
| Measured tasks / completed | 10,000 / 10,000 | 10,000 / 10,000 | 10,000 / 10,000 | 10,000 / 10,000 |
| Successful authoritative attempts | 10,000 | 10,000 | 10,000 | 10,000 |
| Running attempts | 0 | 0 | 0 | 0 |
| Pending measured tasks | 0 | 0 | 0 | 0 |
| Pending outbox rows at completion | 0 | 0 | 0 | 0 |
| Broker messages at completion | 0 | 0 | 0 | 0 |

The raw task-latency CSVs each contained exactly 10,000 rows; SQLite write CSV
counts were 40,182, 40,181, 40,194, and 40,182; resource sample counts matched
their metrics properties; worker-metric row counts matched 1/2/4/8; every
worker produced ready, stop, and metrics evidence, exited cleanly, and
produced no failure signal. `PRAGMA integrity_check` returned `ok` for all four
databases.

## Bottleneck interpretation

Two workers delivered the highest observed throughput. Four workers regressed
5.9% from that peak, and eight workers remained 1.190% below it. At the same
time, aggregate worker compute utilization fell from 8.260% to 1.215%, while
coordinator-harness CPU stayed around 35-38% of one logical core and broker
queue depth never accumulated. Adding executor compute capacity therefore
does not address the limiting path for this task shape.

Assignment and end-to-end percentiles differ by only milliseconds to tens of
milliseconds, while assignment p50 is 40-54 seconds. Because all measured
jobs are submitted near the start, the dominant observed latency is waiting
for the coordinator to durably assign work, not executing the lightweight
task after assignment.

These observations narrow the bottleneck to the serial coordinator control
path around scheduling, SQLite mutations, durable outbox publication, broker
confirms, result handling, and capacity updates. The experiment did not
profile those components separately, so it does not claim that one specific
method, SQLite alone, or RabbitMQ alone is the root cause. The stable
approximately 2.1 ms SQLite write p95 and low process CPU are consistent with
wait-heavy serialized control-plane work, but further profiling would be
required before tuning.

## Scope and limitations

- This is one ordered run per point on one host. It has no repetitions,
  randomized order, confidence intervals, controlled thermal state, or
  multi-host network effects.
- The coordinator, requester, and samplers share one JVM. Coordinator CPU and
  heap therefore include requester publication, result collection, the
  RabbitMQ management client, and raw sample retention.
- Executors are separate JVMs but run a synchronous lightweight integer-mix
  fixture at capacity one. The result does not predict native conversion,
  plugin, object-store, disk-streaming, or externally rate-limited workloads.
- Payloads are 128 bytes and remain in broker messages. MinIO transfer and
  object-orphan cleanup are outside this experiment.
- Each point uses one coordinator, one local SQLite database, and one
  single-node RabbitMQ container. Multiple coordinators, RabbitMQ clustering,
  broker failover, and distributed storage are not tested.
- The fast assignment publisher is a test-only seam described above.
  Production mandatory-return observation behavior can have different
  latency.
- RabbitMQ management sampling at 100 ms perturbs the coordinator-harness
  process and cannot observe shorter queue spikes.
- Worker utilization measures only deterministic task-body wall time. It is
  not worker-process CPU utilization.
- At-least-once extra executions are included in worker busy time but excluded
  from authoritative completion throughput.
- Recovery timing is assigned to TF-0708, overload behavior to TF-0709, and
  neither is established by this report.
- No target RPS or latency SLO existed for TF-0707, and none is inferred from
  this single-host experiment. These numbers are reproducible evidence for
  this exact harness and revision, not a production sizing recommendation.
