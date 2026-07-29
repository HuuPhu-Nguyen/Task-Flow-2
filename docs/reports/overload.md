# TF-0709 Sustained Overload Experiment

The report-grade run passed on 2026-07-29. A real RabbitMQ broker retained
submissions while the production scheduler's capacity-`1` ordinary mailbox
lane and fixed capacity-`1` task-result reserve were both occupied. With 16
pending durable outbox rows holding new admission closed, the coordinator
classified 1,000 unique flood submissions as typed
`MAX_PENDING_OUTBOX_ROWS` rejections, continued committing accepted results
and expiring leases, replayed every durable outbox row, and accepted and
completed a fresh job without restarting.

All 1,004 submitted jobs were accounted for: 4 were durably accepted and
completed, and 1,000 were explicitly rejected. The final three retained-heap
samples spanned 46,048 bytes and had a maximum of 17,803,432 bytes in a fixed
256 MiB experiment JVM.

This is a one-host correctness and boundedness observation. It is not a
throughput benchmark, production sizing result, or overload RPS target.

## Reproduce

Prerequisites are Git, Docker Engine, PowerShell 5.1 or newer, and a JDK
version 21 or newer on `PATH`. From a clean checkout of the measured harness:

```powershell
git checkout 85d431e96cbfd2f7b05ad1f49bc7dd1df6e1a6cd
git status --short
.\scripts\verify-overload.ps1
```

The [PowerShell verifier](../../scripts/verify-overload.ps1) rejects a dirty
report-grade checkout, changed workload constants, invalid bounded inputs, or
an existing output directory. It launches the opt-in
[overload experiment](../../taskflow-coordinator/src/test/java/server/OverloadExperiment.java)
with its fixed JVM, validates the exact configuration, response and assignment
cardinalities, typed rejection fields, durable audit, broker drain, heap
criteria, and recovery state, and creates and re-verifies a SHA-256 manifest.

`-Calibration` and its optional `-AllowDirty` switch exist only for harness
development. Calibration values are not report-grade evidence.

Generated raw evidence is intentionally ignored under
`target/overload/report/`; rerunning the command regenerates these links:

- [run identity and environment](../../target/overload/report/environment.properties);
- [experiment configuration](../../target/overload/report/run/configuration.properties);
- [metrics](../../target/overload/report/run/metrics.properties);
- [durable audit](../../target/overload/report/run/audit.properties);
- [retained-heap samples](../../target/overload/report/run/heap-samples.csv);
- [assignment generations](../../target/overload/report/run/assignments.csv);
- [protocol response classifications](../../target/overload/report/run/responses.csv);
- [schema-v14 SQLite evidence](../../target/overload/report/run/overload.db);
- [complete Maven output](../../target/overload/report/overload.maven.log);
- [raw-file SHA-256 manifest](../../target/overload/report/checksums.sha256).

The report bundle contained nine checksummed files. Independent recalculation
matched every entry. The manifest file itself had SHA-256
`334848021235561242c2163bdb354489a29f3623122bd07a896f8ca5b929740e`.

## Tested revision and environment

| Item | Recorded value |
|---|---|
| Harness commit | `85d431e96cbfd2f7b05ad1f49bc7dd1df6e1a6cd` |
| Worktree / report grade | Clean / `true` |
| Start time | 2026-07-29 14:19:10 UTC+07:00 |
| Time zone | Asia/Bangkok |
| OS | Microsoft Windows, JVM-reported `10.0.26200.0` |
| CPU | 13th Gen Intel Core i5-13500HX, 20 logical processors |
| Physical memory | 68,425,736,192 bytes (63.73 GiB) |
| Java | Oracle Java `25.0.2` LTS |
| Compiler target | Java release 21 |
| Docker Engine | `29.6.1` |
| Experiment JVM | `-Xms256m -Xmx256m -XX:+UseSerialGC` |
| RabbitMQ | `rabbitmq:3.13-management` |

The complete wrapper took 282.143 seconds, including Maven startup, container
lifecycle, serial publisher-confirmed submissions, fixture execution, raw
output, validation, and checksums. That elapsed time is not used as a
coordinator throughput measurement.

## Workload and measurement definitions

| Input | Value |
|---|---:|
| Flood waves | 5 |
| Unique flood submissions per wave | 200 |
| Total unique flood submissions | 1,000 |
| Initially accepted one-task jobs | 3 |
| Post-pressure one-task job | 1 |
| Scheduler ordinary-lane capacity | 1 message |
| Fixed task-result reserve | 1 message |
| RabbitMQ `JOB_SUBMIT` prefetch | 1 |
| Active-job admission limit | 32 |
| Pending-outbox admission threshold | 16 rows |
| Task lease | 5,000 ms |
| Completion timeout | 300 seconds |
| Retained-heap plateau definition | final-three sample span at most 16 MiB |
| Retained-heap ceiling | final-three maximum below 128 MiB |
| Warm-up / repetitions | no separate warm-up / one report-grade run |

The experiment uses the production `RabbitMqTransport`, protocol-v2
envelopes, scheduler, priority mailbox, admission policy, schema-v14
`DatabaseManager`, and `RabbitMqOutboxReplayer`. RabbitMQ runs in a real
Testcontainers container. The scheduler and a synthetic executor fixture run
as separate threads in the same test JVM.

A test-only gated `BrokerOutboxPublisher` pauses one accepted assignment at
the existing durable-publication boundary. While the scheduler is paused, the
experiment inserts exactly 16 valid durable protocol messages through the
production outbox API, fills the ordinary lane, places accepted result traffic
in the fixed result reserve, leaves excess submissions broker-owned, and
allows an accepted lease to become due. It then opens the gate but deliberately
keeps outbox replay stopped while all five flood waves are classified.

The pending-outbox threshold is a precondition for accepting a new job, not a
universal hard cap on intents belonging to work that is already accepted. The
expected high-water mark is therefore 17 rows: 16 seeded rows plus the one
assignment intent already durably accepted at the controlled gate. The
experiment requires the overload projection to expose the threshold and
requires every flood response to name configured and observed values of 16.

After every wave, the experiment requests full GC and records retained heap.
The plateau calculation is the maximum minus the minimum of the final three
samples. This boundary measures retained heap under the fixed synthetic
fixture; it does not measure GC pause latency or prescribe production GC
tuning.

## Results

### Exact admission and queue accounting

| Metric | Observed result |
|---|---:|
| All submitted jobs | 1,004 |
| Durably accepted jobs | 4 |
| Typed flood rejections | 1,000 |
| Completed accepted jobs | 4 |
| Rejected IDs / unique rejected IDs | 1,000 / 1,000 |
| Rejection limit | 1,000 × `MAX_PENDING_OUTBOX_ROWS` |
| Rejection configured / observed value | 1,000 × `16` / `16` |
| Ordinary mailbox high water / capacity | 1 / 1 |
| Result-reserve high water / capacity | 1 / 1 |
| Broker-ready submission high water | 32 |
| Pending-outbox high water / threshold | 17 / 16 |
| Active-jobs high water / configured limit | 3 / 32 |

The outbox limit was intentionally made the first admission bottleneck. The
active-job high water therefore remained 3 rather than reaching 32. The real
broker queue's observed ready-message high water was 32 while one delivery
was permitted by the production route-local prefetch and one submission
occupied the scheduler ordinary lane.

The `responses.csv` artifact contains one header plus 1,004 responses: 1,000
`REJECTED` and 4 `COMPLETED`. There was no unexplained accepted, rejected, or
broker-owned submission after final drain.

### Result, expiry, and durable progress

| Metric or audit | Observed result |
|---|---:|
| Current task results committed | 4 |
| Lease-expiration retry transitions | 4 |
| Assignment rows / unique assignment IDs | 8 / 8 |
| Maximum attempt number | 3 |
| Succeeded attempts | 4 |
| `lease_expired` retry-scheduled attempts | 4 |
| Running attempts after recovery | 0 |
| Durable jobs / completed | 4 / 4 |
| Durable tasks / completed | 4 / 4 |
| Published outbox rows / pending | 28 / 0 |
| SQLite integrity | `ok` |

Before the flood had drained, the scheduler metrics showed both at least one
current result commitment and at least one lease expiration while fewer than
the initial accepted-plus-burst submissions had been classified. The
capacity-`1` result reserve was independently observed full during the same
controlled gate. Some execution overlapped the 5-second lease boundary, so
the final durable attempt audit is the authoritative statement: four expired
generations were retry-scheduled and four current generations succeeded. No
stale generation became a second authoritative result.

The assignment artifact records the exact generation pattern:

- `job-after-pressure` succeeded on attempt 1;
- `job-gate` and `job-initial-expiry` expired once and succeeded on attempt 2;
- `job-initial-result` expired twice and succeeded on attempt 3.

All accepted jobs reached a single durable terminal state, and all 28 outbox
rows were publisher-confirmed and marked published before the audit.

### Retained heap

| Wave | Retained heap |
|---:|---:|
| 1 | 18,389,680 bytes |
| 2 | 17,999,912 bytes |
| 3 | 17,803,432 bytes |
| 4 | 17,763,984 bytes |
| 5 | 17,757,384 bytes |

The final-three range was 17,757,384 to 17,803,432 bytes: a 46,048-byte
(approximately 0.044 MiB) span and a 17,803,432-byte (approximately
16.98 MiB) maximum. Those results are below the predefined 16 MiB span and
128 MiB retained-heap ceilings. The JVM-reported maximum heap was 259,522,560
bytes.

### Recovery without restart

Once the fixed waves were classified, the production outbox replayer started
and drained the pending backlog to zero in its configured bounded batches.
Both broker queues ended at zero ready messages. The same scheduler then
accepted and completed `job-after-pressure`; the final overload projection
cleared, `freshJobAcceptedAfterRecovery` was `true`, and `restartCount` was
zero.

## Verification

Before the report-grade run, the harness commit passed:

- `OverloadExperimentConfigTest` (3/3);
- the focused overload/configuration slice
  `SchedulerOverloadTest,SchedulerOverloadStatusTest,OverloadExperimentConfigTest`
  (9 tests across the affected reactor modules);
- the complete 25-module `mvn test` reactor;
- a real-RabbitMQ/SQLite calibration of the wrapper;
- PowerShell parsing, `git diff --check`, clean-tree, structured-commit, push,
  and exact remote-hash checks.

The clean report-grade verifier then checked every fixed property and raw
cardinality. Separate read-only SQLite queries confirmed schema version 14,
integrity `ok`, four terminal jobs and tasks, eight audited attempts, 28
published outbox rows, and zero pending rows. Independent checksum
recalculation found zero mismatches. Testcontainers left no running container,
and the tracked worktree remained clean at the measured harness revision.

After the report was written, all local Markdown targets resolved, the
prescribed `RabbitMqTransportLiveTest` selector passed 8/8, the
`RabbitMqCoordinatorLiveIntegrationTest` selector passed 10/10, and the
complete 25-module `mvn test` reactor passed in 40.821 seconds. The repository
RabbitMQ service was returned to its original stopped state afterward.

## Scope and limitations

- This is one run on one host, with no repeated trials, confidence interval,
  controlled thermal state, multi-host latency, or competing production
  workload. Small timing or heap differences should not be generalized.
- The 16 pressure rows are valid durable protocol messages inserted through
  the production outbox API to create a deterministic backlog. This is not an
  organic broker outage, so broker-disconnect recovery remains separate
  evidence.
- Jobs have one lightweight synthetic task. The publisher waits for confirms
  and the harness writes verbose evidence. The 282.143-second wrapper duration
  is not an ingestion-rate, execution-throughput, or capacity-sizing result,
  and the experiment defines no target RPS.
- Full-GC retained-heap samples measure one fixed-heap synthetic profile. They
  do not measure allocation rate, GC pauses, tail latency, or production heap
  requirements.
- Broker depth is sampled as ready-message count at the controlled gate, not
  as unacknowledged deliveries or a continuous management time series. The
  measured high water is therefore a lower-bound observation for that gate,
  not a broker-capacity limit.
- RabbitMQ is one container, and the scheduler and executor fixture are
  threads in one JVM. Clustered brokers, multi-coordinator authority, process
  isolation, network partitions, and executor operating-system crashes are
  outside this run.
- Native plugins, object-store payloads, large or varied payload
  distributions, participant-specific adaptive throttling, and exactly-once
  plugin side effects are outside scope. The existing contract remains
  generation-fenced authoritative state with at-least-once delivery.
