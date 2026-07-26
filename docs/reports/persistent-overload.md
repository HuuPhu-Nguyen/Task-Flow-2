# TF-0406 Persistent Overload Experiment

## Contract

This report compares the pre-TF-0406 single FIFO mailbox with the TF-0406
capacity-`1` result reserve using the same deterministic workload in
`SchedulerOverloadTest`. It measures the narrow claims that a full submission
lane cannot exclude one accepted result, the deadline stage still runs, memory
remains bounded, and cleanup permits fresh admission without restart.

The experiment is not a throughput benchmark. It uses scheduler cycles rather
than wall-clock latency assertions.

## Revisions

- Baseline: `604cef8530dd2537fd6251263c0d28a95e1295d0`
- Changed implementation: `3de6cee49ec0caa1d192cf1e85b765e8f1095624`
- Report/evidence commit: recorded in `agents/LOG_v2.md` after push

The test source is compatible with both revisions. For the baseline command it
was copied unchanged into a detached worktree because that revision predates
the test. `taskflow.overload.expectation` changes only the expected assertion;
it does not change the workload or mailbox implementation.

## Reproduction

Changed tree:

```powershell
.\mvnw.cmd -pl taskflow-core -am `
  "-Dtest=SchedulerOverloadTest" `
  "-Dsurefire.failIfNoSpecifiedTests=false" `
  "-Dtaskflow.overload.experiment=true" `
  "-Dtaskflow.overload.expectation=changed" `
  "-DargLine=-Xms64m -Xmx128m -XX:+UseSerialGC" test
```

Baseline detached worktree:

```powershell
git worktree add --detach ..\FP-tf406-baseline `
  604cef8530dd2537fd6251263c0d28a95e1295d0
Copy-Item `
  .\taskflow-core\src\test\java\server\scheduler\SchedulerOverloadTest.java `
  ..\FP-tf406-baseline\taskflow-core\src\test\java\server\scheduler\
..\FP-tf406-baseline\mvnw.cmd -f ..\FP-tf406-baseline\pom.xml `
  -pl taskflow-core -am `
  "-Dtest=SchedulerOverloadTest" `
  "-Dsurefire.failIfNoSpecifiedTests=false" `
  "-Dtaskflow.overload.experiment=true" `
  "-Dtaskflow.overload.expectation=baseline" `
  "-DargLine=-Xms64m -Xmx128m -XX:+UseSerialGC" test
```

Live acceptance gates:

```powershell
.\mvnw.cmd -pl taskflow-transport-rabbitmq -am `
  "-Dtaskflow.rabbitmq.live=true" `
  "-Dtest=RabbitMqTransportLiveTest" `
  "-Dsurefire.failIfNoSpecifiedTests=false" test
.\mvnw.cmd -pl taskflow-coordinator -am `
  "-Dtaskflow.rabbitmq.live=true" `
  "-Dtest=RabbitMqCoordinatorLiveIntegrationTest" `
  "-Dsurefire.failIfNoSpecifiedTests=false" test
```

## Environment

| Item | Value |
|---|---|
| Date | 2026-07-27 |
| OS | Microsoft Windows 11 Pro 10.0.26200, 64-bit |
| CPU | Intel Core i5-13500HX, 14 cores / 20 logical processors |
| Memory | 63.7 GiB visible |
| Java | Oracle HotSpot 25.0.2, compiling/running Java release 21 sources |
| Maven | Wrapper-managed Apache Maven 3.9.9 |
| Experiment JVM | `-Xms64m -Xmx128m -XX:+UseSerialGC` |
| Reported max heap | 129,761,280 bytes |
| Persistence in mailbox profile | None; deterministic in-process scheduler seam |
| Broker in mailbox profile | None |
| Live broker | Docker `rabbitmq:3.13-management` |

## Workload

- Ordinary lane capacity: `1`.
- Waves: `5`.
- Cycles per wave: `20,000`.
- Total unique submission envelopes: `100,000`.
- Total unique task-result offers: `100,000`.
- Scheduler message budget: `1`.
- Scheduler deadline budget: `1`.
- Every cycle fills the ordinary lane, offers one result, runs one bounded
  scheduler cycle, records one due-deadline unit, then clears the remaining
  ordinary test envelope before the next controlled cycle.
- Heap is sampled after two explicit full-GC requests at every wave boundary.
- The companion recovery scenario uses `maxActiveJobs=1` and
  `maxActiveTasks=1`: first job accepted, one fresh job typed-rejected, first
  job completed, then one final fresh job accepted on the same scheduler.

## Results

| Metric | Baseline FIFO | TF-0406 changed |
|---|---:|---:|
| Submission attempts | 100,000 | 100,000 |
| Result offers | 100,000 | 100,000 |
| Result reserve admissions | 0 | 100,000 |
| Result full-lane retry outcomes | 100,000 | 0 |
| Results processed | 0 | 100,000 |
| Result commit latency | Not committed in measured cycle | 1 scheduler cycle |
| Deadline cycles | 100,000 | 100,000 |
| Ordinary-lane high-water | 1 | 1 |
| Total mailbox high-water | 1 | 2 |
| Broker retry/quarantine in in-process profile | 0 / 0 | 0 / 0 |
| Typed admission rejections in companion scenario | 1 | 1 |
| Exact replays in experiment | 0 | 0 |
| Store commit-boundary calls in companion scenario | 2 | 2 |
| Restart count before final admission | 0 | 0 |
| Final eligible job accepted | Yes | Yes |

Retained heap:

| Wave | Baseline bytes | Changed bytes |
|---:|---:|---:|
| 1 | 5,889,360 | 5,897,184 |
| 2 | 5,795,352 | 5,804,072 |
| 3 | 5,803,704 | 5,811,888 |
| 4 | 5,808,640 | 5,816,832 |
| 5 | 5,811,688 | 5,820,152 |
| Last-three range | 7,984 | 8,264 |

Both last-three ranges are far below the test's 16 MiB plateau bound. The
changed design retains one additional bounded envelope slot; the observed
roughly 8 KiB difference is smaller than normal JVM/test-harness noise and is
not treated as a per-envelope memory estimate.

The live transport selector passed 8/8 tests. It proved that a deferred
prefetch-`1` `JOB_SUBMIT` does not block `TASK_RESULT`, and that the dedicated
consumer recovers after broker-side connection loss. The live coordinator
selector passed 10/10 tests. Its overload case used eight flood submissions:
one became eligible after the accepted result cleaned up the initial job,
seven received typed `MAX_ACTIVE_JOBS` rejection, the accepted flood job
completed, and a final fresh job was assigned and completed without scheduler
restart.

## Interpretation

The baseline reproduces the failure mode exactly: when the only FIFO slot holds
a submission, every measured task-result offer is rejected into the existing
retry path. TF-0406 admits one result independently and processes it in the
next bounded scheduler cycle while the ordinary lane remains full. Deadline
work runs in all 100,000 cycles for both revisions, so result priority does not
skip the deadline stage.

The result reserve, configured ordinary lane, active-work limits, pending
outbox limit, message/deadline batches, and broker prefetch remain finite. The
changed result therefore supports I7 without weakening accepted-work
preservation or the existing finite retry/quarantine contract.

## Limitations

- The fixed-heap loop isolates mailbox/deadline behavior; it does not include
  RabbitMQ serialization, SQLite I/O, plugin execution, or real task payloads.
- Store commit counts in the companion scenario are calls through the
  `JobStateStore` commit boundary, not SQLite row counts. SQLite no-rejected-row,
  outbox aggregate, replay, fencing, and crash windows remain covered by their
  existing focused suites.
- The live overload test uses one broker and one coordinator. It is not a
  cluster failover, multi-coordinator, or Phase 7 chaos-scale result.
- Broker retry/quarantine counts are zero in the successful overload scenario;
  separate live retry tests prove finite delayed retry and quarantine.
- Full GC is only a reproducible retained-heap observation. It is not a
  production GC recommendation or latency guarantee.
