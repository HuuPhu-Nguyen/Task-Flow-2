# TF-0405 Admission Overload Experiment

Date: 2026-07-26

Implementation commit: `PENDING_TF_0405_IMPLEMENTATION_COMMIT`

## Contract

TF-0405 bounds new coordinator admission before durable J0/T0 acceptance.
This experiment checks the memory consequence of that boundary; it does not
claim the persistent-overload progress policy assigned to TF-0406.

The opt-in test installs exactly 64 accepted jobs with 64 tasks each
(4,096 active tasks), then submits five waves of 20,000 unique jobs while the
active-job bound remains full. Rejected jobs are not retained, no additional
durable job submission is committed, and active metrics must remain exactly
64 jobs and 4,096 tasks after every wave.

The harness uses the configured scheduler mailbox at its default capacity of
1,000; its producer blocks while that bounded mailbox drains. Production
broker overflow instead uses the separately tested `RETRY_TRANSIENT` path.

Acceptance thresholds fixed before the run:

- last-three post-GC sample span no greater than 16 MiB;
- maximum of the last three post-GC samples below 128 MiB;
- exactly 64 durable job-submission commits throughout all five rejection
  waves.

## Reproduction

From the repository root:

```powershell
.\mvnw.cmd -pl taskflow-core -am `
  "-Dtaskflow.admission.experiment=true" `
  "-DargLine=-Xms256m -Xmx256m -XX:+UseSerialGC" `
  "-Dtest=AdmissionOverloadExperiment" `
  "-Dsurefire.failIfNoSpecifiedTests=false" test
```

The test contains no timing sleeps. It uses two explicit full-GC requests
before each `MemoryMXBean` sample so the retained-heap comparison is not a
young-generation allocation snapshot.

## Environment

- OS: Windows 11 Pro 10.0.26200, 64-bit
- CPU: Intel Core i5-13500HX, 14 physical / 20 logical cores
- Memory: 63.73 GiB
- JVM: Oracle HotSpot 25.0.2, compiling the project at Java release 21
- Heap: fixed 256 MiB, Serial GC

## Observations

| Rejected submissions completed | Active jobs | Active tasks | Durable commits | Post-GC used heap |
|---:|---:|---:|---:|---:|
| 20,000 | 64 | 4,096 | 64 | 7,025,248 bytes (6.700 MiB) |
| 40,000 | 64 | 4,096 | 64 | 7,977,784 bytes (7.608 MiB) |
| 60,000 | 64 | 4,096 | 64 | 6,730,344 bytes (6.419 MiB) |
| 80,000 | 64 | 4,096 | 64 | 6,600,920 bytes (6.295 MiB) |
| 100,000 | 64 | 4,096 | 64 | 6,618,088 bytes (6.312 MiB) |

The last three samples span 129,424 bytes (0.123 MiB). Their maximum is
6,730,344 bytes (6.419 MiB). Both are well inside the documented thresholds.
The test passed in 1.225 seconds; its Maven reactor run completed in
7.281 seconds.

## Interpretation and limits

The result supports the narrow TF-0405 claim: once configured active work is
full, a large sequence of unique rejected submissions does not accumulate
jobs, tasks, durable commits, or retained coordinator heap. It is a synthetic
in-process experiment with an empty executor and a counting state store. Full
GC is useful for a repeatable retained-heap check but is not a production
latency model.

This experiment does not simulate a persistent RabbitMQ submission flood,
prove task-result or expiry progress while intake stays saturated, test
adaptive broker intake, or prove automatic recovery after pressure falls.
Those behaviors remain explicitly assigned to TF-0406.
