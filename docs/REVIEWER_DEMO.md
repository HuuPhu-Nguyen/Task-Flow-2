# Five-Minute Reviewer Demo

This demo connects TaskFlow's focused proofs into one correlated job narrative:
real RabbitMQ delivery, schema-v14 SQLite authority, MinIO readiness, two
executor endpoints, lease-based reassignment, generation fencing, terminal
outbox publication, coordinator reconstruction, and authorized persisted-result
retrieval.

It is a presentation index over the implemented mechanisms, not a new
guarantee. The normative scope remains in
[Guarantees and non-goals](GUARANTEES.md), and the individual crash windows
remain in the [failure model](FAILURE_MODEL.md).

## Run

Prerequisites are Java 21 or newer, Windows PowerShell, a running Docker Engine,
and the repository checkout. RabbitMQ and MinIO images may be pulled on the
first run.

From the repository root:

```powershell
.\scripts\demo-reviewer.ps1
```

That one command uses the Maven wrapper and starts disposable
`rabbitmq:3.13-management` and
`minio/minio:RELEASE.2025-04-22T22-12-26Z` Testcontainers. It exits nonzero if
a trace line changes, a durable or delivery assertion fails, the broker
settlement is incomplete, or the scenario takes five minutes or more after the
dependencies report ready.

Image download and container startup are deliberately outside the five-minute
measurement. On the measured local path, the asserted workflow itself normally
takes only a few seconds.

## Expected output

The duration is measured at runtime; it must be less than `300000` ms.

```text
TF0804 TRACE 1 STACK_READY coordinator_instance=COORDINATOR_tf0804_demo_1 rabbitmq=UP sqlite_schema=14 minio=UP workers=reviewer-worker-a,reviewer-worker-b
TF0804 TRACE 2 SUBMITTED job_id=job-reviewer-demo task_id=task-job-reviewer-demo-0 accepted=true
TF0804 TRACE 3 ASSIGNED worker_id=reviewer-worker-a attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000801 lease_expires_at_epoch_ms=1767225601000
TF0804 TRACE 4 WORKER_PAUSED worker_id=reviewer-worker-a transport=closed registry_status=DISCONNECTED
TF0804 TRACE 5 LEASE_EXPIRED attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000801 at_epoch_ms=1767225601000 outcome=RETRY_SCHEDULED
TF0804 TRACE 6 REASSIGNED worker_id=reviewer-worker-b attempt_number=2 assignment_id=00000000-0000-0000-0000-000000000802 lease_expires_at_epoch_ms=1767225602000
TF0804 TRACE 7 STALE_REJECTED attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000801 disposition=ACK_DUPLICATE_OR_STALE authoritative_assignment_id=00000000-0000-0000-0000-000000000802
TF0804 TRACE 8 CURRENT_COMMITTED attempt_number=2 assignment_id=00000000-0000-0000-0000-000000000802 disposition=ACK_SUCCESS result=current-result
TF0804 TRACE 9 COMPLETED job_id=job-reviewer-demo authoritative_results=1 final_result=current-result
TF0804 TRACE 10 COORDINATOR_RESTARTED coordinator_instance=COORDINATOR_tf0804_demo_2 recovered_running_jobs=0 persisted_job_status=COMPLETED
TF0804 TRACE 11 PERSISTED_RESULT_RETRIEVED job_id=job-reviewer-demo delivery=JOB_RESULT result=current-result
TF0804 TRACE 12 OBSERVED metrics_assignments=2 metrics_lease_expirations=1 metrics_stale=1 metrics_committed=1 metrics_jobs_completed=1 outbox_published=3 outbox_pending=0 minio_bucket=taskflow-reviewer-demo
TF0804 RESULT PASS trace_steps=12 duration_ms=<less-than-300000>
TF0804 LOG <repository>\target\tf0804-demo\reviewer-demo.log
```

The wrapper asserts every fixed trace line. Full Maven, Testcontainers,
structured-event, and broker output is retained in the ignored runtime file
`target/tf0804-demo/reviewer-demo.log`.

## What happens

| Demo step | Mechanism and durable assertion |
|---|---|
| Stack ready | Real RabbitMQ and MinIO containers are healthy. The production SQLite adapter creates schema 14, the production scheduler/RabbitMQ output starts, and two capacity-advertising executor routes register. |
| Submit and assign X | A protocol-v2 submission creates one durable job/task. SQLite conditionally commits attempt 1, fixed assignment X, its lease, attempt audit, and assignment outbox row before confirmed publication to executor A. The demo waits for the published mark before pausing A. |
| Pause executor A | A's RabbitMQ transport closes and its registry status becomes disconnected. X remains the current durable assignment until its exact lease deadline. |
| Expire and reassign | An injected clock reaches the recorded deadline without a wall-clock sleep. SQLite closes X as `RETRY_SCHEDULED`; executor B receives attempt 2 with distinct fixed assignment Y. |
| Inject old X result | A fresh broker connection publishes a successful result using executor A's old full assignment tuple. SQLite returns the stale classification, the delivery settles as `ACK_DUPLICATE_OR_STALE`, Y remains current, and no terminal result appears. |
| Commit Y and complete | B publishes the current result. SQLite commits exactly Y, terminal aggregation produces `current-result`, and the final result/outbox intent becomes durable before confirmed delivery. |
| Restart coordinator | The first scheduler, RabbitMQ coordinator connection, and SQLite connection stop. A second coordinator composition opens the same SQLite file and executes the production startup-recovery path; the already completed job remains terminal and no running job is reconstructed. |
| Retrieve and observe | An authorized protocol-v2 `JOB_RESULT_REQUEST` traverses RabbitMQ's existing job-input route and returns the persisted payload. Its broker delivery is fully acknowledged before shutdown. Metrics report two generations, one lease expiration, one stale result, one committed result, and one completed job; SQLite reports three published outbox rows and zero pending. |

The executable assertions are in
[`ReviewerDemoTest#runsFiveMinuteFailureRecoveryNarrative`](../taskflow-coordinator/src/test/java/server/ReviewerDemoTest.java).
The same fence has a smaller Docker-free explanation in the
[stale-result demo](STALE_RESULT_DEMO.md).

## Real boundaries and controlled seams

The demo intentionally says exactly what it runs:

- RabbitMQ, MinIO, the SQLite adapter, scheduler, capacity registry, assignment
  and final-result outbox publisher, typed settlement, protocol codec, and
  startup recovery are the repository's production components.
- The two executor endpoints use real RabbitMQ participant routes and capacity
  heartbeats. Their execution/result decisions are test-controlled so the old
  and current results arrive in a fixed order.
- The coordinator components are stopped and reconstructed inside one test
  JVM. This is coordinator restart evidence, not operating-system process-kill
  or zero-downtime failover evidence.
- Executor A is paused by closing its broker transport and durably recording it
  disconnected. The old result is then injected through a new real broker
  connection under A's identity.
- Lease time and assignment UUIDs are injected through the existing production
  ports. No `Thread.sleep` controls the state transition.
- The lightweight task payload is inline. MinIO health and bucket creation are
  asserted, but this demo does not claim that the task exercises the
  object-payload lifecycle.

Process-level crash evidence remains in the
[crash-window matrix](CRASH_WINDOW_MATRIX.md). Measured broker restart, lease,
startup reconstruction, outbox replay, and MinIO cleanup remain in the
[recovery report](reports/recovery.md). Object streaming and integrity remain
in the [payload-storage contract](PAYLOAD_STORAGE.md).

## Scope

The workflow makes I1, I2, I3, I4, I5, and I6 visible for one deterministic
job and shows successful progress under the I10 assumptions. It does not prove
multi-coordinator authority, clustered broker/storage recovery, arbitrary
plugin side-effect safety, production sizing, native media throughput,
exactly-once execution, or high availability.

Testcontainers remove the disposable RabbitMQ and MinIO containers when the
test JVM exits. If the command reports that Docker is unavailable, start Docker
Desktop or another compatible engine and rerun it; do not replace the live
infrastructure path with an in-memory substitute.
