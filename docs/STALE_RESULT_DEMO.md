# Deterministic Stale-Result Fencing Demo

This demo makes TaskFlow's assignment-generation fence visible in one local
command. It runs the real coordinator scheduler against a temporary SQLite
database with an injected clock and two fixed assignment IDs. RabbitMQ, Docker,
wall-clock sleeps, and external services are not required.

Prerequisites are Java 21, Windows PowerShell, and the repository checkout; the
command uses the checked-in Maven wrapper.

## Run

From the repository root on Windows PowerShell:

```powershell
.\scripts\demo-stale-result-fencing.ps1
```

The command exits nonzero if the integration test fails, if a trace step is
missing, if the order changes, or if any line differs from the fixed contract.
Full Maven and structured-event output is retained in the ignored runtime file
`target/tf0604-demo/stale-result-fencing.log`.

## Expected output

```text
TF0604 TRACE 1 SUBMITTED job_id=job-fencing-demo task_id=task-job-fencing-demo-0 accepted=true
TF0604 TRACE 2 ASSIGNED worker_id=executor-a attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000601 lease_expires_at_epoch_ms=1767225601000
TF0604 TRACE 3 LEASE_EXPIRED attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000601 at_epoch_ms=1767225601000 outcome=RETRY_SCHEDULED
TF0604 TRACE 4 REASSIGNED worker_id=executor-a attempt_number=2 assignment_id=00000000-0000-0000-0000-000000000602 lease_expires_at_epoch_ms=1767225602000
TF0604 TRACE 5 STALE_REJECTED attempt_number=1 assignment_id=00000000-0000-0000-0000-000000000601 disposition=ACK_DUPLICATE_OR_STALE authoritative_assignment_id=00000000-0000-0000-0000-000000000602
TF0604 TRACE 6 CURRENT_COMMITTED attempt_number=2 assignment_id=00000000-0000-0000-0000-000000000602 disposition=ACK_SUCCESS result=current-result
TF0604 TRACE 7 COMPLETED job_id=job-fencing-demo authoritative_results=1 stale_results=1 final_result=current-result
TF0604 RESULT PASS trace_steps=7 docker_required=false
```

## What the command proves

The scenario deliberately reuses `executor-a`, making it the same-participant
ABA case:

1. One job and one task become durable in SQLite.
2. Attempt 1 receives assignment X with an exact lease deadline.
3. The injected clock advances to that deadline. SQLite closes X as
   `RETRY_SCHEDULED` with reason `lease_expired`.
4. The same executor receives attempt 2 with distinct assignment Y.
5. A successful X result is classified `STALE_ASSIGNMENT`, acknowledged with
   `ACK_DUPLICATE_OR_STALE`, increments only the stale counter, produces no job
   result, and leaves Y unchanged in SQLite.
6. The successful Y result commits and is acknowledged with `ACK_SUCCESS`.
7. SQLite contains exactly two attempt rows—expired X and succeeded Y—plus one
   completed job result whose only payload is `current-result`.

The test also fixes the final counters: two assignment generations, one lease
expiration, one retry, one stale result, one committed result, one completed
job, and zero failed jobs. Its implementation is
[`StaleResultTraceDemoTest#printsAndAssertsLeaseExpiryStaleFenceAndCurrentCommitTrace`](../taskflow-coordinator/src/test/java/server/demo/StaleResultTraceDemoTest.java).

The full log contains the corresponding structured scheduler events:
`task_assignment_created`, `task_lease_expired`,
`task_result_stale_rejected`, `task_result_committed`, and `job_completed`.
Their normative field schema remains in
[`OBSERVABILITY.md`](OBSERVABILITY.md).

## Scope

This is deterministic mechanism and presentation evidence, not a live-broker,
process-kill, or chaos experiment. RabbitMQ redelivery/recovery evidence remains
in the live suites, while generated event sequences and process crash windows
remain Phase 7 scope. The demo adds no schema, protocol, retry, settlement, or
runtime configuration behavior.
