# Coordinator Event Schema

This document is the normative machine-readable schema for coordinator
scheduler events. It covers the authoritative job/task state transitions and
the scheduler-owned persistence, outbox, overload, and delivery decisions that
support them.

The broader event inventory, operational commands, current log-based metrics,
and deferred exporter work remain in
[Observability scope](OBSERVABILITY_SCOPE.md).

## Wire shape

Events are one line of UTF-8 `key=value` fields. Fields are ordered with the
common envelope first:

```text
event=task_result_stale_rejected timestamp=2025-01-01T00:00:00Z coordinator_instance_id=COORDINATOR_... outcome=STALE_REJECTED failure_reason_code=STALE_ASSIGNMENT job_id=job-1 task_id=task-1 attempt_number=2 assignment_id=... worker_id=worker-a commit_outcome=STALE_ASSIGNMENT
```

Unquoted values contain only machine-safe characters. Values containing
whitespace, quotes, backslashes, carriage returns, or newlines are
double-quoted and escaped. Automation must use `failure_reason_code`, not parse
human `reason` or `error` text.

Field and event names use lower `snake_case`. Outcome and failure-reason values
use upper `SNAKE_CASE`.

## Common fields

| Field | Presence | Meaning |
|---|---|---|
| `event` | Always | Stable event name. |
| `timestamp` | Always | UTC RFC 3339 instant from the scheduler's injected clock. |
| `coordinator_instance_id` | Always | Process-lifetime scheduler identity. It is the same identity used as the assignment lease owner. It changes on restart and is not a leader epoch or an authorization identity. |
| `job_id` | When the decision concerns one job | Stable job correlation identity. |
| `task_id` | When the decision concerns one task | Stable logical task identity. |
| `attempt_number` | When an assignment generation exists | Monotonic task assignment generation. It is not a broker delivery attempt. |
| `assignment_id` | When an assignment generation exists | Coordinator-created identity for that exact generation. |
| `worker_id` | When an assignment generation exists | Participant identity while acting in the executor role. |
| `outcome` | Always | Bounded classification from the table below. |
| `failure_reason_code` | Always | Stable machine reason. It is `NONE` when no failure or rejection applies. |

The complete assignment tuple is mandatory for assignment creation, task
result commitment, stale/duplicate result decisions, executor-reported task
failure, dispatch failure, timeout, lease expiry, and per-task
participant-unavailability events. Worker identity alone never correlates an
authoritative result.

Job acceptance, replay/conflict/admission, recovery, and terminal events carry
`job_id`. Scheduler-wide overload and metrics snapshots deliberately carry no
job/task/assignment identities.

## Outcome classifications

| Outcome | Meaning |
|---|---|
| `ACCEPTED` | A new job passed its durable acceptance boundary. |
| `COMMITTED` | The authoritative durable transition committed. |
| `REPLAYED` | An already-applied durable decision was reused without another logical transition. |
| `RECOVERED` | Durable work or scheduler availability was restored. |
| `STALE_REJECTED` | An obsolete assignment/state transition was rejected. |
| `DUPLICATE_IGNORED` | A repeated already-committed result changed no authority. |
| `RETRY_SCHEDULED` | The current attempt closed and bounded task retry remains permitted. |
| `TERMINAL_FAILURE` | The task or job closed unsuccessfully with no logical task retry. |
| `REJECTED` | Input, admission, ownership, or entity checks refused the operation. |
| `CONFLICT` | A job ID/idempotency identity was reused with different durable meaning. |
| `DEFERRED` | An external or durable effect did not finish and remains retryable/pending. |
| `ABANDONED` | A bounded delivery policy exhausted without changing the terminal job decision. |
| `DISPOSED` | A broker delivery received a typed non-success settlement decision. |
| `RELEASED` | Assignment-owned work/capacity was released after a failure condition. |
| `OVERLOADED` | At least one bounded scheduler pressure reason is active. |
| `OBSERVED` | A non-transition metrics snapshot was emitted. |
| `IGNORED` | A non-authoritative event required no state change. |
| `FAILED` | An infrastructure or processing operation failed without implying a domain commit. |

## Failure reason codes

`failure_reason_code` is always present. `NONE` is the only no-failure value.
The principal transition codes are:

| Code | Used for |
|---|---|
| `NONE` | Accepted, committed, replayed, recovered, or observed outcomes without a failure. |
| `STALE_ASSIGNMENT` | Old attempt/assignment/worker result rejected by the current generation fence. |
| `DUPLICATE_RESULT` | Repeated result for an assignment already committed authoritatively. |
| `RETRYABLE` | Executor reported a retryable processing failure. |
| `PERMANENT_PAYLOAD_INTEGRITY` | Exact current assignment failed the length/digest integrity boundary terminally. |
| `DISPATCH_FAILED` | Direct assignment delivery failed and the assignment was released. |
| `TASK_TIMEOUT` | Task execution exceeded the configured timeout. |
| `LEASE_EXPIRED` | The persisted assignment lease expired before result commitment. |
| `WORKER_UNAVAILABLE`, `HEARTBEAT_TIMEOUT` | Executor unavailability released or closed current assignments. |
| `STORAGE_FAILURE` | Required SQLite operation or observation failed. |
| `UNKNOWN_TASK` | Result names no known task at the authoritative store boundary. |
| `JOB_FAILED` | Job terminalization completed unsuccessfully; human `reason` provides bounded detail. |
| `FINAL_RESULT_DELIVERY_FAILED`, `FINAL_RESULT_DELIVERY_EXHAUSTED` | Terminal state exists but requester delivery is deferred or exhausted. |

Admission failures use their bounded limit enum, such as
`MAX_ACTIVE_JOBS` or `MAX_PENDING_OUTBOX_ROWS`. Message settlement uses the
existing bounded `reason_code`, normalized to upper `SNAKE_CASE`. Transition
specific durable classifications remain visible in detail fields such as
`commit_outcome`, `durable_transition_outcome`, and `submission_outcome`; they
do not replace the common `outcome`.

Adding a new scheduler event requires adding an explicit outcome/reason
classification. An unclassified event is rejected at the centralized logging
boundary rather than emitted with an incomplete schema.

## Data protection

Coordinator scheduler events must never contain:

- requester tokens or token hashes;
- credentials or passwords;
- public/private key material;
- raw/full protocol payloads;
- full binary bodies.

The event boundary rejects common sensitive and full-payload field names. IDs,
bounded counts, enum classifications, content length/digest metadata, and
controlled object keys may be logged when they are needed for correlation.
Human `reason` and `error` text is diagnostic rather than a machine contract
and must not contain a secret or full payload.

## Evidence

- [`SchedulerEventLogTest#majorStateTransitionsCarryTheCommonSchemaAndApplicableCorrelation`](../taskflow-core/src/test/java/server/scheduler/SchedulerEventLogTest.java)
  fixes the common envelope, stable outcomes/reasons, job correlation, and
  complete assignment tuple for the major transition families.
- [`SchedulerEventLogTest#rejectsSchemaReplacementSecretsFullPayloadsAndUnclassifiedEvents`](../taskflow-core/src/test/java/server/scheduler/SchedulerEventLogTest.java)
  proves reserved-field, secret/full-payload, duplicate-field, and
  unclassified-event rejection.
- [`SchedulerEventLogTest#everySchedulerEventProducerUsesTheClassifiedSchemaBoundary`](../taskflow-core/src/test/java/server/scheduler/SchedulerEventLogTest.java)
  prevents scheduler components from bypassing the common boundary and checks
  that every emitted event name has an explicit classification.
- [`AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit`](../taskflow-coordinator/src/test/java/server/rabbitmq/AssignmentFencingIntegrationTest.java)
  and the scheduler persistence/failure suites continue to prove that the
  emitted correlation describes the durable decision rather than replacing it.

## Limits

This schema standardizes coordinator scheduler state-transition evidence. It is
not distributed tracing: no cross-process trace/span propagation is claimed.
Participant, RabbitMQ-adapter, object-store adapter, and startup/shutdown
operational events retain their existing component-specific fields. They must
still obey the no-secret/no-full-payload rule, but a
`coordinator_instance_id` would be meaningless for participant-only events.

The coordinator's aggregate, bounded-label metrics are exported at
`GET /metrics`; names, units, lifecycle semantics, configuration, and
cardinality constraints are defined in
[Observability scope](OBSERVABILITY_SCOPE.md). Health/readiness is TF-0603
scope, and the one-command stale-result demonstration is TF-0604 scope. This
document does not claim those later Phase 6 items.
