# 0013: Use Requester-Scoped Job-Submission Idempotency

Status: Accepted

Date: 2026-07-23

Scope: Implemented for the single-authoritative-coordinator SQLite runtime.

## Context

RabbitMQ delivery is at least once, a publisher confirmation or result message
can be lost, and a requester may not know whether a submission committed.
Rejecting every duplicate job ID prevents duplicate work, but it also prevents
a legitimate requester from discovering the accepted job after an uncertain
response. Treating every duplicate as the same request would let a changed
payload silently alias existing work.

TaskFlow already has per-job bearer tokens, optional signed requester keys, a
globally unique job primary key, and an existing `JOB_RESULT` status/result
response shape. It needs a durable request identity without adding a second job
namespace or claiming full account authentication.

## Decision

The client-supplied `jobId` is an idempotency key scoped to the persisted owner
tuple: requester-token hash plus the verified requester public key when one is
present. The routing peer ID is not part of durable ownership, although each
submission's outer sender must match its inner `nodeId`.

For each new submission, TaskFlow computes a versioned SHA-256 request hash
over the owner tuple, normalized task type, null-to-empty but otherwise exact
parameter string, and the ordered list of canonical JSON payload digests. JSON
object keys are sorted, equivalent numeric representations are normalized, and
array/payload order remains significant. The job ID, envelope route, message
time, and signature are excluded.

SQLite schema v12 commits the hash with the job and complete initial task set.
The store returns typed `COMMITTED`, `REPLAY`, `REQUEST_CONFLICT`,
`OWNER_CONFLICT`, `LEGACY_CONFLICT`, or `STORAGE_FAILURE` outcomes. The
scheduler performs a read preflight to avoid plugin initialization for known
duplicates, then repeats classification inside the atomic insert boundary.

An exact replay creates no tasks and returns the current running status, a
durably pending terminal result, or a reconstructed completed result through
the existing `JOB_RESULT` path. Changed requests and owners are permanent
conflicts. Pre-v12 rows retain an empty hash and are legacy conflicts because
plugin-created task snapshots cannot reliably reconstruct the original
submitted payload list.

## Alternatives Considered

- **Reject every duplicate job ID:** safe against duplicate creation but cannot
  recover an accepted submission after an uncertain response.
- **Replay based only on job ID:** rejected because a changed payload or task
  type could be mistaken for the accepted request.
- **Scope by routing peer ID:** rejected because routes are compatibility
  identities that may change across reconnect and are not authentication.
- **Reconstruct old hashes from task rows:** rejected because plugins may
  validate, split, normalize, or transform submitted payloads while creating
  tasks.
- **Add a new wire response/type:** rejected because running and terminal
  replay already fit the validated `JOB_RESULT` result-request response shape.

## Consequences

- Requesters must retain and reuse the original per-job token and optional
  signing key. The JavaFX token store now issues a token once per job ID and
  retains it after uncertain send/confirm failure.
- Exact replay can use a new time and newly signed route after reconnect; it
  cannot change task semantics or ownership.
- The canonicalization algorithm is versioned independently inside the stored
  hash prefix. Changing it requires an explicit compatibility/migration plan.
- Older persisted jobs remain protected from replacement but cannot receive
  exact-submission replay.
- TaskFlow still performs no hidden automatic submit retry and does not claim
  account authentication, replay-proof sessions, or exactly-once delivery.

## Conditions That Would Invalidate This Decision

A replacement ADR is required if TaskFlow adopts account-scoped job namespaces,
token rotation for live jobs, multiple active coordinators with a shared state
store, or an API whose accepted-submission response cannot be represented by
`JOB_RESULT`. Such a change must preserve conflict detection and atomic
job/task creation.

## Evidence And Implementation Status

- `JobSubmissionHasherTest` proves canonical stability and semantic
  distinctions.
- `DatabaseManagerTest#submissionCommitIsTypedAndDeterministicAcrossRestart`
  proves typed owner/hash outcomes after reopen.
- `DatabaseManagerTest#concurrentIdenticalSubmissionCommitsOneJobAndOneTaskSet`
  and `#failedTaskInsertRollsBackSubmissionHashAndJobTogether` prove convergence
  and transaction rollback.
- `DuplicateSubmissionIntegrationTest#lostAcceptanceResponseReplaysAcceptedJob`
  proves deterministic coordinator restart replay with original task IDs.
- `DuplicateSubmissionIntegrationTest#exactDuplicateReplaysPersistedTerminalResult`
  proves terminal result replay.

## Related Documents

- [ADR 0005: Per-job result ownership](0005-per-job-result-ownership.md)
- [ADR 0007: Single authoritative coordinator](0007-single-authoritative-coordinator.md)
- [ADR 0008: SQLite single-writer state store](0008-sqlite-single-writer-state-store.md)
- [Failure model](../FAILURE_MODEL.md)
- [Execution guarantees](../EXECUTION_GUARANTEES.md)
