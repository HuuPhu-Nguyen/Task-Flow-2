# Task and Job State Machine

This document is the complete task/job transition contract for the current
single-coordinator TaskFlow runtime. It gives every lifecycle mutation a stable
transition ID and records the trigger, guard, durable effects, outbox effects,
in-memory projection, replay behavior, observability, and forbidden edges.

The authoritative guarantees and evidence status remain in
[Guarantees and non-goals](GUARANTEES.md). This document describes current
behavior precisely; it does not promote a partial invariant or a later Phase 2
mechanism to a current guarantee.

## Scope and Vocabulary

The persisted task states are `PENDING`, `ASSIGNED`, `COMPLETED`, and `FAILED`.
The persisted job states are `RUNNING`, `FINALIZING`, `COMPLETED`, and
`FAILED`. `FINALIZING` is a durable, nonterminal intent: every expected task
and result snapshot has committed, and terminal aggregation can be replayed
without another task execution.

`ACCEPTED` is an API-boundary description, not a fourth persisted job value. A
submission is accepted only after the job row and all initial task rows commit;
its stored job state is then `RUNNING`. Thus the requested
`ACCEPTED/RUNNING -> COMPLETED|FAILED` edges below are persisted as
`RUNNING -> FINALIZING -> COMPLETED` for successful execution or
`RUNNING -> FAILED` for unsuccessful execution.

Task-attempt outcomes such as `RUNNING`, `SUCCEEDED`, `RETRY_SCHEDULED`,
`TERMINAL_FAILURE`, `DISPATCH_FAILED`, and `JOB_FAILED` are audit values, not
task states. Likewise, pending/published outbox rows are effect-delivery state,
not job or task state.

When SQLite is enabled, its conditional transaction is authoritative and
`TaskUnit` is the scheduler's in-memory projection. The persistence-disabled
fallback has only the projection and immediate broker effects; it does not
satisfy durable-acceptance or transactional-outbound-intent claims.

## Lifecycle Overview

```text
task creation
    |
    v
 PENDING ---------> ASSIGNED ----------> COMPLETED
    |                  |
    |                  +---------------> PENDING
    |                  |                  retry/release, then a new generation
    |                  +---------------> FAILED
    |
    +----------------------------------> FAILED
                      job-wide terminalization only

job acceptance
    |
    v
 RUNNING ------------> FINALIZING ------> COMPLETED
    |
    +------------------------------------> FAILED
```

Normal task execution uses T1 through T4. T5 is the narrower administrative
edge that terminalizes work which will never execute because its whole job is
failing. T0 and J0 are creation edges. R1 and H1 below are recovery operations,
not new business-state edges.

## Rules Shared by Every Transition

1. A terminal task or job never returns to a nonterminal state.
2. Every assignment generation has a monotonically increasing attempt number,
   a coordinator-created assignment UUID, an executor-participant identity,
   and a lease deadline.
3. A successful result can cross T2 only when task ID, `ASSIGNED` state,
   attempt number, assignment ID, and executor-participant ID all match the
   current persisted tuple.
4. A failed conditional predicate produces no partial durable effects. The
   caller receives a typed outcome or a failed operation and applies the
   disposition documented for that transition.
5. RabbitMQ assignment creation couples T1 to one exact `TASK_ASSIGN` outbox
   envelope. RabbitMQ terminal job transitions couple J1/J2 to one exact
   `JOB_RESULT` outbox envelope.
6. The last successful T2 also persists `FINALIZING` when the expected task
   set is complete and every task has a durable result snapshot. J1 derives
   its payload deterministically from those snapshots.
7. Outbox replay republishes the stored envelope; it never creates a task
   generation or repeats a terminal job transition.
8. In-memory restoration from a committed snapshot is hydration, not evidence
   that a new domain transition occurred.
9. A correctness-relevant live transition follows one order: decide; attempt
   its conditional transaction; project only `COMMITTED` or exact
   `ALREADY_APPLIED`; preserve the projection and classify `STALE_STATE`; and
   suppress the requested projection/outbound effect on `UNKNOWN_ENTITY` or
   `STORAGE_FAILURE`.
10. A new submission crosses J0 only when its plugin declares retry safety and
    that declaration permits the configured retry policy. Rejection creates no
    job/task transition; exact replay classification occurs before this check.

The general SQLite transition result is
`COMMITTED | ALREADY_APPLIED | STALE_STATE | UNKNOWN_ENTITY | STORAGE_FAILURE`.
Successful task results retain their more specific equivalent result-commit
enum. `ALREADY_APPLIED` means the requested durable state is already present,
so installing the matching projection is safe. It is not permission to project
a merely similar state. Every non-committing outcome is logged with its
operation, job, task, and classification.

## Executable Decision Table

[`TaskStateMachine`](../taskflow-core/src/main/java/server/scheduler/transition/TaskStateMachine.java)
is the infrastructure-free executable form of the task transition rules. Its
input is an immutable
[`TaskState`](../taskflow-core/src/main/java/server/scheduler/transition/TaskState.java)
plus one closed
[`SchedulerEvent`](../taskflow-core/src/main/java/server/scheduler/transition/SchedulerEvent.java),
and its output is a typed
[`TransitionDecision`](../taskflow-core/src/main/java/server/scheduler/transition/TransitionDecision.java).
The decision classifies the event as accepted, duplicate, stale, invalid, or
ignored and names the required durable transition, logical outbox intent,
resulting projection, metric intents, and structured-event intents.

The table covers `JobSubmitted`, `AssignmentRequested`,
`TaskResultReceived`, `TaskExecutionFailed`, `LeaseExpired`, `TaskTimedOut`,
`WorkerUnavailable`, and `CoordinatorRecovered`. Assignment-bearing events,
including participant-unavailability handling, carry the exact
attempt/assignment/executor fencing tuple rather than using executor identity
as a substitute. Assignment creation also carries lease owner/deadline state.
The logical `TASK_ASSIGN` intent becomes a transactional outbox row when the
state store supports `BrokerOutboxStore`; otherwise the same RabbitMQ output is
published through the non-outbox fallback. T2 does not construct the semantic
final message inside its transaction, but its last-task branch persists the
replayable `FINALIZING` intent that requires J1.

This reducer describes effects but does not execute them. Every mandatory live
scheduler event now passes through `TaskTransitionDecisions`, which adapts the
current `TaskUnit` projection to this immutable model. Focused services execute
accepted effects, while SQLite conditional transactions remain authoritative
when durable state and memory disagree.

### Runtime responsibility map

| Boundary | Runtime owner |
|---|---|
| Mailbox polling and cycle order | `SchedulerLoop`; it delegates bounded message, combined deadline, dispatch, terminal-retry, and metric stages through an in-memory-testable `Work` seam, then blocks until the next scheduler-owned due time when no work remains immediate. |
| Work discovery | `SchedulerWorkloadIndex` owns post-commit pending/runnable/deadline/worker-assignment projections; every popped deadline is revalidated against the exact current assignment before a transition decision. |
| J0/T0 submission and broker disposition | `SchedulerMessageService` validates/routes envelopes and adds an active projection only after startup persistence succeeds. |
| T1 assignment | `AssignmentService` owns placement, assignment preparation, transactional assignment/outbox creation, non-outbox dispatch, and projection installation. |
| T2 successful result | `ResultCommitService` owns the typed SQLite commit outcome and applies the result projection only after `COMMITTED`. |
| T3/T4a failed attempts | `AttemptService` executes one reducer-approved retry or terminal projection and its matching persistence call. `ResultCommitService` supplies executor-failure events; `LeaseService` supplies timeout, lease-expiry, and participant-unavailability events. |
| J1/J2 and T4b/T5 cascade | `JobCompletionService` owns deterministic aggregation, terminal persistence/outbox intent, delivery retry, and active-projection cleanup. |
| R1/T3 startup reconciliation and H1 hydration | `CoordinatorStartupRecovery` owns store reconstruction/reconciliation; `RecoveryService` installs the reconciled jobs and invokes terminal aggregation when needed. |
| Composition and compatibility API | `TaskScheduler` constructs the services and delegates `Runnable`, metrics, and restore calls; it contains no transition rule or infrastructure effect. |

## Task Transitions

### T0 — Create task: `∅ -> PENDING`

- **Trigger:** a validated job submission creates its plugin-defined task set.
- **Preconditions:** the requester-owner/job-ID pair is new after typed durable
  submission inspection; plugin initialization produced at least one uniquely
  identified task; the job startup transaction has not already committed.
- **Durable writes:** one `RUNNING` job row and every `PENDING` task row are
  inserted in one SQLite transaction. Task payload snapshots, requester
  ownership fields, job parameters, and the versioned canonical submission
  request hash are stored with that transaction.
- **Emitted outbox messages:** none. Acceptance does not itself publish a task
  assignment or final result.
- **In-memory projection:** plugin initialization constructs each `TaskUnit` as
  `PENDING`; only after durable startup succeeds is the job placed in
  `activeJobs`.
- **Idempotent replay:** an exact owner/job/request-hash replay returns current
  status or the terminal result without plugin initialization, task insertion,
  or another T0. A changed request is `REQUEST_CONFLICT`; a changed owner is
  `OWNER_CONFLICT`; a pre-schema-v12 row without a request hash is
  `LEGACY_CONFLICT`. The atomic insert repeats this classification to close the
  preflight/commit race.
- **Metrics/events:** active-job gauge is refreshed and `job_started` records
  job ID, type, requester, and task count after acceptance.
- **Forbidden transitions:** a failed startup transaction must not leave only
  some task rows, admit the job to `activeJobs`, or dispatch a task.

### T1 — Create assignment generation: `PENDING -> ASSIGNED`

- **Trigger:** scheduler dispatch selects a compatible executor participant
  with capacity for a pending task.
- **Preconditions:** the job is active and not already finalizing; task state is
  exactly `PENDING`; the selected participant advertises the task type and has
  capacity; the next attempt number is strictly greater than the stored last
  attempt.
- **Durable writes:** task state becomes `ASSIGNED`; assigned participant,
  start time, lease owner/deadline, next attempt number, and assignment UUID are
  stored. A matching `task_attempts` row is inserted with outcome `RUNNING`.
- **Emitted outbox messages:** when the state store implements
  `BrokerOutboxStore`, it inserts the exact version-2 `TASK_ASSIGN` envelope in
  the same transaction as the task/audit writes. The non-outbox fallback has no
  durable outbound intent.
- **In-memory projection:** the outbox path installs the identity returned by the
  committed store transaction. The scheduler supplies an assignment UUID from
  `AssignmentIdGenerator`; SQLite still owns the conditional next-attempt
  number and atomically validates/persists that UUID with the task, audit, and
  outbox row. The non-outbox path creates the identity through the same injected
  generator, commits that exact tuple, installs it in `TaskUnit`, increments
  participant capacity, and only then sends.
- **Idempotent replay:** the SQLite predicate accepts only `PENDING`. Repeating
  assignment creation for an already assigned task changes nothing. Replaying
  the committed outbox row republishes the same attempt and UUID.
- **Metrics/events:** `taskflow_assignment_generations_total` increments once,
  dispatch latency is sampled, and `task_assignment_created` carries the full
  job/task/attempt/assignment/executor tuple. Publish failure leaves a RabbitMQ
  outbox row pending.
- **Forbidden transitions:** `ASSIGNED -> ASSIGNED` may not create another
  generation; a non-next committed identity may not enter memory; an outbox
  retry may not advance the attempt number.

### T2 — Commit successful result: `ASSIGNED -> COMPLETED`

- **Trigger:** the scheduler receives a validated, successful version-2
  `TASK_RESULT` for an active task whose lease has not already been released.
- **Preconditions:** SQLite matches task ID, `ASSIGNED`, attempt number,
  assignment UUID, and reporting executor participant. The exact audit row is
  still `RUNNING`. The payload can be parsed before projection.
- **Durable writes:** the task receives `COMPLETED`, result payload, completion
  time, duration, and cleared lease fields. The matching attempt receives
  `SUCCEEDED`, finish time, and duration in the same transaction. Persisted
  assignment/participant identity remains available for duplicate
  classification. If the expected task count now matches the complete task
  set and every result column is present, the same transaction changes the
  parent job from `RUNNING` to `FINALIZING`. A valid JSON `null` result is
  stored as JSON text and remains distinguishable from a missing SQL value.
- **Emitted outbox messages:** none at the task commit point. `FINALIZING` is
  the durable instruction for J1 to aggregate the ordered committed result
  snapshots and create the final `JOB_RESULT` intent.
- **In-memory projection:** only after `COMMITTED` does `TaskUnit` become
  `COMPLETED`, clear its live assignment/lease projection, add the parsed
  result to job aggregation, increment completed count, and release participant
  capacity.
- **Idempotent replay:** a repeated exact result after completion is
  `DUPLICATE_ALREADY_COMPLETED`; an obsolete or mismatched tuple is
  `STALE_ASSIGNMENT`; a missing task is `UNKNOWN_TASK`. None crosses T2.
- **Metrics/events:** only `COMMITTED` increments
  `taskflow_task_results_committed_total`, participant completion/duration
  metrics, and emits `task_result_committed`. Duplicate and stale dispositions
  use their distinct counters/events.
- **Forbidden transitions:** `PENDING -> COMPLETED`, a completion after
  `FAILED` or `FINALIZING`, a wrong-participant completion, and a
  stale-generation completion are forbidden. A task, attempt, or
  finalization-intent storage failure rolls the transaction back and leaves
  the in-memory task assigned.

### T3 — Release for another assignment: `ASSIGNED -> PENDING`

T3 has several triggers but one postcondition: the old assignment is closed,
its live identity is removed, and any later dispatch must use generation N+1.

- **Trigger:** one of the typed runtime or recovery conditions in the table
  below.

| Trigger | Attempt outcome | Retry-count rule | Primary event |
|---|---|---|---|
| Executor reports retryable failure | `RETRY_SCHEDULED` | Increment | `task_failed` |
| Task timeout | `RETRY_SCHEDULED` | Increment | `task_timeout` |
| Participant disconnect/heartbeat loss | `RETRY_SCHEDULED` | Increment | `task_peer_unavailable` |
| Active lease expiry | `RETRY_SCHEDULED` | Increment | `task_lease_expired` |
| Non-outbox assignment preparation/persistence fails before durable T1 | None | Preserve | assignment/persistence failure followed by J2 |
| Non-outbox assignment send fails | `DISPATCH_FAILED` | Preserve | `task_dispatch_failed` |
| Coordinator restart releases incomplete legacy identity | `RETRY_SCHEDULED` when an audit row is running | Preserve | `legacy_task_assignment_released` |
| Coordinator restart releases expired lease | `RETRY_SCHEDULED` | Preserve | recovery warning plus `running_job_resumed` |

- **Preconditions:** the live transition starts from `ASSIGNED`. Runtime
  failure triggers must identify the currently assigned participant; recovery
  release must find an expired lease or incomplete assignment/lease metadata.
- **Durable writes:** when T1 was already durable, any matching running attempt
  is closed with its typed outcome, finish time, duration, and reason. The task
  becomes `PENDING`; assigned participant, current assignment UUID, timing, and
  lease fields are cleared; the monotonic attempt number is retained. Runtime
  failures store the incremented retry count; dispatch/recovery releases
  preserve it. A non-outbox rollback before T1 persisted has no durable row to
  reverse.
- **Emitted outbox messages:** none. Any already committed assignment outbox
  row remains historical delivery intent; the next T1 creates a new row and
  generation.
- **In-memory projection:** only after the exact T3 write commits are current
  participant/identity/start/lease fields cleared, pending time refreshed,
  participant capacity released, and the task returned to scheduler selection.
  Preparation or T1 persistence failure creates no assignment projection. A
  non-outbox send failure retains the assigned projection until its fenced
  `DISPATCH_FAILED` T3 write commits.
- **Idempotent replay:** the runtime SQLite predicate accepts only `ASSIGNED`,
  so a repeated failure/release cannot close the attempt or increment retry
  twice. Re-running recovery sees `PENDING` and performs only R1 normalization.
- **Metrics/events:** runtime processor/timeout/participant/lease failures
  increment failure metrics; retryable ones increment scheduler retry count.
  Dispatch failure and restart release do not invent a processor retry.
- **Forbidden transitions:** T3 cannot reopen `COMPLETED` or `FAILED`, cannot
  decrease the last attempt number, and cannot let a late old result commit.

### T4 — Terminalize assigned task: `ASSIGNED -> FAILED`

T4a is normal retry-budget exhaustion. T4b is administrative terminalization
of an assigned task because J2 makes further execution irrelevant.

- **Trigger:** for T4a, executor failure, timeout, participant loss, or lease
  expiry closes an attempt whose incremented failure count reaches the
  configured maximum. For T4b, runtime/startup job failure cascades to a still
  assigned task.
- **Preconditions:** task is `ASSIGNED`. T4a additionally requires the current
  reporting/affected participant and a terminal retry-policy decision. T4b
  requires the containing `RUNNING` job to be crossing J2.
- **Durable writes:** the running attempt closes as `TERMINAL_FAILURE` for T4a
  or `JOB_FAILED` for T4b, with finish time, duration, and reason. The task
  becomes `FAILED`, gets a terminal timestamp, clears lease fields, and stores
  the retry count produced by the accepted retry-policy decision.
- **Emitted outbox messages:** none directly. T4a triggers J2; T4b participates
  in J2's cascade. RabbitMQ J2 stores the failed final `JOB_RESULT` with the
  job terminal transaction.
- **In-memory projection:** after the durable write, T4a makes `TaskUnit`
  `FAILED`, clears its live assignment/lease identity, releases participant
  capacity, and makes `hasTerminalFailure()` true. After the atomic J2 cascade,
  T4b projects each remaining task as failed, releases any assigned participant
  capacity exactly once, and then removes the containing active job.
- **Idempotent replay:** the task failure predicate excludes `FAILED` and
  `COMPLETED`; repeating the failure does not create another terminal edge.
- **Metrics/events:** T4a increments failure and terminal-failure counters once;
  the trigger-specific task event records `terminal_failure=true`. T4b does not
  invent another executor failure metric; J2 explains the administrative
  terminalization.
- **Forbidden transitions:** a stale participant cannot cause T4a; T4b requires
  a real J2 decision. `FAILED -> PENDING|ASSIGNED|COMPLETED` is forbidden.

### T5 — Job-wide task terminalization: `PENDING -> FAILED`

T5 is not a failed execution attempt. It is the administrative edge for a
nonterminal task that will never run because its containing job is becoming
`FAILED`. Current persistence guards also allow the same job-wide operation to
terminalize an `ASSIGNED` task; that assigned branch is T4b with attempt outcome
`JOB_FAILED`, not T4a's `TERMINAL_FAILURE`.

- **Trigger:** J2 finalization or startup reconciliation of a
  non-resumable/abandoned running job. Exhausted delivery occurs only after J2
  is durable and is not another task transition.
- **Preconditions:** task belongs to the failing `RUNNING` job and is neither
  `COMPLETED` nor already `FAILED`.
- **Durable writes:** task becomes `FAILED`, receives the job failure time, and
  clears lease fields. A running assigned attempt, if present, closes as
  `JOB_FAILED`; a pending task has no running attempt to close.
- **Emitted outbox messages:** in outbox-backed runtime finalization, all T5
  writes, J2, and the failed `JOB_RESULT` outbox row share one transaction. Startup
  reconciliation emits no final-result outbox row.
- **In-memory projection:** ordinary job finalization projects each remaining
  `TaskUnit` as failed after the durable job decision, releases capacity for
  assigned tasks exactly once, and then discards the active job projection.
  Startup recovery constructs no active projection for a job it terminalizes.
- **Idempotent replay:** terminal task predicates exclude `COMPLETED` and
  `FAILED`. Repeating J2 cannot repeat T5.
- **Metrics/events:** no executor-attempt metric is invented for a task that
  never ran. `job_failed`, `job_result_delivery_abandoned`, or startup recovery
  events explain the administrative cause.
- **Forbidden transitions:** T5 must not overwrite `COMPLETED`, must not look
  like retry exhaustion, and must not make an independently active task fail.

## Job Transitions

### J0 — Accept job: `∅ -> ACCEPTED/RUNNING`

- **Trigger:** validated `JOB_SUBMIT` plus successful plugin task creation.
- **Preconditions:** the requester token/optional identity signature and route
  binding are valid; the owner/job-ID pair is classified `NEW_SUBMISSION`; the
  server plugin has a non-null retry-safety declaration compatible with the
  configured retry policy; at least one task was created; T0 can commit for the
  complete task set. With the current positive `maxTaskRetries` contract,
  `UNSAFE_TO_RETRY` fails before plugin job construction or T0.
- **Durable writes:** the job row is inserted as `RUNNING` in the same
  transaction as all T0 rows and the schema-v12 canonical request hash.
- **Emitted outbox messages:** none.
- **In-memory projection:** after commit, the job enters `activeJobs`, requester
  authorization metadata is indexed, and scheduler dispatch may begin.
- **Idempotent replay:** `REPLAY` sends the same running-status shape used by
  `JOB_RESULT_REQUEST`, an in-memory durable pending result, or a reconstructed
  completed result. Request, owner, and unverifiable-legacy conflicts return a
  failed `JOB_RESULT`. None performs another J0.
- **Metrics/events:** active-job gauge increments and `job_started` is emitted
  with the accepted plugin's `retry_safety` declaration.
- **Forbidden transitions:** no accepted response, active projection, or task
  dispatch may precede successful durable creation when SQLite is enabled.

`DatabaseManager.insertJob(...)` and `insertTask(...)` remain low-level
compatibility/store-fixture operations. Calling either one alone does not meet
J0's runtime acceptance boundary; production scheduler startup uses the atomic
job-plus-task operation.

### J1 — Finish successfully: `FINALIZING -> COMPLETED`

- **Trigger:** all task projections are `COMPLETED`, including a recovered
  `FINALIZING` job restored with every durable result snapshot present.
- **Preconditions:** persisted job is `FINALIZING`; the complete expected task
  set is `COMPLETED` and result-bearing; deterministic final payload
  aggregation is available; a terminal completion has not already won. The
  completion primitive also accepts a fully result-bearing legacy `RUNNING`
  row as a repair-compatible source, but schema-v11 live T2 creates
  `FINALIZING` first.
- **Durable writes:** job becomes `COMPLETED`, receives completion time and the
  semantic final payload. The SQLite outbox path performs this write atomically
  with the final outbox insert. The non-outbox path commits J1 before delivery. The
  committed task snapshots are the deterministic inputs across the T2/J1
  boundary.
- **Emitted outbox messages:** the SQLite outbox path inserts one exact
  successful `JOB_RESULT`. The non-outbox path has no durable outbox and sends the response
  only after J1 commits.
- **In-memory projection:** the job first enters `pendingJobCompletions` while
  durable finalization and delivery are attempted. A write failure preserves
  that active projection and suppresses delivery. After commit, successful
  delivery or bounded delivery exhaustion removes the job from `activeJobs`
  and requester indexes without changing its terminal status.
- **Idempotent replay:** a repeated J1 changes no terminal state. On the outbox
  path, exact replay returns the already stored final outbox row; concurrent
  finalizers therefore converge on one terminal state and one logical final
  result. A pending outbox row may still be published more than once. Recovery
  rehydrates `FINALIZING` and invokes J1 again from the ordered committed task
  snapshots.
- **Metrics/events:** active-job gauge decrements; `job_completed` records
  `success=true`, result count, and outbox metadata when applicable.
- **Forbidden transitions:** any missing, failed, or result-less task forbids
  J1; `COMPLETED -> RUNNING|FINALIZING|FAILED` and a second semantic final
  payload are forbidden.

### J2 — Finish unsuccessfully: `ACCEPTED/RUNNING -> FAILED`

- **Trigger:** T4, unrecoverable transition persistence/preparation failure, or
  startup determination that a running job cannot be resumed safely. Final
  delivery exhaustion occurs after J2 and is not another J2 edge.
- **Preconditions:** persisted job is `RUNNING`; startup reconciliation may
  also fail a `FINALIZING` row whose plugin inputs cannot be reconstructed. No
  prior terminal job edge has committed. Runtime finalization has a failure
  reason and failed final result.
- **Durable writes:** job becomes `FAILED` with completion time. Remaining
  nonterminal tasks cross T5 or T4b for assigned work. The SQLite outbox path
  couples those task writes, J2, and the final outbox insert in one
  transaction. Startup reconciliation couples task/audit cleanup and J2 but
  creates no outbox row. The non-outbox path also commits the remaining task
  failures and J2 atomically before final-result delivery.
- **Emitted outbox messages:** outbox-backed runtime finalization inserts one
  exact failed `JOB_RESULT`; the non-outbox path publishes after commit; startup
  reconciliation emits none.
- **In-memory projection:** the job enters pending finalization. A write failure
  preserves its current task/capacity state and suppresses delivery. After J2
  commits, remaining task failures and capacity releases are projected; the
  job leaves active/requester indexes after delivery or bounded abandonment.
- **Idempotent replay:** the runtime job predicate accepts `RUNNING`; the
  startup-reconciliation predicate also accepts `FINALIZING`. Terminal replay
  changes nothing. Final-result outbox replay republishes the stored response.
- **Metrics/events:** active-job gauge decrements. Normal runtime failure emits
  `job_completed success=false` plus `job_failed`; delivery exhaustion emits
  `job_result_delivery_abandoned`; recovery emits its typed reconciliation
  event.
- **Forbidden transitions:** J2 cannot overwrite `COMPLETED`, J1 cannot later
  overwrite `FAILED`, and failed terminalization cannot silently leave a job in
  active memory.

## Recovery and State-Preserving Operations

These operations are explicitly mapped so they cannot be mistaken for new
execution transitions.

### R1 — Pending normalization: `PENDING -> PENDING`

Startup recovery may clear leftover assignment/timing/lease fields and backfill
the task row's last attempt number from the attempt audit. It does not increment
retry count or create an assignment. Repetition preserves `PENDING` and the
maximum known generation. R1 is permitted only during recovery/rollback
normalization; it is not a normal scheduler progress edge.

### H1 — Projection hydration: `persisted state -> fresh in-memory object`

`restorePendingForResume`, `restoreAssignedForResume`,
`restoreCompletedForResume`, and `restoreFailedForResume` initialize a newly
created `TaskUnit` from one committed snapshot. Exact unexpired assignments are
preserved; completed results rebuild aggregation; attempt numbers may not
decrease. The restored job is then added to the active projection without
creating another J0. H1 changes no durable state and therefore is not T0
through T5 or J0 through J2.

### Recovery decisions

- Complete, unexpired assignment identity: H1 preserves `ASSIGNED`; a pending
  assignment outbox envelope may replay the same T1 identity.
- Expired or incomplete assignment identity: H1 prepares a pending projection,
  then T3 must release it durably before the job can resume; failure makes the
  job non-resumable.
- Persisted pending task: R1 normalizes its fields/generation, then H1 restores
  it.
- Persisted completed/failed task: H1 preserves the terminal state; it never
  reopens it.
- Non-resumable `RUNNING` or `FINALIZING` job: T4b/T5 close nonterminal tasks
  where any remain and J2 closes the job in one startup transaction.
- `FINALIZING` job whose restored tasks are all complete: scheduler restoration
  invokes J1. A legacy `RUNNING` job whose complete durable result set predates
  schema v11 is migrated to `FINALIZING` first.
- Schema migration copies an existing state value into the new table shape; it
  is representation migration, not a lifecycle edge.

## Mutation-Site Ledger

Each state-changing branch in current production code maps to exactly one ID
below. A method appears more than once only when distinct guarded branches have
different transition IDs.

| Mutation site or branch | Transition ID |
|---|---|
| [`TaskUnit`](../taskflow-spi/src/main/java/server/job/TaskUnit.java) field/constructor initialization | T0 |
| `TaskUnit.markAssigned(...)` / `applyAssignment(...)` | T1 |
| `TaskUnit.markCompletedBy(peer, attempt, assignment, time)` | T2 projection |
| `TaskUnit.markCompletedBy(peer)` compatibility overload | T2 projection helper only; not an authoritative scheduler edge |
| `TaskUnit.resetToPending()` after `commitAssignedTaskFailure(...)` | T3 projection |
| `TaskUnit.resetToPending()` when already pending | R1 |
| `TaskUnit.failAttemptBy(...)` retry branch after durable acceptance | T3 projection |
| `TaskUnit.failAttemptBy(...)` terminal branch after durable acceptance | T4a projection |
| `TaskUnit.projectCommittedJobFailure()` | T4b/T5 projection and capacity-release identity |
| `TaskUnit.restore*ForResume(...)` methods | H1 |
| [`EmbarrassinglyParallelJob.applyCommittedResult(...)`](../taskflow-spi/src/main/java/server/job/EmbarrassinglyParallelJob.java) | T2 projection |
| `EmbarrassinglyParallelJob.recordResult(...)` compatibility helper | T2 projection helper only; not an authoritative scheduler edge |
| `EmbarrassinglyParallelJob.restoreTaskForResume(...)` | H1 |
| [`SchedulerMessageService.handleJobSubmit(...)`](../taskflow-core/src/main/java/server/scheduler/SchedulerMessageService.java) active insertion after startup persistence | J0/T0 projection |
| [`RecoveryService.restoreJobs(...)`](../taskflow-core/src/main/java/server/scheduler/RecoveryService.java) active-job insertion | H1 job projection |
| [`AssignmentService.assign(...)`](../taskflow-core/src/main/java/server/scheduler/AssignmentService.java) successful non-outbox branch | T1 |
| `AssignmentService.assign(...)` guarded send-failure release after `commitAssignedTaskFailure(...)` | T3 |
| `AssignmentService.assignWithBrokerOutbox(...)` | T1 |
| [`ResultCommitService.handleSuccessfulResult(...)`](../taskflow-core/src/main/java/server/scheduler/ResultCommitService.java) committed branch | T2 |
| `ResultCommitService.handleFailedResult(...)` retry branch | T3 |
| `ResultCommitService.handleFailedResult(...)` terminal branch | T4a |
| [`LeaseService.processDueDeadlines(...)`](../taskflow-core/src/main/java/server/scheduler/LeaseService.java) timeout retry branch | T3 |
| `LeaseService.processDueDeadlines(...)` timeout terminal branch | T4a |
| `LeaseService.handlePeerUnavailable(...)` retry branch | T3 |
| `LeaseService.handlePeerUnavailable(...)` terminal branch | T4a |
| `LeaseService.expireTaskLeaseIfNeeded(...)` retry branch | T3 |
| `LeaseService.expireTaskLeaseIfNeeded(...)` terminal branch | T4a |
| [`AttemptService.closeFailedAttempt(...)`](../taskflow-core/src/main/java/server/scheduler/AttemptService.java) retry commit then projection | T3 |
| `AttemptService.closeFailedAttempt(...)` terminal commit then projection | T4a |
| [`JobCompletionService.taskFailureUpdatesForJobFailure(...)`](../taskflow-core/src/main/java/server/scheduler/JobCompletionService.java) for pending tasks | T5 |
| `JobCompletionService.taskFailureUpdatesForJobFailure(...)` for assigned tasks | T4b |
| `JobCompletionService.persistTerminalState(...)` / `projectTerminalState(...)` / success removal | J1 commit then projection |
| `JobCompletionService.persistTerminalState(...)` / `projectTerminalState(...)` / failure removal | T4b/T5/J2 commit then projection |
| `RecoveryService.restoreJobs(...)` all-completed branch | J1 orchestration |
| `RecoveryService.restoreJobs(...)` terminal-failure branch | J2 orchestration |
| `JobCompletionService.abandonIfResultDeliveryExhausted(...)` | delivery-state cleanup after already committed J1/J2; no lifecycle edge |
| [`DatabaseManager.insertJobWithTasks(...)`](../taskflow-persistence-sqlite/src/main/java/server/db/DatabaseManager.java) job row | J0 |
| `DatabaseManager.insertJob(...)` low-level job-row bootstrap | J0 storage state; not runtime acceptance by itself |
| `DatabaseManager.insertJobWithTasks(...)` / `insertTask(...)` task rows | T0 |
| `DatabaseManager.commitTaskAssignment(...)` / compatibility `markTaskAssigned(...)` / `markTaskAssignedInCurrentTransaction(...)` | T1 |
| `DatabaseManager.commitTaskAssignmentAndEnqueueBrokerOutbox(...)` / compatibility `createTaskAssignmentAndEnqueueBrokerOutbox(...)` | T1 plus assignment outbox intent |
| `DatabaseManager.markTaskCompleted(...)` / `commitTaskResult(...)` | T2, plus atomic `RUNNING -> FINALIZING` intent when the complete result set commits |
| `DatabaseManager.commitAssignedTaskFailure(...)` retry/dispatch branch / compatibility `markTaskRetried(...)` | T3 |
| `DatabaseManager.commitAssignedTaskFailure(...)` terminal branch / compatibility `markTaskFailed(...)` | T4a |
| `DatabaseManager.markTaskFailedInCurrentTransaction(...)` from assigned with `JOB_FAILED` | T4b |
| `DatabaseManager.markTaskFailedInCurrentTransaction(...)` from pending | T5 |
| `DatabaseManager.commitJobCompleted(...)` / compatibility `markJobCompleted(...)` / `markJobCompletedInCurrentTransaction(...)` | J1 |
| `DatabaseManager.commitJobCompletedAndEnqueueBrokerOutbox(...)` / compatibility `markJobCompletedAndEnqueueBrokerOutbox(...)` | J1 plus final-result outbox intent |
| `DatabaseManager.commitJobFailed(...)` task branches | T4b/T5 atomically with J2 |
| `DatabaseManager.commitJobFailed(...)` / compatibility `markJobFailed(...)` job branch | J2 |
| `DatabaseManager.commitJobFailedAndEnqueueBrokerOutbox(...)` / compatibility `markJobFailedAndEnqueueBrokerOutbox(...)` task branches | T4b/T5 atomically with J2 and outbox intent |
| `DatabaseManager.commitJobFailedAndEnqueueBrokerOutbox(...)` / compatibility `markJobFailedAndEnqueueBrokerOutbox(...)` job branch | J2 plus final-result outbox intent |
| `DatabaseManager.markRunningJobsFailedOnStartup(...)` assigned-task branch | T4b |
| `DatabaseManager.markRunningJobsFailedOnStartup(...)` pending-task branch | T5 |
| `DatabaseManager.markRunningJobsFailedOnStartup(...)` job branch | J2 |
| `DatabaseManager.markRunningJobFailedOnStartup(...)` assigned-task branch | T4b |
| `DatabaseManager.markRunningJobFailedOnStartup(...)` pending-task branch | T5 |
| `DatabaseManager.markRunningJobFailedOnStartup(...)` job branch | J2 |
| `DatabaseManager.migrateFinalizationIntentSchema()` | schema-v10 repair from fully result-bearing `RUNNING` to `FINALIZING`; no task lifecycle edge |
| `DatabaseManager.resetTaskForResume(...)` from assigned | T3 |
| `DatabaseManager.resetTaskForResume(...)` from pending | R1 |
| `DatabaseManager.releaseExpiredTaskLeaseForResume(...)` | T3 |
| [`CoordinatorStartupRecovery.restoreJob(...)`](../taskflow-coordinator/src/main/java/server/CoordinatorStartupRecovery.java) object restoration | H1 |
| `CoordinatorStartupRecovery.restoreJob(...)` expired/incomplete assignment decisions | T3 |
| `CoordinatorStartupRecovery.restoreJob(...)` pending normalization | R1 |
| `CoordinatorStartupRecovery.recoverPersistedJobs(...)` non-resumable assigned-task cleanup | T4b |
| `CoordinatorStartupRecovery.recoverPersistedJobs(...)` non-resumable pending-task cleanup | T5 |
| `CoordinatorStartupRecovery.recoverPersistedJobs(...)` non-resumable job cleanup | J2 |
| `DatabaseManager.migrateTasksTableToForeignKey()` state copy | representation migration, no lifecycle edge |

The ledger's combined cells explicitly name their guard. For example, the
non-outbox assignment branch crosses T1 once; only a later guarded failure crosses
T3. The startup cleanup SQL applies T4b to assigned rows and T5 to pending rows,
never one ambiguous edge.

## Forbidden Transition Matrix

| Source | Forbidden destination or action | Enforcing guard |
|---|---|---|
| `PENDING` | `COMPLETED` | Result commit requires `ASSIGNED` plus exact generation tuple. |
| `PENDING` | new `PENDING` progress edge | Only R1 normalization may preserve pending state. |
| `ASSIGNED` | another `ASSIGNED` generation | Assignment creation requires `PENDING`. |
| `ASSIGNED` | completion/failure by stale participant or generation | In-memory identity check and SQLite conditional predicate. |
| `COMPLETED` | `PENDING`, `ASSIGNED`, or `FAILED` | Terminal predicates exclude `COMPLETED`. |
| `FAILED` | `PENDING`, `ASSIGNED`, or `COMPLETED` | Terminal predicates exclude `FAILED`. |
| `RUNNING` job | `COMPLETED` with any non-completed or result-less task | J1 requires the exact expected completed/result-bearing task set; live success first persists `FINALIZING`. |
| `FINALIZING` job | New assignment or task result | Schema validation requires every expected task already be completed and result-bearing, leaving no `PENDING` task for T1; T2 additionally requires a `RUNNING` parent. |
| `COMPLETED` job | `RUNNING`, `FINALIZING`, or `FAILED` | Job terminal predicates exclude `COMPLETED`. |
| `FAILED` job | `RUNNING`, `FINALIZING`, or `COMPLETED` | Job terminal predicates exclude `FAILED`. |
| Published/pending outbox replay | new task/job lifecycle edge | Replay reads the stored envelope and only marks effect-delivery state. |

## Current Boundaries and Follow-on Ownership

- Coordinator lifecycle time comes from `TaskFlowClock`; assignment UUID
  candidates come from `AssignmentIdGenerator`. Production entry points share
  `SystemTaskFlowClock` and `UuidAssignmentIdGenerator` across startup recovery,
  the scheduler, plugin-created task units, scheduler protocol timestamps, and
  transactional assignment creation. Tests can instead bind fixed/mutable
  clocks and exact UUID sequences. The database retains ownership of the
  conditional monotonic attempt number and the atomic assignment/outbox commit.
- These ports deliberately cover coordinator task/job transition inputs.
  Participant liveness sampling, compatibility peer/job-ID creation, and
  payload-storage keys are separate runtime concerns and are not transition
  decision inputs in this state machine.
- `TaskStateMachine` centralizes pure classification and effect descriptions for
  the mandatory scheduler events. `TaskTransitionDecisions` adapts live
  projections into that model, and the focused services above own distinct
  effect boundaries. `TaskScheduler` is a composition facade and
  `SchedulerLoop` is orchestration-only. SQLite executes conditional durable
  primitives; `TaskUnit`, scheduler indexes, participant capacity, and metrics
  are post-commit projections. Typed store outcomes make stale, duplicate,
  unknown-entity, and storage-failure paths explicit instead of treating a
  boolean write as permission to mutate memory.
- The SQLite T5 primitive guards the task pre-state but cannot by itself prove
  that its containing job is simultaneously failing; current scheduler call
  sites own that context. J1, by contrast, conditionally checks
  `RUNNING|FINALIZING`, exact task cardinality, every task's `COMPLETED` state,
  and every result snapshot before it can commit. The scheduler owns semantic
  aggregation from that durable input set.
- The legacy `TaskUnit.markCompletedBy(peer)` and
  `EmbarrassinglyParallelJob.recordResult(...)` projection helpers do not take
  a caller-supplied generation tuple, and the low-level
  `DatabaseManager.markTaskCompleted(...)` wrapper loads the current tuple.
  The supported scheduler result path uses `commitTaskResult(...)` followed by
  the exact `markCompletedBy(peer, attempt, assignment, time)` overload; the
  compatibility helpers are not authoritative commit entry points.
- The non-outbox fallback has no durable outbound intent, but J1/J2 commits
  before `JOB_RESULT` publication. The SQLite outbox path additionally couples
  the terminal job decision to durable outbound intent.
- T2 for the last task and J1 remain separate transactions because plugin
  aggregation runs outside SQLite. The T2 transaction closes that window by
  persisting `FINALIZING`; restart reconstructs the plugin job from task
  snapshots in canonical task order, reruns deterministic aggregation, and
  atomically commits J1 plus the RabbitMQ final-result outbox row. The
  in-memory job model remains an active completed-task projection during this
  durable intermediate state; it does not expose `FINALIZING` as a second
  authority.
- T5 is an administrative job-cascade edge, not evidence that pending work
  exhausted retries. A future reducer must retain that distinction.
- Submission idempotency is scoped to the stored token hash plus optional
  verified public key, not the routing peer ID. Exact canonical-request replay
  is supported across reconnect and coordinator restart. Pre-schema-v12 rows
  remain collision-safe but cannot be replayed as exact submissions.

## Automated Evidence

- [`TaskStateMachineTest#implementsTransitionTable`](../taskflow-core/src/test/java/server/scheduler/transition/TaskStateMachineTest.java)
  parameterizes accepted, ignored, retry, terminal, lease/timeout-boundary, and
  recovery decisions for all mandatory event families.
- [`TaskStateMachineTest#classifiesInvalidTransitionsExplicitly`](../taskflow-core/src/test/java/server/scheduler/transition/TaskStateMachineTest.java)
  and
  [`TaskStateMachineTest#replayOfEveryAcceptedEventIsClassifiedDuplicate`](../taskflow-core/src/test/java/server/scheduler/transition/TaskStateMachineTest.java)
  prove explicit invalid edges and duplicate replay without another durable or
  outbound intent.
- [`DuplicateSubmissionIntegrationTest#lostAcceptanceResponseReplaysAcceptedJob`](../taskflow-coordinator/src/test/java/server/DuplicateSubmissionIntegrationTest.java)
  restarts after the accepted job/task/hash transaction and proves an exact
  signed replay returns running status with the original two task identities;
  [`DuplicateSubmissionIntegrationTest#exactDuplicateReplaysPersistedTerminalResult`](../taskflow-coordinator/src/test/java/server/DuplicateSubmissionIntegrationTest.java)
  proves the terminal replay path.
- [`DatabaseManagerTest#submissionCommitIsTypedAndDeterministicAcrossRestart`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java),
  [`DatabaseManagerTest#concurrentIdenticalSubmissionCommitsOneJobAndOneTaskSet`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java),
  and
  [`DatabaseManagerTest#failedTaskInsertRollsBackSubmissionHashAndJobTogether`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  cover typed replay/conflicts, concurrent convergence, restart stability, and
  atomic rollback at J0/T0.
- [`SchedulerLoopTest#oneCycleAppliesExactStageLimitsInRequiredOrder`](../taskflow-core/src/test/java/server/scheduler/SchedulerLoopTest.java)
  proves exact cycle order and stage budgets using an in-memory mailbox and fake
  work boundary, while
  [`SchedulerArchitectureTest#schedulerFacadeAndLoopCannotOwnTransitionEffects`](../taskflow-core/src/test/java/server/scheduler/SchedulerArchitectureTest.java)
  enforces the dependency and responsibility split and
  [`SchedulerArchitectureTest#correctnessEffectsCommitBeforeProjectionOrDelivery`](../taskflow-core/src/test/java/server/scheduler/SchedulerArchitectureTest.java)
  locks the durable-before-projection/delivery call order at each focused
  service boundary.
- [`TaskSchedulerFailureTest#samePeerStaleFailureCannotCloseNewerAssignmentGeneration`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java)
  proves that the runtime reducer guard applies the complete assignment tuple
  to unsuccessful results as well as successful result commitment.
- [`TaskSchedulerPersistenceTest#assignmentPersistenceFailureReturnsFailureWithoutDispatchingTask`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java),
  [`TaskSchedulerPersistenceTest#dispatchReleaseWriteFailurePreservesAssignmentUntilJobFailureCommits`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java),
  [`TaskSchedulerPersistenceTest#taskCompletionStorageFailureRequeuesWithoutMutatingMemoryOrMetrics`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java),
  and
  [`TaskSchedulerPersistenceTest#terminalTaskWriteFailurePreservesAssignedProjectionAndCapacity`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java)
  inject assignment, release, successful-result, and terminal-failure write
  failures and prove the requested projections do not occur.
- [`TaskSchedulerPersistenceTest#finalResultOutboxWriteFailureKeepsProjectionUntilRetryCommits`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java),
  [`TaskSchedulerPersistenceTest#failedFinalOutboxWriteFailurePreservesRemainingAssignmentUntilRetryCommits`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java),
  [`TaskSchedulerPersistenceTest#oneShotJobCompletionWriteFailureProjectsOnlyAfterRetryCommits`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java),
  and
  [`TaskSchedulerPersistenceTest#oneShotJobFailureWriteFailureProjectsOnlyAfterRetryCommits`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java)
  cover successful/failed final outbox and non-outbox J1/J2 one-shot faults.

- [`TaskUnitLifecycleTest#exactCompletionRequiresAttemptAssignmentAndPeerWithoutPartialMutation`](../taskflow-spi/src/test/java/server/job/TaskUnitLifecycleTest.java)
  covers T1/T2 guards and no partial stale mutation.
- [`TaskUnitLifecycleTest#transitionsToTerminalFailureAfterRetryLimit`](../taskflow-spi/src/test/java/server/job/TaskUnitLifecycleTest.java)
  covers T3/T4 retry-budget branching.
- [`DatabaseManagerTest#taskStatusUpdatesRejectInvalidTransitions`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  and
  [`DatabaseManagerTest#jobStatusUpdatesRejectTerminalOverwrites`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  cover persisted terminal guards, including the current T5 administrative
  edge.
- [`DatabaseManagerTest#assignmentCommitBeforePublishLeavesOneDurableIdentityAndPendingOutbox`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  and
  [`DatabaseManagerTest#taskAssignmentStorageFaultPreservesPendingStateAndReplayIsTyped`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java),
  plus
  [`DatabaseManagerTest#repeatedTypedAssignmentCommitReturnsExactDurableProjectionAndOutbox`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  cover atomic outbox/non-outbox T1, rollback, replay classification, exact
  replay projection data, and outbound intent.
- [`DatabaseManagerTest#matchingAssignmentCommitsExactlyOnceAndDuplicateIsTyped`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  covers authoritative T2 and duplicate replay;
  [`DatabaseManagerTest#lastResultAndFinalizingIntentRollbackTogetherOnIntentFault`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  proves the last result, successful attempt close, and `FINALIZING` intent
  roll back together, while
  [`DatabaseManagerTest#jsonNullTaskResultRemainsPresentForFinalizationRecovery`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  distinguishes a valid JSON `null` result from a missing snapshot.
- [`DatabaseManagerTest#retriedTaskRowsClearPreviousAssignmentState`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  and
  [`DatabaseManagerTest#releaseExpiredTaskLeaseForResumeClearsAssignmentAndClosesAttempt`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  cover runtime/recovery forms of T3.
- [`DatabaseManagerTest#completedJobOutboxCommitsTerminalStateAndResultMessage`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  and
  [`DatabaseManagerTest#failedJobOutboxCommitsTaskFailuresAndResultMessage`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  cover RabbitMQ J1/J2, T5, and final-result intent;
  [`DatabaseManagerTest#repeatedTypedFinalOutboxCommitsReturnExactDurableRecord`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  proves exact final-outbox replay data, while
  [`DatabaseManagerTest#failedFinalResultOutboxFaultRollsBackTasksJobAndAttemptBeforeRetry`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  proves the failed-result transaction has no partial projection source.
- [`JobFinalizationCrashTest#lastTaskCommitCannotStrandJob`](../taskflow-coordinator/src/test/java/server/JobFinalizationCrashTest.java)
  crashes at the durable `FINALIZING` boundary, recovers ordered aggregation,
  converges duplicate finalizers on one outbox identity, and replays that row
  after restart;
  [`DatabaseManagerTest#concurrentFinalizationCreatesOneTerminalStateAndOneOutbox`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  proves the same single logical result under two SQLite finalizers.
- [`CoordinatorStartupRecoveryTest#preservesAssignedTasksWithUnexpiredLeasesOnResume`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java)
  and
  [`CoordinatorStartupRecoveryTest#releasesExpiredAssignedLeasesOnResume`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java)
  distinguish H1 assignment preservation from recovery T3, while
  [`CoordinatorStartupRecoveryTest#sqliteRestartRecoveryReconstructsCommittedRetryProjectionAndGeneration`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java)
  proves SQLite reconstructs retry count, pending status, cleared assignment and
  lease state, and the monotonic generation.
- [`TaskSchedulerPersistenceTest#successfulJobPersistsLifecycleTransitions`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java)
  and
  [`TaskSchedulerPersistenceTest#terminalTaskFailurePersistsFailedTaskAndJob`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java)
  cover scheduler orchestration through T0/T1/T2/J1 and T4/J2.
