# Bounded Event-Driven Scheduler

## Authority and ownership

SQLite remains the authoritative source for accepted jobs, task states,
assignment generations, leases, results, and terminal state. The structures in
`SchedulerWorkloadIndex` are scheduler-owned in-memory projections used only to
find work. A service changes an index only after the same durable boundary that
allows its `TaskUnit` projection:

- committed job admission or reconciled recovery indexes the job;
- committed assignment creation removes the task from pending work and indexes
  its timeout and lease;
- committed result, retry release, dispatch release, or terminal cascade
  removes the exact assignment;
- retry release appends the task to its job's retry-priority FIFO lane;
- terminal job cleanup removes the job-local pending and assignment indexes.

An index entry never authorizes a transition. The pure state-machine decision
and the conditional store transition still decide whether an event may act.

## Structures and bounds

| Structure | Representation | Live cardinality bound |
|---|---|---|
| Pending tasks | Per-job insertion-ordered retry and ordinary FIFO/deque lanes of task IDs | At most one entry per active `PENDING` task |
| Runnable jobs | One insertion-ordered set used as a round-robin deque of job IDs | At most one entry per active job with indexed pending work |
| Capacity-wait jobs | Separate insertion-ordered job IDs tagged with the capacity-signal generation observed when blocked | At most one entry per active job with pending work and no compatible available capacity |
| Deadlines | Priority-ordered timeout and lease sets keyed by exact assignment identity | At most two entries per live indexed assignment |
| Worker assignments | Worker ID to exact assignment keys | At most one entry per live indexed assignment |
| Worker capacity | Task type to capable live worker IDs plus an exact assignment-ID reservation ledger with per-worker unit/type counters | One capability membership per advertised worker/task-type pair and one reservation per current durable assignment |
| Inbound submission/control lane | Bounded FIFO lane | `inboundQueueCapacity`, default `1000` |
| Inbound task-result reserve | Bounded priority lane | Fixed capacity `1`; total envelope capacity is configured capacity plus one |
| Active jobs/tasks | Scheduler-owned map plus exact task counter | `maxActiveJobs`, default `1000`; `maxActiveTasks`, default `100000` |
| Terminal retry deadlines | Priority-ordered due times keyed by job ID | At most one entry per pending terminal delivery |

## Fair bounded cycle

`SchedulerLoop` executes this order on every cycle:

1. process at most `schedulerMessageBatchSize` mailbox envelopes;
2. pop at most `schedulerDeadlineBatchSize` combined timeout/lease entries;
3. attempt at most `schedulerDispatchBatchSize` placements or no-capacity probes;
4. retry at most `schedulerOutboxBatchSize` due terminal deliveries;
5. refresh metrics.

All four defaults are `100`. The YAML keys use the names above. Environment
overrides are `TASKFLOW_SCHEDULER_MESSAGE_BATCH_SIZE`,
`TASKFLOW_SCHEDULER_DEADLINE_BATCH_SIZE`,
`TASKFLOW_SCHEDULER_DISPATCH_BATCH_SIZE`, and
`TASKFLOW_SCHEDULER_OUTBOX_BATCH_SIZE`. Values must be positive. Cross-job
dispatch additionally uses `schedulerMaxAssignmentsPerJobPerRound`, default
`1`, with environment override
`TASKFLOW_SCHEDULER_MAX_ASSIGNMENTS_PER_JOB_PER_ROUND`. The quota must not
exceed `schedulerDispatchBatchSize`. The pre-TF-0402 13-argument, TF-0402
17-argument, and TF-0403/0404 18-argument `SchedulerConfig` constructors remain
compatibility overloads and supply the newer defaults.

The work units deliberately count unsuccessful discovery:

- one admitted envelope is one message unit;
- one popped timeout or lease entry is one deadline unit, including an entry
  rejected as stale before it reaches a transition;
- one task placement attempt or one runnable-job probe with no compatible
  capacity is one dispatch unit;
- one due pending terminal-result delivery is one outbound unit.

This prevents invalid, stale, unavailable-capacity, or transient-failure work
from bypassing a limit merely because it did not produce a successful
transition. Message work precedes deadlines in every cycle, so a due-deadline
backlog cannot block a queued task result. Deadlines still run after every
bounded message batch, so continuous mailbox traffic cannot block lease expiry.

The durable SQLite broker-outbox replayer remains an independent coordinator
component rather than moving onto the scheduler thread. It uses
`schedulerOutboxBatchSize` as its database load bound. The scheduler's outbound
stage owns only due in-memory terminal delivery/persistence attempts; RabbitMQ
terminal intent that has committed to SQLite is replayed by the independent
replayer.

### Explicit cross-job rounds

A scheduler round is one persistent pass over the runnable jobs present when
that round begins. The round cursor survives the end of a TF-0402 dispatch
batch, so a small per-cycle budget cannot restart the pass at the front. Each
job receives one turn and at most
`schedulerMaxAssignmentsPerJobPerRound` successful assignments in that turn.
A failed placement ends the turn. Retry tasks are selected before ordinary
tasks only inside the selected job; retry status never moves the whole job
ahead of other jobs. A job that becomes runnable after a round starts joins the
next round.

At the compatibility default of one assignment, one 10,000-task job followed
by ten one-task jobs consumes exactly 11 successful dispatch units in the first
complete round when at least 11 compatible slots are available: one assignment
for the large job, then one for every small job. Therefore every small job in
that scenario is assigned by the end of round one.
[`AssignmentServiceBatchTest#oneLargeJobAndTenSmallJobsAllDispatchInTheirFirstCompleteRound`](../taskflow-core/src/test/java/server/scheduler/AssignmentServiceBatchTest.java)
constructs that workload deterministically.

When any stage reports immediately runnable work after exhausting its budget,
the next cycle starts without blocking. Otherwise the loop waits on the mailbox
until the earliest assignment deadline, terminal retry, no-capacity recheck, or
metrics deadline. A no-capacity probe moves the job out of the runnable
rotation and into the capacity-wait set without removing its pending-task
index. Pending-task reindexing cannot make that job runnable again. A later
capacity-availability generation or the deterministic 500 ms fallback makes
only the previously waiting generation eligible for a later round. Jobs that
block after that signal must await a newer signal or deadline. Executor
heartbeats that add or change capabilities wake the scheduler even though
heartbeat registration occurs outside the scheduler mailbox. This avoids both
continuous incompatible-job polling and delayed observation of newly
compatible capacity. Shutdown interrupts its own idle wait, then drains only
envelopes already admitted through the bounded ingress gate.

Insertion-ordered sets provide FIFO/deque ordering plus keyed constant-time removal,
so deleting a task or job does not search a queue. Deadline sets are paired with
an assignment-key map; assignment closure removes both timer entries in
`O(log A)` without scanning the deadline queue. Repeated dispatch failures
therefore do not accumulate timers: the index remains at two deadline entries
for the one live assignment and returns to zero after its committed release.

These are exact bounds relative to already accepted active work, and TF-0405
now bounds that owner set absolutely. An index insertion is never dropped or
evicted under pressure, because silently losing already accepted work would
violate scheduler progress.

## New-job admission

`SchedulerMessageService` remains the single production owner of J0/T0. For a
new `JOB_SUBMIT`, it validates protocol fields and the submitted task/inline/
referenced-payload limits, calculates the canonical request hash, and
classifies the idempotency key before capacity admission. Exact replay
therefore bypasses admission and creates no task set while the coordinator is
full.

For a new key, the scheduler reads the constant-time active counts and the
SQLite aggregate pending-outbox count, then the pure `AdmissionPolicy` checks:

| Limit | Default | Admission boundary |
|---|---:|---|
| `maxActiveJobs` / `TASKFLOW_MAX_ACTIVE_JOBS` | `1000` | Allow resulting active jobs `<=`; reject `>` |
| `maxActiveTasks` / `TASKFLOW_MAX_ACTIVE_TASKS` | `100000` | Allow resulting retained task objects `<=`; reject `>` |
| `TASKFLOW_MAX_TASKS_PER_JOB` | `256` | Allow both submitted payload count and plugin-produced tasks `<=` |
| `TASKFLOW_MAX_JOB_PAYLOAD_BYTES` | `67108864` | Allow UTF-8 JSON bytes of submitted task payloads plus parameter `<=` |
| `TASKFLOW_MAX_INPUT_BYTES` | `33554432` | Allow each recursively discovered `ObjectReference.contentLength` `<=` |
| `TASKFLOW_MAX_INLINE_PAYLOAD_BYTES` | `8388608` | Allow recursively discovered Base64 file data only when raw bytes are `<`; reject the exact boundary and above |
| `maxPendingOutboxRows` / `TASKFLOW_MAX_PENDING_OUTBOX_ROWS` | `100000` | Reject when the current pending count is `>=` |
| `inboundQueueCapacity` / `TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY` | `1000` | Queue exactly the capacity; the next broker delivery receives `RETRY_TRANSIENT` |

The early dynamic check uses submitted task count and avoids plugin job
construction when admission is already impossible. After initialization, the
plugin-produced task count and resulting active-task count are checked again
before the transaction. A limit rejection writes no job, task, attempt, lease,
or outbox row. SQLite pending-count failure is a storage failure, never an
observed zero. After `commitJobSubmission` returns `COMMITTED`, the active job
and exact task count enter the projection.

Recovery is deliberately not admission. It retains every accepted recovered
job even if an operator lowered a bound; new submissions remain rejected until
normal terminal cleanup returns active counts within range. The pending-outbox
threshold gates only new J0/T0. Assignment and final-result transactions for
already accepted work may add required outbox rows.

## Persistent overload

The production mailbox has two internal lanes behind the existing
`BlockingQueue<MessageEnvelope>` surface. Ordinary submissions and control
events use the configured capacity. A validated `TaskResultMessage` uses one
fixed reserve slot. Polling selects a queued result before ordinary FIFO work,
but the result still consumes one `schedulerMessageBatchSize` unit. A second
result offered while the reserve is occupied receives the existing bounded
transient broker handoff; no envelope is evicted or replaced. Deadlines still
run after every bounded message stage.

RabbitMQ `JOB_SUBMIT` intake uses route-local prefetch `1` on one dedicated
channel. Result and heartbeat intake keep the configured prefetch on the
primary channel. Intake remains continuously subscribed: exact replay can
still be classified before capacity admission, a genuinely new request at a
dynamic limit receives its typed pre-J0/T0 rejection, and the next eligible
request can commit immediately after terminal/outbox/lane cleanup without a
restart or consumer transition.

`SchedulerOverloadStatus` is an infrastructure-free, thread-safe projection,
not an admission authority. Its immutable snapshot orders active reasons as
result reserve, submission lane, pending outbox, active jobs, then active
tasks. Each active reason includes its configured maximum, observed value, and
activation time. A failed pending-outbox observation retains its last known
value and marks health false rather than clearing pressure. Startup recovery,
lane ownership boundaries, active-state changes, outbox commits, and outbox
sent replay refresh the projection.

## Deadline fencing

Each scheduled deadline carries:

- job ID and task ID;
- monotonic assignment attempt number;
- assignment UUID;
- worker ID;
- deadline kind and due time.

The priority index only makes an entry discoverable. When an entry is popped,
`SchedulerState` resolves the current job/task in constant time and compares
the attempt number, assignment UUID, and worker ID with the task's current
assignment. A mismatch is rejected as stale and cannot call the timeout or
lease transition. This exact UUID check also fences the same-worker ABA case.

Normal closure removes deadlines eagerly by assignment key. Stale entries are
still treated as possible—such as after a deliberately injected projection
race—and are always validated on pop. A due transition whose durable write
cannot complete keeps its authoritative assignment and schedules a 500 ms
recheck; it does not spin on a past-due entry.

## Steady-state complexity

Let `J` be runnable jobs, `A` live assignments, `D` deadlines handled in a
stage, `Wtype` capable workers advertising a task type, and `Aw` assignments
owned by one unavailable worker.

| Operation | Scheduler discovery work |
|---|---|
| Non-due timeout stage | `O(1)` head check |
| Non-due lease stage | `O(1)` head check |
| Handle `D` due deadlines | `O(D log A)` plus `D` exact task-map lookups |
| Find the next runnable job or pending task | `O(1)` |
| Close/retry one assignment | `O(log A)` deadline removal plus `O(1)` pending insertion |
| Dispatch with no compatible capacity | One task-type capacity lookup, then `O(1)` removal from runnable rotation and insertion into capacity wait |
| Choose compatible workers | `O(Wtype log Wtype)` for the score-ordered snapshot; unrelated workers are not visited |
| Handle one worker loss | `O(Aw log A)`; other jobs and assignments are not visited |
| Hydrate one job at admission/recovery | `O(Tjob log Tjob)` deterministic pending ordering plus deadline insertion, then indexed steady state |

TF-0402 bounds all four cycle stages and prevents one busy stage from
permanently excluding the next. TF-0403 adds the persistent cross-job round,
the configurable per-job assignment quota, and generation-gated
capacity-wait eviction/reactivation. Retry tasks retain priority within a job
while remaining FIFO relative to other retries. TF-0404 adds immutable plugin
costs, versioned executor snapshots, unit/type hard eligibility, and exact
assignment-generation reservation/release.

## Observability and evidence

Periodic `scheduler_metrics` events include:

- `overloaded`
- `overload_primary_reason`
- `overload_configured_maximum`
- `overload_observed_value`
- `overload_reasons`
- `job_submit_prefetch`
- `pending_outbox_observation_healthy`
- `pending_tasks_indexed`
- `runnable_jobs_indexed`
- `capacity_waiting_jobs_indexed`
- `live_assignments_indexed`
- `deadline_entries_indexed`
- `deadline_head_checks_total`
- `deadline_entries_popped_total`
- `deadline_entries_validated_total`
- `deadline_stale_rejected_total`
- `deadline_reschedules_total`
- `taskflow_capacity_snapshots_accepted_total`
- `taskflow_capacity_snapshots_stale_total`
- `taskflow_capacity_snapshots_incompatible_total`
- `taskflow_capacity_reservations_created_total`
- `taskflow_capacity_reservations_released_total`
- `taskflow_capacity_projection_failures_total`
- `taskflow_capacity_active_reservations`
- `taskflow_capacity_reserved_units`

[`SchedulerWorkloadIndexTest#profileOneTickWithOneHundredThousandNonDueAssignmentsUsesOnlyDeadlineHeads`](../taskflow-core/src/test/java/server/scheduler/SchedulerWorkloadIndexTest.java)
loads 100,000 non-due assigned tasks, which produce 200,000 live deadline
entries. One timeout/lease maintenance tick performs exactly two priority-head
checks and performs zero pops and zero task validations. The proof uses
operation counters rather than a wall-clock threshold, so it is deterministic
across machines.

Run the profile proof from the repository root:

```powershell
.\mvnw.cmd -pl taskflow-core -am "-Dtest=SchedulerWorkloadIndexTest#profileOneTickWithOneHundredThousandNonDueAssignmentsUsesOnlyDeadlineHeads" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

### Reproducible profile record

The profile was recorded on 2026-07-24 with these revisions:

- Baseline `ad7b10c039c175a204c5b8e34f2ac3605d8933fc`: the timeout
  stage and lease stage each traversed every active job and task. With 100,000
  assigned tasks, one timeout/lease maintenance pair therefore made 200,000
  task visits before evaluating the transitions. This is a source-derived
  operation baseline; that revision did not expose traversal counters.
- Changed `d157c1ea738b58e0829ea1c0e4a5763b077b2ebb`: the deterministic
  workload above retained 200,000 deadline entries while the same non-due
  maintenance pair made two head checks, zero deadline pops, and zero task
  validations. Maven reported 0.172 seconds for the whole test, including
  building the index, so that wall-clock value is recorded but is not used as
  the tick-complexity assertion.

The recording machine used a 14-core/20-thread Intel Core i5-13500HX, 63.7 GiB
RAM, 64-bit Windows 11 Pro 10.0.26200, and Oracle HotSpot Java 25.0.2. The
operation-count assertion is the portable result; absolute time will vary by
machine and JVM. The changed design moves work into `O(log A)` assignment
deadline insertion/removal and still sorts the compatible-worker snapshot.
TF-0405 now bounds the accepted active-work owner set.

Additional boundary evidence:

- [`SchedulerWorkloadIndexTest#poppedDeadlineMustMatchExactCurrentAssignmentId`](../taskflow-core/src/test/java/server/scheduler/SchedulerWorkloadIndexTest.java)
  injects an obsolete deadline and proves the replacement assignment is not
  returned for timeout handling.
- [`SchedulerWorkloadIndexTest#repeatedDispatchClosureKeepsDeadlineIndexAtTwoEntriesPerLiveAssignment`](../taskflow-core/src/test/java/server/scheduler/SchedulerWorkloadIndexTest.java)
  exercises 10,000 assignment/closure cycles without stale timer growth.
- [`InMemoryPeerRegistryTest#weightedUnitsAndTypeConcurrencyAreBothHardEligibilityFilters`](../taskflow-core/src/test/java/server/registry/InMemoryPeerRegistryTest.java)
  proves weighted-unit and task-type concurrency hard filtering.
- [`InMemoryPeerRegistryTest#reservationIdentityMismatchDisablesFurtherDispatch`](../taskflow-core/src/test/java/server/registry/InMemoryPeerRegistryTest.java)
  proves an unexpected exact-ledger mismatch fails closed.
- [`TaskSchedulerPersistenceTest#duplicateResultIsAcknowledgedWithoutRequeueOrSecondSuccess`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java)
  proves an authoritative result releases one exact reservation and its
  duplicate releases nothing.
- [`ExecutorCapacityTrackerTest`](../taskflow-core/src/test/java/peer/engine/ExecutorCapacityTrackerTest.java)
  proves local weighted reservation, lower-cost packing, overcommit clamping,
  monotonic snapshots, and idempotent local release.
- [`SchedulerArchitectureTest#normalSchedulerMaintenanceUsesIndexesInsteadOfFullTaskOrPeerScans`](../taskflow-core/src/test/java/server/scheduler/SchedulerArchitectureTest.java)
  prevents the normal timeout, lease, dispatch, and participant-loss paths from
  regressing to active-job/task/peer scans.
- [`SchedulerAdmissionTest`](../taskflow-core/src/test/java/server/scheduler/SchedulerAdmissionTest.java)
  proves typed job/task/payload rejection, exact replay while full, plugin task
  recounting, and recovery above lowered bounds.
- [`TaskSchedulerPersistenceTest#pendingOutboxThresholdRejectsWithoutDurableJobMutation`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java)
  and its active-job/task/storage-failure companions prove no rejected J0/T0
  commit or projection mutation.
- [`AdmissionOverloadExperiment#coordinatorHeapPlateausAtConfiguredBounds`](../taskflow-core/src/test/java/server/scheduler/AdmissionOverloadExperiment.java)
  provides the opt-in fixed-heap overload evidence recorded in
  [`reports/admission-overload.md`](reports/admission-overload.md).
- [`SchedulerOverloadTest#persistentMailboxSaturationPreservesAcceptedWorkAndProgress`](../taskflow-core/src/test/java/server/scheduler/SchedulerOverloadTest.java)
  provides the fixed-heap capacity-`1` baseline/changed result-progress record
  in [`reports/persistent-overload.md`](reports/persistent-overload.md).
- [`SchedulerOverloadTest#activeLimitClearsAndAllowsFreshAdmissionWithoutSchedulerRestart`](../taskflow-core/src/test/java/server/scheduler/SchedulerOverloadTest.java)
  proves typed rejection performs no candidate commit and cleanup permits a
  fresh durable admission on the same scheduler thread.
- [`SchedulerLoopTest#oneCycleAppliesExactStageLimitsInRequiredOrder`](../taskflow-core/src/test/java/server/scheduler/SchedulerLoopTest.java),
  [`SchedulerLoopTest#continuousMailboxBacklogStillProcessesDeadlinesEveryCycle`](../taskflow-core/src/test/java/server/scheduler/SchedulerLoopTest.java), and
  [`SchedulerLoopTest#continuousDueDeadlineBacklogCannotStarveQueuedTaskResult`](../taskflow-core/src/test/java/server/scheduler/SchedulerLoopTest.java)
  prove exact stage limits, ordering, and both starvation boundaries using
  deterministic in-memory queues.
- [`SchedulerWorkloadIndexTest#staleDeadlinePopsRemainVisibleAsIndividualBatchWork`](../taskflow-core/src/test/java/server/scheduler/SchedulerWorkloadIndexTest.java)
  proves stale timer entries consume the shared deadline budget.
- [`LeaseServiceBatchTest#combinedTimeoutAndLeaseWorkStopsAtDeadlineBudget`](../taskflow-core/src/test/java/server/scheduler/LeaseServiceBatchTest.java)
  proves timeout and lease entries share one enforced service-level budget.
- [`AssignmentServiceBatchTest#configuredQuotaPersistsOneRoundAcrossDispatchBatchBoundaries`](../taskflow-core/src/test/java/server/scheduler/AssignmentServiceBatchTest.java)
  proves a round cursor survives a dispatch-batch boundary and enforces the
  configured quota.
- [`AssignmentServiceBatchTest#retryPriorityRemainsInsideOneJobsQuota`](../taskflow-core/src/test/java/server/scheduler/AssignmentServiceBatchTest.java)
  proves retry priority stays local to the selected job.
- [`AssignmentServiceBatchTest#noCapacitySweepPersistsAcrossBatchesAndDoesNotSpin`](../taskflow-core/src/test/java/server/scheduler/AssignmentServiceBatchTest.java)
  proves capacity-blocked jobs leave the runnable rotation and re-enter after
  compatible capacity is registered.
- [`AssignmentServiceBatchTest#timedRecheckRestoresWaitingJobsWithoutWallClockSleep`](../taskflow-core/src/test/java/server/scheduler/AssignmentServiceBatchTest.java)
  proves the deterministic recheck fallback without sleeping.
- [`SchedulerLoopTest#externalSchedulingSignalWakesIdleLoopWithoutStoppingIt`](../taskflow-core/src/test/java/server/scheduler/SchedulerLoopTest.java)
  proves an external heartbeat/capacity signal can wake the idle loop without
  being mistaken for shutdown.
- [`JobCompletionServiceBatchTest#dueTerminalDeliveryRetriesUseBoundedBatchAndExactNextWake`](../taskflow-core/src/test/java/server/scheduler/JobCompletionServiceBatchTest.java)
  proves due terminal retries are batch-bounded and publish an exact next wake
  time.
- [`RabbitMqOutboxReplayerTest#replayLoadsAtMostConfiguredSchedulerOutboxBatch`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqOutboxReplayerTest.java)
  proves the independent durable replayer loads no more than the configured
  outbound batch.
