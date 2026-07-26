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
| Worker capacity | Task type to capable and currently available live worker IDs | One capable membership and at most one available membership per advertised worker/task-type pair |
| Inbound mailbox | Bounded blocking queue | `inboundQueueCapacity`, default `1000` |
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
exceed `schedulerDispatchBatchSize`. The pre-TF-0402 14-argument and TF-0402
18-argument `SchedulerConfig` constructors remain compatibility overloads and
supply the default quota.

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

These are exact bounds relative to already accepted active work. TaskFlow does
not yet impose absolute active-job or active-task admission limits; TF-0405
owns those pre-acceptance limits. An index insertion is consequently not
dropped or evicted under pressure, because silently losing already accepted
work would violate scheduler progress.

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
stage, `Wtype` available workers advertising a task type, and `Aw` assignments
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
while remaining FIFO relative to other retries. TF-0404 still owns weighted
capacity units and task-type concurrency.

## Observability and evidence

Periodic `scheduler_metrics` events include:

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
Absolute active-work bounds remain assigned to TF-0405.

Additional boundary evidence:

- [`SchedulerWorkloadIndexTest#poppedDeadlineMustMatchExactCurrentAssignmentId`](../taskflow-core/src/test/java/server/scheduler/SchedulerWorkloadIndexTest.java)
  injects an obsolete deadline and proves the replacement assignment is not
  returned for timeout handling.
- [`SchedulerWorkloadIndexTest#repeatedDispatchClosureKeepsDeadlineIndexAtTwoEntriesPerLiveAssignment`](../taskflow-core/src/test/java/server/scheduler/SchedulerWorkloadIndexTest.java)
  exercises 10,000 assignment/closure cycles without stale timer growth.
- [`InMemoryPeerRegistryTest#availableCapacityIndexIsKeyedByTaskTypeAndTracksTheConfiguredBoundary`](../taskflow-core/src/test/java/server/registry/InMemoryPeerRegistryTest.java)
  proves task-type isolation and exact removal/re-entry at the configured
  capacity boundary.
- [`SchedulerArchitectureTest#normalSchedulerMaintenanceUsesIndexesInsteadOfFullTaskOrPeerScans`](../taskflow-core/src/test/java/server/scheduler/SchedulerArchitectureTest.java)
  prevents the normal timeout, lease, dispatch, and participant-loss paths from
  regressing to active-job/task/peer scans.
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
