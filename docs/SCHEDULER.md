# Scheduler Workload Indexes

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
| Deadlines | Priority-ordered timeout and lease sets keyed by exact assignment identity | At most two entries per live indexed assignment |
| Worker assignments | Worker ID to exact assignment keys | At most one entry per live indexed assignment |
| Worker capacity | Task type to capable and currently available live worker IDs | One capable membership and at most one available membership per advertised worker/task-type pair |
| Inbound mailbox | Bounded blocking queue | `inboundQueueCapacity`, default `1000` |

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
| Dispatch with no compatible capacity | No task scan; one task-type capacity lookup |
| Choose compatible workers | `O(Wtype log Wtype)` for the score-ordered snapshot; unrelated workers are not visited |
| Handle one worker loss | `O(Aw log A)`; other jobs and assignments are not visited |
| Hydrate one job at admission/recovery | `O(Tjob log Tjob)` deterministic pending ordering plus deadline insertion, then indexed steady state |

The current TF-0401 loop may process every deadline already due or every
assignment that fits currently available capacity in one stage. TF-0402 owns
configurable batch sizes, fair mailbox/deadline ordering, and blocking until
the next deadline. TF-0403 owns the final configurable per-job assignment quota;
the runnable deque introduced here provides its rotation primitive. Retry tasks
retain priority within a job while remaining FIFO relative to other retries.

## Observability and evidence

Periodic `scheduler_metrics` events include:

- `pending_tasks_indexed`
- `runnable_jobs_indexed`
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
