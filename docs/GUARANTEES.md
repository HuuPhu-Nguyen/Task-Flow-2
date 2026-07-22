# Guarantees and Non-goals

This document is TaskFlow's authoritative guarantee contract and claim-to-test
evidence ledger. It distinguishes the required architecture contract from the
behavior currently proved on `main`. A target invariant is not a current
runtime guarantee until its evidence row is complete.

The required end-state contract is:

> **TaskFlow provides at-least-once task execution with generation-fenced, single-authoritative result commitment.**

In the evidence table, **existing** means the linked automated test runs in the
repository today, **partial** means existing tests prove only part of the
invariant, and **planned** names the stable automated test ID required by the
listed fixing-queue task. Planned evidence is not proof of current behavior.

## Supported deployment model

The supported architecture is deliberately narrow:

- Java 21 with one authoritative coordinator process.
- Multiple participant-node processes. Each participant may enable the
  requester role, the executor role, or both roles.
- Embarrassingly parallel jobs whose tasks have no inter-task dependencies.
- SQLite as the coordinator's authoritative state store.
- RabbitMQ as the target supported runtime transport. On the current baseline,
  RabbitMQ is the default but remains transitional, and TCP remains a deprecated
  compatibility/demo path until Phase 3 removes or quarantines it.
- MinIO/S3-compatible object storage for large payloads is part of the frozen
  target scope after Phase 5. The current baseline still has documented inline
  and local-file-reference limitations.

SQLite is suitable only because the supported design has one coordinator writer.
Sharing one SQLite database between multiple coordinator writers, using
network-filesystem locking as coordinator consensus, or treating SQLite as a
multi-region store is outside this contract.

## Safety invariants I1–I9

These invariants are normative requirements. Their evidence links identify
whether the current baseline fully proves them or still has an explicit gap.

### I1 — Durable acceptance

Once a job is reported as accepted, its durable job and task records exist. It
cannot be silently lost after a coordinator restart.

Automated evidence: [E-I1 — `TaskSchedulerPersistenceTest#startupPersistenceFailureReturnsFailureWithoutDispatchingTask`](#e-i1).

### I2 — Single authoritative result

A task can have at most one authoritative committed result, even if execution
or message delivery occurs more than once.

Automated evidence: [E-I2 — `RabbitMqCoordinatorLiveIntegrationTest#replayedTaskAssignmentDoesNotCreateDuplicateAcceptedResults`](#e-i2).

### I3 — Assignment fencing

A result from an obsolete assignment generation cannot commit, even when the
obsolete and current assignments use the same executor participant (`workerId`
in the planned protocol-v2 terminology).

Planned automated evidence: [E-I3 — `AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit`](#e-i3).

### I4 — Monotonic terminal state

A completed or failed job cannot return to `RUNNING`. A completed task cannot
return to `PENDING` or `ASSIGNED`.

Automated evidence: [E-I4 — `DatabaseManagerTest#taskStatusUpdatesRejectInvalidTransitions`](#e-i4).

### I5 — Transactional outbound intent

An outbound assignment or terminal result committed by the coordinator is
either published or remains durably replayable in the outbox.

Automated evidence: [E-I5 — `DatabaseManagerTest#taskAssignmentOutboxCommitsWithAssignedTask`](#e-i5).

### I6 — Duplicate tolerance

Duplicate submissions, assignments, task results, and outbox publications do
not create duplicate authoritative state transitions.

Automated evidence: [E-I6 — `TaskSchedulerFailureTest#duplicateActiveJobIdReturnsFailureWithoutReplacingOriginalJob`](#e-i6).

### I7 — Bounded coordinator memory

Scheduler ingress, pending-work indexes, deduplication caches, and
result-delivery tracking have explicit bounds.

Automated evidence: [E-I7 — `SchedulerMailboxTest#createsBoundedMailboxFromSchedulerConfig`](#e-i7).

### I8 — Poison-message termination

A permanently invalid or deterministically failing broker delivery cannot be
requeued forever.

Planned automated evidence: [E-I8 — `BrokerRetryContractTest#poisonMessageQuarantinesAfterBoundedAttempts`](#e-i8).

### I9 — Payload integrity

An executor participant or coordinator never accepts object-store content whose
length or SHA-256 digest differs from the message metadata.

Planned automated evidence: [E-I9 — `ObjectStoreIntegrityContractTest#corruptPayloadIsRejectedBeforeProcessorInvocation`](#e-i9).

## Liveness invariant I10 and assumptions

### I10 — Eventual terminality under stated assumptions

After new failures stop, every valid nonterminal job eventually becomes
terminal, provided that:

- the coordinator continues running;
- RabbitMQ and SQLite are available again;
- at least one compatible execution-capable participant eventually remains
  available;
- plugins terminate or reach their configured timeout; and
- retry and resource-admission policies permit execution.

I10 makes no progress promise while any assumption is false. It also does not
promise success: exhausting a bounded retry policy may satisfy liveness by
moving a task and its job to `FAILED`.

Planned automated evidence: [E-I10 — `CorrectnessChaosExperiment#allJobsTerminateAfterFailuresStop`](#e-i10).

## Delivery semantics

- RabbitMQ may redeliver messages.
- Broker delivery is at least once, not exactly once. A consumer may receive the
  same logical submission, assignment, task result, or final result again after
  connection loss, consumer death, negative acknowledgement, or an
  acknowledgement crash window.
- Coordinator-originated RabbitMQ `TASK_ASSIGN` and terminal `JOB_RESULT`
  messages use the SQLite outbox when persistence is enabled. Publication may be
  repeated if RabbitMQ accepted a message but the coordinator crashed before
  recording the row as published.
- A confirmed publish establishes broker acceptance, not consumer processing
  and not exactly-once delivery.
- Participant-originated `TASK_RESULT` messages are not stored in the
  coordinator outbox. When result publication fails, assignment redelivery or
  lease-based reassignment may cause another execution.
- Duplicate and stale domain messages must be acknowledged after durable
  classification rather than requeued indefinitely. Bounded delayed retry and
  poison quarantine remain planned Phase 3 work where the evidence table says
  the current behavior is incomplete.

## Execution semantics

- Execution-capable participants may execute a task more than once.
- Re-execution may follow assignment redelivery, an executor crash, a timeout,
  lease expiry, lost result publication, or eviction/restart of the planned
  bounded executor deduplication cache.
- Coordinator fencing limits which result becomes authoritative; it does not
  undo work already performed by a plugin.
- Plugin external side effects are not made exactly once by the coordinator.
  A plugin that calls an external system must be pure, idempotent, use an
  appropriate idempotency key, or explicitly reject retry-capable operation.
- The current runtime bounds task attempts with its configured retry policy,
  but the complete plugin retry-safety contract is planned in TF-0208.

## Result-commit semantics

Only the current persisted assignment generation may commit the authoritative result.
The target commit predicate includes task ID, `ASSIGNED` state,
monotonic attempt number, assignment ID, and assigned executor-participant
identity. SQLite's conditional transition is the authority; an in-memory state
change alone is not a commit.

A matching result may commit once. A repeated result for an already completed
assignment is a duplicate; a result for an older attempt, a different
assignment ID, or a different executor participant is stale. Duplicate and
stale results must not change authoritative task data, job completion, lease
state, or executor-success accounting.

The current baseline does **not** yet meet the full generation-fencing contract:
it checks `ASSIGNED` state and current participant identity but does not persist
and echo a monotonic assignment generation plus assignment ID. Therefore a late
result from an earlier assignment to the same participant is a known ABA gap,
and the target statement at the top of this document must not be presented as a
proved current release guarantee until TF-0101 through TF-0106 and their tests
pass.

## Crash-recovery semantics

- Recovery means restart of the single authoritative coordinator, not seamless
  failover or concurrent coordinator operation.
- Job/task creation is transactional in SQLite, and assignment is persisted
  before dispatch. A persistence failure before acceptance must fail the
  submission rather than dispatch untracked work.
- On current startup recovery, resumable running jobs are rebuilt from durable
  snapshots. Assigned tasks with unexpired leases remain assigned; expired or
  missing leases return to pending; non-resumable running jobs become failed.
- Pending coordinator outbox rows remain replayable after restart. Replay may
  duplicate publication, so correctness depends on duplicate classification and
  assignment fencing rather than assuming exactly-once delivery.
- A participant crash may lose in-process execution and result-publication
  state. The coordinator may reassign after failure detection or lease expiry,
  allowing duplicate execution.
- Recovery makes progress only under the I10 assumptions. Full broker-outage
  recovery, the complete crash-window matrix, and correctness-chaos evidence are
  planned work and are not implied by today's unit-test baseline.

## Explicit non-goals

- Exactly-once execution of arbitrary plugin side effects.
- DAG or workflow-dependency execution.
- Multi-region deployment.
- Byzantine-fault tolerance.
- Zero-downtime coordinator availability.
- Multiple coordinator writers before the optional high-availability phase.
- Kubernetes-style general-purpose resource scheduling.
- Arbitrary payloads transported through RabbitMQ without size limits.

These non-goals do not waive the safety invariants. For example, duplicate
plugin execution is allowed, but duplicate authoritative result commitment is
not.

## Claim-to-test evidence table

Test IDs use `Class#method`. Existing IDs link to source. Planned IDs are stable
names for the listed queue task and must be implemented or explicitly redirected
when that task lands; until then, the associated invariant remains partial or
planned.

| Invariant | Evidence status | Existing automated test IDs | Planned automated test ID and queue owner | Remaining proof gap |
|---|---|---|---|---|
| <a id="e-i1"></a> I1 | Partial | [`TaskSchedulerPersistenceTest#startupPersistenceFailureReturnsFailureWithoutDispatchingTask`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`DatabaseManagerTest#rollsBackAtomicJobStartupWhenTaskInsertFails`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java) | `PersistenceContractTest#acceptedJobSurvivesCoordinatorRestart` (TF-0701/TF-0705) | Prove the post-commit/pre-response crash window and restart survival end to end. |
| <a id="e-i2"></a> I2 | Partial; blocked by I3 gap | [`TaskSchedulerFailureTest#staleResultFromWrongPeerIsIgnoredUntilAssignedPeerCompletesTask`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#replayedTaskAssignmentDoesNotCreateDuplicateAcceptedResults`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java) | `PersistenceContractTest#matchingAssignmentCommitsExactlyOnce` (TF-0105/TF-0701) | Add durable conditional commitment and cover same-participant reassignment. |
| <a id="e-i3"></a> I3 | Planned; known correctness gap | [`TaskSchedulerFailureTest#expiredLeaseReassignsTaskAndRejectsLateResultFromOldPeer`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java) proves only different-participant rejection | `AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit` (TF-0106) | Persist/echo attempt number and assignment ID, then fence the same-participant ABA case. |
| <a id="e-i4"></a> I4 | Existing persistence guards; broader property proof planned | [`DatabaseManagerTest#taskStatusUpdatesRejectInvalidTransitions`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#jobStatusUpdatesRejectTerminalOverwrites`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java) | `PersistenceContractTest#terminalStatesAreMonotonic` (TF-0701/TF-0704) | Exercise monotonicity across generated event sequences and recovery. |
| <a id="e-i5"></a> I5 | Partial | [`DatabaseManagerTest#taskAssignmentOutboxCommitsWithAssignedTask`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#completedJobOutboxCommitsTerminalStateAndResultMessage`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`RabbitMqOutboxReplayerTest#replayRecordsFailedAttemptAndLeavesRowPending`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqOutboxReplayerTest.java) | `CrashWindowMatrixTest#publishedAssignmentRemainsReplayableUntilMarked` (TF-0304/TF-0705) | Prove every publish/mark crash window and atomic finalization from the last task result. |
| <a id="e-i6"></a> I6 | Partial | [`TaskSchedulerFailureTest#duplicateActiveJobIdReturnsFailureWithoutReplacingOriginalJob`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#replayedTaskAssignmentDoesNotCreateDuplicateAcceptedResults`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java) | `TaskFlowModelPropertyTest#duplicateEventsDoNotDuplicateAuthoritativeTransitions` (TF-0207/TF-0704) | Add idempotent same-request submission replay, executor assignment deduplication, and generated duplicate sequences. |
| <a id="e-i7"></a> I7 | Partial | [`SchedulerMailboxTest#createsBoundedMailboxFromSchedulerConfig`](../taskflow-core/src/test/java/server/scheduler/SchedulerMailboxTest.java); [`SchedulerMailboxTest#repeatedBrokerOverflowRequeuesDeliveriesWithoutReplacingAcceptedWork`](../taskflow-core/src/test/java/server/scheduler/SchedulerMailboxTest.java) | `OverloadExperiment#coordinatorHeapPlateausAtConfiguredBounds` (TF-0401–TF-0406/TF-0709) | Bound pending indexes, dedupe caches, active work, outbox growth, and result-delivery tracking. |
| <a id="e-i8"></a> I8 | Partial | [`TaskSchedulerFailureTest#invalidBrokerTaskResultIsRejectedWithoutRequeue`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java) | `BrokerRetryContractTest#poisonMessageQuarantinesAfterBoundedAttempts` (TF-0302/TF-0303/TF-0702) | Replace generic repeated handler requeue with typed disposition, bounded delayed retry, and quarantine. |
| <a id="e-i9"></a> I9 | Planned | None; object storage is not implemented on the current baseline. | `ObjectStoreIntegrityContractTest#corruptPayloadIsRejectedBeforeProcessorInvocation` (TF-0504) | Add object references, streaming length/digest verification, and corrupt-byte tests. |
| <a id="e-i10"></a> I10 | Partial | [`TaskSchedulerFailureTest#taskFailureAtRetryLimitReturnsFailedJobResult`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`TaskSchedulerFailureTest#taskTimeoutAtRetryLimitReturnsFailedJobResult`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`CoordinatorStartupRecoveryTest#releasesExpiredAssignedLeasesOnResume`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java) | `CorrectnessChaosExperiment#allJobsTerminateAfterFailuresStop` (TF-0706) | Prove eventual terminality after coordinator, broker, executor, redelivery, and delay faults stop. |

Detailed current implementation behavior remains documented in
[Current execution behavior](EXECUTION_GUARANTEES.md). That document is
subordinate to this contract and must not promote a partial/planned evidence row
to a current guarantee.
