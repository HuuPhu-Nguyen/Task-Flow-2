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

The [task and job state machine](STATE_MACHINE.md) maps every current lifecycle
mutation to its guard, durable/outbox effects, projection order, replay rule,
observability, and forbidden edges. It remains subordinate to the evidence
status in this guarantee contract.

## Supported deployment model

The supported architecture is deliberately narrow:

- Java 21 with one authoritative coordinator process.
- Multiple participant-node processes. Each participant may enable the
  requester role, the executor role, or both roles.
- Embarrassingly parallel jobs whose tasks have no inter-task dependencies.
- SQLite as the coordinator's authoritative state store.
- RabbitMQ as the sole supported runtime transport. Its current implementation
  remains transitional rather than production-ready.
- MinIO/S3-compatible object storage for large payloads is part of the frozen
  target scope after Phase 5. Portable conversion input and attempt-output
  references and bounded orphan-output collection are active.

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
or message delivery occurs more than once. For object-backed results, upload
alone is not authority: only the exact SQLite result transaction can store the
winning reference.

Automated evidence: [E-I2 — `DatabaseManagerTest#matchingAssignmentCommitsExactlyOnceAndDuplicateIsTyped`](#e-i2).

### I3 — Assignment fencing

A result from an obsolete assignment generation cannot commit, even when the
obsolete and current assignments use the same executor participant (`workerId`
in protocol-v2 terminology). Its staged object may exist, but its pointer
cannot replace the current assignment's committed pointer.

Automated deterministic and live-broker evidence: [E-I3 — `AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit`](#e-i3).

### I4 — Monotonic terminal state

A `FINALIZING`, completed, or failed job cannot return to `RUNNING`; a
completed or failed job cannot return to `FINALIZING`. A completed task cannot
return to `PENDING` or `ASSIGNED`.

Automated evidence: [E-I4 — `DatabaseManagerTest#taskStatusUpdatesRejectInvalidTransitions`](#e-i4).

### I5 — Transactional outbound intent

An outbound assignment or terminal result committed by the coordinator is
either published or remains durably replayable in the outbox. Before a
successful terminal result exists, the last authoritative task-result
transaction durably records `FINALIZING`, so restart can recreate the terminal
payload and outbox intent from committed task results.

Automated evidence: [E-I5 — `DatabaseManagerTest#assignmentCommitBeforePublishLeavesOneDurableIdentityAndPendingOutbox`](#e-i5).

### I6 — Duplicate tolerance

Duplicate submissions, assignments, task results, and outbox publications do
not create duplicate authoritative state transitions.

Automated evidence: [E-I6 — `DuplicateSubmissionIntegrationTest#lostAcceptanceResponseReplaysAcceptedJob`](#e-i6).

### I7 — Bounded coordinator memory

Scheduler ingress and every scheduler-cycle stage have explicit bounds.
Pending-work indexes are bounded relative to accepted active work, and
deduplication caches have configured capacity/TTL limits. Active jobs, retained
active tasks, submitted/plugin-produced tasks, inline bytes, per-reference
bytes, and pending SQLite outbox rows have explicit pre-acceptance limits.
Recovered accepted work is retained rather than evicted. The scheduler's
configured ordinary lane has one additional fixed task-result reserve, and
RabbitMQ submission intake is independently bounded at prefetch `1`.

Heap plateau evidence:
[`AdmissionOverloadExperiment#coordinatorHeapPlateausAtConfiguredBounds`](../taskflow-core/src/test/java/server/scheduler/AdmissionOverloadExperiment.java)
and [`docs/reports/admission-overload.md`](reports/admission-overload.md), plus
the capacity-`1` baseline/changed result-progress profile in
[`SchedulerOverloadTest#persistentMailboxSaturationPreservesAcceptedWorkAndProgress`](../taskflow-core/src/test/java/server/scheduler/SchedulerOverloadTest.java)
and [`docs/reports/persistent-overload.md`](reports/persistent-overload.md).

Automated evidence: [E-I7 — bounded admission and persistent-overload progress](#e-i7).

### I8 — Poison-message termination

A permanently invalid or deterministically failing broker delivery cannot be
requeued forever.

Classification and bounded-quarantine evidence are listed in
[E-I8](#e-i8).

### I9 — Payload integrity

An executor participant or coordinator never accepts object-store content whose
length or SHA-256 digest differs from the message metadata.

Object-backed conversion input and attempt-output uploads verify the streamed
bytes against their portable reference. Executor input downloads and requester
result downloads verify exact length and digest before processor or output-file
acceptance. Immutable corruption is reported as a permanent classified task
failure, so a current coordinator closes the exact assignment durably and
terminally without creating a retry generation. See [E-I9](#e-i9).

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
- Every consumer chooses one of `ACK_SUCCESS`, `ACK_DUPLICATE_OR_STALE`,
  `RETRY_TRANSIENT`, `REJECT_INVALID`, or `QUARANTINE_POISON`. Duplicate and
  stale domain messages are acknowledged; invalid messages reject without
  requeue; explicit transient failures and deterministic poison enter a bounded
  TTL retry schedule and automatically reach final quarantine after exhaustion.
  The attempt, original route, and classified failure reason are observable in
  message headers, logs, and operator inspection.
- Coordinator submissions and task results remain unacknowledged through
  bounded mailbox admission and scheduler/store classification. Connection
  loss before acknowledgement causes broker redelivery. If the durable result
  transition already committed, redelivery is a harmless typed duplicate and
  is acknowledged without another authoritative transition.
- Graceful coordinator shutdown closes intake before cancelling consumers and
  draining admitted envelopes. A delivery that races after intake closure is
  left unsettled; closing the broker channel returns it to RabbitMQ. SQLite is
  closed only after the scheduler, peer monitor, and outbox replayer have
  stopped. Broker-process restart is proved separately through the managed
  single-broker recovery scenario rather than inferred from this
  acknowledgement boundary.

## Execution semantics

- Execution-capable participants may execute a task more than once.
- Re-execution may follow assignment redelivery, an executor crash, a timeout,
  lease expiry, lost result publication, or eviction/restart of the bounded
  executor deduplication cache.
- Coordinator fencing limits which result becomes authoritative; it does not
  undo work already performed by a plugin.
- Plugin external side effects are not made exactly once by the coordinator.
  A plugin that calls an external system must be pure, idempotent, use an
  appropriate idempotency key, or explicitly reject retry-capable operation.
- Every paired server/executor plugin declares `PURE`, `IDEMPOTENT`,
  `REQUIRES_IDEMPOTENCY_KEY`, or `UNSAFE_TO_RETRY`. The coordinator permits the
  normal task retry policy for the first three and rejects a new unsafe job
  before acceptance when `maxTaskRetries > 0`; current configuration requires a
  positive value.
- For a keyed plugin, logical `taskId` is stable across retry generations and
  `assignmentId` is stable only within one generation. Plugin documentation
  must identify the external key used. TaskFlow validates the declaration but
  cannot prove the external operation honored it.
- Current evidence includes
  [`TaskSchedulerFailureTest#unsafePluginIsRejectedBeforeJobAcceptanceWhenRetriesAreEnabled`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java),
  [`PeerExecutionEngineTest#retryGenerationKeepsLogicalTaskIdAndSuppliesNewStableAssignmentId`](../taskflow-core/src/test/java/peer/engine/PeerExecutionEngineTest.java),
  and
  [`ExamplePluginContractHarnessTest#examplePluginRunsAcrossClientServerPeerAndResultHandlerContracts`](../plugins/example/harness/src/test/java/example/harness/ExamplePluginContractHarnessTest.java).

## Result-commit semantics

Only the current persisted assignment generation may commit the authoritative result.
The implemented commit predicate includes task ID, `ASSIGNED` state,
monotonic attempt number, assignment ID, and assigned executor-participant
identity. SQLite's conditional transition is the authority; an in-memory state
change alone is not a commit.

A matching result may commit once. A repeated result for an already completed
assignment is a duplicate; a result for an older attempt, a different
assignment ID, or a different executor participant is stale. Duplicate and
stale results must not change authoritative task data, job completion, lease
state, or executor-success accounting.

The current baseline implements this SQLite predicate and the typed scheduler
dispositions. Store-level tests prove matching, duplicate, older-attempt,
wrong-assignment-ID, wrong-participant, same-participant ABA, unknown-task, and
storage-failure outcomes; scheduler tests prove that stale and duplicate broker
deliveries are acknowledged without success-state mutation and storage failure
receives bounded delayed retry without an in-memory completion. The complete same-participant
attempt-1/X to attempt-2/Y scenario is proved both deterministically against the
real scheduler/SQLite boundary and through live RabbitMQ delivery. Live
post-commit/pre-ack connection loss also proves that RabbitMQ redelivery reaches
SQLite's duplicate classification with one succeeded attempt and one success
metric. Within the supported single-coordinator SQLite/RabbitMQ scope,
assignment fencing and acknowledgement-loss duplicate safety are current
guarantees.

## Crash-recovery semantics

- Recovery means restart of the single authoritative coordinator, not seamless
  failover or concurrent coordinator operation.
- Job/task creation is transactional in SQLite, and assignment is persisted
  before dispatch. A persistence failure before acceptance must fail the
  submission rather than dispatch untracked work.
- Correctness-relevant assignment, result, retry/terminal failure, job
  terminalization, and final-outbox transitions are conditional durable writes
  followed by in-memory projection. A one-shot write fault does not change task
  status, participant capacity, active-job membership, transition metrics, or
  outbound delivery as though the requested transition committed.
- The last successful task-result transaction atomically closes the task and
  attempt and moves the job to `FINALIZING` when the exact expected task set is
  complete and result-bearing. Semantic aggregation runs outside that
  transaction, deterministically from the ordered committed task results; job
  `COMPLETED`, final payload, and RabbitMQ outbox intent then commit together.
- On current startup recovery, resumable `RUNNING` and `FINALIZING` jobs are rebuilt from durable
  snapshots, including task status, retry count, assignment generation/UUID,
  participant, lease, and committed result payload. Assigned tasks with
  complete identities and unexpired leases remain assigned; expired leases and
  incomplete legacy assignments return to pending without resetting the last
  known generation; a valid `FINALIZING` job is aggregated again, and
  non-resumable jobs become failed.
- Pending coordinator outbox rows remain replayable after restart. Replay may
  duplicate publication, so correctness depends on duplicate classification and
  assignment fencing rather than assuming exactly-once delivery.
- A participant crash may lose in-process execution and result-publication
  state. The coordinator may reassign after failure detection or lease expiry,
  allowing duplicate execution.
- Recovery makes progress only under the I10 assumptions. The
  [formal failure model](FAILURE_MODEL.md) defines every required crash,
  duplicate-delivery, outage, overload, and recovery window while marking
  incomplete evidence explicitly. Managed single-broker outage recovery is
  implemented; cluster failover, the broader automated process-crash matrix,
  and correctness-chaos evidence remain planned work and are not implied by
  that focused scenario.

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
| <a id="e-i1"></a> I1 | Partial; deterministic restart boundary exists | [`TaskSchedulerPersistenceTest#startupPersistenceFailureReturnsFailureWithoutDispatchingTask`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`DatabaseManagerTest#failedTaskInsertRollsBackSubmissionHashAndJobTogether`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DuplicateSubmissionIntegrationTest#lostAcceptanceResponseReplaysAcceptedJob`](../taskflow-coordinator/src/test/java/server/DuplicateSubmissionIntegrationTest.java) | `PersistenceContractTest#acceptedJobSurvivesCoordinatorRestart` / `CrashWindowMatrixTest#acceptedJobSurvivesLostAcceptanceResponse` (TF-0701/TF-0705) | Re-run the implemented post-commit/lost-response mechanism through the reusable contract and process-kill harness. |
| <a id="e-i2"></a> I2 | Partial; conditional commit and live acknowledgement-loss mechanisms exist | [`DatabaseManagerTest#matchingAssignmentCommitsExactlyOnceAndDuplicateIsTyped`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`TaskSchedulerPersistenceTest#duplicateResultIsAcknowledgedWithoutRequeueOrSecondSuccess`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#outboxMarkFailureReplaysDuplicateAssignmentWithoutDuplicateAcceptedResult`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#durableCommitBeforeLostAcknowledgementRedeliversAsHarmlessDuplicate`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java) | `PersistenceContractTest#matchingAssignmentCommitsExactlyOnce` (TF-0701) | Re-run the mechanism through the reusable store contract and broader process-kill/property suites. |
| <a id="e-i3"></a> I3 | Existing for the supported single-coordinator SQLite/RabbitMQ scope | [`DatabaseManagerTest#conditionalResultCommitRejectsOldAttemptWrongAssignmentAndWrongWorker`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#sameWorkerAbaResultIsStaleAtStoreBoundary`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`TaskSchedulerPersistenceTest#staleResultIsAcknowledgedWithoutRequeueOrSuccessAccounting`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`TaskSchedulerFailureTest#samePeerStaleFailureCannotCloseNewerAssignmentGeneration`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit`](../taskflow-coordinator/src/test/java/server/rabbitmq/AssignmentFencingIntegrationTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#sameWorkerAbaResultCannotCommitThroughLiveBroker`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`CoordinatorStartupRecoveryTest#sqliteRestartRecoveryReconstructsCommittedRetryProjectionAndGeneration`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java) | — | No TF-0106 mechanism gap remains; broader generated-event proof is tracked separately by TF-0704. |
| <a id="e-i4"></a> I4 | Existing persistence guards and durable-first fault evidence; broader property proof planned | [`DatabaseManagerTest#taskStatusUpdatesRejectInvalidTransitions`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#jobStatusUpdatesRejectTerminalOverwrites`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#lastResultAndFinalizingIntentRollbackTogetherOnIntentFault`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`TaskSchedulerPersistenceTest#terminalTaskWriteFailurePreservesAssignedProjectionAndCapacity`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`TaskSchedulerPersistenceTest#oneShotJobCompletionWriteFailureProjectsOnlyAfterRetryCommits`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`TaskSchedulerPersistenceTest#oneShotJobFailureWriteFailureProjectsOnlyAfterRetryCommits`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java) | `PersistenceContractTest#terminalStatesAreMonotonic` (TF-0701/TF-0704) | Exercise monotonicity across generated event sequences and recovery. |
| <a id="e-i5"></a> I5 | Partial; transactional intent, live publish/mark fault, and last-result finalization boundaries are implemented | [`DatabaseManagerTest#assignmentCommitBeforePublishLeavesOneDurableIdentityAndPendingOutbox`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#repeatedTypedAssignmentCommitReturnsExactDurableProjectionAndOutbox`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#outboxInsertFailureRollsBackTaskAndAttemptTogether`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#concurrentAssignmentCallsCreateOnlyOneGenerationAndOutbox`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`RabbitMqOutboxReplayerTest#replayPublishesOriginalDatabaseCommittedAssignmentIdentity`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqOutboxReplayerTest.java); [`RabbitMqOutboxReplayerTest#failedSentMarkLeavesConfirmedMessagePendingForIdenticalReplay`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqOutboxReplayerTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#confirmedPersistentWorkerAssignmentMarksOutboxPublished`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#connectionLossDuringWorkerAssignmentPublishLeavesOutboxReplayable`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#unroutableWorkerAssignmentLeavesOutboxReplayable`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#outboxMarkFailureReplaysDuplicateAssignmentWithoutDuplicateAcceptedResult`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`TaskSchedulerPersistenceTest#failedFinalOutboxWriteFailurePreservesRemainingAssignmentUntilRetryCommits`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`DatabaseManagerTest#completedJobOutboxCommitsTerminalStateAndResultMessage`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#repeatedTypedFinalOutboxCommitsReturnExactDurableRecord`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#finalResultOutboxInsertFaultRollsBackTerminalJobState`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#failedFinalResultOutboxFaultRollsBackTasksJobAndAttemptBeforeRetry`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`DatabaseManagerTest#concurrentFinalizationCreatesOneTerminalStateAndOneOutbox`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`JobFinalizationCrashTest#lastTaskCommitCannotStrandJob`](../taskflow-coordinator/src/test/java/server/JobFinalizationCrashTest.java) | `CrashWindowMatrixTest#publishedAssignmentRemainsReplayableUntilMarked` / `CrashWindowMatrixTest#publishedFinalResultRemainsReplayableUntilMarked` (TF-0705) | Re-run the implemented failure boundaries through a process-kill crash harness. |
| <a id="e-i6"></a> I6 | Existing mechanisms for submission, assignment, result, acknowledgement-loss, and outbox duplicates; broader generated proof planned | [`DuplicateSubmissionIntegrationTest#lostAcceptanceResponseReplaysAcceptedJob`](../taskflow-coordinator/src/test/java/server/DuplicateSubmissionIntegrationTest.java); [`DuplicateSubmissionIntegrationTest#exactDuplicateReplaysPersistedTerminalResult`](../taskflow-coordinator/src/test/java/server/DuplicateSubmissionIntegrationTest.java); [`DatabaseManagerTest#concurrentIdenticalSubmissionCommitsOneJobAndOneTaskSet`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java); [`TaskSchedulerFailureTest#activeJobIdWithDifferentRequestReturnsIdempotencyConflictWithoutReplacingOriginalJob`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#deliveryBeforeAcknowledgementIsRedeliveredAfterCoordinatorConnectionCloses`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#durableCommitBeforeLostAcknowledgementRedeliversAsHarmlessDuplicate`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`RabbitMqCoordinatorShutdownLiveIntegrationTest#shutdownDrainsAcceptedDeliveryAndReturnsPostStopDeliveryToRabbitMq`](../taskflow-coordinator/src/test/java/server/RabbitMqCoordinatorShutdownLiveIntegrationTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#outboxMarkFailureReplaysDuplicateAssignmentWithoutDuplicateAcceptedResult`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`WorkerAssignmentDeduplicationIntegrationTest#duplicateRunningAssignmentExecutesOnce`](../taskflow-peer/src/test/java/peer/WorkerAssignmentDeduplicationIntegrationTest.java); [`WorkerAssignmentDeduplicationIntegrationTest#duplicateCompletedAssignmentRepublishesSameResult`](../taskflow-peer/src/test/java/peer/WorkerAssignmentDeduplicationIntegrationTest.java) | `TaskFlowModelPropertyTest#duplicateEventsDoNotDuplicateAuthoritativeTransitions` (TF-0704) | Exercise all duplicate event families through generated sequences; no TF-0207 submission or TF-0305 acknowledgement mechanism gap remains. |
| <a id="e-i7"></a> I7 | Existing for configured active-work, submitted-payload, pending-outbox, two-lane mailbox, bounded cycle, fairness, executor-cache bounds, and persistent-overload progress | [`SchedulerMailboxTest#fullSubmissionLaneRetainsOneTaskResultReserveAndDequeuesResultFirst`](../taskflow-core/src/test/java/server/scheduler/SchedulerMailboxTest.java); [`AdmissionPolicyTest#activeJobBoundaryAllowsExactMaximumAndRejectsNextJob`](../taskflow-core/src/test/java/server/scheduler/AdmissionPolicyTest.java); [`AdmissionPolicyTest#activeTaskBoundaryAllowsExactMaximumAndRejectsNextTask`](../taskflow-core/src/test/java/server/scheduler/AdmissionPolicyTest.java); [`TaskSchedulerPersistenceTest#pendingOutboxThresholdRejectsWithoutDurableJobMutation`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`TaskSchedulerPersistenceTest#exactReplayBypassesPendingOutboxThreshold`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`AdmissionOverloadExperiment#coordinatorHeapPlateausAtConfiguredBounds`](../taskflow-core/src/test/java/server/scheduler/AdmissionOverloadExperiment.java); [`SchedulerOverloadTest#persistentMailboxSaturationPreservesAcceptedWorkAndProgress`](../taskflow-core/src/test/java/server/scheduler/SchedulerOverloadTest.java); [`SchedulerOverloadTest#activeLimitClearsAndAllowsFreshAdmissionWithoutSchedulerRestart`](../taskflow-core/src/test/java/server/scheduler/SchedulerOverloadTest.java); [`RabbitMqTransportLiveTest#routeLocalJobPrefetchDoesNotBlockTaskResultIntakeAgainstLiveBroker`](../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqTransportLiveTest.java); [`RabbitMqCoordinatorLiveIntegrationTest#submissionFloodPreservesAcceptedResultsAndRecoversAdmissionAgainstLiveBroker`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java); [`AssignmentServiceBatchTest#oneLargeJobAndTenSmallJobsAllDispatchInTheirFirstCompleteRound`](../taskflow-core/src/test/java/server/scheduler/AssignmentServiceBatchTest.java); [`PeerExecutionEngineTest#cacheBoundsAndCapacityEvictionPermitReexecution`](../taskflow-core/src/test/java/peer/engine/PeerExecutionEngineTest.java) | — | General Phase 7 chaos-scale overload remains outside I7; the TF-0406 result/expiry and automatic-admission mechanism gap is closed. See the [persistent-overload report](reports/persistent-overload.md). |
| <a id="e-i8"></a> I8 | Existing for the configured live-broker retry/quarantine scope | [`DeliveryFailureClassifierTest#everyExceptionCategoryMapsToOneDisposition`](../taskflow-spi/src/test/java/transport/DeliveryFailureClassifierTest.java); [`RabbitMqTransportDeliveryDispositionTest#deterministicHandlerFailureUsesTheSameBoundedDelayBeforeQuarantine`](../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqTransportDeliveryDispositionTest.java); [`RabbitMqTransportLiveTest#poisonMessageQuarantinesAfterBoundedAttempts`](../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqTransportLiveTest.java); [`RabbitMqDlqClientTest#quarantinedMessageCanBeInspectedAndManuallyRedrivenWithFreshAttemptBudget`](../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqDlqClientTest.java); [`TaskSchedulerFailureTest#invalidBrokerTaskResultIsRejectedWithoutRequeue`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`WorkerAssignmentDeduplicationIntegrationTest#assignmentIdentityCollisionIsQuarantinedAsDeterministicPoison`](../taskflow-peer/src/test/java/peer/WorkerAssignmentDeduplicationIntegrationTest.java); [`RabbitMqBrokerRecoveryIntegrationTest#unavailableStartupAndActiveBrokerRestartRecoverOutboxConsumersAndFencing`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqBrokerRecoveryIntegrationTest.java) | — | No Phase 3 topology, acknowledgement-window, or single-broker restart mechanism gap remains. An offline participant route remains outside the finite delayed-return guarantee. |
| <a id="e-i9"></a> I9 | Existing for object-backed conversion input processing, attempt-output staging/authority, requester result-file acceptance, and bounded orphan-output cleanup | [`PayloadIntegrityVerifierTest#rejectsTruncatedAndExtendedContentWithObservedLengths`](../taskflow-spi/src/test/java/objectstore/PayloadIntegrityVerifierTest.java); [`PayloadIntegrityVerifierTest#rejectsSameLengthCorruptionWithObservedDigest`](../taskflow-spi/src/test/java/objectstore/PayloadIntegrityVerifierTest.java); [`ObjectStoreIntegrityContractTest#corruptPayloadIsRejectedBeforeProcessorInvocation`](../taskflow-spi/src/test/java/objectstore/ObjectStoreIntegrityContractTest.java); [`ObjectStoreContractTest#putRejectsBytesThatDoNotMatchTheReference`](../taskflow-objectstore-minio/src/test/java/objectstore/minio/ObjectStoreContractTest.java); [`ObjectStoreContractTest#putIfAbsentNeverReplacesAnExistingObject`](../taskflow-objectstore-minio/src/test/java/objectstore/minio/ObjectStoreContractTest.java); [`ImageConversionProcessorTest#rejectsCorruptReferencedInputBeforeImageProcessing`](../plugins/conversion/peer/src/test/java/peer/processors/ImageConversionProcessorTest.java); [`ImageConversionProcessorTest#reusesIdenticalAssignmentOutputAndKeepsAttemptsIndependent`](../plugins/conversion/peer/src/test/java/peer/processors/ImageConversionProcessorTest.java); [`ConversionClientPluginTest#rejectsCorruptObjectResultBeforeWritingOutput`](../plugins/conversion/client/src/test/java/client/plugins/conversion/ConversionClientPluginTest.java); [`MinioObjectStoreContractTest#corruptObjectBytesAreRejectedBeforeImageProcessing`](../taskflow-objectstore-minio/src/test/java/objectstore/minio/MinioObjectStoreContractTest.java); [`PeerExecutionEngineTest#payloadIntegrityFailureIsPermanentAndEmitsStructuredEvent`](../taskflow-core/src/test/java/peer/engine/PeerExecutionEngineTest.java); [`TaskSchedulerFailureTest#permanentPayloadIntegrityFailureTerminatesWithoutReplacementAssignment`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`TaskSchedulerPersistenceTest#payloadIntegrityFailurePersistsTerminalAttemptWithoutRetry`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java); [`OrphanOutputGcTest#uploadedOutputWithoutResultBecomesCollectable`](../taskflow-coordinator/src/test/java/server/objectstore/OrphanOutputGcTest.java); [`OrphanOutputGcTest#staleAttemptIsDeletedWhileActiveAndAuthoritativeOutputsArePreserved`](../taskflow-coordinator/src/test/java/server/objectstore/OrphanOutputGcTest.java); [`DatabaseManagerTest#classifiesExactAttemptOutputAgainstActiveAndAuthoritativeState`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java) | — | Phase 5 is complete for attempt outputs. Automatic referenced-input retention/deletion remains separate scope; Phase 6 observability does not alter payload authority. |
| <a id="e-i10"></a> I10 | Partial; deterministic cross-job progress and single-broker restart progress are proved | [`TaskSchedulerFailureTest#taskFailureAtRetryLimitReturnsFailedJobResult`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`TaskSchedulerFailureTest#taskTimeoutAtRetryLimitReturnsFailedJobResult`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java); [`AssignmentServiceBatchTest#oneLargeJobAndTenSmallJobsAllDispatchInTheirFirstCompleteRound`](../taskflow-core/src/test/java/server/scheduler/AssignmentServiceBatchTest.java); [`AssignmentServiceBatchTest#timedRecheckRestoresWaitingJobsWithoutWallClockSleep`](../taskflow-core/src/test/java/server/scheduler/AssignmentServiceBatchTest.java); [`CoordinatorStartupRecoveryTest#releasesExpiredAssignedLeasesOnResume`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java); [`JobFinalizationCrashTest#lastTaskCommitCannotStrandJob`](../taskflow-coordinator/src/test/java/server/JobFinalizationCrashTest.java); [`RabbitMqBrokerRecoveryIntegrationTest#unavailableStartupAndActiveBrokerRestartRecoverOutboxConsumersAndFencing`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqBrokerRecoveryIntegrationTest.java) | `CorrectnessChaosExperiment#allJobsTerminateAfterFailuresStop` (TF-0706) | Generalize eventual terminality beyond the implemented fairness, finalization, and single-broker restart scenarios to the Phase 7 coordinator/executor/redelivery/delay chaos matrix. |

Fencing observability evidence is explicit as well:
[`SchedulerMetricsTest#exposesExactFencingMetricNamesAndTypedCounters`](../taskflow-core/src/test/java/server/scheduler/SchedulerMetricsTest.java)
fixes the four counter names, while
[`AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit`](../taskflow-coordinator/src/test/java/server/rabbitmq/AssignmentFencingIntegrationTest.java)
and
[`TaskSchedulerPersistenceTest#duplicateResultIsAcknowledgedWithoutRequeueOrSecondSuccess`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java)
prove the complete trace tuple and distinct stale-versus-duplicate events.
[`StaleResultTraceDemoTest#printsAndAssertsLeaseExpiryStaleFenceAndCurrentCommitTrace`](../taskflow-coordinator/src/test/java/server/demo/StaleResultTraceDemoTest.java)
and its [one-command wrapper](STALE_RESULT_DEMO.md) make the same-worker
lease-expiry fence reproducible as a fixed seven-step operator trace.

Detailed current implementation behavior remains documented in
[Current execution behavior](EXECUTION_GUARANTEES.md). That document is
subordinate to this contract and must not promote a partial/planned evidence row
to a current guarantee.
