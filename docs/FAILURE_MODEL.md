# Failure Model

This document defines TaskFlow's required recovery behavior for the
single-coordinator scope in Phases 0–8. It is a failure contract, not proof that
every row is implemented today. The authoritative invariants and evidence
status remain in [Guarantees and non-goals](GUARANTEES.md). The required-test
catalog below assigns every planned test to a concrete fixing-queue task no
later than Phase 7.

The matrix uses these evidence labels:

- **Existing** — the linked automated test runs on the current baseline and
  directly exercises the stated behavior.
- **Partial** — linked current evidence covers only part of the window; the
  linked stable planned test must close the named gap.
- **Planned** — the behavior belongs to a linked task in Phase 7 or earlier and
  is not a current runtime guarantee.

“Durable state” includes committed SQLite rows, RabbitMQ-held messages and
delivery metadata, and MinIO/S3 objects after Phase 5. Process memory is never
treated as durable. “Duplicate allowed?” distinguishes permitted at-least-once
delivery or execution from authoritative state: duplicate delivery and
execution may be allowed, but a second authoritative task result is never
allowed. Recovery progress is subject to I10's assumptions, including restored
SQLite and RabbitMQ availability, a compatible executor participant, and
terminating or timed-out plugin execution.

## Failure-window matrix

| Failure window | Durable state before failure | Expected recovery | Duplicate allowed? | Required test |
|---|---|---|---|---|
| 1. Job persisted; submitter does not observe acceptance. | The SQLite job, task, requester-ownership, and canonical request-hash rows have committed. The submitter has no durable coordinator acknowledgement. | The requester may resubmit the same requester identity, `jobId`, and canonical request. SQLite uniqueness plus the persisted request hash must return or replay the existing status/result without creating tasks. A hash mismatch is an idempotency-key conflict. Request replay state is the persisted job and request hash. TaskFlow performs no hidden automatic submission loop; each explicit requester call ends with a response or transport failure, and no further replay occurs after a permanent conflict. | Submission delivery: **yes**. A second job or task set: **no**. | **Planned:** [`DuplicateSubmissionIntegrationTest#lostAcceptanceResponseReplaysAcceptedJob`](FAILURE_MODEL.md#tf-0207--formalize-duplicate-job-submission-semantics) and [`CrashWindowMatrixTest#acceptedJobSurvivesLostAcceptanceResponse`](FAILURE_MODEL.md#tf-0705--automate-the-crash-window-matrix). |
| 2. Job and tasks persisted; coordinator crashes before dispatch. | The SQLite job and all task rows have committed; tasks are `PENDING`. No assignment generation or assignment outbox row exists. | Startup recovery reconstructs the job from SQLite, indexes its pending tasks, and later creates an assignment only through the atomic assignment transaction. No client resubmission is required. Recovery scanning is bounded by the recovery batch policy and ends when all resumable rows are reconstructed or the coordinator records a storage failure. | Later task execution remains at least once: **yes**. Duplicate durable job/task creation: **no**. | **Partial:** [`CoordinatorStartupRecoveryTest#resumesRunningJobsWithPersistedPendingPayloads`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java). **Planned:** [`PersistenceContractTest#acceptedJobSurvivesCoordinatorRestart`](FAILURE_MODEL.md#tf-0701--add-reusable-persistence-contract-tests). |
| 3. Assignment and outbox row committed; publish not attempted. | In one SQLite transaction the task is `ASSIGNED`, the current attempt number, assignment ID, worker ID, lease, and attempt-audit row are stored, and the exact serialized `TASK_ASSIGN` outbox row is `PENDING`. | Startup or periodic outbox replay publishes that stored message without generating a new assignment identity. Replay state and attempt metadata remain in SQLite; each cycle loads at most the configured outbox batch. Publication of this row stops after a confirmed, routable publish marks it sent, or pauses when the coordinator shuts down. Transient broker outage never deletes the committed intent. | Assignment publication or execution: **yes** if broker acceptance was uncertain. A second assignment generation from publication retry: **no**. A second authoritative result: **no**. | **Partial:** [`DatabaseManagerTest#taskAssignmentOutboxCommitsWithAssignedTask`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java) and [`RabbitMqOutboxReplayerTest#replayMarksPublishedRowsSent`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqOutboxReplayerTest.java). **Planned:** [`CrashWindowMatrixTest#assignmentCommitBeforePublishReplaysExactIdentity`](FAILURE_MODEL.md#tf-0705--automate-the-crash-window-matrix). |
| 4. Assignment published; outbox row not marked published. | RabbitMQ has accepted the assignment, while SQLite still contains the same `PENDING` outbox row and current assignment identity. | The coordinator must treat publish outcome as uncertain and replay the exact stored envelope. The executor deduplicates by assignment ID when possible; SQLite fences every result by current task state, attempt number, assignment ID, and worker ID. The SQLite outbox schedule/batch owns replay and stops for that row only after sent marking succeeds; coordinator shutdown merely pauses it. | Assignment delivery and possibly execution: **yes**. A new assignment generation or second authoritative result: **no**. | **Partial:** [`RabbitMqCoordinatorLiveIntegrationTest#replayedTaskAssignmentDoesNotCreateDuplicateAcceptedResults`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java). **Planned:** [`CrashWindowMatrixTest#publishedAssignmentRemainsReplayableUntilMarked`](FAILURE_MODEL.md#tf-0705--automate-the-crash-window-matrix). |
| 5. Worker receives duplicate assignment delivery. | SQLite retains one current assignment. RabbitMQ may hold more than one delivery of its envelope. The executor's assignment-ID cache is bounded and in memory, so it is an optimization rather than authority. | While the assignment is running, the executor acknowledges the duplicate without starting another invocation. If a cached result exists, it republishes the identical result. After cache eviction or executor restart, re-execution is permitted; coordinator fencing still limits authoritative commitment to one result. Cache entries stop participating after configurable TTL/size eviction. | Delivery: **yes**. Execution after cache loss: **yes**. Concurrent execution while cached as running: **no**. Second authoritative result: **no**. | **Planned:** [`WorkerAssignmentDeduplicationIntegrationTest#duplicateRunningAssignmentExecutesOnce`](FAILURE_MODEL.md#tf-0107--define-worker-side-duplicate-assignment-behavior) and [`WorkerAssignmentDeduplicationIntegrationTest#duplicateCompletedAssignmentRepublishesSameResult`](FAILURE_MODEL.md#tf-0107--define-worker-side-duplicate-assignment-behavior). |
| 6. Worker finishes; task-result publish fails. | SQLite still shows the task's current `ASSIGNED` identity and lease. RabbitMQ retains the original unacknowledged assignment delivery because the executor defers its acknowledgement until `TASK_RESULT` publish confirmation; the computed result may exist only in the bounded executor cache or staged attempt object. | The executor negatively acknowledges/requeues the original assignment after the failed publish. RabbitMQ redelivers it; a live cache republishes the same result, while cache loss permits re-execution. Broker delivery state owns redelivery, and task retry/lease state is persisted by the coordinator. Processing ends on confirmed result publication and assignment acknowledgement, terminal task retry exhaustion, permanent invalid-message disposition, or executor shutdown. | Assignment delivery and execution: **yes**. Result publication: **yes**. Authoritative result commitment: **no more than once**. | **Partial:** [`RabbitMqCoordinatorConnectionTest#taskResultPublishFailureRequeuesAssignment`](../taskflow-gui/src/test/java/gui/RabbitMqCoordinatorConnectionTest.java). **Planned:** [`BrokerAcknowledgementIntegrationTest#resultPublishFailureRequeuesOriginalAssignment`](FAILURE_MODEL.md#tf-0305--prove-consumer-acknowledgement-semantics). |
| 7. Task result published; broker acknowledgement is lost. | RabbitMQ may already contain the `TASK_RESULT`; the executor did not observe its publish confirmation and may still have the original assignment unacknowledged. SQLite may still be `ASSIGNED` or may already contain the one committed result, depending on delivery timing. | The executor treats the publish outcome as unknown and may republish or re-execute through assignment redelivery. RabbitMQ may redeliver the result if the coordinator's consumer acknowledgement was also interrupted. The SQLite conditional result transition commits a matching assignment once; later matching results are duplicates and obsolete identities are stale. Broker redelivery stops after acknowledgement; task retries stop at the configured task retry limit. | Result delivery, result publication, and execution: **yes**. A second authoritative commit: **no**. | **Planned:** [`CrashWindowMatrixTest#lostResultPublishConfirmCannotDoubleCommit`](FAILURE_MODEL.md#tf-0705--automate-the-crash-window-matrix) and [`PersistenceContractTest#matchingAssignmentCommitsExactlyOnce`](FAILURE_MODEL.md#tf-0701--add-reusable-persistence-contract-tests). |
| 8. Coordinator receives obsolete result after reassignment to a different worker. | SQLite stores the newer current assignment generation and worker B; the delivered result names the older generation and worker A. | The database conditional commit updates zero rows and classifies the delivery as `STALE_ASSIGNMENT`. The coordinator acknowledges it and does not alter result payload, lease, job completion, capacity, or success metrics. No retry is scheduled for the stale result. | Obsolete execution/result delivery: **yes**. Authoritative commit or current-state mutation: **no**. | **Partial:** [`TaskSchedulerFailureTest#expiredLeaseReassignsTaskAndRejectsLateResultFromOldPeer`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java). **Planned:** [`ResultCommitServiceTest#differentWorkerObsoleteResultIsStale`](FAILURE_MODEL.md#tf-0105--add-database-enforced-conditional-result-commitment). |
| 9. Coordinator receives obsolete result after reassignment to the same worker. | SQLite stores attempt N+1 and assignment ID Y for worker A; the delivered result carries attempt N and assignment ID X for the same worker A. | The database predicate rejects N/X as `STALE_ASSIGNMENT`, and the coordinator acknowledges it without changing authoritative task, job, lease, capacity, or metrics state. Worker identity alone is never sufficient. No retry is scheduled for the stale result. | Obsolete execution/result delivery: **yes**. Authoritative commit or current-state mutation: **no**. | **Planned — known current ABA gap:** [`AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit`](FAILURE_MODEL.md#tf-0106--implement-same-worker-aba-protection-end-to-end). |
| 10. Task completion committed; coordinator crashes before job completion evaluation. | The task result is durable. The job may still be nonterminal and no final-result outbox row exists at this injected boundary. In the target transaction design, a last-task commit is coupled to terminal job state plus final-result intent, or to a durable replayable `FINALIZING` intent when aggregation cannot run in the transaction. | Recovery derives completion only from committed task rows or resumes the durable finalization intent. If all tasks are terminal, it atomically commits terminal job state and one logical final-result outbox record. It never reopens a terminal task or trusts a pre-crash in-memory aggregate. Finalization replay stops once the terminal state and corresponding outbox intent commit. | Completion evaluation/finalization invocation: **yes**. Task result commit, terminal transition, and logical final-result intent: **no more than once**. | **Partial:** [`TaskSchedulerPersistenceTest#restoredCompletedJobPublishesFinalResultAndPersistsCompletion`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java). **Planned:** [`JobFinalizationCrashTest#lastTaskCommitCannotStrandJob`](FAILURE_MODEL.md#tf-0206--make-job-completion-and-final-result-publication-transactional). |
| 11. Terminal job and final-result outbox row committed; publish not attempted. | One SQLite transaction contains terminal job status, final aggregate payload or deterministic inputs, and a `PENDING` exact `JOB_RESULT` outbox envelope. | Startup or periodic outbox replay publishes the stored final result. SQLite owns replay state, failed-attempt metadata, and bounded batch selection; retry for this row ends after confirmed, routable publication is marked sent, or pauses on coordinator shutdown. | Final-result delivery: **yes** if publication outcome later becomes uncertain. A second terminal transition or logical final result: **no**. | **Partial:** [`DatabaseManagerTest#completedJobOutboxCommitsTerminalStateAndResultMessage`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java) and [`RabbitMqCoordinatorLiveIntegrationTest#replaysSeededPendingOutboxRowsThroughLiveBroker`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java). **Planned:** [`CrashWindowMatrixTest#terminalResultCommitBeforePublishReplays`](FAILURE_MODEL.md#tf-0705--automate-the-crash-window-matrix). |
| 12. Final result published; outbox row not marked published. | RabbitMQ has accepted `JOB_RESULT`, the job is terminal in SQLite, and the exact final-result outbox row remains `PENDING`. | The coordinator replays the same stored envelope and eventually marks it sent after a confirmed, routable publish. The requester must tolerate duplicate final-result delivery by job identity. SQLite's terminal state is monotonic and aggregation is not recomputed from mutable memory. Outbox replay for the row stops after sent marking succeeds or pauses at shutdown. | Final-result delivery: **yes**. A second job terminal transition or different authoritative final payload: **no**. | **Partial:** [`RabbitMqCoordinatorLiveIntegrationTest#replaysSeededPendingOutboxRowsThroughLiveBroker`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java). **Planned:** [`CrashWindowMatrixTest#publishedFinalResultRemainsReplayableUntilMarked`](FAILURE_MODEL.md#tf-0705--automate-the-crash-window-matrix). |
| 13. Worker uploads staged output; crashes before result publication. | The object store contains immutable output under the attempt-specific job/task/attempt/assignment key. SQLite still has no authoritative output reference for that object. | Lease expiry or worker-loss handling closes/releases the attempt and may assign a newer generation. The orphan collector later selects only inactive, non-authoritative staged outputs older than the safety window, deletes them idempotently in configured-size batches, and records deletion failure for a later bounded-rate batch. Collection for an object stops when deletion succeeds or it becomes authoritative; outage retry pauses while the object store is unavailable. | Staged objects from multiple attempts and re-execution: **yes**. More than one authoritative output reference: **no**. | **Planned:** [`OrphanOutputGcTest#uploadedOutputWithoutResultBecomesCollectable`](FAILURE_MODEL.md#tf-0506--add-orphan-output-garbage-collection) and [`CrashWindowMatrixTest#uploadedOutputBeforeResultIsEventuallyCollected`](FAILURE_MODEL.md#tf-0705--automate-the-crash-window-matrix). |
| 14. Coordinator restarts with unexpired leases. | SQLite stores `ASSIGNED` tasks with current assignment identity, owner, and lease deadline later than the injected clock; an assignment outbox row may be sent or pending. | Recovery preserves the assignment and schedules its deadline without creating a generation. Any pending outbox row replays the exact identity. A result may still commit while the lease is current; otherwise the lease service handles it when the injected clock reaches expiry. Deadline processing is batch-bounded and the entry stops being active after result commitment, release, or expiry. | Exact assignment redelivery and execution: **yes**. A replacement generation before expiry: **no**. A second authoritative result: **no**. | **Existing baseline behavior:** [`CoordinatorStartupRecoveryTest#preservesAssignedTasksWithUnexpiredLeasesOnResume`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java). **Planned contract proof:** [`PersistenceContractTest#restartPreservesUnexpiredLease`](FAILURE_MODEL.md#tf-0701--add-reusable-persistence-contract-tests). |
| 15. Coordinator restarts with expired leases. | SQLite stores an `ASSIGNED` task whose persisted lease deadline is at or before the injected recovery time, plus its attempt audit and assignment identity. | Startup reconciliation conditionally closes the expired attempt, releases the task to `PENDING` without treating restart recovery as a processor retry, and later creates generation N+1 in the atomic assignment transaction. Any N result is stale, including reassignment to the same worker. Reconciliation is batch-bounded and each row stops qualifying after its conditional release or a concurrent terminal transition. | Old and new execution may overlap: **yes**. Old-result commitment or two authoritative results: **no**. | **Partial:** [`CoordinatorStartupRecoveryTest#releasesExpiredAssignedLeasesOnResume`](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java). **Planned:** [`PersistenceContractTest#restartReleasesExpiredLeaseAndFencesOldResult`](FAILURE_MODEL.md#tf-0701--add-reusable-persistence-contract-tests). |
| 16. RabbitMQ is unavailable during coordinator startup. | SQLite remains authoritative for accepted jobs, assignments, leases, terminal jobs, and pending outbox rows. RabbitMQ has no usable connection; no process-local publisher/consumer state is durable. | The process stays live but is not ready for new jobs. Connection recovery permits only bounded concurrent reconnect work and uses a configured delay/backoff; it stops on successful connection or coordinator shutdown. Once RabbitMQ returns, topology/consumers are restored and the SQLite outbox replayer publishes pending rows in bounded batches. Accepted state is not discarded because the broker is down. | Publication/delivery after recovery: **yes**. Duplicate authoritative transitions: **no**. | **Planned:** [`BrokerRecoveryIntegrationTest#coordinatorStartsBeforeBrokerAndReplaysOutboxAfterRecovery`](FAILURE_MODEL.md#tf-0306--add-broker-outage-and-restart-recovery). |
| 17. RabbitMQ disconnects while work is active. | SQLite contains authoritative accepted work, current assignments/leases, and coordinator outbox rows. RabbitMQ retains durable queued messages and requeues unacknowledged deliveries after connection loss; an in-flight publish may have an unknown outcome. | Consumers and publishers reconnect with bounded concurrent recovery and configured delay/backoff. Broker-held deliveries are redelivered, and pending SQLite outbox rows replay in bounded batches. Lease expiry may create a newer assignment, so old results are fenced. Recovery attempts stop on reconnection or shutdown; message redelivery stops on acknowledgement, permanent rejection, or bounded poison quarantine. | Assignment, result, and final-result delivery and task execution: **yes**. Duplicate authoritative state: **no**. | **Partial:** [`RabbitMqTransportLiveTest#recoversConsumerAndPublisherAfterBrokerSideConnectionDropAgainstLiveBroker`](../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqTransportLiveTest.java). **Planned:** [`BrokerRecoveryIntegrationTest#activeWorkCompletesAfterBrokerRestart`](FAILURE_MODEL.md#tf-0306--add-broker-outage-and-restart-recovery). |
| 18. Scheduler mailbox remains full. | Accepted jobs/tasks stay in SQLite. The mailbox is at its configured bound; a broker delivery remains broker-held and unacknowledged, and a not-yet-accepted submission has no accepted SQLite state. | Consumer prefetch/intake is paused or reduced instead of growing memory. Results and expirations for accepted work receive bounded fair processing, while new submissions are rejected before acceptance when admission limits are reached. Capacity waiting is owned by broker backpressure, not an in-memory side queue and not the poison counter; it ends when mailbox capacity returns or the coordinator shuts down, at which point RabbitMQ retains/redelivers the unsettled delivery. | Broker redelivery: **yes**. Silent loss of accepted work or replacement of an accepted mailbox item: **no**. | **Partial:** [`SchedulerMailboxTest#repeatedBrokerOverflowRequeuesDeliveriesWithoutReplacingAcceptedWork`](../taskflow-core/src/test/java/server/scheduler/SchedulerMailboxTest.java). **Planned:** [`SchedulerOverloadTest#persistentMailboxSaturationPreservesAcceptedWorkAndProgress`](FAILURE_MODEL.md#tf-0406--define-persistent-overload-behavior). |
| 19. A deterministic poison message repeatedly reaches the coordinator. | Authoritative SQLite domain state is unchanged unless valid work committed before the failure. RabbitMQ stores the delivery and retry/dead-letter metadata, including the delivery-attempt count and original route. | Validation failures are rejected without immediate requeue. A deterministic processing failure moves through explicit delayed retry queues; the broker header/death metadata owns the count. After the configured maximum attempts it is published to quarantine with its reason and original route, then the source delivery is acknowledged. Retry ends at successful handling, permanent rejection, or quarantine—never an immediate infinite requeue loop. | Delivery before the bound: **yes**. Domain transition from an invalid message: **no**. Delivery after quarantine without explicit operator redrive: **no**. | **Partial:** [`TaskSchedulerFailureTest#invalidBrokerTaskResultIsRejectedWithoutRequeue`](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerFailureTest.java). **Planned:** [`BrokerRetryContractTest#poisonMessageQuarantinesAfterBoundedAttempts`](FAILURE_MODEL.md#tf-0303--implement-bounded-delayed-broker-retry). |

## Required test catalog

The linked IDs below are stable acceptance-test contracts. They name the
mechanism-owning task and the minimum deterministic assertion; they do not
claim that a planned class already exists.

<a id="tf-0105--add-database-enforced-conditional-result-commitment"></a>
### TF-0105 — Database-enforced result commitment

- `ResultCommitServiceTest#differentWorkerObsoleteResultIsStale` must make the
  SQLite conditional update return the typed stale outcome and prove that no
  authoritative fields change.

<a id="tf-0106--implement-same-worker-aba-protection-end-to-end"></a>
### TF-0106 — Same-worker ABA protection

- `AssignmentFencingIntegrationTest#sameWorkerAbaResultCannotCommit` must
  reassign to the same worker under a new attempt/assignment ID, reject the old
  result, and commit only the new result.

<a id="tf-0107--define-worker-side-duplicate-assignment-behavior"></a>
### TF-0107 — Executor assignment deduplication

- `WorkerAssignmentDeduplicationIntegrationTest#duplicateRunningAssignmentExecutesOnce`
  must hold the first execution under controlled synchronization and observe
  one processor invocation after duplicate delivery.
- `WorkerAssignmentDeduplicationIntegrationTest#duplicateCompletedAssignmentRepublishesSameResult`
  must replay the byte-equivalent cached result with the same assignment
  identity and prove the cache bound.

<a id="tf-0206--make-job-completion-and-final-result-publication-transactional"></a>
### TF-0206 — Transactional job finalization

- `JobFinalizationCrashTest#lastTaskCommitCannotStrandJob` must fail at the
  last-task/finalization boundary, restart, and observe one terminal job plus
  one logical final-result outbox intent.

<a id="tf-0207--formalize-duplicate-job-submission-semantics"></a>
### TF-0207 — Duplicate submission semantics

- `DuplicateSubmissionIntegrationTest#lostAcceptanceResponseReplaysAcceptedJob`
  must repeat the same owner/job/request hash after a lost response and observe
  the original job and task identities without new rows.

<a id="tf-0303--implement-bounded-delayed-broker-retry"></a>
### TF-0303 — Bounded delayed broker retry

- `BrokerRetryContractTest#poisonMessageQuarantinesAfterBoundedAttempts` must
  assert the configured delays/count, preserved reason/original route, and one
  terminal quarantine delivery.

<a id="tf-0305--prove-consumer-acknowledgement-semantics"></a>
### TF-0305 — Consumer acknowledgement semantics

- `BrokerAcknowledgementIntegrationTest#resultPublishFailureRequeuesOriginalAssignment`
  must fail result publication before assignment acknowledgement and observe
  broker redelivery with no silently lost task.

<a id="tf-0306--add-broker-outage-and-restart-recovery"></a>
### TF-0306 — Broker outage and restart recovery

- `BrokerRecoveryIntegrationTest#coordinatorStartsBeforeBrokerAndReplaysOutboxAfterRecovery`
  must start with RabbitMQ unavailable, restore it, and drain committed pending
  outbox intent without losing accepted work.
- `BrokerRecoveryIntegrationTest#activeWorkCompletesAfterBrokerRestart` must
  disconnect/restart RabbitMQ during active work, reconnect, tolerate
  redelivery, and make every valid job terminal under I10's assumptions.

<a id="tf-0406--define-persistent-overload-behavior"></a>
### TF-0406 — Persistent overload behavior

- `SchedulerOverloadTest#persistentMailboxSaturationPreservesAcceptedWorkAndProgress`
  must hold the mailbox at its bound, prove result/expiry progress and stable
  memory, then prove automatic admission recovery after pressure falls.

<a id="tf-0506--add-orphan-output-garbage-collection"></a>
### TF-0506 — Orphan-output collection

- `OrphanOutputGcTest#uploadedOutputWithoutResultBecomesCollectable` must
  upload under an attempt key, omit result publication, advance an injected
  clock beyond the safety window, and delete only the non-authoritative object.

<a id="tf-0701--add-reusable-persistence-contract-tests"></a>
### TF-0701 — Persistence contract proof

- `PersistenceContractTest#acceptedJobSurvivesCoordinatorRestart` must reopen
  the store after committed acceptance and reconstruct the same job/tasks.
- `PersistenceContractTest#matchingAssignmentCommitsExactlyOnce` must submit
  a matching result twice and observe one commit plus one typed duplicate.
- `PersistenceContractTest#restartPreservesUnexpiredLease` must recover with
  an injected time before expiry and retain the exact assignment identity.
- `PersistenceContractTest#restartReleasesExpiredLeaseAndFencesOldResult` must
  recover after expiry, create the next generation, and reject the old result.

<a id="tf-0705--automate-the-crash-window-matrix"></a>
### TF-0705 — Process/failpoint crash-window proof

- `CrashWindowMatrixTest#acceptedJobSurvivesLostAcceptanceResponse` must kill
  after the job transaction but before the response and then replay the same
  logical submission.
- `CrashWindowMatrixTest#assignmentCommitBeforePublishReplaysExactIdentity`
  must kill after assignment/outbox commit and observe the exact stored
  assignment after restart.
- `CrashWindowMatrixTest#publishedAssignmentRemainsReplayableUntilMarked` must
  kill after broker confirm but before sent marking and observe harmless
  duplicate delivery of the same assignment identity.
- `CrashWindowMatrixTest#lostResultPublishConfirmCannotDoubleCommit` must make
  result publication outcome uncertain and observe one authoritative commit.
- `CrashWindowMatrixTest#terminalResultCommitBeforePublishReplays` must kill
  after terminal job/final-outbox commit and recover the pending final result.
- `CrashWindowMatrixTest#publishedFinalResultRemainsReplayableUntilMarked`
  must kill after final-result confirm but before sent marking and observe the
  same terminal payload on replay.
- `CrashWindowMatrixTest#uploadedOutputBeforeResultIsEventuallyCollected`
  must kill after object upload but before result publication and prove safe
  bounded orphan collection.

## Retry ownership and termination rules

The matrix deliberately names the component that owns every retry. The compact
rules below apply wherever that component appears:

| Retry owner | Durable retry state | Bound and stop condition |
|---|---|---|
| Requester submission replay | SQLite `jobId`, requester identity, and canonical request hash | TaskFlow performs no hidden automatic submission loop. Each explicit call ends with a response or transport failure; a permanent idempotency conflict forbids replay as the same request. |
| Coordinator outbox replayer | SQLite outbox row, exact envelope, publication-attempt metadata, and next eligible time | Each pass and retry rate are bounded. A transient-outage row is retained rather than discarded; work for that row stops after confirmed/routable publication is marked sent and pauses while the coordinator is stopped. |
| RabbitMQ consumer delivery | Broker queue/unacknowledged delivery plus explicit retry/death metadata | Prefetch bounds concurrent deliveries. Retry-classified handling stops on acknowledgement, permanent rejection, or quarantine after the configured attempt maximum. Mailbox pressure does not consume poison attempts; it keeps the delivery broker-held until capacity returns or shutdown causes redelivery. |
| Task lease/retry service | SQLite task assignment, lease deadline, attempt audit, and distinct task `retry_count` | Deadline/dispatch work is batch-bounded. A task stops retrying on success, permanent failure, or configured task retry exhaustion. Assignment generation never substitutes for retry count. |
| Executor duplicate/result handling | RabbitMQ assignment delivery plus bounded assignment-ID cache; staged output when applicable | Cache size and TTL are bounded. Broker handling stops on acknowledged confirmed result, permanent rejection, terminal retry outcome, or executor shutdown; cache loss may permit re-execution. |
| Broker connection recovery | Client recovery state plus SQLite outbox and RabbitMQ durable queues | Only bounded concurrent recovery work runs, with configured delay/backoff. It stops on reconnection or coordinator shutdown; I10 makes no progress claim while RabbitMQ remains unavailable. |
| Orphan-output collector | Attempt-specific object key plus SQLite assignment/output-reference state and recorded deletion failure | Selection is safety-window and batch bounded. Processing stops when deletion succeeds or the object is authoritative; a store outage defers the next bounded-rate batch. |

## Current proof boundary

This matrix exposes target behavior before implementation so later mechanisms
can be judged against a stable failure contract. On the current baseline:

- the same-worker ABA row now has persisted assignment generation, but result
  commitment does not yet conditionally enforce it;
- lost-response submission replay does not yet use the required canonical
  request hash;
- executor assignment deduplication is not yet the required bounded
  assignment-ID cache;
- delayed broker retry and automatic poison quarantine are incomplete;
- last-task result commitment and final-result intent are not yet one complete
  finalization contract;
- MinIO/S3 attempt output staging and orphan collection are not implemented;
- full broker stop/restart recovery is not proved; and
- event-driven fairness, complete admission control, and persistent-overload
  recovery remain planned.

Accordingly, a linked current unit or live test marked **Partial** must not be
used to claim the corresponding row is closed. Phase 7 completes the matrix
only when every row links to an automated failpoint test or reproducible
experiment and all earlier mechanism-owning tasks have passed their gates.
