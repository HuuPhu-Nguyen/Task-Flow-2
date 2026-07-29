# Architecture Diagrams Tied to Invariants

These diagrams describe the implemented single-coordinator TaskFlow runtime on
`main`. They are views of the same current system, not alternative or planned
architectures. The invariant definitions are normative in
[Guarantees and non-goals](GUARANTEES.md), and the durable transition IDs are
defined in the [task and job state machine](STATE_MACHINE.md).

## 1. Component and context

**Protected invariants:** I1 durable acceptance; I2 single authoritative
result; I3 assignment fencing; I4 monotonic terminal state; I5 transactional
outbound intent; I6 duplicate tolerance; I7 bounded coordinator memory; I8
poison-message termination; I9 payload integrity; and I10 eventual terminality
under stated assumptions.

```mermaid
flowchart LR
    subgraph Participants["Participant processes"]
        Requester["Requester role"]
        Executor["Executor role"]
        Dual["Requester + executor roles"]
        Plugins["Role-split workload plugins"]
        Requester --- Plugins
        Executor --- Plugins
        Dual --- Plugins
    end

    RabbitMQ[("RabbitMQ<br/>control-message transport")]

    subgraph Coordinator["Single authoritative coordinator process"]
        Ingress["Typed ingress<br/>settlement and retry classification"]
        Mailbox["Bounded scheduler mailbox<br/>ordinary lane + result reserve"]
        Scheduler["Bounded scheduler loop<br/>assignment / result / lease / completion"]
        Registry["Participant registry<br/>capacity snapshots and reservations"]
        Publisher["Outbox publisher<br/>and bounded durable replayer"]
        Collector["Bounded orphan-output collector"]
        Operations["Health / metrics / status"]
        Ingress --> Mailbox --> Scheduler
        Registry <--> Scheduler
        Scheduler --> Operations
    end

    SQLite[("SQLite<br/>authoritative state + outbox")]
    MinIO[("MinIO / S3-compatible storage<br/>optional large-payload tier")]

    Requester -->|"JOB_SUBMIT"| RabbitMQ
    Executor -->|"PONG / TASK_RESULT"| RabbitMQ
    Dual -->|"both role routes"| RabbitMQ
    RabbitMQ -->|"JOB_SUBMIT / TASK_RESULT"| Ingress
    RabbitMQ -->|"HEARTBEAT"| Registry
    Publisher -->|"confirmed TASK_ASSIGN / JOB_RESULT"| RabbitMQ
    RabbitMQ -->|"JOB_RESULT"| Requester
    RabbitMQ -->|"JOB_RESULT"| Dual
    RabbitMQ -->|"TASK_ASSIGN"| Executor
    RabbitMQ -->|"TASK_ASSIGN"| Dual

    Scheduler -->|"conditional transactions"| SQLite
    Scheduler -->|"immediate committed-record publish"| Publisher
    Publisher <-->|"bounded pending rows / sent marks"| SQLite
    Collector -->|"exact active / authoritative classification"| SQLite
    Collector -->|"bounded idempotent deletion"| MinIO
    Plugins <-->|"streamed, verified object references"| MinIO
```

The coordinator is the only scheduling and result-commit authority.
Participant symmetry does not create shared authority; SQLite is the durable
source, RabbitMQ is the sole supported transport, and MinIO is a payload tier
rather than a state authority. Current boundary tests include
[RabbitMqOnlyRuntimeArchitectureTest](../taskflow-coordinator/src/test/java/server/RabbitMqOnlyRuntimeArchitectureTest.java),
[SchedulerArchitectureTest](../taskflow-core/src/test/java/server/scheduler/SchedulerArchitectureTest.java),
and
[ObjectStoreArchitectureTest](../taskflow-spi/src/test/java/objectstore/ObjectStoreArchitectureTest.java).

## 2. Assignment transaction and outbox sequence

**Protected invariants:** I3 assignment fencing, I5 transactional outbound
intent, and I6 duplicate tolerance.

```mermaid
sequenceDiagram
    participant Sched as Scheduler loop
    participant Assign as AssignmentService
    participant DB as SQLite JobStateStore
    participant Memory as Scheduler projection
    participant Registry as Participant registry
    participant Outbox as Outbox publisher / replayer
    participant Broker as RabbitMQ
    participant Executor as Executor participant

    Sched->>Assign: Pending task + compatible executor candidate
    Assign->>DB: Propose assignment UUID for next generation
    Note over DB: SQLite transaction requires task state = PENDING<br/>Persist ASSIGNED + attempt + lease + exact TASK_ASSIGN outbox row
    DB-->>Assign: Projection-allowed committed identity or typed non-commit

    alt Durable outcome allows projection
        Assign->>Memory: Install returned identity and deadline indexes
        Assign->>Registry: Reserve exact assignment capacity
        Assign-->>Sched: Assignment created
        Assign->>Outbox: Publish exact committed outbox record
        Outbox->>Broker: Persistent mandatory publish, await confirm
        alt Confirmed and routable
            Outbox->>DB: Mark exact outbox row SENT
        else Publish outcome or sent mark is uncertain
            Note over Outbox,DB: Keep the same row PENDING
        end
        opt A PENDING row remains for periodic or startup replay
            Outbox->>DB: Load a bounded batch of PENDING rows
            DB-->>Outbox: Stored versioned TASK_ASSIGN envelope
            Outbox->>Broker: Replay the identical attempt and UUID
        end
        Broker-->>Executor: Deliver TASK_ASSIGN (redelivery is allowed)
    else Durable outcome does not allow projection
        Assign-->>Sched: Do not project, reserve, or publish a generation
    end
```

The commit point is the SQLite transaction, not the scheduler proposal or
broker publish. Replay reuses the serialized identity returned by the
transaction and never advances the generation. See the T1 transition in the
[state machine](STATE_MACHINE.md),
[DatabaseManagerTest](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java),
[RabbitMqOutboxReplayerTest](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqOutboxReplayerTest.java),
and the assignment failpoints in
[CrashWindowMatrixTest](../taskflow-coordinator/src/test/java/server/CrashWindowMatrixTest.java).

## 3. Result fencing and conditional commit sequence

**Protected invariants:** I2 single authoritative result, I3 assignment
fencing, I4 monotonic terminal state, I5 transactional outbound intent, and I6
duplicate tolerance.

```mermaid
sequenceDiagram
    participant Executor as Executor participant
    participant Broker as RabbitMQ
    participant Ingress as Coordinator ingress
    participant Result as ResultCommitService
    participant DB as SQLite JobStateStore
    participant Memory as Scheduler projection
    participant Finish as JobCompletionService
    participant Outbox as Outbox publisher / replayer

    Executor->>Broker: TASK_RESULT(task, attempt, assignment UUID, executor)
    Broker->>Ingress: Deliver with manual settlement
    Ingress->>Result: Validated result envelope
    Result->>DB: Conditional result commit
    Note over DB: Match task ID + ASSIGNED state + attempt<br/>+ assignment UUID + executor identity

    alt Current tuple commits
        Note over DB: Atomically set task COMPLETED,<br/>close attempt SUCCEEDED,<br/>and set job FINALIZING when last result commits
        DB-->>Result: COMMITTED(finalizing flag)
        Result->>Memory: Project completion and release exact capacity
        Result-->>Ingress: ACK_SUCCESS
        opt Parent is durably FINALIZING
            Result->>Finish: Aggregate ordered committed task snapshots
            Finish->>DB: Commit terminal job + semantic payload + JOB_RESULT outbox
            DB-->>Finish: One terminal state and one logical outbox record
            Finish->>Outbox: Publish committed final-result record
            Outbox->>DB: Replay stored envelope while PENDING
        end
    else Exact result already completed
        DB-->>Result: DUPLICATE_ALREADY_COMPLETED
        Result-->>Ingress: ACK_DUPLICATE_OR_STALE, no mutation
    else Attempt, UUID, executor, or state is obsolete
        DB-->>Result: STALE_ASSIGNMENT
        Result-->>Ingress: ACK_DUPLICATE_OR_STALE, no mutation
    else Storage operation fails
        DB-->>Result: STORAGE_FAILURE with rollback
        Result-->>Ingress: RETRY_TRANSIENT, projection remains assigned
    end
```

Task commitment and terminal aggregation are deliberately separate durable
transactions. `FINALIZING` bridges their crash window, while the full
assignment tuple fences same-executor ABA results. Evidence is in
[AssignmentFencingIntegrationTest](../taskflow-coordinator/src/test/java/server/rabbitmq/AssignmentFencingIntegrationTest.java),
[TaskSchedulerPersistenceTest](../taskflow-core/src/test/java/server/scheduler/TaskSchedulerPersistenceTest.java),
[JobFinalizationCrashTest](../taskflow-coordinator/src/test/java/server/JobFinalizationCrashTest.java),
and
[RabbitMqCoordinatorLiveIntegrationTest](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqCoordinatorLiveIntegrationTest.java).

## 4. Coordinator restart and recovery sequence

**Protected invariants:** I1 durable acceptance, I3 assignment fencing, I4
monotonic terminal state, I5 transactional outbound intent, and I10 eventual
terminality under stated assumptions.

```mermaid
sequenceDiagram
    participant Start as Coordinator startup
    participant DB as SQLite JobStateStore
    participant Recovery as RecoveryService
    participant Memory as Fresh scheduler projection
    participant Finish as JobCompletionService
    participant Replay as Outbox replayer
    participant Broker as RabbitMQ

    Start->>DB: Open database, apply versioned migrations
    Start->>Recovery: Restore resumable RUNNING and FINALIZING jobs
    Recovery->>DB: Load committed jobs, tasks, attempts, leases, and results

    loop Each persisted nonterminal job and task at startup
        alt PENDING task
            Recovery->>DB: Normalize leftover fields, preserve max generation
            Recovery->>Memory: Index pending task
        else ASSIGNED with complete unexpired identity
            Recovery->>Memory: Preserve assignment and exact deadlines
        else ASSIGNED with expired or incomplete identity
            Recovery->>DB: Conditionally close/release exact attempt to PENDING
            Recovery->>Memory: Index only after durable release
        else COMPLETED or FAILED task
            Recovery->>Memory: Restore terminal snapshot without reopening it
        end
    end

    opt Job is FINALIZING with complete committed results
        Recovery->>Finish: Rebuild plugin job and aggregate ordered snapshots
        Finish->>DB: Commit terminal payload + final-result outbox atomically
    end

    Start->>Replay: Start bounded pending-outbox replay
    Replay->>DB: Load stored PENDING envelopes
    Replay->>Broker: Republish exact committed envelopes
    Note over Start,Broker: Progress resumes only while the I10<br/>coordinator, SQLite, broker, executor,<br/>plugin, and policy assumptions hold
```

Recovery hydrates projections; it does not create another acceptance or reset a
generation. Unexpired assignments retain identity, expired assignments release
conditionally, terminal states stay terminal, and pending outbox rows replay
their stored envelope. See
[CoordinatorStartupRecoveryTest](../taskflow-coordinator/src/test/java/server/CoordinatorStartupRecoveryTest.java),
[PersistenceContractTest](../taskflow-core/src/test/java/server/db/PersistenceContractTest.java),
[JobFinalizationCrashTest](../taskflow-coordinator/src/test/java/server/JobFinalizationCrashTest.java),
and
[RabbitMqBrokerRecoveryIntegrationTest](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqBrokerRecoveryIntegrationTest.java).

## 5. Object-storage staged-output lifecycle

**Protected invariants:** I2 single authoritative result, I3 assignment
fencing, and I9 payload integrity.

```mermaid
flowchart TD
    Execute["Executor produces output for<br/>job / task / attempt / assignment UUID"]
    Upload["Create immutable attempt key with put-if-absent"]
    VerifyUpload{"Streamed length and<br/>SHA-256 match metadata?"}
    Staged["Staged attempt output<br/>upload alone has no authority"]
    Result["TASK_RESULT carries the exact<br/>outer assignment identity and reference"]
    Validate{"Protocol key and metadata valid?"}
    Commit{"SQLite conditional result commit<br/>matches current assignment tuple?"}
    Authoritative["Authoritative output reference<br/>stored in tasks.result_payload_json"]
    NoAuthority["No pointer commit<br/>integrity / invalid / stale / duplicate / storage outcome"]
    Download["Requester streams object"]
    VerifyDownload{"Length and SHA-256 match?"}
    Accept["Accept output file"]
    List["Collector lists old taskflow/jobs objects<br/>in bounded lexical pages"]
    Classify{"SQLite classifies exact key"}
    PreserveActive["ACTIVE<br/>preserve current generation"]
    PreserveAuthority["AUTHORITATIVE<br/>preserve committed pointer"]
    Orphan["ORPHAN_CANDIDATE<br/>closed generation + safety window"]
    Delete{"Idempotent delete succeeds<br/>or object is already absent?"}
    Deleted["DELETED"]
    Retry["Persist bounded failure metadata<br/>then reclassify in a later batch"]

    Execute --> Upload --> VerifyUpload
    VerifyUpload -->|yes| Staged
    VerifyUpload -->|no| NoAuthority
    Staged --> Result --> Validate
    Validate -->|yes| Commit
    Validate -->|no| NoAuthority
    Commit -->|COMMITTED| Authoritative
    Commit -->|stale / duplicate / failure| NoAuthority
    Authoritative --> Download --> VerifyDownload
    VerifyDownload -->|yes| Accept
    VerifyDownload -->|no| NoAuthority

    Staged -.-> List
    Authoritative -.-> List
    List --> Classify
    Classify -->|current assignment| PreserveActive
    Classify -->|committed reference| PreserveAuthority
    Classify -->|neither| Orphan --> Delete
    Delete -->|yes| Deleted
    Delete -->|no| Retry --> Classify
```

The collector automatically considers only attempt outputs under
`taskflow/jobs/`; referenced inputs under `taskflow/inputs/` are outside its
deletion policy. Object creation time supplies the safety window, and every
retry reclassifies against SQLite before deletion. Current evidence includes
[ObjectStoreIntegrityContractTest](../taskflow-spi/src/test/java/objectstore/ObjectStoreIntegrityContractTest.java),
[MinioObjectStoreContractTest](../taskflow-objectstore-minio/src/test/java/objectstore/minio/MinioObjectStoreContractTest.java),
[OrphanOutputGcTest](../taskflow-coordinator/src/test/java/server/objectstore/OrphanOutputGcTest.java),
and the staged-output failpoints in
[CrashWindowMatrixTest](../taskflow-coordinator/src/test/java/server/CrashWindowMatrixTest.java).

## 6. Scheduler queues and deadline flow

**Protected invariants:** I4 monotonic terminal state, I7 bounded coordinator
memory, and I10 eventual terminality under stated assumptions.

```mermaid
flowchart TD
    Submit["RabbitMQ JOB_SUBMIT<br/>dedicated prefetch 1"]
    Control["Admitted control envelopes"]
    Heartbeat["RabbitMQ HEARTBEAT delivery"]
    CapacitySignal["Registry capacity snapshot<br/>and scheduler wake signal"]
    Result["Validated TASK_RESULT deliveries"]
    Ordinary["Bounded ordinary FIFO lane<br/>capacity = inboundQueueCapacity"]
    Reserve["Fixed task-result reserve<br/>capacity = 1"]
    Dequeue["Result-first bounded dequeue"]

    subgraph Cycle["One SchedulerLoop cycle"]
        Messages["1. Process at most<br/>schedulerMessageBatchSize envelopes"]
        Deadlines["2. Pop at most schedulerDeadlineBatchSize<br/>combined timeout / lease entries"]
        Dispatch["3. Attempt at most<br/>schedulerDispatchBatchSize placements"]
        Terminal["4. Retry at most schedulerOutboxBatchSize<br/>due terminal delivery/persistence work"]
        Metrics["5. Refresh metrics"]
        Messages --> Deadlines --> Dispatch --> Terminal --> Metrics
    end

    Identity{"Popped deadline matches current<br/>attempt + UUID + executor?"}
    Durable["Apply conditional durable<br/>release / retry / terminal transition"]
    Stale["Count and discard stale entry"]
    Index["SchedulerWorkloadIndex<br/>pending / runnable / capacity-wait<br/>assignments / exact deadlines"]
    Capacity{"Compatible executor capacity available?"}
    Assignment["Commit assignment transaction<br/>then update indexes"]
    WaitCapacity["Move job to capacity-wait generation<br/>or exact timed recheck"]
    Wake{"Immediately runnable work remains?"}
    Await["Await mailbox, capacity signal,<br/>earliest deadline, retry, or metrics time"]
    Cleanup["Terminal/outbox cleanup releases bounds<br/>and refreshes admission"]

    Submit --> Ordinary
    Control --> Ordinary
    Heartbeat --> CapacitySignal --> Index
    CapacitySignal --> Await
    Result --> Reserve
    Ordinary --> Dequeue
    Reserve --> Dequeue
    Dequeue --> Messages

    Deadlines --> Identity
    Identity -->|no| Stale
    Identity -->|yes| Durable --> Index
    Dispatch --> Index --> Capacity
    Capacity -->|yes| Assignment --> Index
    Capacity -->|no| WaitCapacity --> Index
    Terminal --> Cleanup --> Index
    Metrics --> Wake
    Wake -->|yes| Messages
    Wake -->|no| Await --> Dequeue
```

Mailbox saturation never evicts accepted work: a result has one reserved slot,
both lanes remain bounded, every cycle reaches deadlines, and stale deadline
entries consume the same finite budget as current entries. Scheduler indexes
are post-commit projections rather than transition authority. Evidence is in
[SchedulerMailboxTest](../taskflow-core/src/test/java/server/scheduler/SchedulerMailboxTest.java),
[SchedulerLoopTest](../taskflow-core/src/test/java/server/scheduler/SchedulerLoopTest.java),
[SchedulerWorkloadIndexTest](../taskflow-core/src/test/java/server/scheduler/SchedulerWorkloadIndexTest.java),
[AssignmentServiceBatchTest](../taskflow-core/src/test/java/server/scheduler/AssignmentServiceBatchTest.java),
and
[SchedulerOverloadTest](../taskflow-core/src/test/java/server/scheduler/SchedulerOverloadTest.java).

## Scope boundary

These views intentionally stop at the current supported boundary: one
coordinator writer, one SQLite authority, RabbitMQ delivery, and optional
MinIO/S3-compatible payload storage. They do not depict active-active
coordinators, leader epochs, PostgreSQL, clustered broker failover,
Kubernetes, multi-region state, or automatic referenced-input deletion.
Those are not implemented Phase 8 architecture.
