# Process Crash-Window Matrix

`CrashWindowMatrixTest` is the operating-system process layer above the
deterministic store, scheduler, broker, and model tests. A parent JUnit process
owns RabbitMQ, Toxiproxy, MinIO, and post-crash verification. For each row it
starts a separate child JVM, waits for a named failpoint signal, calls
`Process.destroyForcibly()`, waits for process exit, and only then opens the
surviving SQLite database or object store.

The failpoints are test-only orchestration. They call existing production
transactions, confirmed publications, recovery, replay, and collection APIs;
they do not add a runtime fault framework or alter production state
transitions.

## Automated windows

| Stable test ID | Child failpoint | Post-kill proof |
|---|---|---|
| `acceptedJobSurvivesLostAcceptanceResponse` | `AFTER_JOB_TRANSACTION_COMMIT` | The same owner/hash submission is a typed replay and no second job/task set exists. |
| `assignmentCommitBeforePublishReplaysExactIdentity` | `AFTER_ASSIGNMENT_OUTBOX_COMMIT` | The pending row replays the exact durable attempt/assignment/lease and then marks sent. |
| `publishedAssignmentRemainsReplayableUntilMarked` | `AFTER_ASSIGNMENT_BROKER_CONFIRM_BEFORE_MARK` | The broker-confirmed assignment remains pending; restart replay duplicates only the same identity and creates no attempt. |
| `lostResultPublishConfirmCannotDoubleCommit` | `AFTER_RESULT_PUBLISH_CONFIRM` | Re-publication of the uncertain result is classified as duplicate after one authoritative commit. |
| `resultCommitBeforeFinalizationRecoversTerminalResult` | `AFTER_RESULT_COMMIT` | SQLite retains `FINALIZING`; startup hydration deterministically creates one terminal result/outbox intent. |
| `terminalResultCommitBeforePublishReplays` | `AFTER_TERMINAL_OUTBOX_COMMIT` | The terminal state and final-result envelope survive together and replay after restart. |
| `publishedFinalResultRemainsReplayableUntilMarked` | `AFTER_FINAL_RESULT_CONFIRM_BEFORE_MARK` | The same confirmed terminal payload is replayed from the still-pending row and then marked sent. |
| `partialObjectUploadCannotBecomeAuthoritative` | `DURING_OBJECT_UPLOAD` | Killing a child blocked after the first streamed chunk leaves no visible object and no task result reference. |
| `uploadedOutputBeforeResultIsEventuallyCollected` | `AFTER_OBJECT_UPLOAD_BEFORE_RESULT` | A complete attempt-keyed object without a result becomes an orphan after exact attempt release and is deleted by the bounded collector. |

The assignment and final-result confirm failpoints are deliberately before the
SQLite sent mark. This is the uncertain publication window: RabbitMQ has
accepted the persistent message while SQLite still owns a replayable
`PENDING` intent.

## Reproduce

Docker Desktop or another Testcontainers-compatible Docker engine must be
available. Run:

```powershell
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-coordinator -am "-Dtaskflow.rabbitmq.live=true" "-Dtest=CrashWindowMatrixTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

The suite uses one managed RabbitMQ container through a stable Toxiproxy
endpoint, one managed MinIO container, isolated topology names and SQLite
files, a 30-second child-failpoint bound, 15-second delivery/collector bounds,
and no wall-clock sleeps. Child output is retained under the JUnit temporary
directory and included when a child exits before its failpoint.

## Limits

This proves abrupt single-child JVM death at the named single-coordinator
windows. It does not prove power-loss disk semantics beyond SQLite/MinIO/
RabbitMQ durability contracts, multi-coordinator fencing, clustered broker or
object-store failover, arbitrary external plugin side effects, or large
randomized workloads. The separate
[100,000-task correctness-chaos experiment](reports/correctness-chaos.md)
adds seeded mixed-failure coverage, but its coordinator and executor failures
are component restarts inside one test JVM rather than additional process-kill
windows.
