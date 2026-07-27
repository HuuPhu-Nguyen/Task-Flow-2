package server.scheduler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import protocol.AdmissionRejection;
import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.RequesterIdentity;
import protocol.RequesterTokens;
import protocol.ProtocolVersions;
import protocol.TaskAssignMessage;
import protocol.TaskFailureClassification;
import protocol.TaskResultMessage;
import server.db.BrokerOutboxStore;
import server.db.JobStateStore;
import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.JobFactory;
import server.job.TaskUnit;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.TaskFlowClock;

import java.time.Instant;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import transport.DeliveryDisposition;
import transport.TransportAcknowledgement;
import transport.TransportRoute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskSchedulerPersistenceTest {

    @Test
    void activeJobAdmissionRejectionPerformsNoSecondDurableCommit() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "1",
                "TASKFLOW_MAX_ACTIVE_TASKS", "10"
        ));
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                config
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-active-job-admission-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-admitted", List.of("a")), "requester-1"));
            mailbox.put(new MessageEnvelope(testJob("job-rejected", List.of("b")), "requester-1"));

            assertTrue(output.awaitResult());
            assertAdmissionRejection(
                    output.result(),
                    AdmissionRejection.Limit.MAX_ACTIVE_JOBS,
                    1L,
                    2L
            );
            assertEquals(1L, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1L, scheduler.getMetricsSnapshot().activeTasks());
            assertEquals(1L, startupCommitCount(store));
        } finally {
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(2_000L);
        }
    }

    @Test
    void activeTaskAdmissionRejectionPerformsNoSecondDurableCommit() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "10",
                "TASKFLOW_MAX_ACTIVE_TASKS", "2"
        ));
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                config
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-active-task-admission-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-two-tasks", List.of("a", "b")),
                    "requester-1"
            ));
            mailbox.put(new MessageEnvelope(
                    testJob("job-task-rejected", List.of("c")),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            assertAdmissionRejection(
                    output.result(),
                    AdmissionRejection.Limit.MAX_ACTIVE_TASKS,
                    2L,
                    3L
            );
            assertEquals(1L, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(2L, scheduler.getMetricsSnapshot().activeTasks());
            assertEquals(1L, startupCommitCount(store));
        } finally {
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(2_000L);
        }
    }

    @Test
    void pendingOutboxThresholdRejectsWithoutDurableJobMutation() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        store.enqueueBrokerOutbox(testPendingOutboxMessage());
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_PENDING_OUTBOX_ROWS", "1"
        ));
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                config
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-outbox-admission-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-outbox-rejected", List.of("a")),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            assertAdmissionRejection(
                    output.result(),
                    AdmissionRejection.Limit.MAX_PENDING_OUTBOX_ROWS,
                    1L,
                    1L
            );
            assertEquals(0L, startupCommitCount(store));
            assertEquals(0L, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1, store.pendingOutboxCountReads());
        } finally {
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(2_000L);
        }
    }

    @Test
    void exactReplayBypassesPendingOutboxThreshold() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "1",
                "TASKFLOW_MAX_PENDING_OUTBOX_ROWS", "1"
        ));
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                config
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-outbox-replay-admission-test");
        schedulerThread.start();
        JobSubmitMessage submit = testJob("job-replay-outbox-full", List.of("a"));

        try {
            mailbox.put(new MessageEnvelope(submit, "requester-1"));
            assertTrue(awaitActiveJobs(scheduler, 1L));
            store.enqueueBrokerOutbox(testPendingOutboxMessage());
            mailbox.put(new MessageEnvelope(submit, "requester-1"));

            assertTrue(output.awaitResult());
            assertEquals("Job is still running.", output.result().getErrorMessage());
            assertNull(output.result().getAdmissionRejection());
            assertEquals(1L, startupCommitCount(store));
            assertEquals(1, store.pendingOutboxCountReads());
        } finally {
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(2_000L);
        }
    }

    @Test
    void pendingOutboxCountFailureRejectsWithoutPretendingCountIsZero() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        store.failPendingOutboxCount();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-outbox-count-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-outbox-count-failure", List.of("a")),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            assertFalse(output.result().isSuccessful());
            assertTrue(output.result().getErrorMessage().contains("could not read"));
            assertNull(output.result().getAdmissionRejection());
            assertEquals(0L, startupCommitCount(store));
            assertEquals(0L, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1, store.pendingOutboxCountReads());
        } finally {
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(2_000L);
        }
    }

    @Test
    void pendingOutboxCountExceptionIsNormalizedToStorageFailure() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        store.throwPendingOutboxCount();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(
                scheduler,
                "scheduler-outbox-count-exception-test"
        );
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-outbox-count-exception", List.of("a")),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            assertFalse(output.result().isSuccessful());
            assertTrue(output.result().getErrorMessage().contains("could not read"));
            assertEquals(0L, startupCommitCount(store));
            assertFalse(scheduler.getOverloadSnapshot().pendingOutboxObservationHealthy());
        } finally {
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(2_000L);
        }
    }

    @Test
    void successfulJobPersistsLifecycleTransitions() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-persistence-success-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-success", List.of("payload")), "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());
            assertEquals(ProtocolVersions.ASSIGNMENT_IDENTITY, assignment.getProtocolVersion());
            assertEquals(1, assignment.getAttemptNumber());
            assertNotNull(assignment.getAssignmentId());
            assertEquals(store.lastLeaseExpiresAt(), assignment.getLeaseExpiresAtEpochMillis());

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobCompleted());
            assertEquals(List.of(
                    "insertJobWithTasks:job-success:TEST_TASK:requester-1:1:task-job-success-0",
                    "markTaskAssigned:task-job-success-0:peer-1",
                    "markTaskCompleted:task-job-success-0",
                    "markJobCompleted:job-success"
            ), store.events());
            List<JobStateStore.TaskAttemptRecord> attempts = store.loadTaskAttempts("job-success");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals("task-job-success-0", attempt.taskId());
            assertEquals(1, attempt.attemptNumber());
            assertEquals(assignment.getAssignmentId(), attempt.assignmentId());
            assertEquals("peer-1", attempt.peerId());
            assertEquals(assignment.getLeaseExpiresAtEpochMillis(), attempt.leaseExpiresAt());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, attempt.outcome());
            assertEquals("", attempt.failureReason());
            assertNotNull(store.lastLeaseOwnerId());
            assertFalse(store.lastLeaseOwnerId().isBlank());
            assertTrue(store.lastLeaseExpiresAt() > attempt.startedAt());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void successfulJobDeliversAndPersistsSemanticResultPayload() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-semantic-result-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-semantic-result", List.of("payload"), "SEMANTIC_RESULT"),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(successResult(assignment, "alpha"), "peer-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobCompleted());
            JobResultMessage result = output.result();
            Map<String, Object> expectedPayload = Map.of(
                    "resultCount", 1,
                    "joined", "alpha");
            assertTrue(result.isSuccessful());
            assertEquals(expectedPayload, result.getResultPayload());
            assertEquals(List.of("alpha"), result.getResultsByTaskId());
            assertEquals(expectedPayload, store.lastCompletedResultPayload());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void successfulJobCompletionPersistsTerminalStateWithBrokerOutbox() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        OutboxCapturingOutput output = new OutboxCapturingOutput(true);
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-outbox-completion-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-outbox-completion", List.of("payload")), "requester-1"));

            assertTrue(store.awaitTaskAssigned());
            TaskAssignMessage assignment = output.awaitTaskAssignmentOutbox();
            assertNotNull(assignment);

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertTrue(store.awaitJobCompleted());
            assertTrue(awaitActiveJobs(scheduler, 0));
            List<BrokerOutboxStore.OutboxRecord> allOutbox = store.allOutboxRecords();
            assertEquals(2, allOutbox.size());
            assertEquals(TransportRoute.TASK_ASSIGN, allOutbox.get(0).message().route());
            assertEquals(TransportRoute.JOB_RESULT, allOutbox.get(1).message().route());
            assertEquals("requester-1", allOutbox.get(1).message().peerNodeId());
            JobResultMessage result = (JobResultMessage) allOutbox.get(1).message().message();
            assertTrue(result.isSuccessful());
            assertEquals(List.of("result"), result.getResultsByTaskId());
            assertEquals(List.of(1L, 2L), store.publishedOutboxIds());
            assertEquals(List.of(), store.failedOutboxIds());
            assertEquals(List.of(
                    "insertJobWithTasks:job-outbox-completion:TEST_TASK:requester-1:1:"
                            + "task-job-outbox-completion-0",
                    "markTaskAssigned:task-job-outbox-completion-0:peer-1",
                    "markTaskCompleted:task-job-outbox-completion-0",
                    "markJobCompleted:job-outbox-completion"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void terminalTaskFailurePersistsFailedTaskAndJob() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "1"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-persistence-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-failure", List.of("payload")), "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(failedResult(assignment, "processor failed"), "peer-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobFailed());
            assertEquals(List.of(
                    "insertJobWithTasks:job-failure:TEST_TASK:requester-1:1:task-job-failure-0",
                    "markTaskAssigned:task-job-failure-0:peer-1",
                    "markTaskFailed:task-job-failure-0",
                    "markJobFailed:job-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void peerReportedFailurePersistsRetryAttemptHistory() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        MultiTaskOutput output = new MultiTaskOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "2"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-attempt-retry-history-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-retry-history", List.of("payload")), "requester-1"));

            TaskAssignMessage first = output.awaitTask();
            assertNotNull(first);
            assertTrue(store.awaitTaskAssigned());
            mailbox.put(new MessageEnvelope(failedResult(first, "processor failed"), "peer-1"));

            TaskAssignMessage retry = output.awaitTask();
            assertNotNull(retry);
            assertEquals(first.getTaskId(), retry.getTaskId());

            List<JobStateStore.TaskAttemptRecord> attempts = store.loadTaskAttempts("job-retry-history");
            assertEquals(2, attempts.size());
            JobStateStore.TaskAttemptRecord failed = attempts.getFirst();
            assertEquals(1, failed.attemptNumber());
            assertEquals("peer-1", failed.peerId());
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, failed.outcome());
            assertEquals("processor failed", failed.failureReason());
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, attempts.get(1).outcome());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void payloadIntegrityFailurePersistsTerminalAttemptWithoutRetry() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_TASK_RETRIES",
                "5"
        ));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(
                scheduler,
                "scheduler-payload-integrity-persistence-test"
        );
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-payload-integrity", List.of("payload")),
                    "requester-1"
            ));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());
            mailbox.put(new MessageEnvelope(
                    failedResult(
                            assignment,
                            "payload digest mismatch",
                            TaskFailureClassification.PERMANENT_PAYLOAD_INTEGRITY
                    ),
                    "peer-1"
            ));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobFailed());
            assertEquals(List.of(
                    "insertJobWithTasks:job-payload-integrity:TEST_TASK:requester-1:1:"
                            + "task-job-payload-integrity-0",
                    "markTaskAssigned:task-job-payload-integrity-0:peer-1",
                    "markTaskFailed:task-job-payload-integrity-0",
                    "markJobFailed:job-payload-integrity"
            ), store.events());
            List<JobStateStore.TaskAttemptRecord> attempts =
                    store.loadTaskAttempts("job-payload-integrity");
            assertEquals(1, attempts.size());
            assertEquals(
                    JobStateStore.TaskAttemptOutcome.TERMINAL_FAILURE,
                    attempts.getFirst().outcome()
            );
            assertEquals("payload digest mismatch", attempts.getFirst().failureReason());
            assertEquals(0, scheduler.getMetricsSnapshot().retryCount());
            assertEquals(1, scheduler.getMetricsSnapshot().payloadIntegrityFailuresTotal());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void timeoutPersistsTerminalAttemptHistory() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MutableClock clock = new MutableClock(10_000L);
        InMemoryPeerRegistry registry = registryWithPeer("slow-peer");
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_TASK_TIMEOUT_MS", "1",
                "TASKFLOW_MAX_TASK_RETRIES", "1"
        ));
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                store,
                output,
                config,
                clock,
                () -> "00000000-0000-0000-0000-000000000302",
                "COORDINATOR_test"
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-attempt-timeout-history-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-timeout-history", List.of("payload")), "requester-1"));

            assertTrue(output.awaitTask());
            clock.advanceMillis(2L);
            mailbox.put(new MessageEnvelope(
                    new protocol.PeerDisconnectedMessage(
                            "clock-wakeup",
                            clock.now().toString(),
                            "test_clock_advanced"
                    ),
                    "clock-wakeup"
            ));
            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobFailed());

            List<JobStateStore.TaskAttemptRecord> attempts = store.loadTaskAttempts("job-timeout-history");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals("slow-peer", attempt.peerId());
            assertEquals(JobStateStore.TaskAttemptOutcome.TERMINAL_FAILURE, attempt.outcome());
            assertEquals("task_timeout", attempt.failureReason());
            assertEquals(10_002L, attempt.finishedAt());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void peerDisconnectPersistsReleaseAttemptHistoryAndIgnoresStaleResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        MultiTaskOutput output = new MultiTaskOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-attempt-disconnect-history-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-disconnect-history", List.of("payload")), "requester-1"));

            TaskAssignMessage first = output.awaitTask();
            assertNotNull(first);
            registry.remove("peer-1");
            registry.register("peer-2", new PeerInfo(
                    "peer-2",
                    SchedulerConfig.defaults(),
                    List.of("TEST_TASK")
            ));
            mailbox.put(new MessageEnvelope(
                    new protocol.PeerDisconnectedMessage("peer-1", "2026-07-01T00:00:00Z", "heartbeat_timeout"),
                    "peer-1"
            ));

            TaskAssignMessage retry = output.awaitTask();
            assertNotNull(retry);
            mailbox.put(new MessageEnvelope(successResult(first, "stale-result"), "peer-1"));
            assertFalse(output.awaitResult(300));

            mailbox.put(new MessageEnvelope(successResult(retry, "accepted-result"), "peer-2"));
            assertTrue(output.awaitResult());

            List<JobStateStore.TaskAttemptRecord> attempts = store.loadTaskAttempts("job-disconnect-history");
            assertEquals(2, attempts.size());
            JobStateStore.TaskAttemptRecord released = attempts.getFirst();
            assertEquals("peer-1", released.peerId());
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, released.outcome());
            assertEquals("heartbeat_timeout", released.failureReason());

            JobStateStore.TaskAttemptRecord accepted = attempts.get(1);
            assertEquals("peer-2", accepted.peerId());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, accepted.outcome());
            assertEquals(List.of("accepted-result"), output.result().getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void startupPersistenceFailureReturnsFailureWithoutDispatchingTask() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(false);
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-persistence-startup-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-persistence-failure", List.of("payload")), "requester-1"));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-persistence-failure", result.getJobId());
            assertEquals("Job could not be persisted.", result.getErrorMessage());
            assertNull(output.task());
            assertEquals(List.of(
                    "insertJobWithTasks:job-persistence-failure:TEST_TASK:requester-1:1:task-job-persistence-failure-0"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void legacyPersistedJobIdCollisionReturnsFailureWithoutStartupWrite() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        store.addExistingJobId("job-persisted-duplicate");
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-persisted-duplicate-job-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-persisted-duplicate", List.of("payload")),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-persisted-duplicate", result.getJobId());
            assertTrue(result.getErrorMessage().contains("legacy submission cannot be verified"));
            assertNull(output.task());
            assertEquals(List.of(), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void assignmentPersistenceFailureReturnsFailureWithoutDispatchingTask() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markTaskAssigned");
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-assignment-persistence-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-assignment-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobFailed());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-assignment-persistence-failure", result.getJobId());
            assertEquals("Persistence write failed during markTaskAssigned.", result.getErrorMessage());
            assertNull(output.task());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(0, scheduler.getMetricsSnapshot().assignmentGenerationsTotal());
            assertEquals(0, registry.get("peer-1").getActiveTasks());
            assertEquals(List.of(
                    "insertJobWithTasks:job-assignment-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-assignment-persistence-failure-0",
                    "markTaskAssigned:task-job-assignment-persistence-failure-0:peer-1",
                    "markTaskFailed:task-job-assignment-persistence-failure-0",
                    "markJobFailed:job-assignment-persistence-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void dispatchReleaseWriteFailurePreservesAssignmentUntilJobFailureCommits() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MutableClock clock = new MutableClock(5_000L);
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        store.failNextTransition("commitAssignedTaskFailure");
        store.failNextTransition("commitJobFailed");
        DispatchFailingOutput output = new DispatchFailingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                store,
                output,
                SchedulerConfig.defaults(),
                clock,
                () -> "00000000-0000-0000-0000-000000000201",
                "COORDINATOR_test"
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-dispatch-release-write-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-dispatch-release-write-failure", List.of("payload")),
                    "requester-1"
            ));

            assertTrue(output.awaitDispatchAttempt());
            assertTrue(awaitTransitionAttempts(store, "commitJobFailed", 1));
            assertFalse(output.awaitResult(300));
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1, scheduler.getMetricsSnapshot().assignmentGenerationsTotal());
            assertEquals(0, scheduler.getMetricsSnapshot().retryCount());
            assertEquals(1, registry.get("peer-1").getActiveTasks());
            assertEquals(1, store.transitionAttempts("commitAssignedTaskFailure"));

            clock.advanceMillis(1_000L);

            assertTrue(output.awaitResult());
            assertFalse(output.result().isSuccessful());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(0, registry.get("peer-1").getActiveTasks());
            assertEquals(2, store.transitionAttempts("commitJobFailed"));
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void brokerAssignmentOutboxWriteFailureDoesNotProjectAssignment() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MutableClock clock = new MutableClock(5_000L);
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        store.failNextAssignmentOutbox();
        OutboxCapturingOutput output = new OutboxCapturingOutput(true);
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                store,
                output,
                SchedulerConfig.defaults(),
                clock,
                () -> "00000000-0000-0000-0000-000000000202",
                "COORDINATOR_test"
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-assignment-outbox-write-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-assignment-outbox-write-failure", List.of("payload")),
                    "requester-1"
            ));

            assertTrue(awaitAssignmentOutboxAttempts(store, 1));
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(1, store.assignmentOutboxAttempts());
            assertEquals(0, scheduler.getMetricsSnapshot().assignmentGenerationsTotal());
            assertEquals(0, registry.get("peer-1").getActiveTasks());
            assertNull(output.awaitTaskAssignmentOutbox(300));
            List<BrokerOutboxStore.OutboxRecord> records = store.allOutboxRecords();
            assertEquals(1, records.size());
            assertEquals(TransportRoute.JOB_RESULT, records.getFirst().message().route());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void brokerOutboxAssignmentStaysPendingWhenPublishFails() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MutableClock clock = new MutableClock(5_000L);
        String assignmentId = "00000000-0000-0000-0000-000000000301";
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        OutboxCapturingOutput output = new OutboxCapturingOutput(false);
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                store,
                output,
                SchedulerConfig.defaults(),
                clock,
                () -> assignmentId,
                "COORDINATOR_test"
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-outbox-assignment-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-outbox-assignment", List.of("payload")), "requester-1"));

            assertTrue(store.awaitTaskAssigned());
            assertTrue(output.awaitPublishAttempt());
            TaskAssignMessage publishedAssignment = output.awaitTaskAssignmentOutbox();
            assertNotNull(publishedAssignment);
            assertEquals(List.of(
                    "insertJobWithTasks:job-outbox-assignment:TEST_TASK:requester-1:1:"
                            + "task-job-outbox-assignment-0",
                    "markTaskAssigned:task-job-outbox-assignment-0:peer-1"
            ), store.events());
            List<BrokerOutboxStore.OutboxRecord> pending = store.loadPendingBrokerOutbox(10);
            assertEquals(1, pending.size());
            assertEquals(TransportRoute.TASK_ASSIGN, pending.getFirst().message().route());
            assertEquals("peer-1", pending.getFirst().message().peerNodeId());
            assertEquals(5_000L, pending.getFirst().createdAt());
            JobStateStore.TaskAttemptRecord attempt = store.loadTaskAttempts("job-outbox-assignment").getFirst();
            assertEquals(attempt.attemptNumber(), publishedAssignment.getAttemptNumber());
            assertEquals(attempt.assignmentId(), publishedAssignment.getAssignmentId());
            assertEquals(assignmentId, publishedAssignment.getAssignmentId());
            assertEquals(125_000L, publishedAssignment.getLeaseExpiresAtEpochMillis());
            assertEquals(attempt.leaseExpiresAt(), publishedAssignment.getLeaseExpiresAtEpochMillis());
            assertTrue(awaitFailedOutboxIds(store, List.of(1L)));
            assertEquals(List.of(1L), store.failedOutboxIds());
            assertEquals(List.of(), store.publishedOutboxIds());
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void finalResultOutboxWriteFailureKeepsProjectionUntilRetryCommits() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MutableClock clock = new MutableClock(5_000L);
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        store.failNextFinalOutbox();
        OutboxCapturingOutput output = new OutboxCapturingOutput(true);
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                store,
                output,
                SchedulerConfig.defaults(),
                clock,
                () -> "00000000-0000-0000-0000-000000000401",
                "COORDINATOR_test"
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-final-outbox-write-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-final-outbox-write-failure", List.of("payload")),
                    "requester-1"
            ));
            TaskAssignMessage assignment = output.awaitTaskAssignmentOutbox();
            assertNotNull(assignment);

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertTrue(awaitFinalOutboxAttempts(store, 1));
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1, store.allOutboxRecords().size());

            clock.advanceMillis(1_000L);

            assertTrue(awaitFinalOutboxAttempts(store, 2));
            assertTrue(awaitActiveJobs(scheduler, 0));
            List<BrokerOutboxStore.OutboxRecord> records = store.allOutboxRecords();
            assertEquals(2, records.size());
            assertEquals(TransportRoute.TASK_ASSIGN, records.get(0).message().route());
            assertEquals(TransportRoute.JOB_RESULT, records.get(1).message().route());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void failedFinalOutboxWriteFailurePreservesRemainingAssignmentUntilRetryCommits() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        MutableClock clock = new MutableClock(5_000L);
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        OutboxRecordingJobStateStore store = new OutboxRecordingJobStateStore();
        store.failNextFinalOutbox();
        OutboxCapturingOutput output = new OutboxCapturingOutput(true);
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "1"));
        AtomicInteger assignmentSequence = new AtomicInteger(500);
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                store,
                output,
                config,
                clock,
                () -> "00000000-0000-0000-0000-000000000" + assignmentSequence.incrementAndGet(),
                "COORDINATOR_test"
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-failed-final-outbox-write-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-failed-final-outbox-write", List.of("first", "second")),
                    "requester-1"
            ));
            TaskAssignMessage first = output.awaitTaskAssignmentOutbox();
            TaskAssignMessage second = output.awaitTaskAssignmentOutbox();
            assertNotNull(first);
            assertNotNull(second);
            assertEquals(2, registry.get("peer-1").getActiveTasks());

            mailbox.put(new MessageEnvelope(failedResult(first, "processor failed"), "peer-1"));

            assertTrue(awaitFinalOutboxAttempts(store, 1));
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1, registry.get("peer-1").getActiveTasks());
            assertEquals(2, store.allOutboxRecords().size());

            clock.advanceMillis(1_000L);

            assertTrue(awaitFinalOutboxAttempts(store, 2));
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(0, registry.get("peer-1").getActiveTasks());
            List<BrokerOutboxStore.OutboxRecord> records = store.allOutboxRecords();
            assertEquals(3, records.size());
            assertEquals(TransportRoute.JOB_RESULT, records.getLast().message().route());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void retryPersistenceFailureReturnsFailureInsteadOfRedispatching() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markTaskRetried");
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "2"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-retry-persistence-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-retry-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(failedResult(assignment, "processor failed"), "peer-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobFailed());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("Persistence write failed during markTaskRetried.", result.getErrorMessage());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(0, scheduler.getMetricsSnapshot().retryCount());
            assertEquals(0, scheduler.getMetricsSnapshot().failureCount());
            assertEquals(0, registry.get("peer-1").getActiveTasks());
            assertEquals(List.of(
                    "insertJobWithTasks:job-retry-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-retry-persistence-failure-0",
                    "markTaskAssigned:task-job-retry-persistence-failure-0:peer-1",
                    "markTaskRetried:task-job-retry-persistence-failure-0:1",
                    "markTaskFailed:task-job-retry-persistence-failure-0",
                    "markJobFailed:job-retry-persistence-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void terminalTaskWriteFailurePreservesAssignedProjectionAndCapacity() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markTaskFailed");
        TaskCapturingOutput output = new TaskCapturingOutput();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "1"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-terminal-task-write-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-terminal-task-write-failure", List.of("payload")),
                    "requester-1"
            ));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(
                    failedResult(assignment, "processor failed"),
                    "peer-1",
                    acknowledgement
            ));

            assertTrue(store.awaitEvent("markTaskFailed:task-job-terminal-task-write-failure-0"));
            assertTrue(acknowledgement.awaitRequeue());
            assertEquals(DeliveryDisposition.RETRY_TRANSIENT, acknowledgement.disposition());
            assertFalse(output.awaitResult(300));
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(0, scheduler.getMetricsSnapshot().retryCount());
            assertEquals(0, scheduler.getMetricsSnapshot().failureCount());
            assertEquals(0, scheduler.getMetricsSnapshot().terminalFailureCount());
            assertEquals(1, registry.get("peer-1").getActiveTasks());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void taskCompletionStorageFailureRequeuesWithoutMutatingMemoryOrMetrics() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markTaskCompleted");
        TaskCapturingOutput output = new TaskCapturingOutput();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-task-completion-persistence-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-completion-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(
                    successResult(assignment, "result"),
                    "peer-1",
                    acknowledgement
            ));

            assertTrue(acknowledgement.awaitRequeue());
            assertFalse(output.awaitResult(300));
            assertEquals(0, acknowledgement.ackCount());
            assertEquals(1, acknowledgement.requeueCount());
            assertEquals(0, acknowledgement.rejectCount());
            assertEquals(DeliveryDisposition.RETRY_TRANSIENT, acknowledgement.disposition());
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(0, scheduler.getMetricsSnapshot().successCount());
            assertEquals(1, scheduler.getMetricsSnapshot().resultStorageFailureCount());
            PeerInfo peer = registry.get("peer-1");
            assertNotNull(peer);
            assertEquals(1, peer.getActiveTasks());
            assertEquals(0L, peer.getCompletedTasks());
            assertEquals(1L, registry.capacityMetricsSnapshot().reservationsCreated());
            assertEquals(0L, registry.capacityMetricsSnapshot().reservationsReleased());
            assertEquals(1L, registry.capacityMetricsSnapshot().activeReservations());
            List<JobStateStore.TaskAttemptRecord> attempts =
                    store.loadTaskAttempts("job-completion-persistence-failure");
            assertEquals(1, attempts.size());
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, attempts.getFirst().outcome());
            assertEquals(List.of(
                    "insertJobWithTasks:job-completion-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-completion-persistence-failure-0",
                    "markTaskAssigned:task-job-completion-persistence-failure-0:peer-1",
                    "markTaskCompleted:task-job-completion-persistence-failure-0"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void staleResultIsAcknowledgedWithoutRequeueOrSuccessAccounting() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        RecordingAcknowledgement staleAcknowledgement = new RecordingAcknowledgement();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-stale-result-disposition-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-stale-disposition", List.of("payload")), "requester-1"));
            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            TaskResultMessage stale = new TaskResultMessage(
                    "peer-1",
                    "2026-07-22T00:00:00Z",
                    assignment.getTaskId(),
                    assignment.getJobId(),
                    assignment.getAttemptNumber(),
                    "550e8400-e29b-41d4-a716-446655440099",
                    "stale-result",
                    true,
                    null
            );
            mailbox.put(new MessageEnvelope(stale, "peer-1", staleAcknowledgement));

            assertTrue(staleAcknowledgement.awaitAck());
            assertEquals(1, staleAcknowledgement.ackCount());
            assertEquals(0, staleAcknowledgement.requeueCount());
            assertEquals(0, staleAcknowledgement.rejectCount());
            assertEquals(
                    DeliveryDisposition.ACK_DUPLICATE_OR_STALE,
                    staleAcknowledgement.disposition()
            );
            assertFalse(output.awaitResult(300));
            assertEquals(0, scheduler.getMetricsSnapshot().successCount());
            assertEquals(1, scheduler.getMetricsSnapshot().staleResultCount());
            PeerInfo peer = registry.get("peer-1");
            assertNotNull(peer);
            assertEquals(1, peer.getActiveTasks());
            assertEquals(0L, peer.getCompletedTasks());
            assertEquals(1L, registry.capacityMetricsSnapshot().reservationsCreated());
            assertEquals(0L, registry.capacityMetricsSnapshot().reservationsReleased());
            assertEquals(1L, registry.capacityMetricsSnapshot().activeReservations());
            assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING,
                    store.loadTaskAttempts("job-stale-disposition").getFirst().outcome());

            mailbox.put(new MessageEnvelope(successResult(assignment, "current-result"), "peer-1"));
            assertTrue(output.awaitResult());
            assertEquals(List.of("current-result"), output.result().getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void duplicateResultIsAcknowledgedWithoutRequeueOrSecondSuccess() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        MultiTaskOutput output = new MultiTaskOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Logger schedulerLogger = (Logger) LoggerFactory.getLogger(TaskScheduler.class);
        Level previousLogLevel = schedulerLogger.getLevel();
        boolean previousAdditive = schedulerLogger.isAdditive();
        schedulerLogger.setLevel(Level.INFO);
        schedulerLogger.setAdditive(false);
        ThreadSafeListAppender logAppender = new ThreadSafeListAppender();
        logAppender.start();
        schedulerLogger.addAppender(logAppender);
        Thread schedulerThread = new Thread(scheduler, "scheduler-duplicate-result-disposition-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-duplicate-disposition", List.of("payload-a", "payload-b")),
                    "requester-1"
            ));
            TaskAssignMessage first = output.awaitTask();
            TaskAssignMessage second = output.awaitTask();
            assertNotNull(first);
            assertNotNull(second);

            RecordingAcknowledgement firstAcknowledgement = new RecordingAcknowledgement();
            mailbox.put(new MessageEnvelope(
                    successResult(first, "first-result"),
                    "peer-1",
                    firstAcknowledgement
            ));
            assertTrue(firstAcknowledgement.awaitAck());
            assertFalse(output.awaitResult(200));

            RecordingAcknowledgement duplicateAcknowledgement = new RecordingAcknowledgement();
            mailbox.put(new MessageEnvelope(
                    successResult(first, "duplicate-result"),
                    "peer-1",
                    duplicateAcknowledgement
            ));
            assertTrue(duplicateAcknowledgement.awaitAck());
            assertEquals(1, duplicateAcknowledgement.ackCount());
            assertEquals(0, duplicateAcknowledgement.requeueCount());
            assertEquals(0, duplicateAcknowledgement.rejectCount());
            assertEquals(
                    DeliveryDisposition.ACK_DUPLICATE_OR_STALE,
                    duplicateAcknowledgement.disposition()
            );
            assertEquals(1, scheduler.getMetricsSnapshot().successCount());
            assertEquals(1, scheduler.getMetricsSnapshot().duplicateResultCount());
            assertEquals(1, scheduler.getMetricsSnapshot().taskResultsCommittedTotal());
            assertEquals(1, scheduler.getMetricsSnapshot().taskResultsDuplicateTotal());
            assertEquals(0, scheduler.getMetricsSnapshot().taskResultsStaleTotal());
            assertTraceEvent(logAppender, "task_result_duplicate_ignored", first, "peer-1");
            assertNoTraceEvent(logAppender, "task_result_stale_rejected", first);
            assertFencingMetricNames(logAppender);
            PeerInfo peer = registry.get("peer-1");
            assertNotNull(peer);
            assertEquals(1L, peer.getCompletedTasks());
            assertEquals(1, peer.getActiveTasks());
            assertEquals(2L, registry.capacityMetricsSnapshot().reservationsCreated());
            assertEquals(1L, registry.capacityMetricsSnapshot().reservationsReleased());
            assertEquals(1L, registry.capacityMetricsSnapshot().activeReservations());

            mailbox.put(new MessageEnvelope(successResult(second, "second-result"), "peer-1"));
            assertTrue(output.awaitResult());
            assertEquals(2, output.result().getResultsByTaskId().size());
            assertEquals(2L, registry.capacityMetricsSnapshot().reservationsReleased());
            assertEquals(0L, registry.capacityMetricsSnapshot().activeReservations());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
            schedulerLogger.detachAppender(logAppender);
            logAppender.stop();
            schedulerLogger.setLevel(previousLogLevel);
            schedulerLogger.setAdditive(previousAdditive);
        }
    }

    @Test
    void finalJobCompletionPersistenceFailureKeepsProjectionActiveAndSuppressesResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markJobCompleted");
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-final-persistence-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-final-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertTrue(store.awaitEvent("markJobCompleted:job-final-persistence-failure"));
            assertFalse(output.awaitResult(300));
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-1",
                            "2026-07-22T00:00:00Z",
                            "job-final-persistence-failure",
                            "token-job-final-persistence-failure"
                    ),
                    "requester-1"
            ));
            assertTrue(output.awaitResult());
            assertFalse(output.result().isSuccessful());
            assertEquals("Job is still running.", output.result().getErrorMessage());
            assertEquals(List.of(
                    "insertJobWithTasks:job-final-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-final-persistence-failure-0",
                    "markTaskAssigned:task-job-final-persistence-failure-0:peer-1",
                    "markTaskCompleted:task-job-final-persistence-failure-0",
                    "markJobCompleted:job-final-persistence-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void finalJobFailurePersistenceFailureKeepsProjectionActiveAndSuppressesResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markJobFailed");
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "1"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-final-failure-persistence-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-final-failure-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(failedResult(assignment, "processor failed"), "peer-1"));

            assertTrue(store.awaitEvent("markJobFailed:job-final-failure-persistence-failure"));
            assertFalse(output.awaitResult(300));
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(List.of(
                    "insertJobWithTasks:job-final-failure-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-final-failure-persistence-failure-0",
                    "markTaskAssigned:task-job-final-failure-persistence-failure-0:peer-1",
                    "markTaskFailed:task-job-final-failure-persistence-failure-0",
                    "markJobFailed:job-final-failure-persistence-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void oneShotJobCompletionWriteFailureProjectsOnlyAfterRetryCommits() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        store.failNextTransition("commitJobCompleted");
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-one-shot-completion-write-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-one-shot-completion", List.of("payload")),
                    "requester-1"
            ));
            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertFalse(output.awaitResult(300));
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1, store.transitionAttempts("commitJobCompleted"));

            assertTrue(output.awaitResult());
            assertTrue(output.result().isSuccessful());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(2, store.transitionAttempts("commitJobCompleted"));
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void oneShotJobFailureWriteFailureProjectsOnlyAfterRetryCommits() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        store.failNextTransition("commitJobFailed");
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "1"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-one-shot-failure-write-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-one-shot-failure", List.of("payload")),
                    "requester-1"
            ));
            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();

            mailbox.put(new MessageEnvelope(failedResult(assignment, "processor failed"), "peer-1"));

            assertFalse(output.awaitResult(300));
            assertEquals(1, scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1, store.transitionAttempts("commitJobFailed"));

            assertTrue(output.awaitResult());
            assertFalse(output.result().isSuccessful());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(2, store.transitionAttempts("commitJobFailed"));
            assertEquals(0, registry.get("peer-1").getActiveTasks());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void undeliverableFinalJobResultIsAbandonedAfterDurableCompletion() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        UndeliverableResultOutput output = new UndeliverableResultOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_JOB_RESULT_MAX_DELIVERY_ATTEMPTS", "1"
        ));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-result-abandonment-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-result-abandoned", List.of("payload")), "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertTrue(output.awaitResultAttempt());
            assertTrue(store.awaitJobCompleted());
            assertEquals(1, output.resultAttempts());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(List.of(
                    "insertJobWithTasks:job-result-abandoned:TEST_TASK:requester-1:1:task-job-result-abandoned-0",
                    "markTaskAssigned:task-job-result-abandoned-0:peer-1",
                    "markTaskCompleted:task-job-result-abandoned-0",
                    "markJobCompleted:job-result-abandoned"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void restoredCompletedJobPublishesFinalResultAndPersistsCompletion() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        EmbarrassinglyParallelJob<?, ?> restoredJob = restoredCompletedJob(
                "job-restored-complete",
                "payload",
                "restored-result"
        );

        scheduler.restoreJobs(List.of(restoredJob));

        assertTrue(output.awaitResult());
        assertTrue(store.awaitJobCompleted());
        JobResultMessage result = output.result();
        assertTrue(result.isSuccessful());
        assertEquals("job-restored-complete", result.getJobId());
        assertEquals(List.of("restored-result"), result.getResultsByTaskId());
        assertEquals(List.of("markJobCompleted:job-restored-complete"), store.events());
    }

    @Test
    void completedResultRequestLoadsPersistedJobResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-completed-result",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                "",
                List.of("result-alpha", "result-beta")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-completed-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-1",
                            "2026-06-25T00:00:00Z",
                            "job-completed-result",
                            requesterToken
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertTrue(result.isSuccessful());
            assertEquals("job-completed-result", result.getJobId());
            assertEquals("TEST_TASK", result.getTaskType());
            assertEquals(List.of("result-alpha", "result-beta"), result.getResultsByTaskId());
            assertEquals(List.of("loadCompletedJobResult:job-completed-result"), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestRejectsWrongRequesterToken() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-owned-result",
                "TEST_TASK",
                RequesterTokens.hashToken("owner-token"),
                "",
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-wrong-token-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-2",
                            "2026-06-25T00:00:00Z",
                            "job-owned-result",
                            "wrong-token"
                    ),
                    "requester-2"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-owned-result", result.getJobId());
            assertEquals("Requester token does not match job owner.", result.getErrorMessage());
            assertEquals(List.of(), result.getResultsByTaskId());
            assertEquals(List.of("loadCompletedJobResult:job-owned-result"), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void signedJobSubmitPersistsRequesterIdentityKey() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-signed-submit-test");
        schedulerThread.start();

        try {
            RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
            mailbox.put(new MessageEnvelope(
                    signedTestJob("job-signed-submit", List.of("payload"), "token-signed-submit", identity),
                    "requester-1"
            ));

            assertTrue(output.awaitTask());
            assertEquals(identity.publicKey(), store.lastRequesterIdentityKey());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void signedJobSubmitRejectsInvalidRequesterIdentitySignature() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registryWithPeer("peer-1"),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-invalid-submit-signature-test");
        schedulerThread.start();

        try {
            RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
            String time = "2026-06-26T00:00:00Z";
            String signature = RequesterIdentity.signJobSubmit(
                    identity.privateKey(),
                    "requester-1",
                    time,
                    "job-invalid-signature",
                    "TEST_TASK",
                    "",
                    "different-token"
            );
            mailbox.put(new MessageEnvelope(
                    new JobSubmitMessage(
                            "requester-1",
                            time,
                            "job-invalid-signature",
                            "TEST_TASK",
                            List.of("payload"),
                            "",
                            "token-invalid-signature",
                            identity.publicKey(),
                            signature
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("Requester identity signature is invalid.", result.getErrorMessage());
            assertTrue(store.events().isEmpty());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestRequiresSignedRequesterIdentityForIdentityBoundJob() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-identity-bound-result",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                identity.publicKey(),
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-missing-identity-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-1",
                            "2026-06-26T00:00:00Z",
                            "job-identity-bound-result",
                            requesterToken
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-identity-bound-result", result.getJobId());
            assertEquals("Requester identity signature is required.", result.getErrorMessage());
            assertEquals(List.of(), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestRejectsMismatchedRequesterIdentityKey() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        RequesterIdentity.Credentials ownerIdentity = RequesterIdentity.newCredentials();
        RequesterIdentity.Credentials wrongIdentity = RequesterIdentity.newCredentials();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-wrong-identity-result",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                ownerIdentity.publicKey(),
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-wrong-identity-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    signedResultRequest(
                            "requester-1",
                            "job-wrong-identity-result",
                            requesterToken,
                            wrongIdentity
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-wrong-identity-result", result.getJobId());
            assertEquals("Requester identity key does not match job owner.", result.getErrorMessage());
            assertEquals(List.of(), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestRejectsInvalidRequesterIdentitySignature() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-invalid-result-signature",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                identity.publicKey(),
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-invalid-result-signature-test");
        schedulerThread.start();

        try {
            String time = "2026-06-26T00:00:00Z";
            String signature = RequesterIdentity.signJobResultRequest(
                    identity.privateKey(),
                    "requester-1",
                    time,
                    "different-job",
                    requesterToken
            );
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-1",
                            time,
                            "job-invalid-result-signature",
                            requesterToken,
                            identity.publicKey(),
                            signature
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-invalid-result-signature", result.getJobId());
            assertEquals("Requester identity signature is invalid.", result.getErrorMessage());
            assertEquals(List.of(), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestAcceptsValidRequesterIdentitySignature() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-signed-result",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                identity.publicKey(),
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-signed-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    signedResultRequest(
                            "requester-1",
                            "job-signed-result",
                            requesterToken,
                            identity
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertTrue(result.isSuccessful());
            assertEquals("job-signed-result", result.getJobId());
            assertEquals(List.of("secret-result"), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestReportsRunningJobWithoutRemovingIt() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-running-result-request-test");
        schedulerThread.start();

        try {
            JobSubmitMessage submit = testJob("job-running-result-request", List.of("payload"));
            mailbox.put(new MessageEnvelope(submit, "requester-1"));

            assertTrue(output.awaitTask());
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-1",
                            "2026-06-25T00:00:00Z",
                            "job-running-result-request",
                            submit.getRequesterToken()
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-running-result-request", result.getJobId());
            assertEquals("Job is still running.", result.getErrorMessage());
            assertTrue(awaitActiveJobs(scheduler, 1));
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    private static InMemoryPeerRegistry registryWithPeer(String peerId) {
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register(peerId, new PeerInfo(
                peerId,
                SchedulerConfig.defaults(),
                List.of("TEST_TASK")
        ));
        return registry;
    }

    private static JobSubmitMessage testJob(String jobId, List<Object> payloads) {
        return testJob(jobId, payloads, "");
    }

    private static JobSubmitMessage testJob(String jobId, List<Object> payloads, String parameter) {
        return new JobSubmitMessage(
                "requester-1",
                "2026-06-12T00:00:00Z",
                jobId,
                "TEST_TASK",
                payloads,
                parameter,
                "token-" + jobId
        );
    }

    private static BrokerOutboxStore.OutboxMessage testPendingOutboxMessage() {
        return new BrokerOutboxStore.OutboxMessage(
                TransportRoute.JOB_RESULT,
                "requester-1",
                "COORDINATOR",
                new JobResultMessage(
                        "COORDINATOR",
                        "2026-07-26T00:00:00Z",
                        "job-pending-outbox",
                        TestTaskPlugin.TASK_TYPE,
                        false,
                        List.of(),
                        "pending"
                )
        );
    }

    private static long startupCommitCount(RecordingJobStateStore store) {
        return store.events().stream()
                .filter(event -> event.startsWith("insertJobWithTasks:"))
                .count();
    }

    private static void assertAdmissionRejection(JobResultMessage result,
                                                 AdmissionRejection.Limit limit,
                                                 long configuredMaximum,
                                                 long observedValue) {
        assertNotNull(result);
        assertFalse(result.isSuccessful());
        assertNotNull(result.getAdmissionRejection());
        assertEquals(limit, result.getAdmissionRejection().limit());
        assertEquals(configuredMaximum, result.getAdmissionRejection().configuredMaximum());
        assertEquals(observedValue, result.getAdmissionRejection().observedValue());
    }

    private static JobSubmitMessage signedTestJob(String jobId,
                                                  List<Object> payloads,
                                                  String requesterToken,
                                                  RequesterIdentity.Credentials identity) {
        String time = "2026-06-26T00:00:00Z";
        String signature = RequesterIdentity.signJobSubmit(
                identity.privateKey(),
                "requester-1",
                time,
                jobId,
                "TEST_TASK",
                "",
                requesterToken
        );
        return new JobSubmitMessage(
                "requester-1",
                time,
                jobId,
                "TEST_TASK",
                payloads,
                "",
                requesterToken,
                identity.publicKey(),
                signature
        );
    }

    private static JobResultRequestMessage signedResultRequest(String requesterId,
                                                               String jobId,
                                                               String requesterToken,
                                                               RequesterIdentity.Credentials identity) {
        String time = "2026-06-26T00:00:00Z";
        String signature = RequesterIdentity.signJobResultRequest(
                identity.privateKey(),
                requesterId,
                time,
                jobId,
                requesterToken
        );
        return new JobResultRequestMessage(
                requesterId,
                time,
                jobId,
                requesterToken,
                identity.publicKey(),
                signature
        );
    }

    private static EmbarrassinglyParallelJob<?, ?> restoredCompletedJob(String jobId,
                                                                        Object payload,
                                                                        Object resultPayload) {
        JobSubmitMessage submit = testJob(jobId, List.of(payload));
        EmbarrassinglyParallelJob<?, ?> job = JobFactory.create(submit, "requester-1");
        job.initializeTasks(submit);
        assertTrue(job.restoreTaskForResume(
                "task-" + jobId + "-0",
                TaskUnit.TaskStatus.COMPLETED,
                resultPayload,
                0
        ));
        return job;
    }

    private static TaskResultMessage successResult(TaskAssignMessage assignment, Object payload) {
        return new TaskResultMessage(
                "peer-1",
                "2026-06-12T00:00:00Z",
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                payload,
                true,
                null
        );
    }

    private static TaskResultMessage failedResult(TaskAssignMessage assignment, String error) {
        return new TaskResultMessage(
                "peer-1",
                "2026-06-12T00:00:00Z",
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                null,
                false,
                error
        );
    }

    private static TaskResultMessage failedResult(TaskAssignMessage assignment,
                                                  String error,
                                                  TaskFailureClassification classification) {
        return new TaskResultMessage(
                "peer-1",
                "2026-06-12T00:00:00Z",
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                null,
                false,
                error,
                classification
        );
    }

    private static void assertTraceEvent(ThreadSafeListAppender appender,
                                         String eventName,
                                         TaskAssignMessage assignment,
                                         String workerId) {
        String event = appender.events().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("event=" + eventName))
                .filter(message -> message.contains("assignment_id=" + assignment.getAssignmentId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Missing " + eventName + " for assignment " + assignment.getAssignmentId()
                ));
        assertTrue(event.contains("job_id=" + assignment.getJobId()));
        assertTrue(event.contains("task_id=" + assignment.getTaskId()));
        assertTrue(event.contains("attempt_number=" + assignment.getAttemptNumber()));
        assertTrue(event.contains("assignment_id=" + assignment.getAssignmentId()));
        assertTrue(event.contains("worker_id=" + workerId));
    }

    private static void assertNoTraceEvent(ThreadSafeListAppender appender,
                                           String eventName,
                                           TaskAssignMessage assignment) {
        assertTrue(appender.events().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .noneMatch(message -> message.contains("event=" + eventName)
                        && message.contains("assignment_id=" + assignment.getAssignmentId())));
    }

    private static void assertFencingMetricNames(ThreadSafeListAppender appender) {
        String event = appender.events().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("event=scheduler_metrics"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing scheduler_metrics event."));
        assertTrue(event.contains(SchedulerMetrics.TASK_RESULTS_COMMITTED_TOTAL_NAME + "="));
        assertTrue(event.contains(SchedulerMetrics.TASK_RESULTS_STALE_TOTAL_NAME + "="));
        assertTrue(event.contains(SchedulerMetrics.TASK_RESULTS_DUPLICATE_TOTAL_NAME + "="));
        assertTrue(event.contains(SchedulerMetrics.ASSIGNMENT_GENERATIONS_TOTAL_NAME + "="));
        assertTrue(event.contains("taskflow_capacity_snapshots_accepted_total="));
        assertTrue(event.contains("taskflow_capacity_snapshots_stale_total="));
        assertTrue(event.contains("taskflow_capacity_snapshots_incompatible_total="));
        assertTrue(event.contains("taskflow_capacity_reservations_created_total="));
        assertTrue(event.contains("taskflow_capacity_reservations_released_total="));
        assertTrue(event.contains("taskflow_capacity_projection_failures_total="));
        assertTrue(event.contains("taskflow_capacity_active_reservations="));
        assertTrue(event.contains("taskflow_capacity_reserved_units="));
        assertTrue(event.contains("overloaded="));
        assertTrue(event.contains("overload_primary_reason="));
        assertTrue(event.contains("overload_configured_maximum="));
        assertTrue(event.contains("overload_observed_value="));
        assertTrue(event.contains("overload_reasons="));
        assertTrue(event.contains("job_submit_prefetch=1"));
        assertTrue(event.contains("pending_outbox_observation_healthy="));
    }

    private static final class ThreadSafeListAppender extends AppenderBase<ILoggingEvent> {
        private final CopyOnWriteArrayList<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            event.prepareForDeferredProcessing();
            events.add(event);
        }

        private List<ILoggingEvent> events() {
            return events;
        }
    }

    private static boolean awaitActiveJobs(TaskScheduler scheduler, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (scheduler.getMetricsSnapshot().activeJobs() == expected) {
                return true;
            }
            Thread.sleep(10);
        }
        return scheduler.getMetricsSnapshot().activeJobs() == expected;
    }

    private static boolean awaitTransitionAttempts(RecordingJobStateStore store,
                                                   String operation,
                                                   int expected) throws InterruptedException {
        return awaitMonitor(store, () -> store.transitionAttempts(operation) >= expected);
    }

    private static boolean awaitFailedOutboxIds(OutboxRecordingJobStateStore store,
                                                List<Long> expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (store.failedOutboxIds().equals(expected)) {
                return true;
            }
            Thread.sleep(10);
        }
        return store.failedOutboxIds().equals(expected);
    }

    private static boolean awaitFinalOutboxAttempts(OutboxRecordingJobStateStore store,
                                                     int expected) throws InterruptedException {
        return awaitMonitor(store, () -> store.finalOutboxAttempts() == expected);
    }

    private static boolean awaitAssignmentOutboxAttempts(OutboxRecordingJobStateStore store,
                                                          int expected) throws InterruptedException {
        return awaitMonitor(store, () -> store.assignmentOutboxAttempts() == expected);
    }

    private static boolean awaitMonitor(Object monitor,
                                        BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        synchronized (monitor) {
            while (!condition.getAsBoolean()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
            }
            return true;
        }
    }

    private static class TaskCapturingOutput implements SchedulerOutput {
        private final CountDownLatch taskReceived = new CountDownLatch(1);
        private final CountDownLatch resultReceived = new CountDownLatch(1);
        private final AtomicReference<TaskAssignMessage> task = new AtomicReference<>();
        private final AtomicReference<JobResultMessage> result = new AtomicReference<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            task.set(message);
            taskReceived.countDown();
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            result.set(message);
            resultReceived.countDown();
            return true;
        }

        boolean awaitTask() throws InterruptedException {
            return taskReceived.await(2, TimeUnit.SECONDS);
        }

        TaskAssignMessage task() {
            return task.get();
        }

        boolean awaitResult() throws InterruptedException {
            return awaitResult(2_000);
        }

        boolean awaitResult(long timeoutMillis) throws InterruptedException {
            return resultReceived.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        JobResultMessage result() {
            return result.get();
        }
    }

    private static final class DispatchFailingOutput extends TaskCapturingOutput {
        private final CountDownLatch dispatchAttempted = new CountDownLatch(1);

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            dispatchAttempted.countDown();
            throw new IllegalStateException("injected dispatch failure");
        }

        private boolean awaitDispatchAttempt() throws InterruptedException {
            return dispatchAttempted.await(2, TimeUnit.SECONDS);
        }
    }

    private static class RecordingAcknowledgement implements TransportAcknowledgement {
        private final AtomicInteger ackCount = new AtomicInteger();
        private final AtomicInteger requeueCount = new AtomicInteger();
        private final AtomicInteger rejectCount = new AtomicInteger();
        private final CountDownLatch acked = new CountDownLatch(1);
        private final CountDownLatch requeued = new CountDownLatch(1);
        private final AtomicReference<DeliveryDisposition> disposition = new AtomicReference<>();

        @Override
        public void settle(DeliveryDisposition requestedDisposition) throws Exception {
            disposition.compareAndSet(null, requestedDisposition);
            TransportAcknowledgement.super.settle(requestedDisposition);
        }

        @Override
        public void ack() {
            ackCount.incrementAndGet();
            acked.countDown();
        }

        @Override
        public void requeue() {
            requeueCount.incrementAndGet();
            requeued.countDown();
        }

        @Override
        public void reject() {
            rejectCount.incrementAndGet();
        }

        boolean awaitAck() throws InterruptedException {
            return acked.await(2, TimeUnit.SECONDS);
        }

        boolean awaitRequeue() throws InterruptedException {
            return requeued.await(2, TimeUnit.SECONDS);
        }

        int ackCount() {
            return ackCount.get();
        }

        int requeueCount() {
            return requeueCount.get();
        }

        int rejectCount() {
            return rejectCount.get();
        }

        DeliveryDisposition disposition() {
            return disposition.get();
        }
    }

    private static class MultiTaskOutput implements SchedulerOutput {
        private final BlockingQueue<TaskAssignMessage> tasks = new LinkedBlockingQueue<>();
        private final CountDownLatch resultReceived = new CountDownLatch(1);
        private final AtomicReference<JobResultMessage> result = new AtomicReference<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            tasks.offer(message);
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            result.set(message);
            resultReceived.countDown();
            return true;
        }

        TaskAssignMessage awaitTask() throws InterruptedException {
            return tasks.poll(2, TimeUnit.SECONDS);
        }

        boolean awaitResult() throws InterruptedException {
            return awaitResult(2_000);
        }

        boolean awaitResult(long timeoutMillis) throws InterruptedException {
            return resultReceived.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        JobResultMessage result() {
            return result.get();
        }
    }

    private static class UndeliverableResultOutput implements SchedulerOutput {
        private final CountDownLatch taskReceived = new CountDownLatch(1);
        private final CountDownLatch resultAttempted = new CountDownLatch(1);
        private final AtomicReference<TaskAssignMessage> task = new AtomicReference<>();
        private final AtomicInteger resultAttempts = new AtomicInteger();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            task.set(message);
            taskReceived.countDown();
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            resultAttempts.incrementAndGet();
            resultAttempted.countDown();
            return false;
        }

        boolean awaitTask() throws InterruptedException {
            return taskReceived.await(2, TimeUnit.SECONDS);
        }

        TaskAssignMessage task() {
            return task.get();
        }

        boolean awaitResultAttempt() throws InterruptedException {
            return resultAttempted.await(2, TimeUnit.SECONDS);
        }

        int resultAttempts() {
            return resultAttempts.get();
        }
    }

    private static class OutboxCapturingOutput implements SchedulerOutput, BrokerOutboxPublisher {
        private final boolean publishResult;
        private final CountDownLatch publishAttempted = new CountDownLatch(1);
        private final BlockingQueue<TaskAssignMessage> taskAssignments = new LinkedBlockingQueue<>();

        private OutboxCapturingOutput(boolean publishResult) {
            this.publishResult = publishResult;
        }

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            throw new AssertionError("Expected broker outbox task delivery.");
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            throw new AssertionError("Expected broker outbox result delivery.");
        }

        @Override
        public BrokerOutboxStore.OutboxMessage taskAssignmentOutboxMessage(PeerInfo peer, TaskAssignMessage message) {
            TaskAssignMessage routed = new TaskAssignMessage(
                    peer.getNodeId(),
                    message.getTime(),
                    message.getTaskId(),
                    message.getJobId(),
                    message.getTaskType(),
                    message.getAttemptNumber(),
                    message.getAssignmentId(),
                    message.getLeaseExpiresAtEpochMillis(),
                    message.getPayload(),
                    message.getParameter()
            );
            return new BrokerOutboxStore.OutboxMessage(
                    TransportRoute.TASK_ASSIGN,
                    peer.getNodeId(),
                    "COORDINATOR",
                    routed
            );
        }

        @Override
        public BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(String requesterNodeId,
                                                                      JobResultMessage message) {
            return new BrokerOutboxStore.OutboxMessage(
                    TransportRoute.JOB_RESULT,
                    requesterNodeId,
                    "COORDINATOR",
                    message
            );
        }

        @Override
        public boolean publishOutbox(BrokerOutboxStore.OutboxRecord record) {
            if (record.message().message() instanceof TaskAssignMessage assignment) {
                taskAssignments.offer(assignment);
            }
            publishAttempted.countDown();
            return publishResult;
        }

        boolean awaitPublishAttempt() throws InterruptedException {
            return publishAttempted.await(2, TimeUnit.SECONDS);
        }

        TaskAssignMessage awaitTaskAssignmentOutbox() throws InterruptedException {
            return taskAssignments.poll(2, TimeUnit.SECONDS);
        }

        TaskAssignMessage awaitTaskAssignmentOutbox(long timeoutMillis) throws InterruptedException {
            return taskAssignments.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    private static class RecordingJobStateStore implements JobStateStore {
        private final List<String> events = new ArrayList<>();
        private final Map<String, String> taskJobIds = new LinkedHashMap<>();
        private final Map<String, Deque<TaskAttemptRecord>> attemptsByJobId = new LinkedHashMap<>();
        private final CountDownLatch taskAssigned = new CountDownLatch(1);
        private final CountDownLatch jobCompleted = new CountDownLatch(1);
        private final CountDownLatch jobFailed = new CountDownLatch(1);
        private final java.util.Set<String> existingJobIds = new java.util.LinkedHashSet<>();
        private final java.util.Set<String> failOnceTransitions = new java.util.LinkedHashSet<>();
        private final Map<String, Integer> transitionAttempts = new LinkedHashMap<>();
        private final boolean jobStartupPersists;
        private final String failingOperation;
        private JobStateStore.CompletedJobResultState completedResult;
        private String lastRequesterIdentityKey = "";
        private Object lastCompletedResultPayload;
        private String lastLeaseOwnerId = "";
        private long lastLeaseExpiresAt;

        private RecordingJobStateStore() {
            this(true);
        }

        private RecordingJobStateStore(boolean jobStartupPersists) {
            this(jobStartupPersists, "");
        }

        private RecordingJobStateStore(boolean jobStartupPersists, String failingOperation) {
            this.jobStartupPersists = jobStartupPersists;
            this.failingOperation = failingOperation == null ? "" : failingOperation;
        }

        @Override
        public synchronized boolean insertJobWithTasks(String jobId,
                                                       String taskType,
                                                       String requesterId,
                                                       String requesterTokenHash,
                                                       String requesterIdentityKey,
                                                       String parameter,
                                                       Collection<TaskStartupState> tasks) {
            lastRequesterIdentityKey = requesterIdentityKey == null ? "" : requesterIdentityKey;
            return JobStateStore.super.insertJobWithTasks(
                    jobId,
                    taskType,
                    requesterId,
                    requesterTokenHash,
                    requesterIdentityKey,
                    parameter,
                    tasks
            );
        }

        @Override
        public synchronized boolean insertJobWithTasks(String jobId,
                                                       String taskType,
                                                       String requesterId,
                                                       int fileCount,
                                                       Collection<String> taskIds) {
            events.add("insertJobWithTasks:" + jobId + ":" + taskType + ":" + requesterId + ":"
                    + fileCount + ":" + String.join(",", taskIds));
            for (String taskId : taskIds) {
                taskJobIds.put(taskId, jobId);
            }
            if (jobStartupPersists) {
                existingJobIds.add(jobId);
            }
            return jobStartupPersists;
        }

        @Override
        public synchronized boolean insertJob(String jobId, String taskType, String requesterId, int fileCount) {
            events.add("insertJob:" + jobId + ":" + taskType + ":" + requesterId + ":" + fileCount);
            existingJobIds.add(jobId);
            return true;
        }

        @Override
        public synchronized boolean hasJob(String jobId) {
            return existingJobIds.contains(jobId);
        }

        @Override
        public synchronized boolean insertTask(String taskId, String jobId) {
            events.add("insertTask:" + taskId + ":" + jobId);
            taskJobIds.put(taskId, jobId);
            return true;
        }

        @Override
        public synchronized boolean markTaskAssigned(String taskId, String peerId, long startedAt) {
            return markTaskAssigned(taskId, peerId, startedAt, "", 0L);
        }

        @Override
        public synchronized boolean markTaskAssigned(String taskId,
                                                     String peerId,
                                                     long startedAt,
                                                     String leaseOwnerId,
                                                     long leaseExpiresAt) {
            return recordTaskAssignment(
                    taskId,
                    peerId,
                    startedAt,
                    leaseOwnerId,
                    leaseExpiresAt,
                    0,
                    null
            );
        }

        @Override
        public synchronized boolean markTaskAssigned(String taskId,
                                                     String peerId,
                                                     long startedAt,
                                                     String leaseOwnerId,
                                                     long leaseExpiresAt,
                                                     int attemptNumber,
                                                     String assignmentId) {
            return recordTaskAssignment(
                    taskId,
                    peerId,
                    startedAt,
                    leaseOwnerId,
                    leaseExpiresAt,
                    attemptNumber,
                    assignmentId
            );
        }

        private boolean recordTaskAssignment(String taskId,
                                             String peerId,
                                             long startedAt,
                                             String leaseOwnerId,
                                             long leaseExpiresAt,
                                             int attemptNumber,
                                             String assignmentId) {
            events.add("markTaskAssigned:" + taskId + ":" + peerId);
            if (succeeds("markTaskAssigned")) {
                lastLeaseOwnerId = leaseOwnerId == null ? "" : leaseOwnerId;
                lastLeaseExpiresAt = leaseExpiresAt;
                String jobId = taskJobIds.getOrDefault(taskId, "");
                Deque<TaskAttemptRecord> attempts = attemptsByJobId.computeIfAbsent(jobId, ignored -> new ArrayDeque<>());
                int persistedAttemptNumber = attemptNumber > 0
                        ? attemptNumber
                        : attemptsForTask(attempts, taskId) + 1;
                attempts.addLast(new TaskAttemptRecord(
                        jobId,
                        taskId,
                        persistedAttemptNumber,
                        assignmentId,
                        peerId,
                        startedAt,
                        leaseExpiresAt,
                        0L,
                        0L,
                        TaskAttemptOutcome.RUNNING,
                        ""
                ));
            }
            taskAssigned.countDown();
            return succeeds("markTaskAssigned");
        }

        @Override
        public synchronized DurableTransitionOutcome commitTaskAssignment(String taskId,
                                                                           String peerId,
                                                                           long startedAt,
                                                                           String leaseOwnerId,
                                                                           long leaseExpiresAt,
                                                                           int attemptNumber,
                                                                           String assignmentId) {
            if (failTransitionOnce("commitTaskAssignment")) {
                return DurableTransitionOutcome.STORAGE_FAILURE;
            }
            return JobStateStore.super.commitTaskAssignment(
                    taskId,
                    peerId,
                    startedAt,
                    leaseOwnerId,
                    leaseExpiresAt,
                    attemptNumber,
                    assignmentId
            );
        }

        @Override
        public synchronized boolean markTaskCompleted(String taskId, long completedAt, long durationMs) {
            events.add("markTaskCompleted:" + taskId);
            if (succeeds("markTaskCompleted")) {
                finishAttempt(
                        taskId,
                        completedAt,
                        Math.max(0L, durationMs),
                        TaskAttemptOutcome.SUCCEEDED,
                        ""
                );
            }
            return succeeds("markTaskCompleted");
        }

        @Override
        public synchronized ResultCommitOutcome commitTaskResult(String taskId,
                                                                 int attemptNumber,
                                                                 String assignmentId,
                                                                 String assignedPeerId,
                                                                 long completedAt,
                                                                 long durationMs,
                                                                 Object resultPayload) {
            events.add("markTaskCompleted:" + taskId);
            if (!succeeds("markTaskCompleted")) {
                return ResultCommitOutcome.STORAGE_FAILURE;
            }
            String jobId = taskJobIds.get(taskId);
            if (jobId == null) {
                return ResultCommitOutcome.UNKNOWN_TASK;
            }
            Deque<TaskAttemptRecord> attempts = attemptsByJobId.get(jobId);
            if (attempts == null) {
                return ResultCommitOutcome.STALE_ASSIGNMENT;
            }
            for (TaskAttemptRecord attempt : attempts) {
                if (sameAssignment(attempt, taskId, attemptNumber, assignmentId, assignedPeerId)
                        && attempt.outcome() == TaskAttemptOutcome.SUCCEEDED) {
                    return ResultCommitOutcome.DUPLICATE_ALREADY_COMPLETED;
                }
            }
            if (!finishExactAttempt(
                    attempts,
                    taskId,
                    attemptNumber,
                    assignmentId,
                    assignedPeerId,
                    completedAt,
                    Math.max(0L, durationMs)
            )) {
                return ResultCommitOutcome.STALE_ASSIGNMENT;
            }
            return ResultCommitOutcome.COMMITTED;
        }

        @Override
        public synchronized boolean markTaskRetried(String taskId, int retryCount) {
            events.add("markTaskRetried:" + taskId + ":" + retryCount);
            return succeeds("markTaskRetried");
        }

        @Override
        public synchronized boolean markTaskRetried(String taskId,
                                                   int retryCount,
                                                   TaskAttemptOutcome outcome,
                                                   String failureReason,
                                                   long finishedAt) {
            if (succeeds("markTaskRetried")) {
                finishAttempt(
                        taskId,
                        finishedAt,
                        0L,
                        outcome == null ? TaskAttemptOutcome.RETRY_SCHEDULED : outcome,
                        failureReason
                );
            }
            return markTaskRetried(taskId, retryCount);
        }

        @Override
        public synchronized boolean markTaskFailed(String taskId) {
            events.add("markTaskFailed:" + taskId);
            if (succeeds("markTaskFailed")) {
                finishAttempt(
                        taskId,
                        System.currentTimeMillis(),
                        0L,
                        TaskAttemptOutcome.TERMINAL_FAILURE,
                        ""
                );
            }
            return succeeds("markTaskFailed");
        }

        @Override
        public synchronized boolean markTaskFailed(String taskId,
                                                  TaskAttemptOutcome outcome,
                                                  String failureReason,
                                                  long finishedAt) {
            if (succeeds("markTaskFailed")) {
                finishAttempt(
                        taskId,
                        finishedAt,
                        0L,
                        outcome == null ? TaskAttemptOutcome.TERMINAL_FAILURE : outcome,
                        failureReason
                );
            }
            return markTaskFailed(taskId);
        }

        @Override
        public synchronized DurableTransitionOutcome commitAssignedTaskFailure(
                String taskId,
                int attemptNumber,
                String assignmentId,
                String assignedPeerId,
                int retryCount,
                TaskAttemptOutcome outcome,
                String failureReason,
                long finishedAt) {
            if (failTransitionOnce("commitAssignedTaskFailure")) {
                return DurableTransitionOutcome.STORAGE_FAILURE;
            }
            return JobStateStore.super.commitAssignedTaskFailure(
                    taskId,
                    attemptNumber,
                    assignmentId,
                    assignedPeerId,
                    retryCount,
                    outcome,
                    failureReason,
                    finishedAt
            );
        }

        @Override
        public synchronized boolean markJobCompleted(String jobId) {
            events.add("markJobCompleted:" + jobId);
            if (succeeds("markJobCompleted")) {
                jobCompleted.countDown();
                return true;
            }
            return false;
        }

        @Override
        public synchronized boolean markJobCompleted(String jobId, Object resultPayload) {
            lastCompletedResultPayload = resultPayload;
            return markJobCompleted(jobId);
        }

        @Override
        public synchronized boolean markJobFailed(String jobId) {
            events.add("markJobFailed:" + jobId);
            if (succeeds("markJobFailed")) {
                jobFailed.countDown();
                return true;
            }
            return false;
        }

        @Override
        public synchronized DurableTransitionOutcome commitJobCompleted(String jobId,
                                                                         Object resultPayload,
                                                                         long completedAt) {
            if (failTransitionOnce("commitJobCompleted")) {
                return DurableTransitionOutcome.STORAGE_FAILURE;
            }
            return JobStateStore.super.commitJobCompleted(jobId, resultPayload, completedAt);
        }

        @Override
        public synchronized DurableTransitionOutcome commitJobFailed(
                String jobId,
                Collection<JobStateStore.TaskFailureUpdate> taskFailures,
                long completedAt) {
            if (failTransitionOnce("commitJobFailed")) {
                return DurableTransitionOutcome.STORAGE_FAILURE;
            }
            return JobStateStore.super.commitJobFailed(jobId, taskFailures, completedAt);
        }

        @Override
        public int markRunningJobsFailedOnStartup(long completedAt) {
            return 0;
        }

        @Override
        public synchronized Optional<JobStateStore.CompletedJobResultState> loadCompletedJobResult(String jobId) {
            events.add("loadCompletedJobResult:" + jobId);
            if (completedResult != null && completedResult.jobId().equals(jobId)) {
                return Optional.of(completedResult);
            }
            return Optional.empty();
        }

        @Override
        public synchronized List<TaskAttemptRecord> loadTaskAttempts(String jobId) {
            return List.copyOf(attemptsByJobId.getOrDefault(jobId, new ArrayDeque<>()));
        }

        boolean awaitTaskAssigned() throws InterruptedException {
            return taskAssigned.await(2, TimeUnit.SECONDS);
        }

        boolean awaitJobCompleted() throws InterruptedException {
            return jobCompleted.await(2, TimeUnit.SECONDS);
        }

        boolean awaitJobFailed() throws InterruptedException {
            return jobFailed.await(2, TimeUnit.SECONDS);
        }

        synchronized List<String> events() {
            return List.copyOf(events);
        }

        synchronized String lastRequesterIdentityKey() {
            return lastRequesterIdentityKey;
        }

        synchronized Object lastCompletedResultPayload() {
            return lastCompletedResultPayload;
        }

        synchronized String lastLeaseOwnerId() {
            return lastLeaseOwnerId;
        }

        synchronized long lastLeaseExpiresAt() {
            return lastLeaseExpiresAt;
        }

        synchronized void setCompletedResult(JobStateStore.CompletedJobResultState completedResult) {
            this.completedResult = completedResult;
        }

        synchronized void addExistingJobId(String jobId) {
            existingJobIds.add(jobId);
        }

        synchronized void failNextTransition(String operation) {
            failOnceTransitions.add(operation);
        }

        synchronized int transitionAttempts(String operation) {
            return transitionAttempts.getOrDefault(operation, 0);
        }

        private boolean failTransitionOnce(String operation) {
            transitionAttempts.merge(operation, 1, Integer::sum);
            notifyAll();
            return failOnceTransitions.remove(operation);
        }

        private boolean succeeds(String operation) {
            return !operation.equals(failingOperation);
        }

        private static int attemptsForTask(Deque<TaskAttemptRecord> attempts, String taskId) {
            return (int) attempts.stream()
                    .filter(attempt -> attempt.taskId().equals(taskId))
                    .count();
        }

        private void finishAttempt(String taskId,
                                   long finishedAt,
                                   long durationMs,
                                   TaskAttemptOutcome outcome,
                                   String failureReason) {
            String jobId = taskJobIds.getOrDefault(taskId, "");
            Deque<TaskAttemptRecord> attempts = attemptsByJobId.get(jobId);
            if (attempts == null || attempts.isEmpty()) {
                return;
            }
            List<TaskAttemptRecord> updated = new ArrayList<>();
            boolean replaced = false;
            for (TaskAttemptRecord attempt : attempts) {
                if (!replaced
                        && attempt.taskId().equals(taskId)
                        && attempt.outcome() == TaskAttemptOutcome.RUNNING) {
                    updated.add(new TaskAttemptRecord(
                            attempt.jobId(),
                            attempt.taskId(),
                            attempt.attemptNumber(),
                            attempt.assignmentId(),
                            attempt.peerId(),
                            attempt.startedAt(),
                            attempt.leaseExpiresAt(),
                            finishedAt,
                            durationMs,
                            outcome,
                            failureReason == null ? "" : failureReason
                    ));
                    replaced = true;
                } else {
                    updated.add(attempt);
                }
            }
            attempts.clear();
            attempts.addAll(updated);
        }

        private static boolean finishExactAttempt(Deque<TaskAttemptRecord> attempts,
                                                  String taskId,
                                                  int attemptNumber,
                                                  String assignmentId,
                                                  String assignedPeerId,
                                                  long finishedAt,
                                                  long durationMs) {
            List<TaskAttemptRecord> updated = new ArrayList<>();
            boolean replaced = false;
            for (TaskAttemptRecord attempt : attempts) {
                if (!replaced
                        && sameAssignment(attempt, taskId, attemptNumber, assignmentId, assignedPeerId)
                        && attempt.outcome() == TaskAttemptOutcome.RUNNING) {
                    updated.add(new TaskAttemptRecord(
                            attempt.jobId(),
                            attempt.taskId(),
                            attempt.attemptNumber(),
                            attempt.assignmentId(),
                            attempt.peerId(),
                            attempt.startedAt(),
                            attempt.leaseExpiresAt(),
                            finishedAt,
                            durationMs,
                            TaskAttemptOutcome.SUCCEEDED,
                            ""
                    ));
                    replaced = true;
                } else {
                    updated.add(attempt);
                }
            }
            if (replaced) {
                attempts.clear();
                attempts.addAll(updated);
            }
            return replaced;
        }

        private static boolean sameAssignment(TaskAttemptRecord attempt,
                                              String taskId,
                                              int attemptNumber,
                                              String assignmentId,
                                              String assignedPeerId) {
            return attempt.taskId().equals(taskId)
                    && attempt.attemptNumber() == attemptNumber
                    && java.util.Objects.equals(attempt.assignmentId(), assignmentId)
                    && attempt.peerId().equals(assignedPeerId);
        }

        private boolean awaitEvent(String expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline) {
                synchronized (this) {
                    if (events.contains(expected)) {
                        return true;
                    }
                }
                Thread.sleep(10);
            }
            synchronized (this) {
                return events.contains(expected);
            }
        }
    }

    private static class OutboxRecordingJobStateStore
            extends RecordingJobStateStore
            implements BrokerOutboxStore {
        private final List<OutboxRecord> outboxRecords = new ArrayList<>();
        private final List<Long> failedOutboxIds = new ArrayList<>();
        private final List<Long> publishedOutboxIds = new ArrayList<>();
        private long nextOutboxId = 1L;
        private boolean failNextAssignmentOutbox;
        private int assignmentOutboxAttempts;
        private boolean failNextFinalOutbox;
        private int finalOutboxAttempts;
        private boolean failPendingOutboxCount;
        private boolean throwPendingOutboxCount;
        private int pendingOutboxCountReads;

        @Override
        public synchronized Optional<OutboxRecord> enqueueBrokerOutbox(OutboxMessage message) {
            OutboxRecord record = new OutboxRecord(nextOutboxId++, message, System.currentTimeMillis(), 0, 0L, "");
            outboxRecords.add(record);
            return Optional.of(record);
        }

        @Override
        public synchronized Optional<CommittedTaskAssignment> createTaskAssignmentAndEnqueueBrokerOutbox(
                String taskId,
                String peerId,
                long startedAt,
                String leaseOwnerId,
                long leaseExpiresAt,
                String assignmentId,
                OutboxMessage messageTemplate) {
            int attemptNumber = (int) outboxRecords.stream()
                    .map(OutboxRecord::message)
                    .map(OutboxMessage::message)
                    .filter(TaskAssignMessage.class::isInstance)
                    .map(TaskAssignMessage.class::cast)
                    .filter(message -> taskId.equals(message.getTaskId()))
                    .count() + 1;
            AssignmentIdentity identity = new AssignmentIdentity(
                    taskId,
                    attemptNumber,
                    assignmentId,
                    peerId,
                    leaseExpiresAt
            );
            TaskAssignMessage message = ((TaskAssignMessage) messageTemplate.message())
                    .withAssignmentIdentity(
                            identity.attemptNumber(),
                            identity.assignmentId(),
                            identity.leaseExpiresAtEpochMillis()
                    );
            OutboxMessage committedMessage = new OutboxMessage(
                    messageTemplate.route(),
                    messageTemplate.peerNodeId(),
                    messageTemplate.fromNodeId(),
                    message
            );
            if (!markTaskAssigned(
                    taskId,
                    peerId,
                    startedAt,
                    leaseOwnerId,
                    leaseExpiresAt,
                    attemptNumber,
                    identity.assignmentId()
            )) {
                return Optional.empty();
            }
            OutboxRecord record = new OutboxRecord(
                    nextOutboxId++,
                    committedMessage,
                    startedAt,
                    0,
                    0L,
                    ""
            );
            outboxRecords.add(record);
            return Optional.of(new CommittedTaskAssignment(identity, record));
        }

        @Override
        public synchronized TaskAssignmentCommit commitTaskAssignmentAndEnqueueBrokerOutbox(
                String taskId,
                String peerId,
                long startedAt,
                String leaseOwnerId,
                long leaseExpiresAt,
                String assignmentId,
            OutboxMessage messageTemplate) {
            assignmentOutboxAttempts++;
            notifyAll();
            if (failNextAssignmentOutbox) {
                failNextAssignmentOutbox = false;
                return new TaskAssignmentCommit(DurableTransitionOutcome.STORAGE_FAILURE, null);
            }
            return BrokerOutboxStore.super.commitTaskAssignmentAndEnqueueBrokerOutbox(
                    taskId,
                    peerId,
                    startedAt,
                    leaseOwnerId,
                    leaseExpiresAt,
                    assignmentId,
                    messageTemplate
            );
        }

        @Override
        public synchronized Optional<OutboxRecord> markJobCompletedAndEnqueueBrokerOutbox(String jobId,
                                                                                         Object resultPayload,
                                                                                         OutboxMessage message) {
            if (!markJobCompleted(jobId, resultPayload)) {
                return Optional.empty();
            }
            return enqueueBrokerOutbox(message);
        }

        @Override
        public synchronized Optional<OutboxRecord> markJobFailedAndEnqueueBrokerOutbox(String jobId,
                                                                                      Collection<BrokerOutboxStore.TaskFailureUpdate> taskFailures,
                                                                                      OutboxMessage message) {
            Collection<BrokerOutboxStore.TaskFailureUpdate> updates = taskFailures == null
                    ? List.of()
                    : taskFailures;
            for (BrokerOutboxStore.TaskFailureUpdate update : updates) {
                if (!markTaskFailed(update.taskId(), update.outcome(), update.failureReason(), update.finishedAt())) {
                    return Optional.empty();
                }
            }
            if (!markJobFailed(jobId)) {
                return Optional.empty();
            }
            return enqueueBrokerOutbox(message);
        }

        @Override
        public synchronized OutboxCommit commitJobCompletedAndEnqueueBrokerOutbox(
                String jobId,
                Object resultPayload,
                OutboxMessage message) {
            finalOutboxAttempts++;
            notifyAll();
            if (failNextFinalOutbox) {
                failNextFinalOutbox = false;
                return new OutboxCommit(DurableTransitionOutcome.STORAGE_FAILURE, null);
            }
            return BrokerOutboxStore.super.commitJobCompletedAndEnqueueBrokerOutbox(
                    jobId,
                    resultPayload,
                    message
            );
        }

        @Override
        public synchronized OutboxCommit commitJobFailedAndEnqueueBrokerOutbox(
                String jobId,
                Collection<BrokerOutboxStore.TaskFailureUpdate> taskFailures,
                OutboxMessage message) {
            finalOutboxAttempts++;
            notifyAll();
            if (failNextFinalOutbox) {
                failNextFinalOutbox = false;
                return new OutboxCommit(DurableTransitionOutcome.STORAGE_FAILURE, null);
            }
            return BrokerOutboxStore.super.commitJobFailedAndEnqueueBrokerOutbox(
                    jobId,
                    taskFailures,
                    message
            );
        }

        @Override
        public synchronized List<OutboxRecord> loadPendingBrokerOutbox(int limit) {
            return outboxRecords.stream()
                    .filter(record -> !publishedOutboxIds.contains(record.outboxId()))
                    .limit(limit)
                    .toList();
        }

        @Override
        public synchronized PendingOutboxCount countPendingBrokerOutbox() {
            pendingOutboxCountReads++;
            if (throwPendingOutboxCount) {
                throw new IllegalStateException("injected pending-outbox count failure");
            }
            if (failPendingOutboxCount) {
                return PendingOutboxCount.storageFailure();
            }
            return PendingOutboxCount.counted(outboxRecords.stream()
                    .filter(record -> !publishedOutboxIds.contains(record.outboxId()))
                    .count());
        }

        @Override
        public synchronized boolean markBrokerOutboxPublished(long outboxId, long publishedAt) {
            publishedOutboxIds.add(outboxId);
            return true;
        }

        @Override
        public synchronized boolean markBrokerOutboxPublishFailed(long outboxId, String error, long attemptedAt) {
            failedOutboxIds.add(outboxId);
            return true;
        }

        synchronized List<OutboxRecord> allOutboxRecords() {
            return List.copyOf(outboxRecords);
        }

        synchronized void failPendingOutboxCount() {
            failPendingOutboxCount = true;
        }

        synchronized void throwPendingOutboxCount() {
            throwPendingOutboxCount = true;
        }

        synchronized int pendingOutboxCountReads() {
            return pendingOutboxCountReads;
        }

        synchronized List<Long> failedOutboxIds() {
            return List.copyOf(failedOutboxIds);
        }

        synchronized List<Long> publishedOutboxIds() {
            return List.copyOf(publishedOutboxIds);
        }

        synchronized void failNextFinalOutbox() {
            failNextFinalOutbox = true;
        }

        synchronized void failNextAssignmentOutbox() {
            failNextAssignmentOutbox = true;
        }

        synchronized int assignmentOutboxAttempts() {
            return assignmentOutboxAttempts;
        }

        synchronized int finalOutboxAttempts() {
            return finalOutboxAttempts;
        }
    }

    private static final class MutableClock implements TaskFlowClock {
        private long epochMillis;

        private MutableClock(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        private synchronized void advanceMillis(long deltaMillis) {
            epochMillis = Math.addExact(epochMillis, deltaMillis);
        }

        @Override
        public synchronized Instant now() {
            return Instant.ofEpochMilli(epochMillis);
        }

        @Override
        public synchronized long nowEpochMillis() {
            return epochMillis;
        }
    }
}
