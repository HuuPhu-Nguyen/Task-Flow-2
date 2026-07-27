package server.demo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.AssignmentIdGenerator;
import server.runtime.TaskFlowClock;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMetrics;
import server.scheduler.SchedulerOutput;
import server.scheduler.TaskScheduler;
import transport.DeliveryDisposition;
import transport.TransportAcknowledgement;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One deterministic operator-facing proof of lease-expiry assignment fencing.
 *
 * <p>The PowerShell wrapper selects this test and prints only the seven
 * {@code TF0604 TRACE} lines. Assertions bind every line to the actual
 * scheduler, SQLite, settlement, and terminal-result state.</p>
 */
class StaleResultTraceDemoTest {
    private static final String REQUESTER_ID = "requester-demo";
    private static final String EXECUTOR_ID = "executor-a";
    private static final String JOB_ID = "job-fencing-demo";
    private static final String TASK_ID = "task-job-fencing-demo-0";
    private static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    private static final String ASSIGNMENT_X =
            "00000000-0000-0000-0000-000000000601";
    private static final String ASSIGNMENT_Y =
            "00000000-0000-0000-0000-000000000602";
    private static final long STARTED_AT = 1_767_225_600_000L;
    private static final long LEASE_MILLIS = 1_000L;

    @TempDir
    Path tempDir;

    @Test
    void printsAndAssertsLeaseExpiryStaleFenceAndCurrentCommitTrace()
            throws Exception {
        MutableClock clock = new MutableClock(STARTED_AT);
        DeterministicIds assignmentIds =
                new DeterministicIds(ASSIGNMENT_X, ASSIGNMENT_Y);
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_TASK_TIMEOUT_MS", "60000",
                "TASKFLOW_TASK_LEASE_MS", Long.toString(LEASE_MILLIS),
                "TASKFLOW_MAX_TASK_RETRIES", "2"
        ));
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        CapturingOutput output = new CapturingOutput();

        try (DatabaseManager database = new DatabaseManager(
                tempDir.resolve("tf0604-stale-result-demo.db").toString()
        )) {
            InMemoryPeerRegistry registry = new InMemoryPeerRegistry(database);
            PeerInfo executor = new PeerInfo(
                    EXECUTOR_ID,
                    config,
                    List.of(TASK_TYPE)
            );
            registry.register(EXECUTOR_ID, executor);
            TaskScheduler scheduler = new TaskScheduler(
                    mailbox,
                    registry,
                    database,
                    output,
                    config,
                    clock,
                    assignmentIds,
                    "COORDINATOR_tf0604_demo"
            );
            Thread schedulerThread = new Thread(
                    scheduler,
                    "tf0604-stale-result-demo-scheduler"
            );
            schedulerThread.start();

            try {
                mailbox.put(new MessageEnvelope(submission(), REQUESTER_ID));
                TaskAssignMessage first = output.awaitAssignment("assignment X");
                assertAssignment(first, 1, ASSIGNMENT_X, STARTED_AT + LEASE_MILLIS);
                assertEquals("RUNNING", job(database).status());
                assertCurrentAssignment(database, first);
                trace(
                        1,
                        "SUBMITTED job_id=%s task_id=%s accepted=true"
                                .formatted(JOB_ID, TASK_ID)
                );
                trace(
                        2,
                        "ASSIGNED worker_id=%s attempt_number=1 assignment_id=%s "
                                .formatted(EXECUTOR_ID, ASSIGNMENT_X)
                                + "lease_expires_at_epoch_ms="
                                + first.getLeaseExpiresAtEpochMillis()
                );

                clock.advanceMillis(LEASE_MILLIS);
                scheduler.requestSchedulingRecheck();
                TaskAssignMessage current =
                        output.awaitAssignment("assignment Y after exact lease expiry");
                assertAssignment(
                        current,
                        2,
                        ASSIGNMENT_Y,
                        STARTED_AT + (2L * LEASE_MILLIS)
                );
                List<JobStateStore.TaskAttemptRecord> expiredAttempts =
                        database.loadTaskAttempts(JOB_ID);
                assertEquals(2, expiredAttempts.size());
                JobStateStore.TaskAttemptRecord expired = expiredAttempts.getFirst();
                assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                        expired.outcome());
                assertEquals("lease_expired", expired.failureReason());
                assertEquals(STARTED_AT + LEASE_MILLIS, expired.finishedAt());
                SchedulerMetrics.Snapshot afterExpiry = scheduler.getMetricsSnapshot();
                assertEquals(1L, afterExpiry.taskLeaseExpirationsTotal());
                assertEquals(1L, afterExpiry.retryCount());
                trace(
                        3,
                        "LEASE_EXPIRED attempt_number=1 assignment_id=%s "
                                .formatted(ASSIGNMENT_X)
                                + "at_epoch_ms=" + clock.nowEpochMillis()
                                + " outcome=RETRY_SCHEDULED"
                );
                trace(
                        4,
                        "REASSIGNED worker_id=%s attempt_number=2 assignment_id=%s "
                                .formatted(EXECUTOR_ID, ASSIGNMENT_Y)
                                + "lease_expires_at_epoch_ms="
                                + current.getLeaseExpiresAtEpochMillis()
                );

                SchedulerMetrics.Snapshot beforeStale =
                        scheduler.getMetricsSnapshot();
                RecordingAcknowledgement staleAck =
                        new RecordingAcknowledgement();
                mailbox.put(new MessageEnvelope(
                        successfulResult(first, "obsolete-result"),
                        EXECUTOR_ID,
                        staleAck
                ));
                staleAck.await(
                        DeliveryDisposition.ACK_DUPLICATE_OR_STALE,
                        "duplicate_or_stale_domain_event"
                );
                assertCurrentAssignment(database, current);
                assertNull(output.pollResult());
                SchedulerMetrics.Snapshot afterStale =
                        scheduler.getMetricsSnapshot();
                assertEquals(
                        beforeStale.taskResultsStaleTotal() + 1L,
                        afterStale.taskResultsStaleTotal()
                );
                assertEquals(
                        beforeStale.taskResultsCommittedTotal(),
                        afterStale.taskResultsCommittedTotal()
                );
                assertEquals(
                        beforeStale.taskResultsDuplicateTotal(),
                        afterStale.taskResultsDuplicateTotal()
                );
                assertEquals(beforeStale.successCount(), afterStale.successCount());
                assertEquals(beforeStale.failureCount(), afterStale.failureCount());
                assertEquals(beforeStale.retryCount(), afterStale.retryCount());
                trace(
                        5,
                        "STALE_REJECTED attempt_number=1 assignment_id=%s "
                                .formatted(ASSIGNMENT_X)
                                + "disposition=ACK_DUPLICATE_OR_STALE "
                                + "authoritative_assignment_id=" + ASSIGNMENT_Y
                );

                RecordingAcknowledgement currentAck =
                        new RecordingAcknowledgement();
                mailbox.put(new MessageEnvelope(
                        successfulResult(current, "current-result"),
                        EXECUTOR_ID,
                        currentAck
                ));
                JobResultMessage result = output.awaitResult();
                currentAck.await(
                        DeliveryDisposition.ACK_SUCCESS,
                        "handled"
                );
                assertTrue(result.isSuccessful());
                assertEquals(List.of("current-result"), result.getResultsByTaskId());
                assertEquals("COMPLETED", job(database).status());
                DatabaseManager.TaskRecord completed =
                        database.getTasksForJob(JOB_ID).getFirst();
                assertEquals("COMPLETED", completed.status());
                assertEquals(2, completed.attemptNumber());
                assertEquals(ASSIGNMENT_Y, completed.assignmentId());
                List<JobStateStore.TaskAttemptRecord> finalAttempts =
                        database.loadTaskAttempts(JOB_ID);
                assertEquals(2, finalAttempts.size());
                assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                        finalAttempts.getFirst().outcome());
                assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED,
                        finalAttempts.get(1).outcome());
                assertEquals(
                        List.of("current-result"),
                        database.loadCompletedJobResult(JOB_ID)
                                .orElseThrow()
                                .resultsByTaskId()
                );
                SchedulerMetrics.Snapshot terminal = scheduler.getMetricsSnapshot();
                assertEquals(1L, terminal.taskResultsCommittedTotal());
                assertEquals(1L, terminal.taskResultsStaleTotal());
                assertEquals(1L, terminal.jobsCompletedTotal());
                assertEquals(0L, terminal.jobsFailedTotal());
                assertEquals(2L, terminal.assignmentGenerationsTotal());
                trace(
                        6,
                        "CURRENT_COMMITTED attempt_number=2 assignment_id=%s "
                                .formatted(ASSIGNMENT_Y)
                                + "disposition=ACK_SUCCESS result=current-result"
                );
                trace(
                        7,
                        "COMPLETED job_id=%s authoritative_results=1 "
                                .formatted(JOB_ID)
                                + "stale_results=1 final_result=current-result"
                );
            } finally {
                scheduler.requestShutdownAfterDrain();
                schedulerThread.join(2_000L);
                assertFalse(
                        schedulerThread.isAlive(),
                        "Deterministic demo scheduler did not stop."
                );
            }
        }
    }

    private static JobSubmitMessage submission() {
        return new JobSubmitMessage(
                REQUESTER_ID,
                Instant.ofEpochMilli(STARTED_AT).toString(),
                JOB_ID,
                TASK_TYPE,
                List.of("demo-payload"),
                "",
                "token-" + JOB_ID
        );
    }

    private static TaskResultMessage successfulResult(
            TaskAssignMessage assignment,
            String result
    ) {
        return new TaskResultMessage(
                EXECUTOR_ID,
                Instant.ofEpochMilli(STARTED_AT).toString(),
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                result,
                true,
                null
        );
    }

    private static void assertAssignment(
            TaskAssignMessage assignment,
            int attempt,
            String assignmentId,
            long leaseExpiresAt
    ) {
        assertEquals(JOB_ID, assignment.getJobId());
        assertEquals(TASK_ID, assignment.getTaskId());
        assertEquals(attempt, assignment.getAttemptNumber());
        assertEquals(assignmentId, assignment.getAssignmentId());
        assertEquals(leaseExpiresAt, assignment.getLeaseExpiresAtEpochMillis());
    }

    private static void assertCurrentAssignment(
            DatabaseManager database,
            TaskAssignMessage assignment
    ) {
        List<DatabaseManager.TaskRecord> tasks = database.getTasksForJob(JOB_ID);
        assertEquals(1, tasks.size());
        DatabaseManager.TaskRecord task = tasks.getFirst();
        assertEquals("ASSIGNED", task.status());
        assertEquals(EXECUTOR_ID, task.assignedPeerId());
        assertEquals(assignment.getAttemptNumber(), task.attemptNumber());
        assertEquals(assignment.getAssignmentId(), task.assignmentId());
        assertEquals(
                assignment.getLeaseExpiresAtEpochMillis(),
                task.leaseExpiresAt()
        );
    }

    private static DatabaseManager.JobRecord job(DatabaseManager database) {
        return database.getJobHistory().stream()
                .filter(candidate -> JOB_ID.equals(candidate.jobId()))
                .findFirst()
                .orElseThrow();
    }

    private static void trace(int step, String message) {
        System.out.println("TF0604 TRACE " + step + " " + message);
    }

    private static final class MutableClock implements TaskFlowClock {
        private long epochMillis;

        private MutableClock(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        @Override
        public synchronized Instant now() {
            return Instant.ofEpochMilli(epochMillis);
        }

        @Override
        public synchronized long nowEpochMillis() {
            return epochMillis;
        }

        private synchronized void advanceMillis(long millis) {
            epochMillis += millis;
        }
    }

    private static final class DeterministicIds implements AssignmentIdGenerator {
        private final Deque<String> ids;

        private DeterministicIds(String... ids) {
            this.ids = new ArrayDeque<>(List.of(ids));
        }

        @Override
        public synchronized String nextAssignmentId() {
            if (ids.isEmpty()) {
                throw new IllegalStateException(
                        "No deterministic assignment ID remains."
                );
            }
            return ids.removeFirst();
        }
    }

    private static final class CapturingOutput implements SchedulerOutput {
        private final BlockingQueue<TaskAssignMessage> assignments =
                new LinkedBlockingQueue<>();
        private final BlockingQueue<JobResultMessage> results =
                new LinkedBlockingQueue<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            assertEquals(EXECUTOR_ID, peer.getNodeId());
            assignments.add(message);
        }

        @Override
        public boolean sendJobResult(
                String requesterNodeId,
                JobResultMessage message
        ) {
            assertEquals(REQUESTER_ID, requesterNodeId);
            results.add(message);
            return true;
        }

        private TaskAssignMessage awaitAssignment(String description)
                throws InterruptedException {
            TaskAssignMessage assignment = assignments.poll(5L, TimeUnit.SECONDS);
            assertNotNull(assignment, "Timed out waiting for " + description);
            return assignment;
        }

        private JobResultMessage awaitResult() throws InterruptedException {
            JobResultMessage result = results.poll(5L, TimeUnit.SECONDS);
            assertNotNull(result, "Timed out waiting for terminal job result.");
            return result;
        }

        private JobResultMessage pollResult() {
            return results.poll();
        }
    }

    private static final class RecordingAcknowledgement
            implements TransportAcknowledgement {
        private final CountDownLatch settled = new CountDownLatch(1);
        private final AtomicReference<DeliveryDisposition> disposition =
                new AtomicReference<>();
        private final AtomicReference<String> reason = new AtomicReference<>();

        @Override
        public void settle(
                DeliveryDisposition requestedDisposition,
                String reasonCode
        ) {
            disposition.set(requestedDisposition);
            reason.set(reasonCode);
            settled.countDown();
        }

        @Override
        public void ack() {
            throw new AssertionError("Typed settlement was expected.");
        }

        @Override
        public void requeue() {
            throw new AssertionError("Typed settlement was expected.");
        }

        @Override
        public void reject() {
            throw new AssertionError("Typed settlement was expected.");
        }

        private void await(
                DeliveryDisposition expectedDisposition,
                String expectedReason
        ) throws InterruptedException {
            assertTrue(settled.await(5L, TimeUnit.SECONDS),
                    "Timed out waiting for typed delivery settlement.");
            assertEquals(expectedDisposition, disposition.get());
            assertEquals(expectedReason, reason.get());
        }
    }
}
