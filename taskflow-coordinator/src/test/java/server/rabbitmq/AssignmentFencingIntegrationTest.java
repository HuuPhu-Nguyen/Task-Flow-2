package server.rabbitmq;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMetrics;
import server.scheduler.SchedulerOutput;
import server.scheduler.TaskScheduler;
import transport.TransportAcknowledgement;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssignmentFencingIntegrationTest {
    private static final String PEER_ID = "peer-aba";

    @TempDir
    Path tempDir;

    @Test
    void sameWorkerAbaResultCannotCommit() throws Exception {
        String jobId = "job-same-worker-aba";
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        SchedulerConfig config = SchedulerConfig.defaults();

        try (DatabaseManager db = new DatabaseManager(tempDir.resolve("same-worker-aba.db").toString())) {
            InMemoryPeerRegistry registry = new InMemoryPeerRegistry(db);
            PeerInfo peer = new PeerInfo(PEER_ID, config, List.of(TestTaskPlugin.TASK_TYPE));
            registry.register(PEER_ID, peer);
            CapturingOutput output = new CapturingOutput();
            TaskScheduler scheduler = new TaskScheduler(mailbox, registry, db, output, config);
            Logger schedulerLogger = (Logger) LoggerFactory.getLogger(TaskScheduler.class);
            Level previousLogLevel = schedulerLogger.getLevel();
            boolean previousAdditive = schedulerLogger.isAdditive();
            schedulerLogger.setLevel(Level.INFO);
            schedulerLogger.setAdditive(false);
            ThreadSafeListAppender logAppender = new ThreadSafeListAppender();
            logAppender.start();
            schedulerLogger.addAppender(logAppender);
            Thread schedulerThread = new Thread(scheduler, "same-worker-aba-integration-test-scheduler");
            schedulerThread.start();

            try {
                mailbox.put(new MessageEnvelope(jobSubmit(jobId), PEER_ID));
                TaskAssignMessage first = output.awaitAssignment("attempt 1 / assignment X");
                assertEquals(1, first.getAttemptNumber());
                assertTraceEvent(logAppender, "task_assignment_created", first);

                RecordingAcknowledgement failedAttemptAcknowledgement = new RecordingAcknowledgement();
                mailbox.put(new MessageEnvelope(
                        failedResult(first, "attempt 1 deliberately failed"),
                        PEER_ID,
                        failedAttemptAcknowledgement
                ));
                failedAttemptAcknowledgement.awaitAck("attempt 1 failure");

                TaskAssignMessage current = output.awaitAssignment("attempt 2 / assignment Y");
                assertEquals(first.getTaskId(), current.getTaskId());
                assertEquals(2, current.getAttemptNumber());
                assertNotEquals(first.getAssignmentId(), current.getAssignmentId());
                assertTraceEvent(logAppender, "task_assignment_created", current);

                DatabaseManager.TaskRecord beforeStale = persistedTask(db, jobId);
                SchedulerMetrics.Snapshot metricsBeforeStale = scheduler.getMetricsSnapshot();
                int activeTasksBeforeStale = peer.getActiveTasks();
                long completedTasksBeforeStale = peer.getCompletedTasks();
                long failedTasksBeforeStale = peer.getFailedTasks();
                assertCurrentAssignment(beforeStale, current);
                assertNull(runningTask(db, jobId).resultPayload());

                RecordingAcknowledgement staleAcknowledgement = new RecordingAcknowledgement();
                mailbox.put(new MessageEnvelope(
                        successfulResult(first, "obsolete-result"),
                        PEER_ID,
                        staleAcknowledgement
                ));
                staleAcknowledgement.awaitAck("obsolete attempt 1 result");

                DatabaseManager.TaskRecord afterStale = persistedTask(db, jobId);
                assertCurrentAssignment(afterStale, current);
                assertEquals(beforeStale.startedAt(), afterStale.startedAt());
                assertEquals(beforeStale.leaseOwnerId(), afterStale.leaseOwnerId());
                assertEquals(beforeStale.leaseExpiresAt(), afterStale.leaseExpiresAt());
                assertEquals(beforeStale.completedAt(), afterStale.completedAt());
                assertEquals(beforeStale.durationMs(), afterStale.durationMs());
                assertNull(runningTask(db, jobId).resultPayload());
                assertEquals("RUNNING", persistedJob(db, jobId).status());
                assertNull(output.pollResult(250), "Obsolete result completed the job.");

                SchedulerMetrics.Snapshot metricsAfterStale = scheduler.getMetricsSnapshot();
                assertEquals(metricsBeforeStale.successCount(), metricsAfterStale.successCount());
                assertEquals(metricsBeforeStale.failureCount(), metricsAfterStale.failureCount());
                assertEquals(metricsBeforeStale.retryCount(), metricsAfterStale.retryCount());
                assertEquals(metricsBeforeStale.staleResultCount() + 1, metricsAfterStale.staleResultCount());
                assertEquals(
                        metricsBeforeStale.taskResultsStaleTotal() + 1,
                        metricsAfterStale.taskResultsStaleTotal()
                );
                assertTraceEvent(logAppender, "task_result_stale_rejected", first);
                assertNoTraceEvent(logAppender, "task_result_duplicate_ignored", first);
                assertEquals(activeTasksBeforeStale, peer.getActiveTasks());
                assertEquals(completedTasksBeforeStale, peer.getCompletedTasks());
                assertEquals(failedTasksBeforeStale, peer.getFailedTasks());

                List<JobStateStore.TaskAttemptRecord> attemptsAfterStale = db.loadTaskAttempts(jobId);
                assertEquals(2, attemptsAfterStale.size());
                assertEquals(first.getAttemptNumber(), attemptsAfterStale.getFirst().attemptNumber());
                assertEquals(first.getAssignmentId(), attemptsAfterStale.getFirst().assignmentId());
                assertEquals(PEER_ID, attemptsAfterStale.getFirst().peerId());
                assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                        attemptsAfterStale.getFirst().outcome());
                assertEquals(current.getAttemptNumber(), attemptsAfterStale.get(1).attemptNumber());
                assertEquals(current.getAssignmentId(), attemptsAfterStale.get(1).assignmentId());
                assertEquals(PEER_ID, attemptsAfterStale.get(1).peerId());
                assertEquals(JobStateStore.TaskAttemptOutcome.RUNNING, attemptsAfterStale.get(1).outcome());

                RecordingAcknowledgement currentAcknowledgement = new RecordingAcknowledgement();
                mailbox.put(new MessageEnvelope(
                        successfulResult(current, "current-result"),
                        PEER_ID,
                        currentAcknowledgement
                ));
                JobResultMessage finalResult = output.awaitResult("final result from assignment Y");
                currentAcknowledgement.awaitAck("current attempt 2 result");

                assertTrue(finalResult.isSuccessful());
                assertEquals(List.of("current-result"), finalResult.getResultsByTaskId());
                assertNull(output.pollResult(250), "A second final result was produced.");
                DatabaseManager.TaskRecord completed = persistedTask(db, jobId);
                assertEquals("COMPLETED", completed.status());
                assertEquals(2, completed.attemptNumber());
                assertEquals(current.getAssignmentId(), completed.assignmentId());
                assertEquals("COMPLETED", persistedJob(db, jobId).status());

                List<JobStateStore.TaskAttemptRecord> finalAttempts = db.loadTaskAttempts(jobId);
                assertEquals(2, finalAttempts.size());
                assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                        finalAttempts.getFirst().outcome());
                assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, finalAttempts.get(1).outcome());
                assertEquals(1, finalAttempts.stream()
                        .filter(attempt -> attempt.outcome() == JobStateStore.TaskAttemptOutcome.SUCCEEDED)
                        .count());
                assertEquals(
                        List.of("current-result"),
                        db.loadCompletedJobResult(jobId).orElseThrow().resultsByTaskId()
                );
                assertEquals(1, scheduler.getMetricsSnapshot().successCount());
                assertEquals(1, scheduler.getMetricsSnapshot().staleResultCount());
                assertEquals(1, scheduler.getMetricsSnapshot().taskResultsCommittedTotal());
                assertEquals(1, scheduler.getMetricsSnapshot().taskResultsStaleTotal());
                assertEquals(0, scheduler.getMetricsSnapshot().taskResultsDuplicateTotal());
                assertEquals(2, scheduler.getMetricsSnapshot().assignmentGenerationsTotal());
                assertTraceEvent(logAppender, "task_result_committed", current);
                assertEquals(0, peer.getActiveTasks());
                assertEquals(1L, peer.getCompletedTasks());
            } finally {
                schedulerThread.interrupt();
                schedulerThread.join(2_000);
                schedulerLogger.detachAppender(logAppender);
                logAppender.stop();
                schedulerLogger.setLevel(previousLogLevel);
                schedulerLogger.setAdditive(previousAdditive);
            }
        }
    }

    private static void assertTraceEvent(ThreadSafeListAppender appender,
                                         String eventName,
                                         TaskAssignMessage assignment) {
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
        assertTrue(event.contains("worker_id=" + PEER_ID));
        assertTrue(event.contains(" timestamp="));
        assertTrue(event.contains(" coordinator_instance_id=COORDINATOR_"));
        assertTrue(event.contains(" outcome="));
        assertTrue(event.contains(" failure_reason_code="));
    }

    private static void assertNoTraceEvent(ThreadSafeListAppender appender,
                                           String eventName,
                                           TaskAssignMessage assignment) {
        assertTrue(appender.events().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .noneMatch(message -> message.contains("event=" + eventName)
                        && message.contains("assignment_id=" + assignment.getAssignmentId())));
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

    private static JobSubmitMessage jobSubmit(String jobId) {
        return new JobSubmitMessage(
                PEER_ID,
                Instant.now().toString(),
                jobId,
                TestTaskPlugin.TASK_TYPE,
                List.of("alpha"),
                "",
                "token-" + jobId
        );
    }

    private static TaskResultMessage failedResult(TaskAssignMessage assignment, String reason) {
        return new TaskResultMessage(
                PEER_ID,
                Instant.now().toString(),
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                null,
                false,
                reason
        );
    }

    private static TaskResultMessage successfulResult(TaskAssignMessage assignment, Object payload) {
        return new TaskResultMessage(
                PEER_ID,
                Instant.now().toString(),
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                payload,
                true,
                null
        );
    }

    private static DatabaseManager.TaskRecord persistedTask(DatabaseManager db, String jobId) {
        List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob(jobId);
        assertEquals(1, tasks.size());
        return tasks.getFirst();
    }

    private static DatabaseManager.JobRecord persistedJob(DatabaseManager db, String jobId) {
        return db.getJobHistory().stream()
                .filter(job -> jobId.equals(job.jobId()))
                .findFirst()
                .orElseThrow();
    }

    private static JobStateStore.ResumableTaskState runningTask(DatabaseManager db, String jobId) {
        return db.loadRunningJobsForResume().stream()
                .filter(job -> jobId.equals(job.jobId()))
                .findFirst()
                .orElseThrow()
                .tasks()
                .getFirst();
    }

    private static void assertCurrentAssignment(DatabaseManager.TaskRecord task,
                                                TaskAssignMessage assignment) {
        assertEquals("ASSIGNED", task.status());
        assertEquals(PEER_ID, task.assignedPeerId());
        assertEquals(assignment.getAttemptNumber(), task.attemptNumber());
        assertEquals(assignment.getAssignmentId(), task.assignmentId());
        assertTrue(task.leaseExpiresAt() > 0L);
    }

    private static class CapturingOutput implements SchedulerOutput {
        private final BlockingQueue<TaskAssignMessage> assignments = new LinkedBlockingQueue<>();
        private final BlockingQueue<JobResultMessage> results = new LinkedBlockingQueue<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            assignments.add(message);
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            results.add(message);
            return true;
        }

        private TaskAssignMessage awaitAssignment(String description) throws InterruptedException {
            TaskAssignMessage assignment = assignments.poll(5, TimeUnit.SECONDS);
            assertNotNull(assignment, "Timed out waiting for " + description);
            return assignment;
        }

        private JobResultMessage awaitResult(String description) throws InterruptedException {
            JobResultMessage result = results.poll(5, TimeUnit.SECONDS);
            assertNotNull(result, "Timed out waiting for " + description);
            return result;
        }

        private JobResultMessage pollResult(long timeoutMillis) throws InterruptedException {
            return results.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    private static class RecordingAcknowledgement implements TransportAcknowledgement {
        private final AtomicInteger ackCount = new AtomicInteger();
        private final AtomicInteger requeueCount = new AtomicInteger();
        private final AtomicInteger rejectCount = new AtomicInteger();
        private final CountDownLatch settled = new CountDownLatch(1);

        @Override
        public void ack() {
            ackCount.incrementAndGet();
            settled.countDown();
        }

        @Override
        public void requeue() {
            requeueCount.incrementAndGet();
            settled.countDown();
        }

        @Override
        public void reject() {
            rejectCount.incrementAndGet();
            settled.countDown();
        }

        private void awaitAck(String description) throws InterruptedException {
            assertTrue(settled.await(5, TimeUnit.SECONDS), "Timed out waiting for " + description);
            assertEquals(1, ackCount.get(), description + " was not acknowledged exactly once.");
            assertEquals(0, requeueCount.get(), description + " was unexpectedly requeued.");
            assertEquals(0, rejectCount.get(), description + " was unexpectedly rejected.");
        }
    }
}
