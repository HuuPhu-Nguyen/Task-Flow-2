package server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.RequesterIdentity;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.DatabaseManager;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerOutput;
import server.scheduler.TaskScheduler;
import transport.TransportAcknowledgement;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuplicateSubmissionIntegrationTest {
    private static final String JOB_ID = "job-lost-acceptance";
    private static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    private static final String REQUESTER_TOKEN = "durable-owner-token";

    @TempDir
    Path tempDir;

    @Test
    void lostAcceptanceResponseReplaysAcceptedJob() throws Exception {
        Path dbPath = tempDir.resolve("duplicate-submission-restart.db");
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        RecordingOutput firstOutput = new RecordingOutput();

        try (DatabaseManager firstStore = new DatabaseManager(dbPath.toString())) {
            BlockingQueue<MessageEnvelope> firstMailbox = new LinkedBlockingQueue<>();
            TaskScheduler firstScheduler = scheduler(firstMailbox, firstStore, firstOutput);
            Thread firstThread = start(firstScheduler, "first-submission-scheduler");
            try {
                RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
                firstMailbox.put(new MessageEnvelope(
                        signedSubmission(
                                "requester-route-before-restart",
                                "2026-07-23T00:00:00Z",
                                List.of("payload-0", "payload-1"),
                                identity
                        ),
                        "requester-route-before-restart",
                        acknowledgement
                ));

                assertTrue(acknowledgement.awaitAck());
                assertEquals(1, firstStore.getJobHistory().size());
                assertEquals(
                        List.of("task-" + JOB_ID + "-0", "task-" + JOB_ID + "-1"),
                        firstStore.getTasksForJob(JOB_ID).stream().map(DatabaseManager.TaskRecord::taskId).toList()
                );
                assertFalse(firstOutput.hasResult());
            } finally {
                stop(firstThread);
            }
        }

        try (DatabaseManager restartedStore = new DatabaseManager(dbPath.toString())) {
            CoordinatorStartupRecovery.RecoveryResult recovery =
                    CoordinatorStartupRecovery.recoverPersistedJobs(restartedStore);
            assertTrue(recovery.successful());
            assertEquals(1, recovery.resumedJobs().size());

            RecordingOutput replayOutput = new RecordingOutput();
            BlockingQueue<MessageEnvelope> replayMailbox = new LinkedBlockingQueue<>();
            TaskScheduler restartedScheduler = scheduler(replayMailbox, restartedStore, replayOutput);
            restartedScheduler.restoreJobs(
                    recovery.resumedJobs(),
                    recovery.requesterTokenHashes(),
                    recovery.requesterIdentityKeys()
            );
            Thread restartedThread = start(restartedScheduler, "replayed-submission-scheduler");
            try {
                RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
                replayMailbox.put(new MessageEnvelope(
                        signedSubmission(
                                "requester-route-after-restart",
                                "2026-07-23T00:05:00Z",
                                List.of("payload-0", "payload-1"),
                                identity
                        ),
                        "requester-route-after-restart",
                        acknowledgement
                ));

                assertTrue(acknowledgement.awaitAck());
                JobResultMessage replay = replayOutput.awaitResult();
                assertNotNull(replay);
                assertEquals(JOB_ID, replay.getJobId());
                assertEquals(TASK_TYPE, replay.getTaskType());
                assertFalse(replay.isSuccessful());
                assertEquals("Job is still running.", replay.getErrorMessage());
                assertEquals(1, restartedStore.getJobHistory().size());
                assertEquals(
                        List.of("task-" + JOB_ID + "-0", "task-" + JOB_ID + "-1"),
                        restartedStore.getTasksForJob(JOB_ID).stream()
                                .map(DatabaseManager.TaskRecord::taskId)
                                .toList()
                );
                assertEquals(0, replayOutput.taskAssignmentCount());
            } finally {
                stop(restartedThread);
            }
        }
    }

    @Test
    void exactDuplicateReplaysPersistedTerminalResult() throws Exception {
        Path dbPath = tempDir.resolve("duplicate-terminal-result.db");
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register("executor-1", new PeerInfo(
                "executor-1",
                SchedulerConfig.defaults(),
                List.of(TASK_TYPE)
        ));
        RecordingOutput output = new RecordingOutput();

        try (DatabaseManager store = new DatabaseManager(dbPath.toString())) {
            TaskScheduler scheduler = new TaskScheduler(
                    mailbox,
                    registry,
                    store,
                    output,
                    SchedulerConfig.defaults()
            );
            Thread thread = start(scheduler, "terminal-submission-replay-scheduler");
            try {
                RecordingAcknowledgement submitAck = new RecordingAcknowledgement();
                mailbox.put(new MessageEnvelope(
                        signedSubmission(
                                "requester-terminal-route",
                                "2026-07-23T01:00:00Z",
                                List.of("payload"),
                                identity
                        ),
                        "requester-terminal-route",
                        submitAck
                ));
                assertTrue(submitAck.awaitAck());

                TaskAssignMessage assignment = output.awaitTaskAssignment();
                assertNotNull(assignment);
                RecordingAcknowledgement resultAck = new RecordingAcknowledgement();
                mailbox.put(new MessageEnvelope(
                        new TaskResultMessage(
                                "executor-1",
                                "2026-07-23T01:00:01Z",
                                assignment.getTaskId(),
                                assignment.getJobId(),
                                assignment.getAttemptNumber(),
                                assignment.getAssignmentId(),
                                "terminal-result",
                                true,
                                null
                        ),
                        "executor-1",
                        resultAck
                ));
                assertTrue(resultAck.awaitAck());
                JobResultMessage originalResult = output.awaitResult();
                assertNotNull(originalResult);
                assertTrue(originalResult.isSuccessful());

                RecordingAcknowledgement replayAck = new RecordingAcknowledgement();
                mailbox.put(new MessageEnvelope(
                        signedSubmission(
                                "requester-terminal-route-after-reconnect",
                                "2026-07-23T01:05:00Z",
                                List.of("payload"),
                                identity
                        ),
                        "requester-terminal-route-after-reconnect",
                        replayAck
                ));
                assertTrue(replayAck.awaitAck());
                JobResultMessage replayedResult = output.awaitResult();
                assertNotNull(replayedResult);
                assertTrue(replayedResult.isSuccessful());
                assertEquals(originalResult.getResultPayload(), replayedResult.getResultPayload());
                assertEquals(originalResult.getResultsByTaskId(), replayedResult.getResultsByTaskId());
                assertEquals(1, store.getJobHistory().size());
                assertEquals(1, store.getTasksForJob(JOB_ID).size());
                assertEquals(1, output.taskAssignmentCount());
            } finally {
                stop(thread);
            }
        }
    }

    private static TaskScheduler scheduler(BlockingQueue<MessageEnvelope> mailbox,
                                           DatabaseManager store,
                                           RecordingOutput output) {
        return new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
    }

    private static JobSubmitMessage signedSubmission(String nodeId,
                                                     String time,
                                                     List<Object> payloads,
                                                     RequesterIdentity.Credentials identity) {
        String signature = RequesterIdentity.signJobSubmit(
                identity.privateKey(),
                nodeId,
                time,
                JOB_ID,
                TASK_TYPE,
                "",
                REQUESTER_TOKEN
        );
        return new JobSubmitMessage(
                nodeId,
                time,
                JOB_ID,
                TASK_TYPE,
                payloads,
                "",
                REQUESTER_TOKEN,
                identity.publicKey(),
                signature
        );
    }

    private static Thread start(TaskScheduler scheduler, String name) {
        Thread thread = new Thread(scheduler, name);
        thread.start();
        return thread;
    }

    private static void stop(Thread thread) throws InterruptedException {
        thread.interrupt();
        thread.join(2_000L);
        assertFalse(thread.isAlive());
    }

    private static final class RecordingOutput implements SchedulerOutput {
        private final BlockingQueue<JobResultMessage> results = new LinkedBlockingQueue<>();
        private final BlockingQueue<TaskAssignMessage> assignments = new LinkedBlockingQueue<>();
        private final AtomicInteger taskAssignments = new AtomicInteger();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            taskAssignments.incrementAndGet();
            assignments.add(message);
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            results.add(message);
            return true;
        }

        private JobResultMessage awaitResult() throws InterruptedException {
            return results.poll(2, TimeUnit.SECONDS);
        }

        private boolean hasResult() {
            return !results.isEmpty();
        }

        private TaskAssignMessage awaitTaskAssignment() throws InterruptedException {
            return assignments.poll(2, TimeUnit.SECONDS);
        }

        private int taskAssignmentCount() {
            return taskAssignments.get();
        }
    }

    private static final class RecordingAcknowledgement implements TransportAcknowledgement {
        private final CountDownLatch acknowledged = new CountDownLatch(1);

        @Override
        public void ack() {
            acknowledged.countDown();
        }

        @Override
        public void requeue() {
            throw new AssertionError("Valid idempotent submissions must not be requeued.");
        }

        @Override
        public void reject() {
            throw new AssertionError("Valid idempotent submissions must not be rejected.");
        }

        private boolean awaitAck() throws InterruptedException {
            return acknowledged.await(2, TimeUnit.SECONDS);
        }
    }
}
