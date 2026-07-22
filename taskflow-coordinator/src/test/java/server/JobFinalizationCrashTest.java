package server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;
import protocol.RequesterTokens;
import protocol.TaskAssignMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.rabbitmq.RabbitMqOutboxReplayer;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.runtime.TaskFlowClock;
import server.scheduler.BrokerOutboxPublisher;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerOutput;
import server.scheduler.TaskScheduler;
import transport.TransportRoute;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobFinalizationCrashTest {
    private static final String JOB_ID = "job-finalization-crash";
    private static final String TASK_TYPE = "RABBITMQ_TEST_TASK";
    private static final String REQUESTER_ID = "requester-1";
    private static final String FIRST_ASSIGNMENT_ID = "550e8400-e29b-41d4-a716-446655440010";
    private static final String SECOND_ASSIGNMENT_ID = "550e8400-e29b-41d4-a716-446655440011";
    private static final FixedClock CLOCK = new FixedClock(300L);

    @TempDir
    Path tempDir;

    @Test
    void lastTaskCommitCannotStrandJob() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-finalization-crash.db");
        seedFinalizingJob(dbPath);

        long durableOutboxId;
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            CoordinatorStartupRecovery.RecoveryResult firstRecovery = recover(db);
            CoordinatorStartupRecovery.RecoveryResult duplicateRecovery = recover(db);
            assertEquals(1, firstRecovery.resumedJobs().size());
            assertEquals(1, duplicateRecovery.resumedJobs().size());

            RecordingOutboxOutput firstOutput = new RecordingOutboxOutput(false);
            restoreRecoveredJobs(db, firstRecovery, firstOutput);

            assertEquals("COMPLETED", db.getJobHistory().getFirst().status());
            List<BrokerOutboxStore.OutboxRecord> pending = db.loadPendingBrokerOutbox(10);
            assertEquals(1, pending.size());
            durableOutboxId = pending.getFirst().outboxId();
            JobResultMessage finalResult = assertInstanceOf(
                    JobResultMessage.class,
                    pending.getFirst().message().message()
            );
            assertEquals(JOB_ID, finalResult.getJobId());
            assertEquals(List.of("result-0", "result-1"), finalResult.getResultsByTaskId());
            assertEquals(1, firstOutput.publishAttempts());

            RecordingOutboxOutput duplicateOutput = new RecordingOutboxOutput(false);
            restoreRecoveredJobs(db, duplicateRecovery, duplicateOutput);

            List<BrokerOutboxStore.OutboxRecord> afterDuplicate = db.loadPendingBrokerOutbox(10);
            assertEquals(1, afterDuplicate.size());
            assertEquals(durableOutboxId, afterDuplicate.getFirst().outboxId());
            assertEquals(1, duplicateOutput.publishAttempts());
        }

        try (DatabaseManager restarted = new DatabaseManager(dbPath.toString())) {
            assertTrue(recover(restarted).resumedJobs().isEmpty());
            assertEquals(1, restarted.loadPendingBrokerOutbox(10).size());

            RecordingOutboxOutput replayOutput = new RecordingOutboxOutput(true);
            try (RabbitMqOutboxReplayer replayer = new RabbitMqOutboxReplayer(restarted, replayOutput)) {
                replayer.start();
                assertEquals(1, replayOutput.publishAttempts());
                assertEquals(durableOutboxId, replayOutput.lastPublished().outboxId());
                assertTrue(restarted.loadPendingBrokerOutbox(10).isEmpty());
            }
        }
    }

    private static void seedFinalizingJob(Path dbPath) throws Exception {
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            assertTrue(db.insertJobWithTasks(
                    JOB_ID,
                    TASK_TYPE,
                    REQUESTER_ID,
                    RequesterTokens.hashToken("requester-token"),
                    "",
                    "",
                    List.of(
                            new JobStateStore.TaskStartupState(taskId(0), "payload-0"),
                            new JobStateStore.TaskStartupState(taskId(1), "payload-1")
                    )
            ));

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    db.commitTaskAssignment(
                            taskId(1),
                            "peer-1",
                            100L,
                            "lease-owner",
                            1_000L,
                            1,
                            FIRST_ASSIGNMENT_ID
                    )
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            taskId(1),
                            1,
                            FIRST_ASSIGNMENT_ID,
                            "peer-1",
                            150L,
                            50L,
                            "result-1"
                    )
            );
            assertEquals("RUNNING", db.getJobHistory().getFirst().status());

            assertEquals(
                    JobStateStore.DurableTransitionOutcome.COMMITTED,
                    db.commitTaskAssignment(
                            taskId(0),
                            "peer-1",
                            200L,
                            "lease-owner",
                            1_000L,
                            1,
                            SECOND_ASSIGNMENT_ID
                    )
            );
            assertEquals(
                    JobStateStore.ResultCommitOutcome.COMMITTED,
                    db.commitTaskResult(
                            taskId(0),
                            1,
                            SECOND_ASSIGNMENT_ID,
                            "peer-1",
                            250L,
                            50L,
                            "result-0"
                    )
            );

            assertEquals("FINALIZING", db.getJobHistory().getFirst().status());
            assertEquals(List.of(), db.loadPendingBrokerOutbox(10));
        }
    }

    private static CoordinatorStartupRecovery.RecoveryResult recover(DatabaseManager db) {
        return CoordinatorStartupRecovery.recoverPersistedJobs(
                db,
                CLOCK,
                () -> FIRST_ASSIGNMENT_ID
        );
    }

    private static void restoreRecoveredJobs(DatabaseManager db,
                                             CoordinatorStartupRecovery.RecoveryResult recovery,
                                             RecordingOutboxOutput output) {
        TaskScheduler scheduler = new TaskScheduler(
                new LinkedBlockingQueue<MessageEnvelope>(),
                new InMemoryPeerRegistry(),
                db,
                output,
                SchedulerConfig.defaults(),
                CLOCK,
                () -> FIRST_ASSIGNMENT_ID
        );
        scheduler.restoreJobs(
                recovery.resumedJobs(),
                recovery.requesterTokenHashes(),
                recovery.requesterIdentityKeys()
        );
    }

    private static String taskId(int index) {
        return "task-" + JOB_ID + "-" + index;
    }

    private record FixedClock(long epochMillis) implements TaskFlowClock {
        @Override
        public Instant now() {
            return Instant.ofEpochMilli(epochMillis);
        }

        @Override
        public long nowEpochMillis() {
            return epochMillis;
        }
    }

    private static final class RecordingOutboxOutput
            implements SchedulerOutput, BrokerOutboxPublisher {
        private final boolean publishResult;
        private int publishAttempts;
        private BrokerOutboxStore.OutboxRecord lastPublished;

        private RecordingOutboxOutput(boolean publishResult) {
            this.publishResult = publishResult;
        }

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            throw new AssertionError("Final results must use the configured broker outbox.");
        }

        @Override
        public BrokerOutboxStore.OutboxMessage taskAssignmentOutboxMessage(
                PeerInfo peer,
                TaskAssignMessage message) {
            return new BrokerOutboxStore.OutboxMessage(
                    TransportRoute.TASK_ASSIGN,
                    peer.getNodeId(),
                    "COORDINATOR",
                    message
            );
        }

        @Override
        public BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(
                String requesterNodeId,
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
            publishAttempts++;
            lastPublished = record;
            return publishResult;
        }

        private int publishAttempts() {
            return publishAttempts;
        }

        private BrokerOutboxStore.OutboxRecord lastPublished() {
            return lastPublished;
        }
    }
}
