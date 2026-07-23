package server.rabbitmq;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.JobResultMessage;
import protocol.TaskAssignMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.scheduler.BrokerOutboxPublisher;
import transport.TransportRoute;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqOutboxReplayerTest {

    @TempDir
    Path tempDir;

    @Test
    void replayMarksPublishedRowsSent() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(outboxRecord(1L)));
        RecordingPublisher publisher = new RecordingPublisher(true);
        RabbitMqOutboxReplayer replayer = new RabbitMqOutboxReplayer(store, publisher, 10, 100L);

        int published = replayer.replayOnce();

        assertEquals(1, published);
        assertEquals(List.of(1L), publisher.publishedIds);
        assertEquals(Set.of(1L), store.publishedIds);
        assertEquals(List.of(), store.failedIds);
        assertEquals(List.of(), store.loadPendingBrokerOutbox(10));
        replayer.close();
    }

    @Test
    void replayRecordsFailedAttemptAndLeavesRowPending() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(outboxRecord(2L)));
        RecordingPublisher publisher = new RecordingPublisher(false);
        RabbitMqOutboxReplayer replayer = new RabbitMqOutboxReplayer(store, publisher, 10, 100L);

        int published = replayer.replayOnce();

        assertEquals(0, published);
        assertEquals(List.of(2L), publisher.publishedIds);
        assertEquals(Set.of(), store.publishedIds);
        assertEquals(List.of(2L), store.failedIds);
        assertEquals(1, store.loadPendingBrokerOutbox(10).size());
        replayer.close();
    }

    @Test
    void connectionLossAfterPublishWriteRecordsFailureAndLeavesRowPending() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(outboxRecord(3L)));
        RecordingPublisher publisher = new RecordingPublisher(
                new IllegalStateException("broker connection lost before confirm")
        );
        RabbitMqOutboxReplayer replayer = new RabbitMqOutboxReplayer(store, publisher, 10, 100L);

        int published = replayer.replayOnce();

        assertEquals(0, published);
        assertEquals(List.of(3L), publisher.publishedIds);
        assertEquals(Set.of(), store.publishedIds);
        assertEquals(List.of(3L), store.failedIds);
        assertEquals(1, store.loadPendingBrokerOutbox(10).size());
        replayer.close();
    }

    @Test
    void failedSentMarkLeavesConfirmedMessagePendingForIdenticalReplay() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(outboxRecord(4L)), 1);
        RecordingPublisher publisher = new RecordingPublisher(true);
        RabbitMqOutboxReplayer replayer = new RabbitMqOutboxReplayer(store, publisher, 10, 100L);

        assertEquals(0, replayer.replayOnce());
        assertEquals(List.of(4L), publisher.publishedIds);
        assertEquals(1, store.loadPendingBrokerOutbox(10).size());

        assertEquals(1, replayer.replayOnce());
        assertEquals(List.of(4L, 4L), publisher.publishedIds);
        assertEquals(
                publisher.publishedRecords.get(0).message(),
                publisher.publishedRecords.get(1).message()
        );
        assertEquals(List.of(), store.loadPendingBrokerOutbox(10));
        replayer.close();
    }

    @Test
    void replayPublishesOriginalDatabaseCommittedAssignmentIdentity() throws Exception {
        Path dbPath = tempDir.resolve("assignment-replay.db");
        BrokerOutboxStore.CommittedTaskAssignment committed;
        try (DatabaseManager db = new DatabaseManager(dbPath.toString())) {
            assertTrue(db.insertJob("job-replay", "TEST_TASK", "requester-1", 1));
            assertTrue(db.insertTask("task-replay-0", "job-replay"));
            committed = db.createTaskAssignmentAndEnqueueBrokerOutbox(
                    "task-replay-0",
                    "peer-1",
                    100L,
                    "coordinator-1",
                    500L,
                    new BrokerOutboxStore.OutboxMessage(
                            TransportRoute.TASK_ASSIGN,
                            "peer-1",
                            "COORDINATOR",
                            new TaskAssignMessage(
                                    "peer-1",
                                    "2026-07-22T00:00:00Z",
                                    "task-replay-0",
                                    "job-replay",
                                    "TEST_TASK",
                                    "payload",
                                    ""
                            )
                    )
            ).orElseThrow();
        }

        try (DatabaseManager reopened = new DatabaseManager(dbPath.toString())) {
            RecordingPublisher publisher = new RecordingPublisher(true);
            RabbitMqOutboxReplayer replayer = new RabbitMqOutboxReplayer(reopened, publisher, 10, 100L);
            try {
                assertEquals(1, replayer.replayOnce());
            } finally {
                replayer.close();
            }

            assertEquals(List.of(committed.outboxRecord().outboxId()), publisher.publishedIds);
            BrokerOutboxStore.OutboxRecord replayedRecord = publisher.publishedRecords.getFirst();
            TaskAssignMessage replayed = assertInstanceOf(
                    TaskAssignMessage.class,
                    replayedRecord.message().message()
            );
            assertEquals(committed.identity().attemptNumber(), replayed.getAttemptNumber());
            assertEquals(committed.identity().assignmentId(), replayed.getAssignmentId());
            assertEquals(committed.identity().leaseExpiresAtEpochMillis(),
                    replayed.getLeaseExpiresAtEpochMillis());
            assertEquals(List.of(), reopened.loadPendingBrokerOutbox(10));
        }
    }

    private static BrokerOutboxStore.OutboxRecord outboxRecord(long id) {
        JobResultMessage result = new JobResultMessage(
                "COORDINATOR",
                "2026-07-02T00:00:00Z",
                "job-" + id,
                "TEST_TASK",
                true,
                List.of()
        );
        return new BrokerOutboxStore.OutboxRecord(
                id,
                new BrokerOutboxStore.OutboxMessage(
                        TransportRoute.JOB_RESULT,
                        "requester-" + id,
                        "COORDINATOR",
                        result
                ),
                100L,
                0,
                0L,
                ""
        );
    }

    private static final class RecordingPublisher implements BrokerOutboxPublisher {
        private final boolean publishResult;
        private final RuntimeException publishFailure;
        private final List<Long> publishedIds = new ArrayList<>();
        private final List<BrokerOutboxStore.OutboxRecord> publishedRecords = new ArrayList<>();

        private RecordingPublisher(boolean publishResult) {
            this.publishResult = publishResult;
            this.publishFailure = null;
        }

        private RecordingPublisher(RuntimeException publishFailure) {
            this.publishResult = false;
            this.publishFailure = publishFailure;
        }

        @Override
        public BrokerOutboxStore.OutboxMessage taskAssignmentOutboxMessage(
                server.registry.PeerInfo peer,
                protocol.TaskAssignMessage message
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BrokerOutboxStore.OutboxMessage jobResultOutboxMessage(String requesterNodeId,
                                                                      JobResultMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean publishOutbox(BrokerOutboxStore.OutboxRecord record) {
            publishedIds.add(record.outboxId());
            publishedRecords.add(record);
            if (publishFailure != null) {
                throw publishFailure;
            }
            return publishResult;
        }
    }

    private static final class RecordingOutboxStore implements BrokerOutboxStore {
        private final List<OutboxRecord> records;
        private final Set<Long> publishedIds = new LinkedHashSet<>();
        private final List<Long> failedIds = new ArrayList<>();
        private int failedPublishedMarksRemaining;

        private RecordingOutboxStore(List<OutboxRecord> records) {
            this(records, 0);
        }

        private RecordingOutboxStore(List<OutboxRecord> records,
                                     int failedPublishedMarksRemaining) {
            this.records = new ArrayList<>(records);
            this.failedPublishedMarksRemaining = failedPublishedMarksRemaining;
        }

        @Override
        public Optional<OutboxRecord> enqueueBrokerOutbox(OutboxMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<CommittedTaskAssignment> createTaskAssignmentAndEnqueueBrokerOutbox(
                String taskId,
                String peerId,
                long startedAt,
                String leaseOwnerId,
                long leaseExpiresAt,
                String assignmentId,
                OutboxMessage messageTemplate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<OutboxRecord> markJobCompletedAndEnqueueBrokerOutbox(String jobId,
                                                                             Object resultPayload,
                                                                             OutboxMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<OutboxRecord> markJobFailedAndEnqueueBrokerOutbox(String jobId,
                                                                          Collection<TaskFailureUpdate> taskFailures,
                                                                          OutboxMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<OutboxRecord> loadPendingBrokerOutbox(int limit) {
            return records.stream()
                    .filter(record -> !publishedIds.contains(record.outboxId()))
                    .limit(limit)
                    .toList();
        }

        @Override
        public boolean markBrokerOutboxPublished(long outboxId, long publishedAt) {
            if (failedPublishedMarksRemaining > 0) {
                failedPublishedMarksRemaining--;
                return false;
            }
            publishedIds.add(outboxId);
            return true;
        }

        @Override
        public boolean markBrokerOutboxPublishFailed(long outboxId, String error, long attemptedAt) {
            failedIds.add(outboxId);
            return true;
        }
    }
}
