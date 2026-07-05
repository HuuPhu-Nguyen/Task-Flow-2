package server;

import org.junit.jupiter.api.Test;
import protocol.PongMessage;
import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.registry.PeerMetricsSnapshot;
import server.registry.PeerRegistryRecord;
import server.registry.PeerStatus;
import server.registry.PeerTransport;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqDlqMessage;
import transport.rabbitmq.RabbitMqQueueStatus;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatusCommandTest {
    @Test
    void summaryPrintsDatabaseRabbitMqAndDlqState() throws Exception {
        FakeRabbitMqStatusClient rabbitMq = new FakeRabbitMqStatusClient(
                List.of(
                        new RabbitMqQueueStatus("JOB_SUBMIT", "taskflow.jobs", 2L, 1L, true, ""),
                        new RabbitMqQueueStatus("DLQ", "taskflow.dead-letter", 1L, 0L, true, "")
                ),
                List.of(dlqMessage("msg-1", true))
        );

        String output = runStatus(fakeDataSource(), rabbitMq, Map.of("TASKFLOW_TRANSPORT", "rabbitmq"),
                "status", "summary", "5");

        assertTrue(output.contains("database status=available path=taskflow.db schema=9"));
        assertTrue(output.contains("jobs total=2 running=1 completed=1 failed=0"));
        assertTrue(output.contains("tasks total=3 pending=1 assigned=1 completed=1 failed=0 retries=1 activeLeases=1 expiredLeases=0"));
        assertTrue(output.contains("attempts total=3 running=1 succeeded=1 retryScheduled=1 terminalFailure=0 dispatchFailed=0 jobFailed=0"));
        assertTrue(output.contains("peers total=2 connected=1 disconnected=1"));
        assertTrue(output.contains("outbox pendingVisible=1 limit=1000 failedAttempts=1"));
        assertTrue(output.contains("rabbitmq status=available queues=2 availableQueues=2 queuedMessages=3 dlqVisible=1 dlqRedrivable=1"));
        assertTrue(rabbitMq.declaredTopology);
    }

    @Test
    void summaryUsesRabbitMqWhenTransportIsUnset() throws Exception {
        FakeRabbitMqStatusClient rabbitMq = new FakeRabbitMqStatusClient(
                List.of(new RabbitMqQueueStatus("JOB_SUBMIT", "taskflow.jobs", 0L, 0L, true, "")),
                List.of()
        );

        String output = runStatus(fakeDataSource(), rabbitMq, Map.of(), "status", "summary");

        assertTrue(output.contains("rabbitmq status=available queues=1"));
        assertTrue(rabbitMq.declaredTopology);
    }

    @Test
    void summarySkipsRabbitMqOnlyWhenTcpIsExplicit() throws Exception {
        FakeRabbitMqStatusClient rabbitMq = FakeRabbitMqStatusClient.empty();

        String output = runStatus(fakeDataSource(), rabbitMq, Map.of("TASKFLOW_TRANSPORT", "tcp"),
                "status", "summary");

        assertTrue(output.contains("rabbitmq status=not_selected transport=tcp"));
        assertFalse(rabbitMq.declaredTopology);
    }

    @Test
    void summaryRejectsUnknownTransport() {
        assertThrows(IllegalArgumentException.class, () ->
                runStatus(fakeDataSource(), FakeRabbitMqStatusClient.empty(),
                        Map.of("TASKFLOW_TRANSPORT", "udp"), "status", "summary"));
    }

    @Test
    void jobsViewPrintsRecentJobsWithRetryAndAttemptCounts() throws Exception {
        String output = runStatus(fakeDataSource(), FakeRabbitMqStatusClient.empty(), Map.of(),
                "status", "jobs", "1");

        assertTrue(output.contains("TaskFlow jobs limit=1"));
        assertTrue(output.contains("job[1] id=job-running type=TEXT_ANALYSIS status=RUNNING requester=requester-1 files=2"));
        assertTrue(output.contains("tasks=2 pending=1 assigned=1 completed=0 failed=0 retries=1 attempts=2"));
        assertFalse(output.contains("job-completed"));
    }

    @Test
    void queuesViewUsesPassiveQueueInspectionWithoutDeclaringTopology() throws Exception {
        FakeRabbitMqStatusClient rabbitMq = new FakeRabbitMqStatusClient(
                List.of(new RabbitMqQueueStatus("JOB_SUBMIT", "taskflow.jobs", 4L, 2L, true, "")),
                List.of()
        );

        String output = runStatus(fakeDataSource(), rabbitMq, Map.of(), "status", "queues");

        assertTrue(output.contains("TaskFlow RabbitMQ queues"));
        assertTrue(output.contains("queue[1] role=JOB_SUBMIT name=taskflow.jobs messages=4 consumers=2 available=true error=\"\""));
        assertFalse(rabbitMq.declaredTopology);
    }

    @Test
    void dlqViewDeclaresTopologyAndPrintsMessagePreview() throws Exception {
        FakeRabbitMqStatusClient rabbitMq = new FakeRabbitMqStatusClient(
                List.of(),
                List.of(dlqMessage("msg-poison", false))
        );

        String output = runStatus(fakeDataSource(), rabbitMq, Map.of(), "status", "dlq", "3");

        assertTrue(output.contains("TaskFlow RabbitMQ DLQ limit=3"));
        assertTrue(output.contains("dlq visible=1 limit=3 redrivable=0 nonRedrivable=1"));
        assertTrue(output.contains("dlq[1] id=msg-poison route=HEARTBEAT"));
        assertTrue(output.contains("bodyPreview=\"{bad json}\""));
        assertTrue(output.contains("nonRedrivableReason=\"bad envelope\""));
        assertTrue(rabbitMq.declaredTopology);
    }

    @Test
    void helpPrintsStatusUsage() throws Exception {
        String output = runStatus(fakeDataSource(), FakeRabbitMqStatusClient.empty(), Map.of(), "status", "--help");

        assertTrue(output.contains("status [summary|jobs|peers|outbox|queues|dlq] [count]"));
        assertTrue(output.contains("Views:"));
    }

    private static String runStatus(FakeStatusDataSource dataSource,
                                    FakeRabbitMqStatusClient rabbitMq,
                                    Map<String, String> environment,
                                    String... args) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
            StatusCommand.run(args, out, () -> dataSource, () -> rabbitMq, environment);
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    private static FakeStatusDataSource fakeDataSource() {
        long activeLease = System.currentTimeMillis() + 60_000L;
        return new FakeStatusDataSource(
                List.of(
                        new DatabaseManager.JobRecord(
                                "job-running",
                                "TEXT_ANALYSIS",
                                "requester-1",
                                "RUNNING",
                                200L,
                                0L,
                                2
                        ),
                        new DatabaseManager.JobRecord(
                                "job-completed",
                                "IMAGE_CONVERSION",
                                "requester-2",
                                "COMPLETED",
                                100L,
                                150L,
                                1
                        )
                ),
                Map.of(
                        "job-running",
                        List.of(
                                new DatabaseManager.TaskRecord(
                                        "task-running-0",
                                        "job-running",
                                        "peer-1",
                                        "ASSIGNED",
                                        210L,
                                        0L,
                                        0L,
                                        1,
                                        "coordinator-1",
                                        activeLease
                                ),
                                new DatabaseManager.TaskRecord(
                                        "task-running-1",
                                        "job-running",
                                        null,
                                        "PENDING",
                                        0L,
                                        0L,
                                        0L,
                                        0,
                                        "",
                                        0L
                                )
                        ),
                        "job-completed",
                        List.of(new DatabaseManager.TaskRecord(
                                "task-completed-0",
                                "job-completed",
                                "peer-2",
                                "COMPLETED",
                                110L,
                                140L,
                                30L,
                                0,
                                "",
                                0L
                        ))
                ),
                Map.of(
                        "job-running",
                        List.of(
                                new JobStateStore.TaskAttemptRecord(
                                        "job-running",
                                        "task-running-0",
                                        1,
                                        "peer-1",
                                        210L,
                                        0L,
                                        0L,
                                        JobStateStore.TaskAttemptOutcome.RUNNING,
                                        ""
                                ),
                                new JobStateStore.TaskAttemptRecord(
                                        "job-running",
                                        "task-running-0",
                                        0,
                                        "peer-1",
                                        100L,
                                        150L,
                                        50L,
                                        JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                                        "timeout"
                                )
                        ),
                        "job-completed",
                        List.of(new JobStateStore.TaskAttemptRecord(
                                "job-completed",
                                "task-completed-0",
                                1,
                                "peer-2",
                                110L,
                                140L,
                                30L,
                                JobStateStore.TaskAttemptOutcome.SUCCEEDED,
                                ""
                        ))
                ),
                List.of(
                        new PeerRegistryRecord(
                                "peer-1",
                                "RABBITMQ_PEER",
                                PeerTransport.RABBITMQ,
                                Set.of("TEXT_ANALYSIS"),
                                10L,
                                250L,
                                0L,
                                PeerStatus.CONNECTED,
                                new PeerMetricsSnapshot(3L, 0L, 12L, 40L)
                        ),
                        new PeerRegistryRecord(
                                "peer-2",
                                "TCP_PEER",
                                PeerTransport.TCP,
                                Set.of("IMAGE_CONVERSION"),
                                20L,
                                120L,
                                180L,
                                PeerStatus.DISCONNECTED,
                                new PeerMetricsSnapshot(1L, 1L, 30L, 80L)
                        )
                ),
                List.of(new BrokerOutboxStore.OutboxRecord(
                        1L,
                        new BrokerOutboxStore.OutboxMessage(
                                TransportRoute.HEARTBEAT,
                                "peer-1",
                                "COORDINATOR",
                                new PongMessage("peer-1", Instant.EPOCH.toString(), List.of("TEXT_ANALYSIS"))
                        ),
                        300L,
                        1,
                        350L,
                        "publish failed"
                ))
        );
    }

    private static RabbitMqDlqMessage dlqMessage(String id, boolean redrivable) {
        return new RabbitMqDlqMessage(
                id,
                "application/json",
                "taskflow.exchange",
                "heartbeats",
                "taskflow.dead-letter",
                "rejected",
                1L,
                Instant.EPOCH,
                Instant.EPOCH,
                0,
                TransportRoute.HEARTBEAT,
                redrivable,
                redrivable ? "" : "bad envelope",
                (redrivable ? "{}" : "{bad json}").getBytes(StandardCharsets.UTF_8),
                Map.of()
        );
    }

    private record FakeStatusDataSource(
            List<DatabaseManager.JobRecord> jobs,
            Map<String, List<DatabaseManager.TaskRecord>> tasks,
            Map<String, List<JobStateStore.TaskAttemptRecord>> attempts,
            List<PeerRegistryRecord> peers,
            List<BrokerOutboxStore.OutboxRecord> outbox
    ) implements StatusCommand.StatusDataSource {
        @Override
        public int schemaVersion() {
            return DatabaseManager.CURRENT_SCHEMA_VERSION;
        }

        @Override
        public List<DatabaseManager.TaskRecord> tasksForJob(String jobId) {
            return tasks.getOrDefault(jobId, List.of());
        }

        @Override
        public List<JobStateStore.TaskAttemptRecord> attemptsForJob(String jobId) {
            return attempts.getOrDefault(jobId, List.of());
        }

        @Override
        public List<BrokerOutboxStore.OutboxRecord> pendingOutbox(int limit) {
            return outbox.stream().limit(limit).toList();
        }
    }

    private static final class FakeRabbitMqStatusClient implements StatusCommand.RabbitMqStatusClient {
        private final List<RabbitMqQueueStatus> queues;
        private final List<RabbitMqDlqMessage> messages;
        private boolean declaredTopology;

        private FakeRabbitMqStatusClient(List<RabbitMqQueueStatus> queues, List<RabbitMqDlqMessage> messages) {
            this.queues = queues;
            this.messages = messages;
        }

        private static FakeRabbitMqStatusClient empty() {
            return new FakeRabbitMqStatusClient(List.of(), List.of());
        }

        @Override
        public void declareTopology() {
            declaredTopology = true;
        }

        @Override
        public List<RabbitMqQueueStatus> inspectQueueStatus() {
            return queues;
        }

        @Override
        public List<RabbitMqDlqMessage> inspectDlq(int limit) {
            return messages.stream().limit(limit).toList();
        }
    }
}
