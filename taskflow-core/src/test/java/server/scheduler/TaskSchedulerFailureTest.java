package server.scheduler;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.PeerDisconnectedMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import transport.TransportAcknowledgement;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskSchedulerFailureTest {

    @Test
    void unsupportedJobTypeReturnsFailedJobResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        CapturingOutput output = new CapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-failure-test");
        schedulerThread.start();

        try {
            JobSubmitMessage unsupportedJob = new JobSubmitMessage(
                    "client-1",
                    "2026-06-03T00:00:00Z",
                    "job-unsupported",
                    "UNSUPPORTED_TASK",
                    List.of("payload"),
                    "",
                    "token-job-unsupported"
            );
            mailbox.put(new MessageEnvelope(unsupportedJob, "requester-1"));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertEquals("job-unsupported", result.getJobId());
            assertEquals("UNSUPPORTED_TASK", result.getTaskType());
            assertFalse(result.isSuccessful());
            assertTrue(result.getErrorMessage().contains("Unsupported job type"));
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void emptyPayloadJobReturnsFailedJobResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        CapturingOutput output = new CapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-empty-payload-test");
        schedulerThread.start();

        try {
            JobSubmitMessage emptyJob = new JobSubmitMessage(
                    "client-1",
                    "2026-06-10T00:00:00Z",
                    "job-empty",
                    "TEST_TASK",
                    List.of(),
                    "",
                    "token-job-empty"
            );
            mailbox.put(new MessageEnvelope(emptyJob, "requester-1"));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertEquals("job-empty", result.getJobId());
            assertFalse(result.isSuccessful());
            assertTrue(result.getErrorMessage().contains("at least one task"));
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void invalidPluginSubmissionReturnsFailedJobResultWithoutDispatching() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        CapturingOutput output = new CapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-invalid-plugin-submit-test");
        schedulerThread.start();

        try {
            JobSubmitMessage invalidJob = new JobSubmitMessage(
                    "client-1",
                    "2026-06-25T00:00:00Z",
                    "job-invalid-submit",
                    "TEST_TASK",
                    List.of("payload"),
                    "INVALID_PARAMETER",
                    "token-job-invalid-submit"
            );
            mailbox.put(new MessageEnvelope(invalidJob, "requester-1"));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertEquals("job-invalid-submit", result.getJobId());
            assertEquals("TEST_TASK", result.getTaskType());
            assertFalse(result.isSuccessful());
            assertTrue(result.getErrorMessage().contains("Invalid TEST_TASK parameter"));
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void brokerDeliveryIsAcknowledgedAfterJobSubmitHandling() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        CapturingOutput output = new CapturingOutput();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-ack-test");
        schedulerThread.start();

        try {
            JobSubmitMessage emptyJob = new JobSubmitMessage(
                    "client-1",
                    "2026-06-12T00:00:00Z",
                    "job-ack",
                    "TEST_TASK",
                    List.of(),
                    "",
                    "token-job-ack"
            );
            mailbox.put(new MessageEnvelope(emptyJob, "requester-1", acknowledgement));

            assertTrue(output.awaitResult());
            assertTrue(acknowledgement.awaitAck());
            assertEquals(1, acknowledgement.ackCount());
            assertEquals(0, acknowledgement.requeueCount());
            assertEquals(0, acknowledgement.rejectCount());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void invalidJobIdReturnsSafeFailureAndAcknowledgesBrokerDelivery() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        CapturingOutput output = new CapturingOutput();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-invalid-job-id-test");
        schedulerThread.start();

        try {
            JobSubmitMessage invalidJob = new JobSubmitMessage(
                    "client-1",
                    "2026-07-04T00:00:00Z",
                    "../job",
                    "TEST_TASK",
                    List.of("payload"),
                    "",
                    "token-invalid-job"
            );
            mailbox.put(new MessageEnvelope(invalidJob, "requester-1", acknowledgement));

            assertTrue(output.awaitResult());
            assertTrue(acknowledgement.awaitAck());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertEquals("", result.getJobId());
            assertFalse(result.isSuccessful());
            assertTrue(result.getErrorMessage().contains("Job id may contain only"));
            assertEquals(1, acknowledgement.ackCount());
            assertEquals(0, acknowledgement.requeueCount());
            assertEquals(0, acknowledgement.rejectCount());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void invalidBrokerTaskResultIsRejectedWithoutRequeue() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        CapturingOutput output = new CapturingOutput();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-invalid-task-result-test");
        schedulerThread.start();

        try {
            TaskResultMessage invalidResult = new TaskResultMessage(
                    "peer-1",
                    "2026-07-04T00:00:00Z",
                    "task/unsafe",
                    "job-1",
                    1,
                    "550e8400-e29b-41d4-a716-446655440000",
                    "payload",
                    true,
                    null
            );
            mailbox.put(new MessageEnvelope(invalidResult, "peer-1", acknowledgement));

            assertTrue(acknowledgement.awaitReject());
            assertEquals(0, acknowledgement.ackCount());
            assertEquals(0, acknowledgement.requeueCount());
            assertEquals(1, acknowledgement.rejectCount());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void versionZeroAndOneBrokerTaskResultsAreRejectedWithoutCommitOrRequeue() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo("peer", SchedulerConfig.defaults(), List.of("TEST_TASK"));
        registry.register(peer.getNodeId(), peer);
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-legacy-task-result-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-legacy-result", List.of("payload")), "requester-1"));
            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();

            for (int legacyVersion : List.of(0, 1)) {
                RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
                TaskResultMessage legacyResult = legacyResult(assignment, legacyVersion);
                mailbox.put(new MessageEnvelope(legacyResult, "peer", acknowledgement));

                assertTrue(acknowledgement.awaitReject());
                assertEquals(0, acknowledgement.ackCount());
                assertEquals(0, acknowledgement.requeueCount());
                assertEquals(1, acknowledgement.rejectCount());
            }
            assertFalse(output.awaitResult(150));
            assertEquals(1, peer.getActiveTasks());

            mailbox.put(new MessageEnvelope(successResult(assignment, "current-result"), "peer"));
            assertTrue(output.awaitResult());
            assertTrue(output.result().isSuccessful());
            assertEquals(List.of("current-result"), output.result().getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void invalidJobResultRequestReturnsSafeFailureAndAcknowledgesBrokerDelivery() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        CapturingOutput output = new CapturingOutput();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-invalid-result-request-test");
        schedulerThread.start();

        try {
            JobResultRequestMessage invalidRequest = new JobResultRequestMessage(
                    "requester-1",
                    "2026-07-04T00:00:00Z",
                    "../job",
                    "token"
            );
            mailbox.put(new MessageEnvelope(invalidRequest, "requester-1", acknowledgement));

            assertTrue(output.awaitResult());
            assertTrue(acknowledgement.awaitAck());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertEquals("", result.getJobId());
            assertFalse(result.isSuccessful());
            assertTrue(result.getErrorMessage().contains("Job id may contain only"));
            assertEquals(1, acknowledgement.ackCount());
            assertEquals(0, acknowledgement.requeueCount());
            assertEquals(0, acknowledgement.rejectCount());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void brokerDeliveryIsRequeuedWhenJobStartFailureResultCannotBeSent() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        FailingResultOutput output = new FailingResultOutput();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-start-failure-requeue-test");
        schedulerThread.start();

        try {
            JobSubmitMessage emptyJob = new JobSubmitMessage(
                    "client-1",
                    "2026-06-13T00:00:00Z",
                    "job-start-result-send-failure",
                    "TEST_TASK",
                    List.of(),
                    "",
                    "token-job-start-result-send-failure"
            );
            mailbox.put(new MessageEnvelope(emptyJob, "requester-1", acknowledgement));

            assertTrue(output.awaitAttempt());
            assertTrue(acknowledgement.awaitRequeue());
            assertEquals(0, acknowledgement.ackCount());
            assertEquals(1, acknowledgement.requeueCount());
            assertEquals(0, acknowledgement.rejectCount());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void brokerDeliveryIsRequeuedWhenJobStartFailureResultIsUnrouted() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        UnroutedResultOutput output = new UnroutedResultOutput();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-start-failure-unrouted-test");
        schedulerThread.start();

        try {
            JobSubmitMessage emptyJob = new JobSubmitMessage(
                    "client-1",
                    "2026-06-13T00:00:00Z",
                    "job-start-result-unrouted",
                    "TEST_TASK",
                    List.of(),
                    "",
                    "token-job-start-result-unrouted"
            );
            mailbox.put(new MessageEnvelope(emptyJob, "requester-1", acknowledgement));

            assertTrue(output.awaitAttempt());
            assertTrue(acknowledgement.awaitRequeue());
            assertEquals(0, acknowledgement.ackCount());
            assertEquals(1, acknowledgement.requeueCount());
            assertEquals(0, acknowledgement.rejectCount());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void duplicateActiveJobIdReturnsFailureWithoutReplacingOriginalJob() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-duplicate-job-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-duplicate", List.of("first-payload")), "requester-1"));
            mailbox.put(new MessageEnvelope(testJob("job-duplicate", List.of("second-payload")), "requester-1"));

            assertTrue(output.awaitResult());
            JobResultMessage duplicateResult = output.result();
            assertNotNull(duplicateResult);
            assertEquals("job-duplicate", duplicateResult.getJobId());
            assertFalse(duplicateResult.isSuccessful());
            assertTrue(duplicateResult.getErrorMessage().contains("already active"));

            registry.register("peer-1", new PeerInfo(
                    "peer-1",
                    SchedulerConfig.defaults(),
                    List.of("TEST_TASK")
            ));

            assertTrue(output.awaitTask());
            assertEquals("first-payload", output.task().getPayload());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void finalJobResultIsRetriedWhenFirstDeliveryFails() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo(
                "peer-1",
                SchedulerConfig.defaults(),
                List.of("TEST_TASK")
        );
        registry.register(peer.getNodeId(), peer);
        ResultRetryOutput output = new ResultRetryOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-result-retry-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-result-retry", List.of("payload")), "requester-1"));

            assertTrue(output.awaitTask());
            mailbox.put(new MessageEnvelope(successResult(output.task(), "accepted-result"), peer.getNodeId()));

            assertTrue(output.awaitFirstDeliveryFailure());
            assertTrue(output.awaitResult());
            assertEquals(2, output.resultSendAttempts());
            assertTrue(output.result().isSuccessful());
            assertEquals(List.of("accepted-result"), output.result().getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void dispatchesOnlyToPeersWithMatchingCapabilities() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register("image-peer", new PeerInfo(
                "image-peer",
                SchedulerConfig.defaults(),
                List.of("IMAGE_CONVERSION")
        ));
        registry.register("test-peer", new PeerInfo(
                "test-peer",
                SchedulerConfig.defaults(),
                List.of("TEST_TASK")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-capability-test");
        schedulerThread.start();

        try {
            JobSubmitMessage supportedJob = new JobSubmitMessage(
                    "requester-1",
                    "2026-06-04T00:00:00Z",
                    "job-capability",
                    "TEST_TASK",
                    List.of("payload"),
                    "",
                    "token-job-capability"
            );
            mailbox.put(new MessageEnvelope(supportedJob, "requester-1"));

            assertTrue(output.awaitTask());
            assertEquals("test-peer", output.peerId());
            assertEquals("TEST_TASK", output.task().getTaskType());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void staleResultFromWrongPeerIsIgnoredUntilAssignedPeerCompletesTask() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo(
                "assigned-peer",
                SchedulerConfig.defaults(),
                List.of("TEST_TASK")
        );
        registry.register(peer.getNodeId(), peer);
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-stale-result-test");
        schedulerThread.start();

        try {
            JobSubmitMessage job = testJob("job-stale", List.of("payload"));
            mailbox.put(new MessageEnvelope(job, "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();

            mailbox.put(new MessageEnvelope(successResult(assignment, "stale-result"), "wrong-peer"));
            assertFalse(output.awaitResult(300));
            assertEquals(1, peer.getActiveTasks());

            mailbox.put(new MessageEnvelope(successResult(assignment, "accepted-result"), peer.getNodeId()));
            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertTrue(result.isSuccessful());
            assertEquals(List.of("accepted-result"), result.getResultsByTaskId());
            assertEquals(0, peer.getActiveTasks());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void peerDisconnectReleasesAssignedTaskForImmediateRetryAndIgnoresStaleResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register("peer-1", new PeerInfo(
                "peer-1",
                SchedulerConfig.defaults(),
                List.of("TEST_TASK")
        ));
        MultiAssignmentOutput output = new MultiAssignmentOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-peer-disconnect-retry-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-disconnect-retry", List.of("payload")), "requester-1"));

            MultiAssignmentOutput.Assignment first = output.awaitAssignment();
            assertNotNull(first);
            assertEquals("peer-1", first.peerId());

            registry.remove("peer-1");
            registry.register("peer-2", new PeerInfo(
                    "peer-2",
                    SchedulerConfig.defaults(),
                    List.of("TEST_TASK")
            ));
            mailbox.put(new MessageEnvelope(
                    new PeerDisconnectedMessage("peer-1", "2026-06-13T00:00:00Z", "tcp_disconnect"),
                    "peer-1"
            ));

            MultiAssignmentOutput.Assignment retry = output.awaitAssignment();
            assertNotNull(retry);
            assertEquals("peer-2", retry.peerId());
            assertEquals(first.task().getTaskId(), retry.task().getTaskId());

            mailbox.put(new MessageEnvelope(successResult(first.task(), "stale-result"), "peer-1"));
            assertFalse(output.awaitResult(300));

            mailbox.put(new MessageEnvelope(successResult(retry.task(), "accepted-result"), "peer-2"));
            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertTrue(result.isSuccessful());
            assertEquals(List.of("accepted-result"), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void expiredLeaseReassignsTaskAndRejectsLateResultFromOldPeer() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_TASK_TIMEOUT_MS", "100000",
                "TASKFLOW_TASK_LEASE_MS", "150",
                "TASKFLOW_MAX_TASK_RETRIES", "2"
        ));
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register("peer-1", new PeerInfo(
                "peer-1",
                config,
                List.of("TEST_TASK")
        ));
        MultiAssignmentOutput output = new MultiAssignmentOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, null, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-expired-lease-retry-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-expired-lease-retry", List.of("payload")), "requester-1"));

            MultiAssignmentOutput.Assignment first = output.awaitAssignment();
            assertNotNull(first);
            assertEquals("peer-1", first.peerId());

            registry.remove("peer-1");
            registry.register("peer-2", new PeerInfo(
                    "peer-2",
                    config,
                    List.of("TEST_TASK")
            ));

            MultiAssignmentOutput.Assignment retry = output.awaitAssignment();
            assertNotNull(retry);
            assertEquals("peer-2", retry.peerId());
            assertEquals(first.task().getTaskId(), retry.task().getTaskId());

            mailbox.put(new MessageEnvelope(successResult(first.task(), "stale-result"), "peer-1"));
            mailbox.put(new MessageEnvelope(successResult(retry.task(), "accepted-result"), "peer-2"));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertTrue(result.isSuccessful());
            assertEquals(List.of("accepted-result"), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void peerDisconnectAtRetryLimitReturnsFailedJobResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register("peer-1", new PeerInfo(
                "peer-1",
                SchedulerConfig.defaults(),
                List.of("TEST_TASK")
        ));
        MultiAssignmentOutput output = new MultiAssignmentOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "1"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, null, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-peer-disconnect-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-disconnect-failure", List.of("payload")), "requester-1"));

            MultiAssignmentOutput.Assignment assignment = output.awaitAssignment();
            assertNotNull(assignment);
            registry.remove("peer-1");
            mailbox.put(new MessageEnvelope(
                    new PeerDisconnectedMessage("peer-1", "2026-06-13T00:00:00Z", "tcp_disconnect"),
                    "peer-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertEquals("job-disconnect-failure", result.getJobId());
            assertFalse(result.isSuccessful());
            assertTrue(result.getErrorMessage().contains("peer became unavailable"));
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void taskFailureAtRetryLimitReturnsFailedJobResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo(
                "peer-1",
                SchedulerConfig.defaults(),
                List.of("TEST_TASK")
        );
        registry.register(peer.getNodeId(), peer);
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "1"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, null, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-terminal-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-terminal-failure", List.of("payload")), "requester-1"));

            assertTrue(output.awaitTask());
            mailbox.put(new MessageEnvelope(failedResult(output.task(), "processor failed"), peer.getNodeId()));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertEquals("job-terminal-failure", result.getJobId());
            assertFalse(result.isSuccessful());
            assertTrue(result.getErrorMessage().contains("reached max retries"));
            assertEquals(0, peer.getActiveTasks());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void taskTimeoutAtRetryLimitReturnsFailedJobResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo(
                "slow-peer",
                SchedulerConfig.defaults(),
                List.of("TEST_TASK")
        );
        registry.register(peer.getNodeId(), peer);
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_TASK_TIMEOUT_MS", "1",
                "TASKFLOW_MAX_TASK_RETRIES", "1"
        ));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, null, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-timeout-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-timeout", List.of("payload")), "requester-1"));

            assertTrue(output.awaitTask());
            assertTrue(output.awaitResult(2_000));
            JobResultMessage result = output.result();
            assertNotNull(result);
            assertEquals("job-timeout", result.getJobId());
            assertFalse(result.isSuccessful());
            assertTrue(result.getErrorMessage().contains("exceeded max retries"));
            assertEquals(0, peer.getActiveTasks());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void dispatchFailureResetsPeerLoadAndRetriesTask() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        PeerInfo peer = new PeerInfo(
                "retry-peer",
                SchedulerConfig.defaults(),
                List.of("TEST_TASK")
        );
        registry.register(peer.getNodeId(), peer);
        FlakyTaskOutput output = new FlakyTaskOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registry,
                null,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-dispatch-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-dispatch-retry", List.of("payload")), "requester-1"));

            assertTrue(output.awaitSuccessfulTask());
            assertEquals(2, output.sendAttempts());
            assertEquals("retry-peer", output.peerId());
            assertEquals(1, peer.getActiveTasks());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    private static JobSubmitMessage testJob(String jobId, List<Object> payloads) {
        return new JobSubmitMessage(
                "requester-1",
                "2026-06-10T00:00:00Z",
                jobId,
                "TEST_TASK",
                payloads,
                "",
                "token-" + jobId
        );
    }

    private static TaskResultMessage successResult(TaskAssignMessage assignment, Object payload) {
        return new TaskResultMessage(
                "peer",
                "2026-06-10T00:00:00Z",
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
                "peer",
                "2026-06-10T00:00:00Z",
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                null,
                false,
                error
        );
    }

    private static TaskResultMessage legacyResult(TaskAssignMessage assignment, int protocolVersion) {
        return new Gson().fromJson("""
                {
                  "protocolVersion":%d,
                  "type":"TASK_RESULT",
                  "nodeId":"peer",
                  "time":"2026-07-22T06:00:00Z",
                  "taskId":"%s",
                  "jobId":"%s",
                  "successful":true,
                  "resultPayload":"legacy-result"
                }
                """.formatted(protocolVersion, assignment.getTaskId(), assignment.getJobId()),
                TaskResultMessage.class);
    }

    private static class CapturingOutput implements SchedulerOutput {
        private final CountDownLatch resultReceived = new CountDownLatch(1);
        private final AtomicReference<JobResultMessage> result = new AtomicReference<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            throw new AssertionError("Unsupported jobs should not dispatch tasks.");
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            result.set(message);
            resultReceived.countDown();
            return true;
        }

        boolean awaitResult() throws InterruptedException {
            return resultReceived.await(2, TimeUnit.SECONDS);
        }

        JobResultMessage result() {
            return result.get();
        }
    }

    private static class TaskCapturingOutput implements SchedulerOutput {
        private final CountDownLatch taskReceived = new CountDownLatch(1);
        private final CountDownLatch resultReceived = new CountDownLatch(1);
        private final AtomicReference<String> peerId = new AtomicReference<>();
        private final AtomicReference<TaskAssignMessage> task = new AtomicReference<>();
        private final AtomicReference<JobResultMessage> result = new AtomicReference<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            peerId.set(peer.getNodeId());
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

        String peerId() {
            return peerId.get();
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

    private static class MultiAssignmentOutput implements SchedulerOutput {
        private final BlockingQueue<Assignment> assignments = new LinkedBlockingQueue<>();
        private final CountDownLatch resultReceived = new CountDownLatch(1);
        private final AtomicReference<JobResultMessage> result = new AtomicReference<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            assignments.offer(new Assignment(peer.getNodeId(), message));
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            result.set(message);
            resultReceived.countDown();
            return true;
        }

        Assignment awaitAssignment() throws InterruptedException {
            return assignments.poll(2, TimeUnit.SECONDS);
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

        record Assignment(String peerId, TaskAssignMessage task) {
        }
    }

    private static class FlakyTaskOutput implements SchedulerOutput {
        private final AtomicInteger attempts = new AtomicInteger();
        private final CountDownLatch successfulTask = new CountDownLatch(1);
        private final AtomicReference<String> peerId = new AtomicReference<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) throws Exception {
            int attempt = attempts.incrementAndGet();
            if (attempt == 1) {
                throw new Exception("transient dispatch failure");
            }
            peerId.set(peer.getNodeId());
            successfulTask.countDown();
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            return true;
        }

        boolean awaitSuccessfulTask() throws InterruptedException {
            return successfulTask.await(2, TimeUnit.SECONDS);
        }

        int sendAttempts() {
            return attempts.get();
        }

        String peerId() {
            return peerId.get();
        }
    }

    private static class ResultRetryOutput implements SchedulerOutput {
        private final CountDownLatch taskReceived = new CountDownLatch(1);
        private final CountDownLatch firstDeliveryFailed = new CountDownLatch(1);
        private final CountDownLatch resultReceived = new CountDownLatch(1);
        private final AtomicInteger resultAttempts = new AtomicInteger();
        private final AtomicReference<TaskAssignMessage> task = new AtomicReference<>();
        private final AtomicReference<JobResultMessage> result = new AtomicReference<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            task.set(message);
            taskReceived.countDown();
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            int attempt = resultAttempts.incrementAndGet();
            if (attempt == 1) {
                firstDeliveryFailed.countDown();
                return false;
            }
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

        boolean awaitFirstDeliveryFailure() throws InterruptedException {
            return firstDeliveryFailed.await(2, TimeUnit.SECONDS);
        }

        boolean awaitResult() throws InterruptedException {
            return resultReceived.await(3, TimeUnit.SECONDS);
        }

        int resultSendAttempts() {
            return resultAttempts.get();
        }

        JobResultMessage result() {
            return result.get();
        }
    }

    private static class FailingResultOutput implements SchedulerOutput {
        private final CountDownLatch attempted = new CountDownLatch(1);

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            throw new AssertionError("Failed job starts should not dispatch tasks.");
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) throws Exception {
            attempted.countDown();
            throw new Exception("transient result send failure");
        }

        boolean awaitAttempt() throws InterruptedException {
            return attempted.await(2, TimeUnit.SECONDS);
        }
    }

    private static class UnroutedResultOutput implements SchedulerOutput {
        private final CountDownLatch attempted = new CountDownLatch(1);

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            throw new AssertionError("Failed job starts should not dispatch tasks.");
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            attempted.countDown();
            return false;
        }

        boolean awaitAttempt() throws InterruptedException {
            return attempted.await(2, TimeUnit.SECONDS);
        }
    }

    private static class RecordingAcknowledgement implements TransportAcknowledgement {
        private final CountDownLatch acked = new CountDownLatch(1);
        private final CountDownLatch requeued = new CountDownLatch(1);
        private final CountDownLatch rejected = new CountDownLatch(1);
        private final AtomicInteger ackCount = new AtomicInteger();
        private final AtomicInteger requeueCount = new AtomicInteger();
        private final AtomicInteger rejectCount = new AtomicInteger();

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
            rejected.countDown();
        }

        boolean awaitAck() throws InterruptedException {
            return acked.await(2, TimeUnit.SECONDS);
        }

        boolean awaitRequeue() throws InterruptedException {
            return requeued.await(2, TimeUnit.SECONDS);
        }

        boolean awaitReject() throws InterruptedException {
            return rejected.await(2, TimeUnit.SECONDS);
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
    }
}
