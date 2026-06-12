package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;

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
                    ""
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
                    ""
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
                    ""
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
                ""
        );
    }

    private static TaskResultMessage successResult(TaskAssignMessage assignment, Object payload) {
        return new TaskResultMessage(
                "peer",
                "2026-06-10T00:00:00Z",
                assignment.getTaskId(),
                assignment.getJobId(),
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
                null,
                false,
                error
        );
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
}
