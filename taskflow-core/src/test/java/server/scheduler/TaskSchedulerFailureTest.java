package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
                output
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
}
