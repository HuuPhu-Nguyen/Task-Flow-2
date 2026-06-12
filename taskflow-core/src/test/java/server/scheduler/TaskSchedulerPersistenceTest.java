package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskSchedulerPersistenceTest {

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

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobCompleted());
            assertEquals(List.of(
                    "insertJob:job-success:TEST_TASK:requester-1:1",
                    "insertTask:task-job-success-0:job-success",
                    "markTaskAssigned:task-job-success-0:peer-1",
                    "markTaskCompleted:task-job-success-0",
                    "markJobCompleted:job-success"
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
                    "insertJob:job-failure:TEST_TASK:requester-1:1",
                    "insertTask:task-job-failure-0:job-failure",
                    "markTaskAssigned:task-job-failure-0:peer-1",
                    "markTaskFailed:task-job-failure-0",
                    "markJobFailed:job-failure"
            ), store.events());
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
        return new JobSubmitMessage(
                "requester-1",
                "2026-06-12T00:00:00Z",
                jobId,
                "TEST_TASK",
                payloads,
                ""
        );
    }

    private static TaskResultMessage successResult(TaskAssignMessage assignment, Object payload) {
        return new TaskResultMessage(
                "peer-1",
                "2026-06-12T00:00:00Z",
                assignment.getTaskId(),
                assignment.getJobId(),
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
                null,
                false,
                error
        );
    }

    private static class TaskCapturingOutput implements SchedulerOutput {
        private final CountDownLatch taskReceived = new CountDownLatch(1);
        private final CountDownLatch resultReceived = new CountDownLatch(1);
        private final AtomicReference<TaskAssignMessage> task = new AtomicReference<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            task.set(message);
            taskReceived.countDown();
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
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
            return resultReceived.await(2, TimeUnit.SECONDS);
        }
    }

    private static class RecordingJobStateStore implements JobStateStore {
        private final List<String> events = new ArrayList<>();
        private final CountDownLatch taskAssigned = new CountDownLatch(1);
        private final CountDownLatch jobCompleted = new CountDownLatch(1);
        private final CountDownLatch jobFailed = new CountDownLatch(1);

        @Override
        public synchronized void insertJob(String jobId, String taskType, String requesterId, int fileCount) {
            events.add("insertJob:" + jobId + ":" + taskType + ":" + requesterId + ":" + fileCount);
        }

        @Override
        public synchronized void insertTask(String taskId, String jobId) {
            events.add("insertTask:" + taskId + ":" + jobId);
        }

        @Override
        public synchronized void markTaskAssigned(String taskId, String peerId, long startedAt) {
            events.add("markTaskAssigned:" + taskId + ":" + peerId);
            taskAssigned.countDown();
        }

        @Override
        public synchronized void markTaskCompleted(String taskId, long completedAt, long durationMs) {
            events.add("markTaskCompleted:" + taskId);
        }

        @Override
        public synchronized void markTaskRetried(String taskId, int retryCount) {
            events.add("markTaskRetried:" + taskId + ":" + retryCount);
        }

        @Override
        public synchronized void markTaskFailed(String taskId) {
            events.add("markTaskFailed:" + taskId);
        }

        @Override
        public synchronized void markJobCompleted(String jobId) {
            events.add("markJobCompleted:" + jobId);
            jobCompleted.countDown();
        }

        @Override
        public synchronized void markJobFailed(String jobId) {
            events.add("markJobFailed:" + jobId);
            jobFailed.countDown();
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
    }
}
