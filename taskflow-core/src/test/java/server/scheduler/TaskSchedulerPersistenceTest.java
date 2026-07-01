package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.RequesterIdentity;
import protocol.RequesterTokens;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.JobStateStore;
import server.job.EmbarrassinglyParallelJob;
import server.job.JobFactory;
import server.job.TaskUnit;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
                    "insertJobWithTasks:job-success:TEST_TASK:requester-1:1:task-job-success-0",
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
    void successfulJobDeliversAndPersistsSemanticResultPayload() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-semantic-result-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-semantic-result", List.of("payload"), "SEMANTIC_RESULT"),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(successResult(assignment, "alpha"), "peer-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobCompleted());
            JobResultMessage result = output.result();
            Map<String, Object> expectedPayload = Map.of(
                    "resultCount", 1,
                    "joined", "alpha");
            assertTrue(result.isSuccessful());
            assertEquals(expectedPayload, result.getResultPayload());
            assertEquals(List.of("alpha"), result.getResultsByTaskId());
            assertEquals(expectedPayload, store.lastCompletedResultPayload());
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
                    "insertJobWithTasks:job-failure:TEST_TASK:requester-1:1:task-job-failure-0",
                    "markTaskAssigned:task-job-failure-0:peer-1",
                    "markTaskFailed:task-job-failure-0",
                    "markJobFailed:job-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void startupPersistenceFailureReturnsFailureWithoutDispatchingTask() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(false);
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-persistence-startup-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-persistence-failure", List.of("payload")), "requester-1"));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-persistence-failure", result.getJobId());
            assertEquals("Job could not be persisted.", result.getErrorMessage());
            assertNull(output.task());
            assertEquals(List.of(
                    "insertJobWithTasks:job-persistence-failure:TEST_TASK:requester-1:1:task-job-persistence-failure-0"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void persistedJobIdCollisionReturnsFailureWithoutStartupWrite() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        store.addExistingJobId("job-persisted-duplicate");
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-persisted-duplicate-job-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    testJob("job-persisted-duplicate", List.of("payload")),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-persisted-duplicate", result.getJobId());
            assertTrue(result.getErrorMessage().contains("already exists in persisted history"));
            assertNull(output.task());
            assertEquals(List.of(), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void assignmentPersistenceFailureReturnsFailureWithoutDispatchingTask() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markTaskAssigned");
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-assignment-persistence-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-assignment-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobFailed());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-assignment-persistence-failure", result.getJobId());
            assertEquals("Persistence write failed during markTaskAssigned.", result.getErrorMessage());
            assertNull(output.task());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(List.of(
                    "insertJobWithTasks:job-assignment-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-assignment-persistence-failure-0",
                    "markTaskAssigned:task-job-assignment-persistence-failure-0:peer-1",
                    "markTaskFailed:task-job-assignment-persistence-failure-0",
                    "markJobFailed:job-assignment-persistence-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void retryPersistenceFailureReturnsFailureInsteadOfRedispatching() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markTaskRetried");
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "2"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-retry-persistence-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-retry-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(failedResult(assignment, "processor failed"), "peer-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobFailed());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("Persistence write failed during markTaskRetried.", result.getErrorMessage());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(List.of(
                    "insertJobWithTasks:job-retry-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-retry-persistence-failure-0",
                    "markTaskAssigned:task-job-retry-persistence-failure-0:peer-1",
                    "markTaskRetried:task-job-retry-persistence-failure-0:1",
                    "markTaskFailed:task-job-retry-persistence-failure-0",
                    "markJobFailed:job-retry-persistence-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void taskCompletionPersistenceFailureReturnsFailedJobResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markTaskCompleted");
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-task-completion-persistence-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-completion-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertTrue(output.awaitResult());
            assertTrue(store.awaitJobFailed());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("Persistence write failed during markTaskCompleted.", result.getErrorMessage());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(List.of(
                    "insertJobWithTasks:job-completion-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-completion-persistence-failure-0",
                    "markTaskAssigned:task-job-completion-persistence-failure-0:peer-1",
                    "markTaskCompleted:task-job-completion-persistence-failure-0",
                    "markTaskFailed:task-job-completion-persistence-failure-0",
                    "markJobFailed:job-completion-persistence-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void finalJobCompletionPersistenceFailureDoesNotKeepJobActiveAfterResultDelivery() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markJobCompleted");
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, SchedulerConfig.defaults());
        Thread schedulerThread = new Thread(scheduler, "scheduler-final-persistence-failure-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-final-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertTrue(result.isSuccessful());
            assertEquals("job-final-persistence-failure", result.getJobId());
            assertTrue(store.awaitEvent("markJobCompleted:job-final-persistence-failure"));
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(List.of(
                    "insertJobWithTasks:job-final-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-final-persistence-failure-0",
                    "markTaskAssigned:task-job-final-persistence-failure-0:peer-1",
                    "markTaskCompleted:task-job-final-persistence-failure-0",
                    "markJobCompleted:job-final-persistence-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void finalJobFailurePersistenceFailureDoesNotKeepJobActiveAfterResultDelivery() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore(true, "markJobFailed");
        TaskCapturingOutput output = new TaskCapturingOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_TASK_RETRIES", "1"));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-final-failure-persistence-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-final-failure-persistence-failure", List.of("payload")),
                    "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(failedResult(assignment, "processor failed"), "peer-1"));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-final-failure-persistence-failure", result.getJobId());
            assertTrue(store.awaitEvent("markJobFailed:job-final-failure-persistence-failure"));
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(List.of(
                    "insertJobWithTasks:job-final-failure-persistence-failure:TEST_TASK:requester-1:1:"
                            + "task-job-final-failure-persistence-failure-0",
                    "markTaskAssigned:task-job-final-failure-persistence-failure-0:peer-1",
                    "markTaskFailed:task-job-final-failure-persistence-failure-0",
                    "markJobFailed:job-final-failure-persistence-failure"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void undeliverableFinalJobResultIsAbandonedAndPersistedFailed() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        InMemoryPeerRegistry registry = registryWithPeer("peer-1");
        RecordingJobStateStore store = new RecordingJobStateStore();
        UndeliverableResultOutput output = new UndeliverableResultOutput();
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_JOB_RESULT_MAX_DELIVERY_ATTEMPTS", "1"
        ));
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-result-abandonment-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(testJob("job-result-abandoned", List.of("payload")), "requester-1"));

            assertTrue(output.awaitTask());
            TaskAssignMessage assignment = output.task();
            assertTrue(store.awaitTaskAssigned());

            mailbox.put(new MessageEnvelope(successResult(assignment, "result"), "peer-1"));

            assertTrue(output.awaitResultAttempt());
            assertTrue(store.awaitJobFailed());
            assertEquals(1, output.resultAttempts());
            assertTrue(awaitActiveJobs(scheduler, 0));
            assertEquals(List.of(
                    "insertJobWithTasks:job-result-abandoned:TEST_TASK:requester-1:1:task-job-result-abandoned-0",
                    "markTaskAssigned:task-job-result-abandoned-0:peer-1",
                    "markTaskCompleted:task-job-result-abandoned-0",
                    "markJobFailed:job-result-abandoned"
            ), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void restoredCompletedJobPublishesFinalResultAndPersistsCompletion() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        EmbarrassinglyParallelJob<?, ?> restoredJob = restoredCompletedJob(
                "job-restored-complete",
                "payload",
                "restored-result"
        );

        scheduler.restoreJobs(List.of(restoredJob));

        assertTrue(output.awaitResult());
        assertTrue(store.awaitJobCompleted());
        JobResultMessage result = output.result();
        assertTrue(result.isSuccessful());
        assertEquals("job-restored-complete", result.getJobId());
        assertEquals(List.of("restored-result"), result.getResultsByTaskId());
        assertEquals(List.of("markJobCompleted:job-restored-complete"), store.events());
    }

    @Test
    void completedResultRequestLoadsPersistedJobResult() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-completed-result",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                "",
                List.of("result-alpha", "result-beta")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-completed-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-1",
                            "2026-06-25T00:00:00Z",
                            "job-completed-result",
                            requesterToken
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertTrue(result.isSuccessful());
            assertEquals("job-completed-result", result.getJobId());
            assertEquals("TEST_TASK", result.getTaskType());
            assertEquals(List.of("result-alpha", "result-beta"), result.getResultsByTaskId());
            assertEquals(List.of("loadCompletedJobResult:job-completed-result"), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestRejectsWrongRequesterToken() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-owned-result",
                "TEST_TASK",
                RequesterTokens.hashToken("owner-token"),
                "",
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-wrong-token-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-2",
                            "2026-06-25T00:00:00Z",
                            "job-owned-result",
                            "wrong-token"
                    ),
                    "requester-2"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-owned-result", result.getJobId());
            assertEquals("Requester token does not match job owner.", result.getErrorMessage());
            assertEquals(List.of(), result.getResultsByTaskId());
            assertEquals(List.of("loadCompletedJobResult:job-owned-result"), store.events());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void signedJobSubmitPersistsRequesterIdentityKey() throws Exception {
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
        Thread schedulerThread = new Thread(scheduler, "scheduler-signed-submit-test");
        schedulerThread.start();

        try {
            RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
            mailbox.put(new MessageEnvelope(
                    signedTestJob("job-signed-submit", List.of("payload"), "token-signed-submit", identity),
                    "requester-1"
            ));

            assertTrue(output.awaitTask());
            assertEquals(identity.publicKey(), store.lastRequesterIdentityKey());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void signedJobSubmitRejectsInvalidRequesterIdentitySignature() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                registryWithPeer("peer-1"),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-invalid-submit-signature-test");
        schedulerThread.start();

        try {
            RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
            String time = "2026-06-26T00:00:00Z";
            String signature = RequesterIdentity.signJobSubmit(
                    identity.privateKey(),
                    "requester-1",
                    time,
                    "job-invalid-signature",
                    "TEST_TASK",
                    "",
                    "different-token"
            );
            mailbox.put(new MessageEnvelope(
                    new JobSubmitMessage(
                            "requester-1",
                            time,
                            "job-invalid-signature",
                            "TEST_TASK",
                            List.of("payload"),
                            "",
                            "token-invalid-signature",
                            identity.publicKey(),
                            signature
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("Requester identity signature is invalid.", result.getErrorMessage());
            assertTrue(store.events().isEmpty());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestRequiresSignedRequesterIdentityForIdentityBoundJob() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-identity-bound-result",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                identity.publicKey(),
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-missing-identity-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-1",
                            "2026-06-26T00:00:00Z",
                            "job-identity-bound-result",
                            requesterToken
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-identity-bound-result", result.getJobId());
            assertEquals("Requester identity signature is required.", result.getErrorMessage());
            assertEquals(List.of(), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestRejectsMismatchedRequesterIdentityKey() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        RequesterIdentity.Credentials ownerIdentity = RequesterIdentity.newCredentials();
        RequesterIdentity.Credentials wrongIdentity = RequesterIdentity.newCredentials();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-wrong-identity-result",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                ownerIdentity.publicKey(),
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-wrong-identity-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    signedResultRequest(
                            "requester-1",
                            "job-wrong-identity-result",
                            requesterToken,
                            wrongIdentity
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-wrong-identity-result", result.getJobId());
            assertEquals("Requester identity key does not match job owner.", result.getErrorMessage());
            assertEquals(List.of(), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestRejectsInvalidRequesterIdentitySignature() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-invalid-result-signature",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                identity.publicKey(),
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-invalid-result-signature-test");
        schedulerThread.start();

        try {
            String time = "2026-06-26T00:00:00Z";
            String signature = RequesterIdentity.signJobResultRequest(
                    identity.privateKey(),
                    "requester-1",
                    time,
                    "different-job",
                    requesterToken
            );
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-1",
                            time,
                            "job-invalid-result-signature",
                            requesterToken,
                            identity.publicKey(),
                            signature
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-invalid-result-signature", result.getJobId());
            assertEquals("Requester identity signature is invalid.", result.getErrorMessage());
            assertEquals(List.of(), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestAcceptsValidRequesterIdentitySignature() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        RecordingJobStateStore store = new RecordingJobStateStore();
        String requesterToken = "requester-token";
        RequesterIdentity.Credentials identity = RequesterIdentity.newCredentials();
        store.setCompletedResult(new JobStateStore.CompletedJobResultState(
                "job-signed-result",
                "TEST_TASK",
                RequesterTokens.hashToken(requesterToken),
                identity.publicKey(),
                List.of("secret-result")
        ));
        TaskCapturingOutput output = new TaskCapturingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                SchedulerConfig.defaults()
        );
        Thread schedulerThread = new Thread(scheduler, "scheduler-signed-result-request-test");
        schedulerThread.start();

        try {
            mailbox.put(new MessageEnvelope(
                    signedResultRequest(
                            "requester-1",
                            "job-signed-result",
                            requesterToken,
                            identity
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertTrue(result.isSuccessful());
            assertEquals("job-signed-result", result.getJobId());
            assertEquals(List.of("secret-result"), result.getResultsByTaskId());
        } finally {
            schedulerThread.interrupt();
            schedulerThread.join(2_000);
        }
    }

    @Test
    void completedResultRequestReportsRunningJobWithoutRemovingIt() throws Exception {
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
        Thread schedulerThread = new Thread(scheduler, "scheduler-running-result-request-test");
        schedulerThread.start();

        try {
            JobSubmitMessage submit = testJob("job-running-result-request", List.of("payload"));
            mailbox.put(new MessageEnvelope(submit, "requester-1"));

            assertTrue(output.awaitTask());
            mailbox.put(new MessageEnvelope(
                    new JobResultRequestMessage(
                            "requester-1",
                            "2026-06-25T00:00:00Z",
                            "job-running-result-request",
                            submit.getRequesterToken()
                    ),
                    "requester-1"
            ));

            assertTrue(output.awaitResult());
            JobResultMessage result = output.result();
            assertFalse(result.isSuccessful());
            assertEquals("job-running-result-request", result.getJobId());
            assertEquals("Job is still running.", result.getErrorMessage());
            assertTrue(awaitActiveJobs(scheduler, 1));
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
        return testJob(jobId, payloads, "");
    }

    private static JobSubmitMessage testJob(String jobId, List<Object> payloads, String parameter) {
        return new JobSubmitMessage(
                "requester-1",
                "2026-06-12T00:00:00Z",
                jobId,
                "TEST_TASK",
                payloads,
                parameter,
                "token-" + jobId
        );
    }

    private static JobSubmitMessage signedTestJob(String jobId,
                                                  List<Object> payloads,
                                                  String requesterToken,
                                                  RequesterIdentity.Credentials identity) {
        String time = "2026-06-26T00:00:00Z";
        String signature = RequesterIdentity.signJobSubmit(
                identity.privateKey(),
                "requester-1",
                time,
                jobId,
                "TEST_TASK",
                "",
                requesterToken
        );
        return new JobSubmitMessage(
                "requester-1",
                time,
                jobId,
                "TEST_TASK",
                payloads,
                "",
                requesterToken,
                identity.publicKey(),
                signature
        );
    }

    private static JobResultRequestMessage signedResultRequest(String requesterId,
                                                               String jobId,
                                                               String requesterToken,
                                                               RequesterIdentity.Credentials identity) {
        String time = "2026-06-26T00:00:00Z";
        String signature = RequesterIdentity.signJobResultRequest(
                identity.privateKey(),
                requesterId,
                time,
                jobId,
                requesterToken
        );
        return new JobResultRequestMessage(
                requesterId,
                time,
                jobId,
                requesterToken,
                identity.publicKey(),
                signature
        );
    }

    private static EmbarrassinglyParallelJob<?, ?> restoredCompletedJob(String jobId,
                                                                        Object payload,
                                                                        Object resultPayload) {
        JobSubmitMessage submit = testJob(jobId, List.of(payload));
        EmbarrassinglyParallelJob<?, ?> job = JobFactory.create(submit, "requester-1");
        job.initializeTasks(submit);
        assertTrue(job.restoreTaskForResume(
                "task-" + jobId + "-0",
                TaskUnit.TaskStatus.COMPLETED,
                resultPayload,
                0
        ));
        return job;
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

    private static boolean awaitActiveJobs(TaskScheduler scheduler, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (scheduler.getMetricsSnapshot().activeJobs() == expected) {
                return true;
            }
            Thread.sleep(10);
        }
        return scheduler.getMetricsSnapshot().activeJobs() == expected;
    }

    private static class TaskCapturingOutput implements SchedulerOutput {
        private final CountDownLatch taskReceived = new CountDownLatch(1);
        private final CountDownLatch resultReceived = new CountDownLatch(1);
        private final AtomicReference<TaskAssignMessage> task = new AtomicReference<>();
        private final AtomicReference<JobResultMessage> result = new AtomicReference<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
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

        TaskAssignMessage task() {
            return task.get();
        }

        boolean awaitResult() throws InterruptedException {
            return resultReceived.await(2, TimeUnit.SECONDS);
        }

        JobResultMessage result() {
            return result.get();
        }
    }

    private static class UndeliverableResultOutput implements SchedulerOutput {
        private final CountDownLatch taskReceived = new CountDownLatch(1);
        private final CountDownLatch resultAttempted = new CountDownLatch(1);
        private final AtomicReference<TaskAssignMessage> task = new AtomicReference<>();
        private final AtomicInteger resultAttempts = new AtomicInteger();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            task.set(message);
            taskReceived.countDown();
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            resultAttempts.incrementAndGet();
            resultAttempted.countDown();
            return false;
        }

        boolean awaitTask() throws InterruptedException {
            return taskReceived.await(2, TimeUnit.SECONDS);
        }

        TaskAssignMessage task() {
            return task.get();
        }

        boolean awaitResultAttempt() throws InterruptedException {
            return resultAttempted.await(2, TimeUnit.SECONDS);
        }

        int resultAttempts() {
            return resultAttempts.get();
        }
    }

    private static class RecordingJobStateStore implements JobStateStore {
        private final List<String> events = new ArrayList<>();
        private final CountDownLatch taskAssigned = new CountDownLatch(1);
        private final CountDownLatch jobCompleted = new CountDownLatch(1);
        private final CountDownLatch jobFailed = new CountDownLatch(1);
        private final java.util.Set<String> existingJobIds = new java.util.LinkedHashSet<>();
        private final boolean jobStartupPersists;
        private final String failingOperation;
        private JobStateStore.CompletedJobResultState completedResult;
        private String lastRequesterIdentityKey = "";
        private Object lastCompletedResultPayload;

        private RecordingJobStateStore() {
            this(true);
        }

        private RecordingJobStateStore(boolean jobStartupPersists) {
            this(jobStartupPersists, "");
        }

        private RecordingJobStateStore(boolean jobStartupPersists, String failingOperation) {
            this.jobStartupPersists = jobStartupPersists;
            this.failingOperation = failingOperation == null ? "" : failingOperation;
        }

        @Override
        public synchronized boolean insertJobWithTasks(String jobId,
                                                       String taskType,
                                                       String requesterId,
                                                       String requesterTokenHash,
                                                       String requesterIdentityKey,
                                                       String parameter,
                                                       Collection<TaskStartupState> tasks) {
            lastRequesterIdentityKey = requesterIdentityKey == null ? "" : requesterIdentityKey;
            return JobStateStore.super.insertJobWithTasks(
                    jobId,
                    taskType,
                    requesterId,
                    requesterTokenHash,
                    requesterIdentityKey,
                    parameter,
                    tasks
            );
        }

        @Override
        public synchronized boolean insertJobWithTasks(String jobId,
                                                       String taskType,
                                                       String requesterId,
                                                       int fileCount,
                                                       Collection<String> taskIds) {
            events.add("insertJobWithTasks:" + jobId + ":" + taskType + ":" + requesterId + ":"
                    + fileCount + ":" + String.join(",", taskIds));
            if (jobStartupPersists) {
                existingJobIds.add(jobId);
            }
            return jobStartupPersists;
        }

        @Override
        public synchronized boolean insertJob(String jobId, String taskType, String requesterId, int fileCount) {
            events.add("insertJob:" + jobId + ":" + taskType + ":" + requesterId + ":" + fileCount);
            existingJobIds.add(jobId);
            return true;
        }

        @Override
        public synchronized boolean hasJob(String jobId) {
            return existingJobIds.contains(jobId);
        }

        @Override
        public synchronized boolean insertTask(String taskId, String jobId) {
            events.add("insertTask:" + taskId + ":" + jobId);
            return true;
        }

        @Override
        public synchronized boolean markTaskAssigned(String taskId, String peerId, long startedAt) {
            events.add("markTaskAssigned:" + taskId + ":" + peerId);
            taskAssigned.countDown();
            return succeeds("markTaskAssigned");
        }

        @Override
        public synchronized boolean markTaskCompleted(String taskId, long completedAt, long durationMs) {
            events.add("markTaskCompleted:" + taskId);
            return succeeds("markTaskCompleted");
        }

        @Override
        public synchronized boolean markTaskRetried(String taskId, int retryCount) {
            events.add("markTaskRetried:" + taskId + ":" + retryCount);
            return succeeds("markTaskRetried");
        }

        @Override
        public synchronized boolean markTaskFailed(String taskId) {
            events.add("markTaskFailed:" + taskId);
            return succeeds("markTaskFailed");
        }

        @Override
        public synchronized boolean markJobCompleted(String jobId) {
            events.add("markJobCompleted:" + jobId);
            if (succeeds("markJobCompleted")) {
                jobCompleted.countDown();
                return true;
            }
            return false;
        }

        @Override
        public synchronized boolean markJobCompleted(String jobId, Object resultPayload) {
            lastCompletedResultPayload = resultPayload;
            return markJobCompleted(jobId);
        }

        @Override
        public synchronized boolean markJobFailed(String jobId) {
            events.add("markJobFailed:" + jobId);
            if (succeeds("markJobFailed")) {
                jobFailed.countDown();
                return true;
            }
            return false;
        }

        @Override
        public int markRunningJobsFailedOnStartup(long completedAt) {
            return 0;
        }

        @Override
        public synchronized Optional<JobStateStore.CompletedJobResultState> loadCompletedJobResult(String jobId) {
            events.add("loadCompletedJobResult:" + jobId);
            if (completedResult != null && completedResult.jobId().equals(jobId)) {
                return Optional.of(completedResult);
            }
            return Optional.empty();
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

        synchronized String lastRequesterIdentityKey() {
            return lastRequesterIdentityKey;
        }

        synchronized Object lastCompletedResultPayload() {
            return lastCompletedResultPayload;
        }

        synchronized void setCompletedResult(JobStateStore.CompletedJobResultState completedResult) {
            this.completedResult = completedResult;
        }

        synchronized void addExistingJobId(String jobId) {
            existingJobIds.add(jobId);
        }

        private boolean succeeds(String operation) {
            return !operation.equals(failingOperation);
        }

        private boolean awaitEvent(String expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline) {
                synchronized (this) {
                    if (events.contains(expected)) {
                        return true;
                    }
                }
                Thread.sleep(10);
            }
            synchronized (this) {
                return events.contains(expected);
            }
        }
    }
}
