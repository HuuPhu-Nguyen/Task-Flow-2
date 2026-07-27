package server.scheduler;

import objectstore.ObjectReference;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.Test;
import protocol.AdmissionRejection;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.PayloadLimits;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.JobFactory;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;

import java.util.List;
import java.util.Map;
import java.util.Base64;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class SchedulerAdmissionTest {

    @Test
    void activeJobRejectionPreservesAcceptedProjection() throws Exception {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "1",
                "TASKFLOW_MAX_ACTIVE_TASKS", "10"
        ));
        try (Fixture fixture = new Fixture(config)) {
            fixture.submit(testSubmission("job-active-1", List.of("a")));
            fixture.submit(testSubmission("job-active-2", List.of("b")));

            JobResultMessage rejected = fixture.awaitResult();

            assertRejection(
                    rejected,
                    AdmissionRejection.Limit.MAX_ACTIVE_JOBS,
                    1L,
                    2L
            );
            assertEquals(1L, fixture.scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1L, fixture.scheduler.getMetricsSnapshot().activeTasks());
            assertEquals(1L, fixture.scheduler.getMetricsSnapshot().jobsAcceptedTotal());
        }
    }

    @Test
    void activeTaskRejectionPreservesAcceptedProjection() throws Exception {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "10",
                "TASKFLOW_MAX_ACTIVE_TASKS", "2"
        ));
        try (Fixture fixture = new Fixture(config)) {
            fixture.submit(testSubmission("job-tasks-1", List.of("a", "b")));
            fixture.submit(testSubmission("job-tasks-2", List.of("c")));

            JobResultMessage rejected = fixture.awaitResult();

            assertRejection(
                    rejected,
                    AdmissionRejection.Limit.MAX_ACTIVE_TASKS,
                    2L,
                    3L
            );
            assertEquals(1L, fixture.scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(2L, fixture.scheduler.getMetricsSnapshot().activeTasks());
        }
    }

    @Test
    void exactReplayBypassesAdmissionWhileActiveStateIsFull() throws Exception {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "1",
                "TASKFLOW_MAX_ACTIVE_TASKS", "1"
        ));
        JobSubmitMessage original = testSubmission("job-replay-full", List.of("a"));
        try (Fixture fixture = new Fixture(config)) {
            fixture.submit(original);
            fixture.submit(new JobSubmitMessage(
                    original.getNodeId(),
                    original.getTime(),
                    original.getJobId(),
                    original.getTaskType(),
                    original.getTaskPayloads(),
                    original.getParameter(),
                    original.getRequesterToken()
            ));

            JobResultMessage replay = fixture.awaitResult();

            assertFalse(replay.isSuccessful());
            assertEquals("Job is still running.", replay.getErrorMessage());
            assertNull(replay.getAdmissionRejection());
            assertEquals(1L, fixture.scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(1L, fixture.scheduler.getMetricsSnapshot().activeTasks());
        }
    }

    @Test
    void recoveredStateAboveLoweredBoundsIsRetainedAndBlocksNewAdmission() throws Exception {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "1",
                "TASKFLOW_MAX_ACTIVE_TASKS", "1"
        ));
        try (Fixture fixture = new Fixture(config, false)) {
            fixture.scheduler.restoreJobs(List.of(
                    initializedJob("job-recovered-1", List.of("a")),
                    initializedJob("job-recovered-2", List.of("b"))
            ));
            fixture.start();
            fixture.submit(testSubmission("job-after-recovery", List.of("c")));

            JobResultMessage rejected = fixture.awaitResult();

            assertRejection(
                    rejected,
                    AdmissionRejection.Limit.MAX_ACTIVE_JOBS,
                    1L,
                    3L
            );
            assertEquals(2L, fixture.scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(2L, fixture.scheduler.getMetricsSnapshot().activeTasks());
        }
    }

    @Test
    void pluginProducedTaskCountIsCheckedBeforeProjection() throws Exception {
        System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, "1");
        try {
            try (Fixture fixture = new Fixture(SchedulerConfig.defaults())) {
                fixture.submit(new JobSubmitMessage(
                        "requester-1",
                        "2026-07-26T00:00:00Z",
                        "job-expanded",
                        ExpandingTestTaskPlugin.TASK_TYPE,
                        List.of("payload"),
                        "",
                        "token-job-expanded"
                ));

                JobResultMessage rejected = fixture.awaitResult();

                assertRejection(
                        rejected,
                        AdmissionRejection.Limit.MAX_TASKS_PER_JOB,
                        1L,
                        2L
                );
                assertEquals(0L, fixture.scheduler.getMetricsSnapshot().activeJobs());
                assertEquals(0L, fixture.scheduler.getMetricsSnapshot().activeTasks());
                assertEquals(0L, fixture.scheduler.getMetricsSnapshot().jobsAcceptedTotal());
            }
        } finally {
            System.clearProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
        }
    }

    @Test
    void submittedTaskLimitReturnsTypedBoundaryDetail() throws Exception {
        System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, "1");
        try {
            try (Fixture fixture = new Fixture(SchedulerConfig.defaults())) {
                fixture.submit(testSubmission("job-submit-count", List.of("a", "b")));

                assertRejection(
                        fixture.awaitResult(),
                        AdmissionRejection.Limit.MAX_TASKS_PER_JOB,
                        1L,
                        2L
                );
            }
        } finally {
            System.clearProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
        }
    }

    @Test
    void inlinePayloadLimitReturnsTypedBoundaryDetail() throws Exception {
        JobSubmitMessage submit = testSubmission("job-inline-limit", List.of("payload"));
        long observed = PayloadLimits.jobPayloadJsonBytes(
                submit.getTaskPayloads(),
                submit.getParameter()
        );
        System.setProperty(
                PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY,
                String.valueOf(observed - 1L)
        );
        try {
            try (Fixture fixture = new Fixture(SchedulerConfig.defaults())) {
                fixture.submit(submit);

                assertRejection(
                        fixture.awaitResult(),
                        AdmissionRejection.Limit.MAX_INLINE_MESSAGE_BYTES,
                        observed - 1L,
                        observed
                );
            }
        } finally {
            System.clearProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void referencedPayloadLimitReturnsTypedBoundaryDetail() throws Exception {
        ObjectReference reference = new ObjectReference(
                TaskFlowObjectKeys.objectKey("inputs", "input"),
                2L,
                "a".repeat(64),
                "application/octet-stream"
        );
        System.setProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY, "1");
        try {
            try (Fixture fixture = new Fixture(SchedulerConfig.defaults())) {
                fixture.submit(testSubmission(
                        "job-reference-limit",
                        List.of(Map.of("nested", List.of(reference)))
                ));

                assertRejection(
                        fixture.awaitResult(),
                        AdmissionRejection.Limit.MAX_REFERENCED_PAYLOAD_BYTES,
                        1L,
                        2L
                );
            }
        } finally {
            System.clearProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY);
        }
    }

    @Test
    void inlineFilePayloadLimitReturnsTypedBoundaryDetail() throws Exception {
        Map<String, Object> inlinePayload = Map.of(
                "fileName", "sample.png",
                "base64Data", Base64.getEncoder().encodeToString(new byte[] {1, 2})
        );
        System.setProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY, "2");
        try {
            try (Fixture fixture = new Fixture(SchedulerConfig.defaults())) {
                fixture.submit(testSubmission(
                        "job-inline-file-limit",
                        List.of(inlinePayload)
                ));

                assertRejection(
                        fixture.awaitResult(),
                        AdmissionRejection.Limit.MAX_INLINE_PAYLOAD_BYTES,
                        2L,
                        2L
                );
            }
        } finally {
            System.clearProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void degradedCoordinatorRejectsNewJobBeforeInMemoryAcceptance() throws Exception {
        try (Fixture fixture = new Fixture(
                SchedulerConfig.defaults(),
                true,
                () -> false
        )) {
            fixture.submit(testSubmission("job-degraded", List.of("payload")));

            JobResultMessage rejected = fixture.awaitResult();

            assertNotNull(rejected);
            assertFalse(rejected.isSuccessful());
            assertEquals(
                    "Coordinator is degraded and not ready for new jobs.",
                    rejected.getErrorMessage()
            );
            assertNull(rejected.getAdmissionRejection());
            assertEquals(0L, fixture.scheduler.getMetricsSnapshot().jobsAcceptedTotal());
            assertEquals(0L, fixture.scheduler.getMetricsSnapshot().activeJobs());
            assertEquals(0L, fixture.scheduler.getMetricsSnapshot().activeTasks());
        }
    }

    @Test
    void degradedCoordinatorStillServesExactAcceptedSubmissionReplay() throws Exception {
        AtomicBoolean accepting = new AtomicBoolean(true);
        JobSubmitMessage original = testSubmission("job-degraded-replay", List.of("payload"));
        try (Fixture fixture = new Fixture(
                SchedulerConfig.defaults(),
                true,
                accepting::get
        )) {
            fixture.submit(original);
            fixture.awaitAcceptedJobs(1L);
            accepting.set(false);
            fixture.submit(new JobSubmitMessage(
                    original.getNodeId(),
                    original.getTime(),
                    original.getJobId(),
                    original.getTaskType(),
                    original.getTaskPayloads(),
                    original.getParameter(),
                    original.getRequesterToken()
            ));

            JobResultMessage replay = fixture.awaitResult();

            assertNotNull(replay);
            assertFalse(replay.isSuccessful());
            assertEquals("Job is still running.", replay.getErrorMessage());
            assertEquals(1L, fixture.scheduler.getMetricsSnapshot().jobsAcceptedTotal());
            assertEquals(1L, fixture.scheduler.getMetricsSnapshot().activeJobs());
        }
    }

    private static void assertRejection(JobResultMessage result,
                                        AdmissionRejection.Limit limit,
                                        long configuredMaximum,
                                        long observedValue) {
        assertNotNull(result);
        assertFalse(result.isSuccessful());
        assertNotNull(result.getAdmissionRejection());
        assertEquals(limit, result.getAdmissionRejection().limit());
        assertEquals(configuredMaximum, result.getAdmissionRejection().configuredMaximum());
        assertEquals(observedValue, result.getAdmissionRejection().observedValue());
    }

    private static JobSubmitMessage testSubmission(String jobId, List<Object> payloads) {
        return new JobSubmitMessage(
                "requester-1",
                "2026-07-26T00:00:00Z",
                jobId,
                TestTaskPlugin.TASK_TYPE,
                payloads,
                "",
                "token-" + jobId
        );
    }

    private static EmbarrassinglyParallelJob<?, ?> initializedJob(
            String jobId,
            List<Object> payloads) {
        JobSubmitMessage submit = testSubmission(jobId, payloads);
        EmbarrassinglyParallelJob<?, ?> job = JobFactory.create(submit, submit.getNodeId());
        job.initializeTasks(submit);
        return job;
    }

    private static final class Fixture implements AutoCloseable {
        private final BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>();
        private final ResultQueueOutput output = new ResultQueueOutput();
        private final TaskScheduler scheduler;
        private final Thread schedulerThread;
        private boolean started;

        private Fixture(SchedulerConfig config) {
            this(config, true);
        }

        private Fixture(SchedulerConfig config, boolean start) {
            this(config, start, () -> true);
        }

        private Fixture(
                SchedulerConfig config,
                boolean start,
                BooleanSupplier newJobAcceptanceAllowed
        ) {
            scheduler = new TaskScheduler(
                    mailbox,
                    new InMemoryPeerRegistry(),
                    null,
                    output,
                    config,
                    server.runtime.SystemTaskFlowClock.INSTANCE,
                    server.runtime.UuidAssignmentIdGenerator.INSTANCE,
                    "scheduler-admission-test",
                    newJobAcceptanceAllowed
            );
            schedulerThread = new Thread(scheduler, "scheduler-admission-test");
            if (start) {
                start();
            }
        }

        private void start() {
            schedulerThread.start();
            started = true;
        }

        private void submit(JobSubmitMessage submit) throws InterruptedException {
            mailbox.put(new MessageEnvelope(submit, submit.getNodeId()));
        }

        private JobResultMessage awaitResult() throws InterruptedException {
            return output.results.poll(2L, TimeUnit.SECONDS);
        }

        private void awaitAcceptedJobs(long expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
            while (scheduler.getMetricsSnapshot().jobsAcceptedTotal() != expected
                    && System.nanoTime() < deadline) {
                Thread.sleep(5L);
            }
            assertEquals(expected, scheduler.getMetricsSnapshot().jobsAcceptedTotal());
        }

        @Override
        public void close() throws InterruptedException {
            if (!started) {
                return;
            }
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(2_000L);
            if (schedulerThread.isAlive()) {
                schedulerThread.interrupt();
                schedulerThread.join(2_000L);
            }
        }
    }

    private static final class ResultQueueOutput implements SchedulerOutput {
        private final BlockingQueue<JobResultMessage> results = new LinkedBlockingQueue<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            results.add(message);
            return true;
        }
    }
}
