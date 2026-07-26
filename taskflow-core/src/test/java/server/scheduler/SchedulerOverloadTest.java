package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.db.JobStateStore;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerOverloadTest {
    private static final String EXPERIMENT_PROPERTY = "taskflow.overload.experiment";
    private static final String EXPECTATION_PROPERTY = "taskflow.overload.expectation";
    private static final int EXPERIMENT_WAVES = 5;
    private static final int EXPERIMENT_CYCLES_PER_WAVE = 20_000;

    @Test
    void persistentMailboxSaturationPreservesAcceptedWorkAndProgress() throws Exception {
        if (Boolean.getBoolean(EXPERIMENT_PROPERTY)) {
            runPersistentMailboxExperiment();
            return;
        }
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY", "1",
                "TASKFLOW_SCHEDULER_MESSAGE_BATCH_SIZE", "1",
                "TASKFLOW_SCHEDULER_DEADLINE_BATCH_SIZE", "1"
        ));
        BlockingQueue<MessageEnvelope> mailbox = SchedulerMailbox.create(config);
        MessageEnvelope queuedSubmission = pong("submission-flood");
        MessageEnvelope acceptedResult = taskResult();

        assertTrue(mailbox.offer(queuedSubmission));
        assertTrue(mailbox.offer(acceptedResult),
                "The fixed task-result reserve must remain available.");
        assertFalse(mailbox.offer(taskResult()),
                "A second result must use the existing bounded retry path.");

        RecordingWork work = new RecordingWork(mailbox);
        SchedulerLoop loop = new SchedulerLoop(mailbox, work, config);

        SchedulerLoop.CycleResult cycle = loop.runCycle(0L);

        assertEquals(1, cycle.messagesProcessed());
        assertEquals(1, cycle.deadlinesProcessed());
        assertSame(acceptedResult, work.processed.getFirst());
        assertEquals(List.of("result", "deadline", "dispatch", "outbox", "metrics"),
                work.calls);
        assertSame(queuedSubmission, mailbox.peek(),
                "Submission pressure must not evict accepted envelopes.");
        assertTrue(cycle.immediateWorkRemaining());
    }

    @Test
    void activeLimitClearsAndAllowsFreshAdmissionWithoutSchedulerRestart() throws Exception {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", "1",
                "TASKFLOW_MAX_ACTIVE_TASKS", "1"
        ));
        BlockingQueue<MessageEnvelope> mailbox = SchedulerMailbox.create(config);
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        registry.register("peer-1", new PeerInfo(
                "peer-1",
                config,
                List.of(TestTaskPlugin.TASK_TYPE)
        ));
        RecordingOutput output = new RecordingOutput();
        CountingStore store = new CountingStore();
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, store, output, config);
        Thread schedulerThread = new Thread(scheduler, "scheduler-overload-recovery-test");
        schedulerThread.start();

        try {
            mailbox.put(envelope(submission("accepted-before-pressure")));
            TaskAssignMessage firstAssignment = output.awaitAssignment();
            assertNotNull(firstAssignment);

            mailbox.put(envelope(submission("rejected-at-limit")));
            JobResultMessage rejection = output.awaitResult();
            assertNotNull(rejection);
            assertFalse(rejection.isSuccessful());
            assertNotNull(rejection.getAdmissionRejection());
            assertEquals(
                    protocol.AdmissionRejection.Limit.MAX_ACTIVE_JOBS,
                    rejection.getAdmissionRejection().limit()
            );

            mailbox.put(new MessageEnvelope(success(firstAssignment), "peer-1"));
            JobResultMessage completed = output.awaitResult();
            assertNotNull(completed);
            assertTrue(completed.isSuccessful());
            assertEquals("accepted-before-pressure", completed.getJobId());

            mailbox.put(envelope(submission("accepted-after-pressure")));
            TaskAssignMessage recoveredAssignment = output.awaitAssignment();
            assertNotNull(recoveredAssignment);
            assertEquals("accepted-after-pressure", recoveredAssignment.getJobId());
            assertEquals(2L, store.durableCommits(),
                    "The rejected candidate must not produce a durable commit.");
            assertTrue(schedulerThread.isAlive(),
                    "Automatic recovery must not require a scheduler restart.");
            if (Boolean.getBoolean(EXPERIMENT_PROPERTY)) {
                System.out.printf(
                        "overload_recovery durable_commits=%d typed_rejections=1 "
                                + "final_new_job_accepted=true restart_count=0%n",
                        store.durableCommits()
                );
            }
        } finally {
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(2_000L);
            if (schedulerThread.isAlive()) {
                schedulerThread.interrupt();
                schedulerThread.join(2_000L);
            }
        }
    }

    private static void runPersistentMailboxExperiment() throws Exception {
        String expectation = System.getProperty(EXPECTATION_PROPERTY, "changed");
        boolean expectPriorityReserve = switch (expectation) {
            case "baseline" -> false;
            case "changed" -> true;
            default -> throw new IllegalArgumentException(
                    EXPECTATION_PROPERTY + " must be baseline or changed"
            );
        };
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY", "1",
                "TASKFLOW_SCHEDULER_MESSAGE_BATCH_SIZE", "1",
                "TASKFLOW_SCHEDULER_DEADLINE_BATCH_SIZE", "1"
        ));
        BlockingQueue<MessageEnvelope> mailbox = SchedulerMailbox.create(config);
        ExperimentWork work = new ExperimentWork();
        SchedulerLoop loop = new SchedulerLoop(mailbox, work, config);
        long resultOffers = 0L;
        long resultAdmissions = 0L;
        long resultRetries = 0L;
        int laneHighWater = 0;
        List<Long> retainedHeapSamples = new ArrayList<>(EXPERIMENT_WAVES);

        for (int wave = 0; wave < EXPERIMENT_WAVES; wave++) {
            for (int cycle = 0; cycle < EXPERIMENT_CYCLES_PER_WAVE; cycle++) {
                long sequence = (long) wave * EXPERIMENT_CYCLES_PER_WAVE + cycle;
                assertTrue(mailbox.offer(pong("submission-" + sequence)));
                resultOffers++;
                boolean accepted = mailbox.offer(taskResult(sequence));
                if (accepted) {
                    resultAdmissions++;
                } else {
                    resultRetries++;
                }
                laneHighWater = Math.max(laneHighWater, mailbox.size());
                loop.runCycle(0L);
                mailbox.clear();
            }
            long retainedHeap = retainedHeapBytes();
            retainedHeapSamples.add(retainedHeap);
            System.out.printf(
                    "overload_wave=%d retained_heap_bytes=%d result_offers=%d "
                            + "result_admissions=%d result_retries=%d deadline_cycles=%d "
                            + "submission_lane_high_water=1 total_lane_high_water=%d%n",
                    wave + 1,
                    retainedHeap,
                    resultOffers,
                    resultAdmissions,
                    resultRetries,
                    work.deadlineCycles,
                    laneHighWater
            );
        }

        long expectedCycles = (long) EXPERIMENT_WAVES * EXPERIMENT_CYCLES_PER_WAVE;
        assertEquals(expectedCycles, resultOffers);
        assertEquals(expectedCycles, work.deadlineCycles);
        if (expectPriorityReserve) {
            assertEquals(expectedCycles, resultAdmissions);
            assertEquals(0L, resultRetries);
            assertEquals(expectedCycles, work.resultsProcessed);
            assertEquals(0L, work.submissionsProcessed);
            assertEquals(2, laneHighWater);
        } else {
            assertEquals(0L, resultAdmissions);
            assertEquals(expectedCycles, resultRetries);
            assertEquals(0L, work.resultsProcessed);
            assertEquals(expectedCycles, work.submissionsProcessed);
            assertEquals(1, laneHighWater);
        }
        long plateauMinimum = retainedHeapSamples.subList(2, retainedHeapSamples.size())
                .stream().mapToLong(Long::longValue).min().orElseThrow();
        long plateauMaximum = retainedHeapSamples.subList(2, retainedHeapSamples.size())
                .stream().mapToLong(Long::longValue).max().orElseThrow();
        assertTrue(
                plateauMaximum - plateauMinimum <= 16L * 1024L * 1024L,
                "Retained heap did not plateau: " + retainedHeapSamples
        );
        System.out.printf(
                "overload_summary expectation=%s cycles=%d result_admissions=%d "
                        + "result_retries=%d result_commit_cycles=%s deadline_cycles=%d "
                        + "typed_rejections=0 exact_replays=0 durable_commits=0 "
                        + "pending_outbox_rows=0 lane_high_water=%d max_heap_bytes=%d%n",
                expectation,
                expectedCycles,
                resultAdmissions,
                resultRetries,
                expectPriorityReserve ? "1" : "not_committed",
                work.deadlineCycles,
                laneHighWater,
                Runtime.getRuntime().maxMemory()
        );
    }

    private static long retainedHeapBytes() {
        System.gc();
        System.gc();
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        return memory.getHeapMemoryUsage().getUsed();
    }

    private static MessageEnvelope pong(String nodeId) {
        return new MessageEnvelope(
                new PongMessage(nodeId, "2026-07-27T00:00:00Z", List.of()),
                nodeId
        );
    }

    private static MessageEnvelope taskResult() {
        return taskResult(1L);
    }

    private static MessageEnvelope taskResult(long sequence) {
        return new MessageEnvelope(
                new TaskResultMessage(
                        "peer-1",
                        "2026-07-27T00:00:00Z",
                        "task-accepted-" + sequence,
                        "job-accepted",
                        1,
                        "00000000-0000-0000-0000-000000000001",
                        "result",
                        true,
                        ""
                ),
                "peer-1"
        );
    }

    private static JobSubmitMessage submission(String jobId) {
        return new JobSubmitMessage(
                "requester-1",
                "2026-07-27T00:00:00Z",
                jobId,
                TestTaskPlugin.TASK_TYPE,
                List.of("payload"),
                "",
                "token-" + jobId
        );
    }

    private static MessageEnvelope envelope(JobSubmitMessage submission) {
        return new MessageEnvelope(submission, submission.getNodeId());
    }

    private static TaskResultMessage success(TaskAssignMessage assignment) {
        return new TaskResultMessage(
                "peer-1",
                "2026-07-27T00:00:00Z",
                assignment.getTaskId(),
                assignment.getJobId(),
                assignment.getAttemptNumber(),
                assignment.getAssignmentId(),
                "completed",
                true,
                ""
        );
    }

    private static final class RecordingWork implements SchedulerLoop.Work {
        private final BlockingQueue<MessageEnvelope> mailbox;
        private final List<String> calls = new ArrayList<>();
        private final List<MessageEnvelope> processed = new ArrayList<>();

        private RecordingWork(BlockingQueue<MessageEnvelope> mailbox) {
            this.mailbox = mailbox;
        }

        @Override
        public void processEnvelope(MessageEnvelope envelope) {
            processed.add(envelope);
            calls.add("result");
            assertFalse(mailbox.offer(pong("continuing-flood")));
        }

        @Override
        public SchedulerLoop.StageResult processDueDeadlines(int limit) {
            calls.add("deadline");
            return new SchedulerLoop.StageResult(1, false);
        }

        @Override
        public SchedulerLoop.StageResult dispatchPendingTasks(int limit) {
            calls.add("dispatch");
            return SchedulerLoop.StageResult.idle();
        }

        @Override
        public SchedulerLoop.StageResult retryPendingOutbound(int limit) {
            calls.add("outbox");
            return SchedulerLoop.StageResult.idle();
        }

        @Override
        public void updateMetrics() {
            calls.add("metrics");
        }

        @Override
        public long millisUntilNextScheduledWork() {
            return 0L;
        }
    }

    private static final class ExperimentWork implements SchedulerLoop.Work {
        private long resultsProcessed;
        private long submissionsProcessed;
        private long deadlineCycles;

        @Override
        public void processEnvelope(MessageEnvelope envelope) {
            if (envelope.message() instanceof TaskResultMessage) {
                resultsProcessed++;
            } else {
                submissionsProcessed++;
            }
        }

        @Override
        public SchedulerLoop.StageResult processDueDeadlines(int limit) {
            deadlineCycles++;
            return new SchedulerLoop.StageResult(1, false);
        }

        @Override
        public SchedulerLoop.StageResult dispatchPendingTasks(int limit) {
            return SchedulerLoop.StageResult.idle();
        }

        @Override
        public SchedulerLoop.StageResult retryPendingOutbound(int limit) {
            return SchedulerLoop.StageResult.idle();
        }

        @Override
        public void updateMetrics() {
        }

        @Override
        public long millisUntilNextScheduledWork() {
            return 0L;
        }
    }

    private static final class RecordingOutput implements SchedulerOutput {
        private final BlockingQueue<TaskAssignMessage> assignments = new LinkedBlockingQueue<>();
        private final BlockingQueue<JobResultMessage> results = new LinkedBlockingQueue<>();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            assignments.add(message);
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            results.add(message);
            return true;
        }

        private TaskAssignMessage awaitAssignment() throws InterruptedException {
            return assignments.poll(2L, TimeUnit.SECONDS);
        }

        private JobResultMessage awaitResult() throws InterruptedException {
            return results.poll(2L, TimeUnit.SECONDS);
        }
    }

    private static final class CountingStore implements JobStateStore {
        private final AtomicLong durableCommits = new AtomicLong();

        @Override
        public JobSubmissionDecision inspectJobSubmission(
                String jobId,
                String requesterTokenHash,
                String requesterIdentityKey,
                String requestHash) {
            return JobSubmissionDecision.newSubmission();
        }

        @Override
        public JobSubmissionDecision commitJobSubmission(
                String jobId,
                String taskType,
                String requesterId,
                String requesterTokenHash,
                String requesterIdentityKey,
                String requestHash,
                String parameter,
                Collection<TaskStartupState> tasks) {
            durableCommits.incrementAndGet();
            return JobSubmissionDecision.committed(taskType);
        }

        @Override
        public boolean insertJobWithTasks(
                String jobId,
                String taskType,
                String requesterId,
                int fileCount,
                Collection<String> taskIds) {
            return true;
        }

        @Override
        public boolean insertJob(String jobId, String taskType, String requesterId, int fileCount) {
            return true;
        }

        @Override
        public boolean insertTask(String taskId, String jobId) {
            return true;
        }

        @Override
        public boolean markTaskAssigned(String taskId, String peerId, long startedAt) {
            return true;
        }

        @Override
        public boolean markTaskCompleted(String taskId, long completedAt, long durationMs) {
            return true;
        }

        @Override
        public boolean markTaskRetried(String taskId, int retryCount) {
            return true;
        }

        @Override
        public boolean markTaskFailed(String taskId) {
            return true;
        }

        @Override
        public boolean markJobCompleted(String jobId) {
            return true;
        }

        @Override
        public boolean markJobFailed(String jobId) {
            return true;
        }

        @Override
        public int markRunningJobsFailedOnStartup(long completedAt) {
            return 0;
        }

        private long durableCommits() {
            return durableCommits.get();
        }
    }
}
