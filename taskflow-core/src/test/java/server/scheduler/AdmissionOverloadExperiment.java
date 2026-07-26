package server.scheduler;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import protocol.AdmissionRejection;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdmissionOverloadExperiment {
    private static final int ACTIVE_JOB_LIMIT = 64;
    private static final int TASKS_PER_ACTIVE_JOB = 64;
    private static final int ACTIVE_TASK_LIMIT =
            ACTIVE_JOB_LIMIT * TASKS_PER_ACTIVE_JOB;
    private static final int WAVE_COUNT = 5;
    private static final int SUBMISSIONS_PER_WAVE = 20_000;
    private static final long MEBIBYTE = 1024L * 1024L;

    @Test
    void coordinatorHeapPlateausAtConfiguredBounds() throws Exception {
        Assumptions.assumeTrue(
                Boolean.getBoolean("taskflow.admission.experiment"),
                "Opt in with -Dtaskflow.admission.experiment=true"
        );
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_ACTIVE_JOBS", String.valueOf(ACTIVE_JOB_LIMIT),
                "TASKFLOW_MAX_ACTIVE_TASKS", String.valueOf(ACTIVE_TASK_LIMIT),
                "TASKFLOW_MAX_PENDING_OUTBOX_ROWS", "100000"
        ));
        BlockingQueue<MessageEnvelope> mailbox = SchedulerMailbox.create(config);
        CountingJobStateStore store = new CountingJobStateStore(ACTIVE_JOB_LIMIT);
        DiscardingOutput output = new DiscardingOutput();
        TaskScheduler scheduler = new TaskScheduler(
                mailbox,
                new InMemoryPeerRegistry(),
                store,
                output,
                config
        );
        Thread schedulerThread = new Thread(scheduler, "admission-overload-experiment");
        schedulerThread.start();

        try {
            List<Object> activePayloads = new ArrayList<>(TASKS_PER_ACTIVE_JOB);
            for (int task = 0; task < TASKS_PER_ACTIVE_JOB; task++) {
                activePayloads.add("payload-" + task);
            }
            activePayloads = List.copyOf(activePayloads);
            for (int job = 0; job < ACTIVE_JOB_LIMIT; job++) {
                mailbox.put(new MessageEnvelope(
                        submission("accepted-" + job, activePayloads),
                        "requester-1"
                ));
            }
            assertTrue(store.awaitCommits());
            assertProjectionBound(scheduler, store);

            List<Long> retainedHeapSamples = new ArrayList<>(WAVE_COUNT);
            for (int wave = 0; wave < WAVE_COUNT; wave++) {
                long expectedRejections = (long) (wave + 1) * SUBMISSIONS_PER_WAVE;
                for (int job = 0; job < SUBMISSIONS_PER_WAVE; job++) {
                    mailbox.put(new MessageEnvelope(
                            submission("rejected-" + wave + "-" + job, List.of("payload")),
                            "requester-1"
                    ));
                }
                assertTrue(output.awaitRejections(expectedRejections));
                assertProjectionBound(scheduler, store);

                long retainedHeap = retainedHeapBytes();
                retainedHeapSamples.add(retainedHeap);
                System.out.printf(
                        "admission_heap_wave=%d retained_bytes=%d active_jobs=%d "
                                + "active_tasks=%d durable_commits=%d rejections=%d%n",
                        wave + 1,
                        retainedHeap,
                        scheduler.getMetricsSnapshot().activeJobs(),
                        scheduler.getMetricsSnapshot().activeTasks(),
                        store.commitCount(),
                        output.rejectionCount()
                );
            }

            List<Long> plateauSamples = retainedHeapSamples.subList(
                    retainedHeapSamples.size() - 3,
                    retainedHeapSamples.size()
            );
            long minimum = plateauSamples.stream().mapToLong(Long::longValue).min().orElseThrow();
            long maximum = plateauSamples.stream().mapToLong(Long::longValue).max().orElseThrow();
            assertTrue(
                    maximum - minimum <= 16L * MEBIBYTE,
                    "Last three retained-heap samples did not plateau: " + plateauSamples
            );
            assertTrue(
                    maximum < 128L * MEBIBYTE,
                    "Retained heap exceeded the documented ceiling: " + plateauSamples
            );
        } finally {
            scheduler.requestShutdownAfterDrain();
            schedulerThread.join(TimeUnit.SECONDS.toMillis(10L));
            if (schedulerThread.isAlive()) {
                schedulerThread.interrupt();
                schedulerThread.join(TimeUnit.SECONDS.toMillis(2L));
            }
        }
    }

    private static void assertProjectionBound(
            TaskScheduler scheduler,
            CountingJobStateStore store) {
        SchedulerMetrics.Snapshot snapshot = scheduler.getMetricsSnapshot();
        assertEquals(ACTIVE_JOB_LIMIT, snapshot.activeJobs());
        assertEquals(ACTIVE_TASK_LIMIT, snapshot.activeTasks());
        assertEquals(ACTIVE_JOB_LIMIT, store.commitCount());
        assertEquals(0L, snapshot.assignmentGenerationsTotal());
        SchedulerWorkloadIndex.Snapshot workload = scheduler.getWorkloadSnapshot();
        assertEquals(ACTIVE_TASK_LIMIT, workload.pendingTasks());
        assertTrue(
                workload.runnableJobs() + workload.capacityWaitingJobs() <= ACTIVE_JOB_LIMIT,
                "Workload job indexes exceeded the accepted-job bound: " + workload
        );
        assertEquals(0, workload.liveAssignments());
        assertEquals(0, workload.deadlineEntries());
    }

    private static long retainedHeapBytes() {
        System.gc();
        System.gc();
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        return memory.getHeapMemoryUsage().getUsed();
    }

    private static JobSubmitMessage submission(String suffix, List<Object> payloads) {
        return new JobSubmitMessage(
                "requester-1",
                "2026-07-26T00:00:00Z",
                "job-" + suffix,
                TestTaskPlugin.TASK_TYPE,
                payloads,
                "",
                "token-" + suffix
        );
    }

    private static final class DiscardingOutput implements SchedulerOutput {
        private final AtomicLong rejectionCount = new AtomicLong();
        private final Object rejectionMonitor = new Object();

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            throw new AssertionError("Experiment intentionally has no executor capacity.");
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            AdmissionRejection rejection = message.getAdmissionRejection();
            if (rejection == null
                    || rejection.limit() != AdmissionRejection.Limit.MAX_ACTIVE_JOBS) {
                throw new AssertionError("Unexpected admission response: " + message.getErrorMessage());
            }
            rejectionCount.incrementAndGet();
            synchronized (rejectionMonitor) {
                rejectionMonitor.notifyAll();
            }
            return true;
        }

        boolean awaitRejections(long expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60L);
            synchronized (rejectionMonitor) {
                while (rejectionCount.get() < expected) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L) {
                        return false;
                    }
                    TimeUnit.NANOSECONDS.timedWait(rejectionMonitor, remaining);
                }
            }
            return rejectionCount.get() == expected;
        }

        long rejectionCount() {
            return rejectionCount.get();
        }
    }

    private static final class CountingJobStateStore implements JobStateStore {
        private final AtomicLong commits = new AtomicLong();
        private final java.util.concurrent.CountDownLatch acceptedCommits;

        private CountingJobStateStore(int expectedAcceptedJobs) {
            acceptedCommits = new java.util.concurrent.CountDownLatch(expectedAcceptedJobs);
        }

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
            commits.incrementAndGet();
            acceptedCommits.countDown();
            return JobSubmissionDecision.committed(taskType);
        }

        boolean awaitCommits() throws InterruptedException {
            return acceptedCommits.await(10L, TimeUnit.SECONDS);
        }

        long commitCount() {
            return commits.get();
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
    }
}
