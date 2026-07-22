package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.PayloadLimits;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in Phase 0 baseline experiment. The class name intentionally does not
 * match Surefire's default *Test patterns; run it explicitly through the
 * verify-baseline scripts.
 */
public class BaselineSchedulerExperiment {
    private static final int DEFAULT_TASK_COUNT = 10_000;
    private static final int DEFAULT_WARMUP_TASK_COUNT = 1_000;
    private static final int DEFAULT_WORK_UNITS = 64;
    private static final int MAX_EXPERIMENT_TASKS = 100_000;
    private static final int MAX_WORK_UNITS = 100_000;
    private static final long COMPLETION_TIMEOUT_SECONDS = 300L;
    private static final String FIXED_TIMESTAMP = "2026-07-22T00:00:00Z";

    @Test
    void runConfiguredBaseline() throws Exception {
        int workerCount = Integer.getInteger("taskflow.baseline.workers", 1);
        int taskCount = Integer.getInteger("taskflow.baseline.tasks", DEFAULT_TASK_COUNT);
        int warmupTaskCount = Integer.getInteger(
                "taskflow.baseline.warmupTasks",
                DEFAULT_WARMUP_TASK_COUNT
        );
        int workUnits = Integer.getInteger("taskflow.baseline.workUnits", DEFAULT_WORK_UNITS);
        Path outputPath = Path.of(System.getProperty(
                "taskflow.baseline.output",
                "target/baseline/workers-" + workerCount + ".properties"
        ));

        assertTrue(workerCount == 1 || workerCount == 4,
                "The fixed baseline supports exactly 1 or 4 executor participants.");
        assertTrue(taskCount > 0 && taskCount <= MAX_EXPERIMENT_TASKS,
                "Task count must be in [1, " + MAX_EXPERIMENT_TASKS + "].");
        assertTrue(warmupTaskCount > 0 && warmupTaskCount <= taskCount,
                "Warm-up task count must be positive and no larger than task count.");
        assertTrue(workUnits > 0 && workUnits <= MAX_WORK_UNITS,
                "Work units must be in [1, " + MAX_WORK_UNITS + "].");

        String previousMaxTasks = System.getProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
        System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, String.valueOf(taskCount));

        SchedulerConfig config = SchedulerConfig.defaults();
        BlockingQueue<MessageEnvelope> mailbox = SchedulerMailbox.create(config);
        InMemoryPeerRegistry registry = new InMemoryPeerRegistry();
        for (int index = 0; index < workerCount; index++) {
            String workerId = "baseline-worker-" + (index + 1);
            registry.register(workerId, new PeerInfo(workerId, config, List.of(TestTaskPlugin.TASK_TYPE)));
        }

        BaselineOutput output = new BaselineOutput(mailbox, workerCount, config.maxTasksPerPeer(), workUnits);
        TaskScheduler scheduler = new TaskScheduler(mailbox, registry, null, output, config);
        Thread schedulerThread = Thread.ofPlatform()
                .name("baseline-scheduler")
                .daemon(true)
                .start(scheduler);

        AtomicBoolean sampleHeap = new AtomicBoolean(false);
        AtomicLong peakHeapBytes = new AtomicLong();
        Thread heapSampler = null;

        try {
            JobResultMessage warmupResult = runJob(
                    mailbox,
                    output,
                    "baseline-warmup-" + workerCount,
                    warmupTaskCount
            );
            assertTrue(warmupResult.isSuccessful(), warmupResult.getErrorMessage());
            assertEquals(warmupTaskCount, warmupResult.getResultsByTaskId().size());

            long assignmentsBefore = output.assignmentCount();
            Map<String, Long> distributionBefore = output.assignmentDistribution();
            long heapUsedBeforeBytes = heapUsedBytes();
            peakHeapBytes.set(heapUsedBeforeBytes);
            sampleHeap.set(true);
            heapSampler = startHeapSampler(sampleHeap, peakHeapBytes);

            long startedAtNanos = System.nanoTime();
            JobResultMessage measuredResult = runJob(
                    mailbox,
                    output,
                    "baseline-measured-" + workerCount,
                    taskCount
            );
            long durationNanos = System.nanoTime() - startedAtNanos;
            updateMaximum(peakHeapBytes, heapUsedBytes());

            assertTrue(measuredResult.isSuccessful(), measuredResult.getErrorMessage());
            assertEquals(taskCount, measuredResult.getResultsByTaskId().size());
            assertEquals(taskCount, output.assignmentCount() - assignmentsBefore);
            assertFalse(output.failure().isDone(), "Executor simulation reported an asynchronous failure.");

            Map<String, Long> measuredDistribution = subtractDistribution(
                    output.assignmentDistribution(),
                    distributionBefore
            );
            assertEquals(workerCount, measuredDistribution.size());
            assertTrue(measuredDistribution.values().stream().allMatch(count -> count > 0L));

            long peakUsedHeapBytes = peakHeapBytes.get();
            long heapDeltaBytes = Math.max(0L, peakUsedHeapBytes - heapUsedBeforeBytes);
            double throughput = taskCount * 1_000_000_000.0 / durationNanos;

            writeMetrics(outputPath, List.of(
                    "formatVersion=1",
                    "workerCount=" + workerCount,
                    "maxConcurrentTasksPerWorker=" + config.maxTasksPerPeer(),
                    "taskCount=" + taskCount,
                    "warmupTaskCount=" + warmupTaskCount,
                    "workUnitsPerTask=" + workUnits,
                    "durationNanos=" + durationNanos,
                    "throughputTasksPerSecond=" + String.format(Locale.ROOT, "%.3f", throughput),
                    "heapUsedBeforeBytes=" + heapUsedBeforeBytes,
                    "peakUsedHeapBytes=" + peakUsedHeapBytes,
                    "heapDeltaBytes=" + heapDeltaBytes,
                    "maxHeapBytes=" + Runtime.getRuntime().maxMemory(),
                    "availableProcessors=" + Runtime.getRuntime().availableProcessors(),
                    "javaVersion=" + System.getProperty("java.version"),
                    "persistence=none",
                    "transport=in-process",
                    "assignmentDistribution=" + formatDistribution(measuredDistribution)
            ));

            System.out.printf(
                    Locale.ROOT,
                    "TASKFLOW_BASELINE_METRIC workers=%d tasks=%d duration_ns=%d throughput_tasks_per_second=%.3f peak_heap_bytes=%d heap_delta_bytes=%d%n",
                    workerCount,
                    taskCount,
                    durationNanos,
                    throughput,
                    peakUsedHeapBytes,
                    heapDeltaBytes
            );
        } finally {
            sampleHeap.set(false);
            if (heapSampler != null) {
                heapSampler.interrupt();
                heapSampler.join(5_000L);
            }
            schedulerThread.interrupt();
            schedulerThread.join(5_000L);
            output.close();
            if (previousMaxTasks == null) {
                System.clearProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
            } else {
                System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, previousMaxTasks);
            }
            assertFalse(schedulerThread.isAlive(), "Scheduler thread did not stop.");
        }
    }

    private static JobResultMessage runJob(BlockingQueue<MessageEnvelope> mailbox,
                                           BaselineOutput output,
                                           String jobId,
                                           int taskCount)
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<JobResultMessage> result = output.expectResult(jobId);
        List<Object> payloads = new ArrayList<>(taskCount);
        for (int index = 0; index < taskCount; index++) {
            payloads.add("payload-" + index);
        }
        JobSubmitMessage submit = new JobSubmitMessage(
                "baseline-requester",
                FIXED_TIMESTAMP,
                jobId,
                TestTaskPlugin.TASK_TYPE,
                payloads,
                "",
                "baseline-token-" + jobId
        );
        mailbox.put(new MessageEnvelope(submit, "baseline-requester"));

        Object completed = CompletableFuture.anyOf(result, output.failure())
                .get(COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (completed instanceof Throwable failure) {
            throw new ExecutionException(failure);
        }
        return result.get(COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static Thread startHeapSampler(AtomicBoolean running, AtomicLong peakHeapBytes) {
        return Thread.ofPlatform().name("baseline-heap-sampler").daemon(true).start(() -> {
            while (running.get()) {
                updateMaximum(peakHeapBytes, heapUsedBytes());
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            }
        });
    }

    private static long heapUsedBytes() {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        return memory.getHeapMemoryUsage().getUsed();
    }

    private static void updateMaximum(AtomicLong maximum, long candidate) {
        maximum.accumulateAndGet(candidate, Math::max);
    }

    private static Map<String, Long> subtractDistribution(Map<String, Long> after,
                                                          Map<String, Long> before) {
        Map<String, Long> difference = new LinkedHashMap<>();
        after.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            long count = entry.getValue() - before.getOrDefault(entry.getKey(), 0L);
            if (count > 0L) {
                difference.put(entry.getKey(), count);
            }
        });
        return difference;
    }

    private static String formatDistribution(Map<String, Long> distribution) {
        return distribution.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .reduce((left, right) -> left + "," + right)
                .orElse("");
    }

    private static void writeMetrics(Path path, List<String> lines) throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(path, lines, StandardCharsets.UTF_8);
    }

    private static long performLightweightWork(String payload, int workUnits) {
        long value = payload.hashCode() ^ 0x9E3779B97F4A7C15L;
        for (int index = 0; index < workUnits; index++) {
            value ^= Long.rotateLeft(value + index + 0xD1B54A32D192ED03L, 17);
            value *= 0x94D049BB133111EBL;
        }
        return value;
    }

    private static final class BaselineOutput implements SchedulerOutput, AutoCloseable {
        private final BlockingQueue<MessageEnvelope> mailbox;
        private final Map<String, ThreadPoolExecutor> executors = new LinkedHashMap<>();
        private final ConcurrentHashMap<String, CompletableFuture<JobResultMessage>> results =
                new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> assignmentsByWorker =
                new ConcurrentHashMap<>();
        private final CompletableFuture<Throwable> failure = new CompletableFuture<>();
        private final AtomicLong assignmentCount = new AtomicLong();
        private final int workUnits;

        private BaselineOutput(BlockingQueue<MessageEnvelope> mailbox,
                               int workerCount,
                               int maxConcurrentTasksPerWorker,
                               int workUnits) {
            this.mailbox = mailbox;
            this.workUnits = workUnits;
            for (int index = 0; index < workerCount; index++) {
                String workerId = "baseline-worker-" + (index + 1);
                AtomicInteger threadNumber = new AtomicInteger();
                ThreadFactory factory = runnable -> Thread.ofPlatform()
                        .name(workerId + "-task-" + threadNumber.incrementAndGet())
                        .daemon(true)
                        .unstarted(runnable);
                executors.put(workerId, new ThreadPoolExecutor(
                        maxConcurrentTasksPerWorker,
                        maxConcurrentTasksPerWorker,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(maxConcurrentTasksPerWorker),
                        factory,
                        new ThreadPoolExecutor.AbortPolicy()
                ));
            }
        }

        private CompletableFuture<JobResultMessage> expectResult(String jobId) {
            CompletableFuture<JobResultMessage> result = new CompletableFuture<>();
            if (results.putIfAbsent(jobId, result) != null) {
                throw new IllegalStateException("Duplicate baseline job id: " + jobId);
            }
            return result;
        }

        @Override
        public void sendTask(PeerInfo peer, TaskAssignMessage message) {
            ThreadPoolExecutor executor = executors.get(peer.getNodeId());
            if (executor == null) {
                throw new IllegalArgumentException("Unknown baseline worker: " + peer.getNodeId());
            }
            assignmentCount.incrementAndGet();
            assignmentsByWorker.computeIfAbsent(peer.getNodeId(), ignored -> new AtomicLong())
                    .incrementAndGet();
            try {
                executor.execute(() -> execute(peer.getNodeId(), message));
            } catch (RuntimeException e) {
                failure.complete(e);
                throw e;
            }
        }

        private void execute(String workerId, TaskAssignMessage assignment) {
            try {
                long resultPayload = performLightweightWork(String.valueOf(assignment.getPayload()), workUnits);
                TaskResultMessage result = new TaskResultMessage(
                        workerId,
                        FIXED_TIMESTAMP,
                        assignment.getTaskId(),
                        assignment.getJobId(),
                        assignment.getAttemptNumber(),
                        assignment.getAssignmentId(),
                        Long.toUnsignedString(resultPayload),
                        true,
                        ""
                );
                mailbox.put(new MessageEnvelope(result, workerId));
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                failure.complete(e);
            }
        }

        @Override
        public boolean sendJobResult(String requesterNodeId, JobResultMessage message) {
            CompletableFuture<JobResultMessage> result = results.remove(message.getJobId());
            if (result == null) {
                failure.complete(new IllegalStateException(
                        "No result expectation for baseline job " + message.getJobId()
                ));
                return false;
            }
            result.complete(message);
            return true;
        }

        private long assignmentCount() {
            return assignmentCount.get();
        }

        private Map<String, Long> assignmentDistribution() {
            Map<String, Long> snapshot = new LinkedHashMap<>();
            assignmentsByWorker.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry ->
                    snapshot.put(entry.getKey(), entry.getValue().get())
            );
            return snapshot;
        }

        private CompletableFuture<Throwable> failure() {
            return failure;
        }

        @Override
        public void close() throws InterruptedException {
            for (ThreadPoolExecutor executor : executors.values()) {
                executor.shutdownNow();
            }
            for (ThreadPoolExecutor executor : executors.values()) {
                assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS),
                        "Baseline executor did not stop.");
            }
        }
    }
}
