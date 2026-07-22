package peer.engine;

import org.junit.jupiter.api.Test;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerExecutionEngineTest {

    @Test
    void submitTaskReportsSuccessfulResultSend() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("TEST", task -> "done");
            StringWriter buffer = new StringWriter();
            PrintWriter out = new PrintWriter(buffer);

            boolean sent = engine.submitTask(testTask(), out).get(2, TimeUnit.SECONDS);

            assertTrue(sent);
            assertTrue(buffer.toString().contains("\"protocolVersion\":2"));
            assertTrue(buffer.toString().contains("\"type\":\"TASK_RESULT\""));
            assertTrue(buffer.toString().contains("\"taskId\":\"task-1\""));
            assertTrue(buffer.toString().contains("\"attemptNumber\":7"));
            assertTrue(buffer.toString().contains(
                    "\"assignmentId\":\"550e8400-e29b-41d4-a716-446655440000\""));
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void submitTaskReportsFailedResultSend() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("TEST", task -> "done");
            PrintWriter out = new PrintWriter(new FailingWriter());

            boolean sent = engine.submitTask(testTask(), out).get(2, TimeUnit.SECONDS);

            assertFalse(sent);
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void failedExecutionEchoesAssignmentIdentity() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("TEST", task -> {
                throw new IllegalStateException("processor failed");
            });

            TaskResultMessage result = engine.executeTask(testTask()).get(2, TimeUnit.SECONDS);

            assertFalse(result.isSuccessful());
            assertEquals(7, result.getAttemptNumber());
            assertEquals("550e8400-e29b-41d4-a716-446655440000", result.getAssignmentId());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void shutdownTerminatesExecutionPool() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");

        engine.shutdown();

        assertTrue(engine.isShutdown());
        assertTrue(engine.awaitTermination(2, TimeUnit.SECONDS));
    }

    @Test
    void duplicateProcessorTaskTypeIsRejected() {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("test", task -> "first");

            IllegalStateException error = assertThrows(IllegalStateException.class,
                    () -> engine.registerProcessor(" TEST ", task -> "second"));

            assertTrue(error.getMessage().contains("TEST"));
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void duplicateRunningAssignmentSharesOneProcessorInvocation() throws Exception {
        CountDownLatch processorEntered = new CountDownLatch(1);
        CountDownLatch releaseProcessor = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        PeerExecutionEngine engine = new PeerExecutionEngine(
                "peer-1",
                new AssignmentCacheConfig(8, TimeUnit.MINUTES.toMillis(1))
        );
        try {
            engine.registerProcessor("TEST", task -> {
                invocations.incrementAndGet();
                processorEntered.countDown();
                if (!releaseProcessor.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("processor release timed out");
                }
                return "done";
            });

            AssignmentExecution first = engine.executeAssignment(testTask());
            assertTrue(processorEntered.await(2, TimeUnit.SECONDS));
            AssignmentExecution duplicate = engine.executeAssignment(testTask());

            assertEquals(AssignmentExecution.Disposition.STARTED, first.disposition());
            assertEquals(AssignmentExecution.Disposition.DUPLICATE_RUNNING, duplicate.disposition());
            assertSame(first.resultFuture(), duplicate.resultFuture());
            assertEquals(1, invocations.get());
            assertEquals(1, engine.assignmentCacheSnapshot().runningDuplicateCount());

            releaseProcessor.countDown();
            assertSame(
                    first.resultFuture().get(2, TimeUnit.SECONDS),
                    duplicate.resultFuture().get(2, TimeUnit.SECONDS)
            );
        } finally {
            releaseProcessor.countDown();
            engine.shutdown();
        }
    }

    @Test
    void completedDuplicateReturnsSameResultInstanceAndIdentity() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("TEST", task -> {
                invocations.incrementAndGet();
                return "done";
            });

            AssignmentExecution first = engine.executeAssignment(testTask());
            TaskResultMessage firstResult = first.resultFuture().get(2, TimeUnit.SECONDS);
            AssignmentExecution duplicate = engine.executeAssignment(testTask());
            TaskResultMessage duplicateResult = duplicate.resultFuture().get(2, TimeUnit.SECONDS);

            assertEquals(AssignmentExecution.Disposition.DUPLICATE_COMPLETED, duplicate.disposition());
            assertSame(first.resultFuture(), duplicate.resultFuture());
            assertSame(firstResult, duplicateResult);
            assertEquals(firstResult.getTaskId(), duplicateResult.getTaskId());
            assertEquals(firstResult.getAttemptNumber(), duplicateResult.getAttemptNumber());
            assertEquals(firstResult.getAssignmentId(), duplicateResult.getAssignmentId());
            assertEquals(1, invocations.get());
            assertEquals(1, engine.assignmentCacheSnapshot().completedDuplicateCount());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void retryGenerationKeepsLogicalTaskIdAndSuppliesNewStableAssignmentId() throws Exception {
        List<TaskAssignMessage> executionContexts = new CopyOnWriteArrayList<>();
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("TEST", task -> {
                executionContexts.add(task);
                return "done-" + task.getAttemptNumber();
            });
            TaskAssignMessage first = testTask(
                    "task-1",
                    1,
                    "550e8400-e29b-41d4-a716-446655440001"
            );
            TaskAssignMessage retry = testTask(
                    "task-1",
                    2,
                    "550e8400-e29b-41d4-a716-446655440002"
            );

            engine.executeAssignment(first).resultFuture().get(2, TimeUnit.SECONDS);
            AssignmentExecution redelivery = engine.executeAssignment(first);
            redelivery.resultFuture().get(2, TimeUnit.SECONDS);
            AssignmentExecution retryExecution = engine.executeAssignment(retry);
            retryExecution.resultFuture().get(2, TimeUnit.SECONDS);

            assertEquals(AssignmentExecution.Disposition.DUPLICATE_COMPLETED, redelivery.disposition());
            assertEquals(AssignmentExecution.Disposition.STARTED, retryExecution.disposition());
            assertEquals(2, executionContexts.size());
            assertEquals("task-1", executionContexts.get(0).getTaskId());
            assertEquals("task-1", executionContexts.get(1).getTaskId());
            assertEquals(1, executionContexts.get(0).getAttemptNumber());
            assertEquals(2, executionContexts.get(1).getAttemptNumber());
            assertEquals(first.getAssignmentId(), executionContexts.get(0).getAssignmentId());
            assertEquals(retry.getAssignmentId(), executionContexts.get(1).getAssignmentId());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void cacheBoundsAndCapacityEvictionPermitReexecution() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        PeerExecutionEngine engine = new PeerExecutionEngine(
                "peer-1",
                new AssignmentCacheConfig(1, TimeUnit.MINUTES.toMillis(1))
        );
        try {
            engine.registerProcessor("TEST", task -> invocations.incrementAndGet());
            TaskAssignMessage firstTask = testTask("task-1", "550e8400-e29b-41d4-a716-446655440001");
            TaskAssignMessage secondTask = testTask("task-2", "550e8400-e29b-41d4-a716-446655440002");

            engine.executeAssignment(firstTask).resultFuture().get(2, TimeUnit.SECONDS);
            engine.executeAssignment(secondTask).resultFuture().get(2, TimeUnit.SECONDS);
            AssignmentCacheSnapshot afterFirstEviction = engine.assignmentCacheSnapshot();
            AssignmentExecution firstTaskAgain = engine.executeAssignment(firstTask);
            firstTaskAgain.resultFuture().get(2, TimeUnit.SECONDS);
            AssignmentCacheSnapshot finalSnapshot = engine.assignmentCacheSnapshot();

            assertEquals(AssignmentExecution.Disposition.STARTED, firstTaskAgain.disposition());
            assertEquals(1, afterFirstEviction.size());
            assertEquals(1, afterFirstEviction.capacityEvictionCount());
            assertEquals(1, finalSnapshot.size());
            assertEquals(2, finalSnapshot.capacityEvictionCount());
            assertEquals(3, invocations.get());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void ttlEvictionPermitsReexecutionAndExposesMetric() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        AtomicLong clock = new AtomicLong(1_000L);
        PeerExecutionEngine engine = new PeerExecutionEngine(
                "peer-1",
                new AssignmentCacheConfig(8, 10L),
                clock::get
        );
        try {
            engine.registerProcessor("TEST", task -> invocations.incrementAndGet());

            engine.executeAssignment(testTask()).resultFuture().get(2, TimeUnit.SECONDS);
            assertEquals(
                    AssignmentExecution.Disposition.DUPLICATE_COMPLETED,
                    engine.executeAssignment(testTask()).disposition()
            );
            clock.set(1_011L);
            AssignmentExecution afterExpiry = engine.executeAssignment(testTask());
            afterExpiry.resultFuture().get(2, TimeUnit.SECONDS);

            assertEquals(AssignmentExecution.Disposition.STARTED, afterExpiry.disposition());
            assertEquals(2, invocations.get());
            assertEquals(1, engine.assignmentCacheSnapshot().ttlEvictionCount());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void completionFromEvictedGenerationDoesNotCompleteNewerCacheEntry() throws Exception {
        AtomicInteger firstTaskInvocations = new AtomicInteger();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        PeerExecutionEngine engine = new PeerExecutionEngine(
                "peer-1",
                new AssignmentCacheConfig(1, TimeUnit.MINUTES.toMillis(1))
        );
        try {
            engine.registerProcessor("TEST", task -> {
                if (!"task-1".equals(task.getTaskId())) {
                    return "other";
                }
                int invocation = firstTaskInvocations.incrementAndGet();
                CountDownLatch started = invocation == 1 ? firstStarted : secondStarted;
                CountDownLatch release = invocation == 1 ? releaseFirst : releaseSecond;
                started.countDown();
                if (!release.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("processor release timed out");
                }
                return "generation-" + invocation;
            });
            TaskAssignMessage firstTask = testTask("task-1", "550e8400-e29b-41d4-a716-446655440001");
            TaskAssignMessage evictingTask = testTask("task-2", "550e8400-e29b-41d4-a716-446655440002");

            AssignmentExecution evictedGeneration = engine.executeAssignment(firstTask);
            assertTrue(firstStarted.await(2, TimeUnit.SECONDS));
            engine.executeAssignment(evictingTask).resultFuture().get(2, TimeUnit.SECONDS);
            AssignmentExecution currentGeneration = engine.executeAssignment(firstTask);
            assertTrue(secondStarted.await(2, TimeUnit.SECONDS));

            releaseFirst.countDown();
            evictedGeneration.resultFuture().get(2, TimeUnit.SECONDS);

            AssignmentCacheSnapshot whileCurrentGenerationRuns = engine.assignmentCacheSnapshot();
            assertEquals(1, whileCurrentGenerationRuns.runningEntries());
            assertEquals(0, whileCurrentGenerationRuns.completedEntries());
            assertFalse(currentGeneration.resultFuture().isDone());

            releaseSecond.countDown();
            currentGeneration.resultFuture().get(2, TimeUnit.SECONDS);
            assertEquals(2, firstTaskInvocations.get());
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
            engine.shutdown();
        }
    }

    @Test
    void newEngineReexecutesBecauseCacheIsNotDurable() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        PeerExecutionEngine firstEngine = new PeerExecutionEngine("peer-1");
        try {
            firstEngine.registerProcessor("TEST", task -> invocations.incrementAndGet());
            firstEngine.executeAssignment(testTask()).resultFuture().get(2, TimeUnit.SECONDS);
        } finally {
            firstEngine.shutdown();
        }

        PeerExecutionEngine restartedEngine = new PeerExecutionEngine("peer-1");
        try {
            restartedEngine.registerProcessor("TEST", task -> invocations.incrementAndGet());

            AssignmentExecution afterRestart = restartedEngine.executeAssignment(testTask());
            afterRestart.resultFuture().get(2, TimeUnit.SECONDS);

            assertEquals(AssignmentExecution.Disposition.STARTED, afterRestart.disposition());
            assertEquals(2, invocations.get());
        } finally {
            restartedEngine.shutdown();
        }
    }

    @Test
    void assignmentIdReuseForDifferentTaskIdentityIsRejected() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine("peer-1");
        try {
            engine.registerProcessor("TEST", task -> "done");
            engine.executeAssignment(testTask()).resultFuture().get(2, TimeUnit.SECONDS);

            AssignmentCacheConflictException error = assertThrows(
                    AssignmentCacheConflictException.class,
                    () -> engine.executeAssignment(testTask(
                            "different-task",
                            "550e8400-e29b-41d4-a716-446655440000"
                    ))
            );

            assertTrue(error.getMessage().contains("was reused"));
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void assignmentCacheConfigurationReadsEnvironmentOverrides() {
        AssignmentCacheConfig defaults = AssignmentCacheConfig.fromEnvironment(Map.of());
        AssignmentCacheConfig configured = AssignmentCacheConfig.fromEnvironment(Map.of(
                AssignmentCacheConfig.MAX_ENTRIES_ENV, " 17 ",
                AssignmentCacheConfig.TTL_MILLIS_ENV, " 2500 "
        ));

        assertEquals(AssignmentCacheConfig.DEFAULT_MAX_ENTRIES, defaults.maxEntries());
        assertEquals(AssignmentCacheConfig.DEFAULT_TTL_MILLIS, defaults.ttlMillis());
        assertEquals(17, configured.maxEntries());
        assertEquals(2_500L, configured.ttlMillis());
        assertThrows(IllegalArgumentException.class, () -> new AssignmentCacheConfig(0, 1L));
        assertThrows(IllegalArgumentException.class, () -> new AssignmentCacheConfig(1, 0L));
    }

    private static TaskAssignMessage testTask() {
        return testTask("task-1", "550e8400-e29b-41d4-a716-446655440000");
    }

    private static TaskAssignMessage testTask(String taskId, String assignmentId) {
        return testTask(taskId, 7, assignmentId);
    }

    private static TaskAssignMessage testTask(String taskId, int attemptNumber, String assignmentId) {
        return new TaskAssignMessage(
                "coordinator",
                "2026-06-13T00:00:00Z",
                taskId,
                "job-1",
                "TEST",
                attemptNumber,
                assignmentId,
                1_780_000_000_000L,
                "payload",
                "param"
        );
    }

    private static final class FailingWriter extends Writer {
        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            throw new IOException("write failed");
        }

        @Override
        public void flush() throws IOException {
            throw new IOException("flush failed");
        }

        @Override
        public void close() {
        }
    }
}
