package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskUnit;
import server.runtime.TaskFlowClock;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class SchedulerWorkloadIndexTest {

    @Test
    void pendingTaskAndRunnableJobIndexesPreserveDequeOrderWithoutQueueSearches() {
        SchedulerWorkloadIndex index = new SchedulerWorkloadIndex();
        index.addPendingTask("job-a", "a-1", false);
        index.addPendingTask("job-a", "a-2", false);
        index.addPendingTask("job-b", "b-1", false);

        assertEquals("job-a", index.pollRunnableJob());
        assertEquals("a-1", index.pollPendingTask("job-a"));
        index.addPendingTask("job-a", "a-retry-1", true);
        index.addPendingTask("job-a", "a-retry-2", true);
        index.requeueRunnableJob("job-a");

        assertEquals("job-b", index.pollRunnableJob());
        assertEquals("b-1", index.pollPendingTask("job-b"));
        assertEquals("job-a", index.pollRunnableJob());
        assertEquals("a-retry-1", index.pollPendingTask("job-a"));
        assertEquals("a-retry-2", index.pollPendingTask("job-a"));

        assertEquals(1, index.pendingTaskCount("job-a"));
        index.removePendingTask("job-a", "a-2");
        assertEquals(0, index.pendingTaskCount("job-a"));
        assertEquals(0, index.snapshot().runnableJobs());
    }

    @Test
    void poppedDeadlineMustMatchExactCurrentAssignmentId() {
        SchedulerConfig config = config(100L, 1_000L);
        FixedClock clock = new FixedClock(1_000L);
        SchedulerState state = new SchedulerState(config);
        IndexedJob job = new IndexedJob("job-stale");
        TaskUnit<String> task = job.addTask("task-stale", clock);
        AssignmentIdentity first = identity(task.getTaskId(), 1, 2_000L);
        task.markAssigned(first, 1_000L, "owner");
        state.addActiveJob(job, "", "");

        task.resetToPending();
        AssignmentIdentity replacement = identity(task.getTaskId(), 2, 3_000L);
        task.markAssigned(replacement, 1_050L, "owner");

        assertNull(state.pollDueTimeout(1_101L));

        SchedulerWorkloadIndex.Snapshot snapshot = state.workloadSnapshot();
        assertEquals(1L, snapshot.deadlineEntriesPopped());
        assertEquals(1L, snapshot.deadlineEntriesValidated());
        assertEquals(1L, snapshot.staleDeadlineEntriesRejected());
        assertEquals(0, snapshot.deadlineEntries());
        assertEquals(0, snapshot.liveAssignments());
    }

    @Test
    void authoritativeAssignmentClosureRemovesDeadlinesAndReindexesRetry() {
        SchedulerConfig config = config(100L, 1_000L);
        FixedClock clock = new FixedClock(1_000L);
        SchedulerState state = new SchedulerState(config);
        IndexedJob job = new IndexedJob("job-retry");
        TaskUnit<String> task = job.addTask("task-retry", clock);
        AssignmentIdentity assignment = identity(task.getTaskId(), 1, 2_000L);
        task.markAssigned(assignment, 1_000L, "owner");
        state.addActiveJob(job, "", "");

        assertEquals(2, state.workloadSnapshot().deadlineEntries());
        task.resetToPending();
        state.indexClosedAssignment(task, assignment);

        SchedulerWorkloadIndex.Snapshot snapshot = state.workloadSnapshot();
        assertEquals(0, snapshot.deadlineEntries());
        assertEquals(0, snapshot.liveAssignments());
        assertEquals(1L, snapshot.pendingTasks());
        assertEquals(1, snapshot.runnableJobs());
        assertSame(task, state.pollPendingTask(job.getJobId()));
    }

    @Test
    void repeatedDispatchClosureKeepsDeadlineIndexAtTwoEntriesPerLiveAssignment() {
        SchedulerWorkloadIndex index = new SchedulerWorkloadIndex();
        String jobId = "job-dispatch-failure";
        String taskId = "task-dispatch-failure";

        for (int attempt = 1; attempt <= 10_000; attempt++) {
            AssignmentIdentity assignment = identity(taskId, attempt, 120_000L + attempt);
            index.scheduleAssignment(jobId, taskId, 1_000L, 60_000L, assignment);
            assertEquals(2, index.snapshot().deadlineEntries());
            index.cancelAssignment(jobId, taskId, assignment);
            assertEquals(0, index.snapshot().deadlineEntries());
        }

        assertEquals(0, index.snapshot().liveAssignments());
        assertEquals(0L, index.snapshot().staleDeadlineEntriesRejected());
    }

    @Test
    void replacementAssignmentCannotLeaveMoreThanOneGenerationIndexedForATask() {
        SchedulerWorkloadIndex index = new SchedulerWorkloadIndex();
        String jobId = "job-replacement";
        String taskId = "task-replacement";

        for (int attempt = 1; attempt <= 10_000; attempt++) {
            AssignmentIdentity assignment = identity(taskId, attempt, 120_000L + attempt);
            index.scheduleAssignment(jobId, taskId, 1_000L, 60_000L, assignment);
            assertEquals(1, index.snapshot().liveAssignments());
            assertEquals(2, index.snapshot().deadlineEntries());
        }
    }

    @Test
    void profileOneTickWithOneHundredThousandNonDueAssignmentsUsesOnlyDeadlineHeads() {
        SchedulerWorkloadIndex index = new SchedulerWorkloadIndex();
        int assignedTasks = 100_000;

        for (int taskIndex = 0; taskIndex < assignedTasks; taskIndex++) {
            String taskId = "task-" + taskIndex;
            AssignmentIdentity assignment = new AssignmentIdentity(
                    taskId,
                    1,
                    new UUID(0L, taskIndex + 1L).toString(),
                    "worker-" + (taskIndex % 100),
                    120_000L
            );
            index.scheduleAssignment("job-profile", taskId, 1_000L, 60_000L, assignment);
        }
        SchedulerWorkloadIndex.Snapshot before = index.snapshot();

        assertNull(index.pollDue(SchedulerWorkloadIndex.DeadlineKind.TASK_TIMEOUT, 1_001L));
        assertNull(index.pollDue(SchedulerWorkloadIndex.DeadlineKind.LEASE_EXPIRY, 1_001L));

        SchedulerWorkloadIndex.Snapshot after = index.snapshot();
        assertEquals(assignedTasks, after.liveAssignments());
        assertEquals(assignedTasks * 2, after.deadlineEntries());
        assertEquals(2L, after.deadlineHeadChecks() - before.deadlineHeadChecks());
        assertEquals(0L, after.deadlineEntriesPopped() - before.deadlineEntriesPopped());
        assertEquals(0L, after.deadlineEntriesValidated() - before.deadlineEntriesValidated());
    }

    private static SchedulerConfig config(long timeoutMillis, long leaseMillis) {
        SchedulerConfig defaults = SchedulerConfig.defaults();
        return new SchedulerConfig(
                timeoutMillis,
                leaseMillis,
                defaults.maxTasksPerPeer(),
                defaults.maxTaskRetries(),
                defaults.inboundQueueCapacity(),
                defaults.jobResultMaxDeliveryAttempts(),
                defaults.metricsLogIntervalMillis(),
                defaults.peerScoreLoadWeight(),
                defaults.peerScoreLatencyWeight(),
                defaults.peerScoreDurationWeight(),
                defaults.peerScoreFailureWeight(),
                defaults.peerScoreLatencyBaselineMillis(),
                defaults.peerScoreDurationBaselineMillis(),
                defaults.peerScoreEwmaAlpha()
        );
    }

    private static AssignmentIdentity identity(String taskId,
                                               int attemptNumber,
                                               long leaseExpiresAtMillis) {
        return new AssignmentIdentity(
                taskId,
                attemptNumber,
                new UUID(0L, attemptNumber).toString(),
                "worker-1",
                leaseExpiresAtMillis
        );
    }

    private static final class FixedClock implements TaskFlowClock {
        private final long nowMillis;

        private FixedClock(long nowMillis) {
            this.nowMillis = nowMillis;
        }

        @Override
        public Instant now() {
            return Instant.ofEpochMilli(nowMillis);
        }

        @Override
        public long nowEpochMillis() {
            return nowMillis;
        }
    }

    private static final class IndexedJob extends EmbarrassinglyParallelJob<String, String> {
        private IndexedJob(String jobId) {
            super(jobId, "requester-1", "TEST_TASK");
        }

        private TaskUnit<String> addTask(String taskId, TaskFlowClock clock) {
            TaskUnit<String> task = new TaskUnit<>(taskId, jobId, "payload", clock, () ->
                    "00000000-0000-0000-0000-000000000001") {
            };
            tasks.put(taskId, task);
            return task;
        }

        @Override
        public void initializeTasks(JobSubmitMessage message) {
        }

        @Override
        protected void onTaskSuccess(TaskUnit<String> task, String resultData) {
        }

        @Override
        public List<Object> aggregateAndSendResult() {
            return List.of();
        }

        @Override
        protected String parseResult(Object payloads) {
            return String.valueOf(payloads);
        }

        @Override
        public TaskAssignMessage createTaskAssignMessage(TaskUnit<?> task) {
            return new TaskAssignMessage(
                    "COORDINATOR",
                    Instant.EPOCH.toString(),
                    task.getTaskId(),
                    task.getJobId(),
                    taskType,
                    task.getPayload(),
                    ""
            );
        }
    }
}
