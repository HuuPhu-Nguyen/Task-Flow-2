package server.job;

import org.junit.jupiter.api.Test;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbarrassinglyParallelJobResultTest {

    private static class DummyJob extends EmbarrassinglyParallelJob<String, String> {
        private final Map<String, String> results = new ConcurrentHashMap<>();

        DummyJob(String jobId, String requesterNodeId) {
            super(jobId, requesterNodeId, "DUMMY");
        }

        @Override
        public void initializeTasks(JobSubmitMessage message) {
            tasks.put("task-1", new TaskUnit<>("task-1", jobId, "payload") {});
        }

        @Override
        protected void onTaskSuccess(TaskUnit<String> task, String resultData) {
            results.put(task.getTaskId(), resultData);
        }

        @Override
        public List<Object> aggregateAndSendResult() {
            return new ArrayList<>(results.values());
        }

        @Override
        protected String parseResult(Object payloads) {
            return String.valueOf(payloads);
        }

        @Override
        public TaskAssignMessage createTaskAssignMessage(TaskUnit<?> task) {
            return new TaskAssignMessage(
                    "coordinator",
                    java.time.Instant.now().toString(),
                    task.getTaskId(),
                    task.getJobId(),
                    "DUMMY",
                    task.getPayload(),
                    ""
            );
        }
    }

    @Test
    void rejectsResultsFromUnassignedOrWrongPeer() {
        DummyJob job = new DummyJob("job-1", "requester");
        job.initializeTasks(new JobSubmitMessage());

        TaskUnit<String> task = (TaskUnit<String>) job.getTasks().get("task-1");
        assertTrue(task.markAssigned("peer-a", System.currentTimeMillis() - 10));

        EmbarrassinglyParallelJob.TaskCompletion wrongPeer = job.recordResult("task-1", "peer-b", "result");
        assertFalse(wrongPeer.accepted());
        assertEquals(TaskUnit.TaskStatus.ASSIGNED, task.getStatus());

        EmbarrassinglyParallelJob.TaskCompletion accepted = job.recordResult("task-1", "peer-a", "result");
        assertTrue(accepted.accepted());
        assertTrue(accepted.durationMs() >= 0L);
        assertTrue(job.isJobComplete());
        assertEquals(TaskUnit.TaskStatus.COMPLETED, task.getStatus());

        EmbarrassinglyParallelJob.TaskCompletion duplicate = job.recordResult("task-1", "peer-a", "other");
        assertFalse(duplicate.accepted());
        assertEquals(1, job.aggregateAndSendResult().size());
    }
}
