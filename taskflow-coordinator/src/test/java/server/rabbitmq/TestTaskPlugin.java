package server.rabbitmq;

import plugin.RetrySafety;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;
import server.job.TaskUnit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TestTaskPlugin implements TaskPlugin {
    static final String TASK_TYPE = "RABBITMQ_TEST_TASK";

    @Override
    public String taskType() {
        return TASK_TYPE;
    }

    @Override
    public RetrySafety retrySafety() {
        return RetrySafety.PURE;
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(JobSubmitMessage message, String requesterId) {
        return new TestJob(message.getJobId(), requesterId);
    }

    private static class TestJob extends EmbarrassinglyParallelJob<String, String> {
        private final List<Object> results = new ArrayList<>();

        TestJob(String jobId, String requesterNodeId) {
            super(jobId, requesterNodeId, TASK_TYPE);
        }

        @Override
        public void initializeTasks(JobSubmitMessage message) {
            List<Object> payloads = message.getTaskPayloads();
            if (payloads == null) {
                return;
            }
            for (int i = 0; i < payloads.size(); i++) {
                String taskId = "task-" + jobId + "-" + i;
                tasks.put(taskId, new TaskUnit<>(taskId, jobId, String.valueOf(payloads.get(i))) {});
            }
        }

        @Override
        protected void onTaskSuccess(TaskUnit<String> task, String resultData) {
            results.add(resultData);
        }

        @Override
        public List<Object> aggregateAndSendResult() {
            return List.copyOf(results);
        }

        @Override
        protected String parseResult(Object payloads) {
            return String.valueOf(payloads);
        }

        @Override
        public TaskAssignMessage createTaskAssignMessage(TaskUnit<?> task) {
            return new TaskAssignMessage(
                    "COORDINATOR",
                    Instant.now().toString(),
                    task.getTaskId(),
                    task.getJobId(),
                    TASK_TYPE,
                    task.getPayload(),
                    ""
            );
        }
    }
}
