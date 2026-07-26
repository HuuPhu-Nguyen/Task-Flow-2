package server.scheduler;

import plugin.RetrySafety;
import plugin.TaskResourceProfile;
import protocol.JobSubmitMessage;
import protocol.TaskAssignMessage;
import server.job.EmbarrassinglyParallelJob;
import server.job.TaskPlugin;
import server.job.TaskUnit;

import java.time.Instant;
import java.util.List;

public final class ExpandingTestTaskPlugin implements TaskPlugin {
    static final String TASK_TYPE = "EXPANDING_TEST_TASK";

    @Override
    public String taskType() {
        return TASK_TYPE;
    }

    @Override
    public RetrySafety retrySafety() {
        return RetrySafety.PURE;
    }

    @Override
    public TaskResourceProfile resourceProfile() {
        return TaskResourceProfile.ofCapacityUnits(1);
    }

    @Override
    public EmbarrassinglyParallelJob<?, ?> createJob(
            JobSubmitMessage message,
            String requesterId) {
        return new ExpandingJob(message.getJobId(), requesterId);
    }

    private static final class ExpandingJob
            extends EmbarrassinglyParallelJob<String, String> {
        private ExpandingJob(String jobId, String requesterNodeId) {
            super(jobId, requesterNodeId, TASK_TYPE);
        }

        @Override
        public void initializeTasks(JobSubmitMessage message) {
            for (int index = 0; index < message.getTaskPayloads().size(); index++) {
                Object payload = message.getTaskPayloads().get(index);
                addTask(index * 2, payload);
                addTask(index * 2 + 1, payload);
            }
        }

        private void addTask(int index, Object payload) {
            String taskId = "task-" + jobId + "-" + index;
            tasks.put(
                    taskId,
                    new TaskUnit<>(taskId, jobId, String.valueOf(payload)) {
                    }
            );
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
                    TASK_TYPE,
                    task.getPayload(),
                    ""
            );
        }
    }
}
