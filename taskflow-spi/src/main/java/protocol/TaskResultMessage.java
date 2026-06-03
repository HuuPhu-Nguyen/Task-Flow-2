package protocol;

import java.util.List;

public class TaskResultMessage extends Message {

    private String taskId;
    private String jobId;
    private Object resultPayload;
    private boolean successful;
    private String errorMessage;

    public TaskResultMessage(String nodeId, String time,
                             String taskId, String jobId,
                             Object resultPayload,
                             boolean successful,
                             String errorMessage) {
        this.type = MessageType.TASK_RESULT;
        this.nodeId = nodeId;
        this.time = time;
        this.taskId = taskId;
        this.jobId = jobId;
        this.resultPayload = resultPayload;
        this.successful = successful;
        this.errorMessage = errorMessage;
    }

    public TaskResultMessage() {
        this.type = MessageType.TASK_RESULT;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public Object getResultPayload() {
        return resultPayload;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}