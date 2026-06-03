package protocol;

import java.util.List;

public class JobResultMessage extends Message {

    private String jobId;
    private String taskType;
    private boolean successful;
    private List<Object> resultsByTaskId;
    private String errorMessage;

    public JobResultMessage(String nodeId, String time,
                            String jobId, String taskType,
                            boolean successful,
                            List<Object> resultsByTaskId) {
        this(nodeId, time, jobId, taskType, successful, resultsByTaskId, null);
    }

    public JobResultMessage(String nodeId, String time,
                            String jobId, String taskType,
                            boolean successful,
                            List<Object> resultsByTaskId,
                            String errorMessage) {
        this.type = MessageType.JOB_RESULT;
        this.nodeId = nodeId;
        this.time = time;
        this.jobId = jobId;
        this.taskType = taskType;
        this.successful = successful;
        this.resultsByTaskId = resultsByTaskId;
        this.errorMessage = errorMessage;
    }

    public JobResultMessage() {
        this.type = MessageType.JOB_RESULT;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskType() {
        return taskType;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public List<Object> getResultsByTaskId() {return resultsByTaskId;}

    public String getErrorMessage() {
        return errorMessage;
    }
}
