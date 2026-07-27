package protocol;

public class TaskResultMessage extends Message {

    private String taskId;
    private String jobId;
    private int attemptNumber;
    private String assignmentId;
    private Object resultPayload;
    private boolean successful;
    private String errorMessage;
    private TaskFailureClassification failureClassification;

    /**
     * Legacy protocol-version-1 constructor retained for explicit compatibility
     * tests. Results created through it are rejected by version-2 coordinators.
     */
    @Deprecated
    public TaskResultMessage(String nodeId, String time,
                             String taskId, String jobId,
                             Object resultPayload,
                             boolean successful,
                             String errorMessage) {
        this.protocolVersion = ProtocolVersions.VERSION_1;
        this.type = MessageType.TASK_RESULT;
        this.nodeId = nodeId;
        this.time = time;
        this.taskId = taskId;
        this.jobId = jobId;
        this.resultPayload = resultPayload;
        this.successful = successful;
        this.errorMessage = errorMessage;
    }

    public TaskResultMessage(String nodeId, String time,
                             String taskId, String jobId,
                             int attemptNumber,
                             String assignmentId,
                             Object resultPayload,
                             boolean successful,
                             String errorMessage) {
        this.type = MessageType.TASK_RESULT;
        this.nodeId = nodeId;
        this.time = time;
        this.taskId = taskId;
        this.jobId = jobId;
        this.attemptNumber = attemptNumber;
        this.assignmentId = assignmentId;
        this.resultPayload = resultPayload;
        this.successful = successful;
        this.errorMessage = errorMessage;
    }

    public TaskResultMessage(String nodeId, String time,
                             String taskId, String jobId,
                             int attemptNumber,
                             String assignmentId,
                             Object resultPayload,
                             boolean successful,
                             String errorMessage,
                             TaskFailureClassification failureClassification) {
        this(
                nodeId,
                time,
                taskId,
                jobId,
                attemptNumber,
                assignmentId,
                resultPayload,
                successful,
                errorMessage
        );
        this.failureClassification = failureClassification;
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

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public String getAssignmentId() {
        return assignmentId;
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

    /**
     * Missing classification retains the protocol-v2 retryable behavior.
     */
    public TaskFailureClassification getFailureClassification() {
        return failureClassification == null
                ? TaskFailureClassification.RETRYABLE
                : failureClassification;
    }

    public boolean hasExplicitFailureClassification() {
        return failureClassification != null;
    }
}
