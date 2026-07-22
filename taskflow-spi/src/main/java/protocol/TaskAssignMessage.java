package protocol;

import com.google.gson.annotations.SerializedName;

public class TaskAssignMessage extends Message {

    private String taskId;
    private String jobId;
    private String taskType;
    private int attemptNumber;
    private String assignmentId;
    private long leaseExpiresAtEpochMillis;
    private Object payload;
    @SerializedName(value = "parameter", alternate = {"param"})
    private String parameter;

    /**
     * Legacy template constructor. Messages created through this overload are
     * protocol version 1 and are not valid wire assignments after the version
     * 2 assignment-identity migration. The coordinator enriches job-plugin
     * templates with {@link #withAssignmentIdentity(int, String, long)} before
     * publication.
     */
    public TaskAssignMessage(String nodeId, String time,
                             String taskId, String jobId,
                             String taskType, Object payload, String param) {
        this.protocolVersion = ProtocolVersions.VERSION_1;
        this.type = MessageType.TASK_ASSIGN;
        this.nodeId = nodeId;
        this.time = time;
        this.taskId = taskId;
        this.jobId = jobId;
        this.taskType = taskType;
        this.payload = payload;
        this.parameter = param;
    }

    public TaskAssignMessage(String nodeId, String time,
                             String taskId, String jobId,
                             String taskType,
                             int attemptNumber,
                             String assignmentId,
                             long leaseExpiresAtEpochMillis,
                             Object payload,
                             String parameter) {
        this.type = MessageType.TASK_ASSIGN;
        this.nodeId = nodeId;
        this.time = time;
        this.taskId = taskId;
        this.jobId = jobId;
        this.taskType = taskType;
        this.attemptNumber = attemptNumber;
        this.assignmentId = assignmentId;
        this.leaseExpiresAtEpochMillis = leaseExpiresAtEpochMillis;
        this.payload = payload;
        this.parameter = parameter;
    }

    public TaskAssignMessage() {
        this.type = MessageType.TASK_ASSIGN;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskType() {
        return taskType;
    }

    public Object getPayload() {
        return payload;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public String getAssignmentId() {
        return assignmentId;
    }

    public long getLeaseExpiresAtEpochMillis() {
        return leaseExpiresAtEpochMillis;
    }

    public String getParam() {
        return parameter;
    }

    public String getParameter() {
        return parameter;
    }

    public TaskAssignMessage withAssignmentIdentity(int attemptNumber,
                                                     String assignmentId,
                                                     long leaseExpiresAtEpochMillis) {
        return new TaskAssignMessage(
                nodeId,
                time,
                taskId,
                jobId,
                taskType,
                attemptNumber,
                assignmentId,
                leaseExpiresAtEpochMillis,
                payload,
                parameter
        );
    }
}
