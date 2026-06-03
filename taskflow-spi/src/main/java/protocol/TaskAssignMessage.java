package protocol;

public class TaskAssignMessage extends Message {

    private String taskId;
    private String jobId;
    private String taskType;
    private Object payload;
    private String param;

    public TaskAssignMessage(String nodeId, String time,
                             String taskId, String jobId,
                             String taskType, Object payload, String param) {
        this.type = MessageType.TASK_ASSIGN;
        this.nodeId = nodeId;
        this.time = time;
        this.taskId = taskId;
        this.jobId = jobId;
        this.taskType = taskType;
        this.payload = payload;
        this.param = param;
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
    public String getParam() {
        return param;
    }
}