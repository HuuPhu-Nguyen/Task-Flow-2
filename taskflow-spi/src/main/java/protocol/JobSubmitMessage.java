package protocol;

import java.util.List;

public class JobSubmitMessage extends Message {

    private String jobId;
    private String taskType;
    private List<Object> taskPayloads;
    private String parameter;

    public JobSubmitMessage(String nodeId, String time,
                            String jobId, String taskType, List<Object> taskPayloads, String parameter) {
        this.type = MessageType.JOB_SUBMIT;
        this.nodeId = nodeId;
        this.time = time;
        this.jobId = jobId;
        this.taskType = taskType;
        this.taskPayloads = taskPayloads;
        this.parameter = parameter;
    }

    public JobSubmitMessage() {
        this.type = MessageType.JOB_SUBMIT;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskType() {
        return taskType;
    }

    public List<Object> getTaskPayloads() {
        return taskPayloads;
    }

    public String getParameter() {
        return parameter;
    }
}