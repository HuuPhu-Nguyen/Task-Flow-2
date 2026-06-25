package protocol;

public class JobResultRequestMessage extends Message {

    private String jobId;

    public JobResultRequestMessage(String nodeId, String time, String jobId) {
        this.type = MessageType.JOB_RESULT_REQUEST;
        this.nodeId = nodeId;
        this.time = time;
        this.jobId = jobId;
    }

    public JobResultRequestMessage() {
        this.type = MessageType.JOB_RESULT_REQUEST;
    }

    public String getJobId() {
        return jobId;
    }
}
