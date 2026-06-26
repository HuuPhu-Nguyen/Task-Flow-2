package protocol;

public class JobResultRequestMessage extends Message {

    private String jobId;
    private String requesterToken;

    public JobResultRequestMessage(String nodeId, String time, String jobId) {
        this(nodeId, time, jobId, "");
    }

    public JobResultRequestMessage(String nodeId, String time, String jobId, String requesterToken) {
        this.type = MessageType.JOB_RESULT_REQUEST;
        this.nodeId = nodeId;
        this.time = time;
        this.jobId = jobId;
        this.requesterToken = requesterToken == null ? "" : requesterToken;
    }

    public JobResultRequestMessage() {
        this.type = MessageType.JOB_RESULT_REQUEST;
    }

    public String getJobId() {
        return jobId;
    }

    public String getRequesterToken() {
        return requesterToken;
    }
}
