package protocol;

public class JobResultRequestMessage extends Message {

    private String jobId;
    private String requesterToken;
    private String requesterPublicKey;
    private String requesterSignature;

    public JobResultRequestMessage(String nodeId, String time, String jobId) {
        this(nodeId, time, jobId, "");
    }

    public JobResultRequestMessage(String nodeId, String time, String jobId, String requesterToken) {
        this(nodeId, time, jobId, requesterToken, "", "");
    }

    public JobResultRequestMessage(String nodeId, String time, String jobId, String requesterToken,
                                   String requesterPublicKey, String requesterSignature) {
        this.type = MessageType.JOB_RESULT_REQUEST;
        this.nodeId = nodeId;
        this.time = time;
        this.jobId = jobId;
        this.requesterToken = requesterToken == null ? "" : requesterToken;
        this.requesterPublicKey = requesterPublicKey == null ? "" : requesterPublicKey;
        this.requesterSignature = requesterSignature == null ? "" : requesterSignature;
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

    public String getRequesterPublicKey() {
        return requesterPublicKey;
    }

    public String getRequesterSignature() {
        return requesterSignature;
    }
}
