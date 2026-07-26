package protocol;

import java.util.List;

public class JobResultMessage extends Message {

    private String jobId;
    private String taskType;
    private boolean successful;
    private Object resultPayload;
    private List<Object> resultsByTaskId;
    private String errorMessage;
    private AdmissionRejection admissionRejection;

    public JobResultMessage(String nodeId, String time,
                            String jobId, String taskType,
                            boolean successful,
                            List<Object> resultsByTaskId) {
        this(nodeId, time, jobId, taskType, successful, resultsByTaskId, (String) null);
    }

    public JobResultMessage(String nodeId, String time,
                            String jobId, String taskType,
                            boolean successful,
                            List<Object> resultsByTaskId,
                            String errorMessage) {
        this(
                nodeId,
                time,
                jobId,
                taskType,
                successful,
                resultsByTaskId,
                resultsByTaskId,
                errorMessage,
                null,
                false
        );
    }

    public JobResultMessage(String nodeId, String time,
                            String jobId, String taskType,
                            boolean successful,
                            Object resultPayload,
                            List<Object> resultsByTaskId,
                            String errorMessage) {
        this(
                nodeId,
                time,
                jobId,
                taskType,
                successful,
                resultPayload,
                resultsByTaskId,
                errorMessage,
                null,
                true
        );
    }

    public JobResultMessage(String nodeId, String time,
                            String jobId, String taskType,
                            boolean successful,
                            Object resultPayload,
                            List<Object> resultsByTaskId) {
        this(nodeId, time, jobId, taskType, successful, resultPayload, resultsByTaskId, null);
    }

    private JobResultMessage(String nodeId, String time,
                             String jobId, String taskType,
                             boolean successful,
                             Object resultPayload,
                             List<Object> resultsByTaskId,
                             String errorMessage,
                             AdmissionRejection admissionRejection,
                             boolean deriveCompatibilityList) {
        if (successful && admissionRejection != null) {
            throw new IllegalArgumentException(
                    "Successful job results cannot carry an admission rejection."
            );
        }
        this.type = MessageType.JOB_RESULT;
        this.nodeId = nodeId;
        this.time = time;
        this.jobId = jobId;
        this.taskType = taskType;
        this.successful = successful;
        this.resultPayload = resultPayload;
        this.resultsByTaskId = deriveCompatibilityList && resultsByTaskId == null
                ? compatibilityResultList(resultPayload)
                : resultsByTaskId;
        this.errorMessage = errorMessage;
        this.admissionRejection = admissionRejection;
    }

    public static JobResultMessage admissionRejected(String nodeId,
                                                     String time,
                                                     String jobId,
                                                     String taskType,
                                                     String errorMessage,
                                                     AdmissionRejection rejection) {
        if (rejection == null) {
            throw new IllegalArgumentException("Admission rejection detail is required.");
        }
        return new JobResultMessage(
                nodeId,
                time,
                jobId,
                taskType,
                false,
                List.of(),
                List.of(),
                errorMessage,
                rejection,
                false
        );
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

    public Object getResultPayload() {
        return resultPayload == null ? resultsByTaskId : resultPayload;
    }

    public List<Object> getResultPayloadList() {
        if (resultsByTaskId != null) {
            return resultsByTaskId;
        }
        return compatibilityResultList(resultPayload);
    }

    public List<Object> getResultsByTaskId() {return resultsByTaskId;}

    public String getErrorMessage() {
        return errorMessage;
    }

    public AdmissionRejection getAdmissionRejection() {
        return admissionRejection;
    }

    private static List<Object> compatibilityResultList(Object payload) {
        if (payload == null) {
            return List.of();
        }
        if (payload instanceof List<?> list) {
            return list.stream()
                    .map(Object.class::cast)
                    .toList();
        }
        return List.of(payload);
    }
}
