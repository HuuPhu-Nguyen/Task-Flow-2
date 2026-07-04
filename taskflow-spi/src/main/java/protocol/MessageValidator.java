package protocol;

import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class MessageValidator {
    private static final Gson GSON = new Gson();
    private static final Set<String> KNOWN_TYPES = Set.of(
            MessageType.PING,
            MessageType.PONG,
            MessageType.JOB_SUBMIT,
            MessageType.JOB_RESULT_REQUEST,
            MessageType.TASK_ASSIGN,
            MessageType.TASK_RESULT,
            MessageType.JOB_RESULT,
            MessageType.PEER_DISCONNECTED
    );
    private static final int MAX_ID_LENGTH = 192;
    private static final int MAX_TASK_TYPE_LENGTH = 120;
    private static final int MAX_TIME_LENGTH = 128;
    private static final int MAX_TEXT_FIELD_BYTES = 8 * 1024;

    private MessageValidator() {
    }

    public static void validate(Message message) {
        if (message == null) {
            throw new MessageValidationException("Message is required.");
        }
        requireKnownType(message.getType());
        validatePeerId(message.getNodeId(), "Message nodeId");
        requireTextBytes(message.getTime(), "Message time", MAX_TIME_LENGTH);

        switch (message) {
            case PingMessage ignored -> {
            }
            case PongMessage pong -> validatePong(pong);
            case JobSubmitMessage submit -> validateJobSubmit(submit);
            case JobResultRequestMessage request -> validateJobResultRequest(request);
            case TaskAssignMessage assignment -> validateTaskAssignment(assignment);
            case TaskResultMessage result -> validateTaskResult(result);
            case JobResultMessage result -> validateJobResult(result);
            case PeerDisconnectedMessage disconnected -> validatePeerDisconnected(disconnected);
            default -> throw new MessageValidationException("Unsupported message class: "
                    + message.getClass().getName());
        }
    }

    public static void validateJobSubmit(JobSubmitMessage submit) {
        if (submit == null) {
            throw new MessageValidationException("Job submit message is required.");
        }
        validateJobId(submit.getJobId(), "Job id");
        validateTaskType(submit.getTaskType(), "Task type");
        if (submit.getTaskPayloads() == null) {
            throw new MessageValidationException("Job submit taskPayloads field is required.");
        }
        if (submit.getTaskPayloads().size() > PayloadLimits.maxTasksPerJob()) {
            throw new MessageValidationException("Job submit task payload count exceeds "
                    + PayloadLimits.MAX_TASKS_PER_JOB_ENV + " (" + PayloadLimits.maxTasksPerJob()
                    + "): " + submit.getTaskPayloads().size());
        }
        requireTextBytes(submit.getRequesterToken(), "Requester token", MAX_TEXT_FIELD_BYTES);
        optionalTextBytes(submit.getRequesterPublicKey(), "Requester public key", MAX_TEXT_FIELD_BYTES);
        optionalTextBytes(submit.getRequesterSignature(), "Requester signature", MAX_TEXT_FIELD_BYTES);

        Map<String, Object> payloadEnvelope = new LinkedHashMap<>();
        payloadEnvelope.put("taskPayloads", submit.getTaskPayloads());
        payloadEnvelope.put("parameter", submit.getParameter() == null ? "" : submit.getParameter());
        validateJsonBytes(payloadEnvelope, PayloadLimits.maxJobPayloadBytes(),
                "Job submit payload", PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV);
    }

    public static void validateJobResultRequest(JobResultRequestMessage request) {
        if (request == null) {
            throw new MessageValidationException("Job result request message is required.");
        }
        validateJobId(request.getJobId(), "Job id");
        optionalTextBytes(request.getRequesterToken(), "Requester token", MAX_TEXT_FIELD_BYTES);
        optionalTextBytes(request.getRequesterPublicKey(), "Requester public key", MAX_TEXT_FIELD_BYTES);
        optionalTextBytes(request.getRequesterSignature(), "Requester signature", MAX_TEXT_FIELD_BYTES);
    }

    public static void validateJobId(String value, String fieldName) {
        requireSafeIdentifier(value, fieldName);
    }

    public static void validatePeerId(String value, String fieldName) {
        String text = requireText(value, fieldName);
        String sanitized = PeerIdentity.require(text);
        if (!sanitized.equals(text)) {
            throw new MessageValidationException(fieldName + " contains unsupported characters.");
        }
    }

    private static void validatePong(PongMessage pong) {
        List<String> supportedTaskTypes = pong.getSupportedTaskTypes();
        if (supportedTaskTypes.size() > PayloadLimits.maxTasksPerJob()) {
            throw new MessageValidationException("PONG supportedTaskTypes count exceeds "
                    + PayloadLimits.MAX_TASKS_PER_JOB_ENV + " (" + PayloadLimits.maxTasksPerJob()
                    + "): " + supportedTaskTypes.size());
        }
        for (String taskType : supportedTaskTypes) {
            validateTaskType(taskType, "PONG supported task type");
        }
    }

    private static void validateTaskAssignment(TaskAssignMessage assignment) {
        validateTaskId(assignment.getTaskId(), "Task id");
        validateJobId(assignment.getJobId(), "Job id");
        validateTaskType(assignment.getTaskType(), "Task type");
        Map<String, Object> payloadEnvelope = new LinkedHashMap<>();
        payloadEnvelope.put("payload", assignment.getPayload());
        payloadEnvelope.put("param", assignment.getParam() == null ? "" : assignment.getParam());
        validateJsonBytes(payloadEnvelope, PayloadLimits.maxJobPayloadBytes(),
                "Task assignment payload", PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV);
    }

    private static void validateTaskResult(TaskResultMessage result) {
        validateTaskId(result.getTaskId(), "Task id");
        validateJobId(result.getJobId(), "Job id");
        Map<String, Object> payloadEnvelope = new LinkedHashMap<>();
        payloadEnvelope.put("resultPayload", result.getResultPayload());
        payloadEnvelope.put("errorMessage", result.getErrorMessage() == null ? "" : result.getErrorMessage());
        validateJsonBytes(payloadEnvelope, PayloadLimits.maxResultBytes(),
                "Task result payload", PayloadLimits.MAX_RESULT_BYTES_ENV);
    }

    private static void validateJobResult(JobResultMessage result) {
        if (result.isSuccessful()) {
            validateJobId(result.getJobId(), "Job id");
            validateTaskType(result.getTaskType(), "Task type");
        } else {
            validateOptionalSafeIdentifier(result.getJobId(), "Job id");
            validateOptionalTaskType(result.getTaskType(), "Task type");
        }
        Map<String, Object> payloadEnvelope = new LinkedHashMap<>();
        payloadEnvelope.put("resultPayload", result.getResultPayload());
        payloadEnvelope.put("resultsByTaskId", result.getResultsByTaskId() == null
                ? List.of()
                : result.getResultsByTaskId());
        payloadEnvelope.put("errorMessage", result.getErrorMessage() == null ? "" : result.getErrorMessage());
        validateJsonBytes(payloadEnvelope, PayloadLimits.maxResultBytes(),
                "Job result payload", PayloadLimits.MAX_RESULT_BYTES_ENV);
    }

    private static void validatePeerDisconnected(PeerDisconnectedMessage disconnected) {
        optionalTextBytes(disconnected.getReason(), "Peer disconnect reason", MAX_TEXT_FIELD_BYTES);
    }

    private static void requireKnownType(String type) {
        String text = requireText(type, "Message type");
        if (!KNOWN_TYPES.contains(text)) {
            throw new MessageValidationException("Unknown message type: " + text);
        }
    }

    public static void validateTaskType(String value, String fieldName) {
        String text = requireText(value, fieldName);
        if (text.length() > MAX_TASK_TYPE_LENGTH) {
            throw new MessageValidationException(fieldName + " exceeds " + MAX_TASK_TYPE_LENGTH + " characters.");
        }
        if (!isSafeIdentifier(text)) {
            throw new MessageValidationException(fieldName
                    + " may contain only letters, numbers, '.', ':', '_' and '-'.");
        }
    }

    private static void validateOptionalTaskType(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return;
        }
        validateTaskType(value, fieldName);
    }

    private static void validateTaskId(String value, String fieldName) {
        requireSafeIdentifier(value, fieldName);
    }

    private static void validateOptionalSafeIdentifier(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return;
        }
        requireSafeIdentifier(value, fieldName);
    }

    private static void requireSafeIdentifier(String value, String fieldName) {
        String text = requireText(value, fieldName);
        if (text.length() > MAX_ID_LENGTH) {
            throw new MessageValidationException(fieldName + " exceeds " + MAX_ID_LENGTH + " characters.");
        }
        if (!isSafeIdentifier(text)) {
            throw new MessageValidationException(fieldName
                    + " may contain only letters, numbers, '.', ':', '_' and '-'.");
        }
    }

    private static boolean isSafeIdentifier(String text) {
        for (int index = 0; index < text.length(); index++) {
            char ch = text.charAt(index);
            boolean safe = (ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '.'
                    || ch == ':'
                    || ch == '_'
                    || ch == '-';
            if (!safe) {
                return false;
            }
        }
        return true;
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new MessageValidationException(fieldName + " is required.");
        }
        String trimmed = value.trim();
        if (!trimmed.equals(value)) {
            throw new MessageValidationException(fieldName + " must not contain leading or trailing whitespace.");
        }
        return value;
    }

    private static void requireTextBytes(String value, String fieldName, int maxBytes) {
        String text = requireText(value, fieldName);
        int bytes = text.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > maxBytes) {
            throw new MessageValidationException(fieldName + " exceeds " + maxBytes + " bytes.");
        }
    }

    private static void optionalTextBytes(String value, String fieldName, int maxBytes) {
        if (value == null || value.isBlank()) {
            return;
        }
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > maxBytes) {
            throw new MessageValidationException(fieldName + " exceeds " + maxBytes + " bytes.");
        }
    }

    private static void validateJsonBytes(Object value, long maxBytes, String fieldName, String envName) {
        long bytes = GSON.toJson(value).getBytes(StandardCharsets.UTF_8).length;
        if (bytes > maxBytes) {
            throw new MessageValidationException(fieldName + " exceeds " + envName
                    + " (" + maxBytes + " bytes): " + bytes + " bytes.");
        }
    }
}
