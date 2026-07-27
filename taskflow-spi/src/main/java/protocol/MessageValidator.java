package protocol;

import com.google.gson.Gson;
import objectstore.ObjectReference;
import objectstore.TaskFlowObjectKeys;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
    public static final String REASON_UNSUPPORTED_PROTOCOL_VERSION = "unsupported_protocol_version";
    public static final String REASON_ASSIGNMENT_PROTOCOL_V2_REQUIRED = "assignment_protocol_v2_required";
    public static final String REASON_INVALID_ASSIGNMENT_IDENTITY = "invalid_assignment_identity";
    public static final String REASON_INVALID_CAPACITY_ADVERTISEMENT = "invalid_capacity_advertisement";
    public static final String REASON_MAX_TASKS_PER_JOB = "max_tasks_per_job";
    public static final String REASON_MAX_INLINE_MESSAGE_BYTES = "max_inline_message_bytes";
    public static final String REASON_MAX_INLINE_PAYLOAD_BYTES = "max_inline_payload_bytes";
    public static final String REASON_INVALID_INLINE_PAYLOAD = "invalid_inline_payload";
    public static final String REASON_MAX_REFERENCED_PAYLOAD_BYTES = "max_referenced_payload_bytes";
    public static final String REASON_INVALID_PAYLOAD_REFERENCE = "invalid_payload_reference";
    public static final String REASON_INVALID_TASK_FAILURE_CLASSIFICATION =
            "invalid_task_failure_classification";
    public static final String REASON_INVALID_TASK_OUTPUT_REFERENCE =
            "invalid_task_output_reference";

    private MessageValidator() {
    }

    public static void validate(Message message) {
        if (message == null) {
            throw new MessageValidationException("Message is required.");
        }
        requireKnownType(message.getType());
        validateProtocolCompatibility(message);
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
            throw new MessageValidationException(REASON_MAX_TASKS_PER_JOB,
                    "Job submit task payload count exceeds "
                    + PayloadLimits.MAX_TASKS_PER_JOB_ENV + " (" + PayloadLimits.maxTasksPerJob()
                    + "): " + submit.getTaskPayloads().size());
        }
        requireTextBytes(submit.getRequesterToken(), "Requester token", MAX_TEXT_FIELD_BYTES);
        optionalTextBytes(submit.getRequesterPublicKey(), "Requester public key", MAX_TEXT_FIELD_BYTES);
        optionalTextBytes(submit.getRequesterSignature(), "Requester signature", MAX_TEXT_FIELD_BYTES);

        long inlineBytes = PayloadLimits.jobPayloadJsonBytes(
                submit.getTaskPayloads(),
                submit.getParameter()
        );
        if (inlineBytes > PayloadLimits.maxJobPayloadBytes()) {
            throw new MessageValidationException(
                    REASON_MAX_INLINE_MESSAGE_BYTES,
                    "Job submit payload exceeds " + PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV
                            + " (" + PayloadLimits.maxJobPayloadBytes() + " bytes): "
                            + inlineBytes
            );
        }

        validatePortablePayloads(
                submit.getTaskPayloads(),
                PayloadLimits.maxInputBytes(),
                PayloadLimits.MAX_INPUT_BYTES_ENV,
                "Job submit"
        );
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
        Set<String> normalizedTypes = new HashSet<>();
        for (String taskType : supportedTaskTypes) {
            validateTaskType(taskType, "PONG supported task type");
            String normalized = taskType.trim().toUpperCase(Locale.ROOT);
            if (!normalizedTypes.add(normalized)) {
                throw invalidCapacity("PONG contains duplicate normalized task type " + normalized + ".");
            }
        }
        if (pong.getProtocolVersion() != ProtocolVersions.CAPACITY_ADVERTISEMENT) {
            return;
        }

        validateCapacityInstanceId(pong.getExecutorInstanceId());
        if (pong.getCapacitySnapshotSequence() <= 0L) {
            throw invalidCapacity("PONG capacitySnapshotSequence must be positive.");
        }
        int totalUnits = pong.getTotalCapacityUnits();
        int availableUnits = pong.getAvailableCapacityUnits();
        Map<String, Integer> concurrency = pong.getMaxConcurrencyByTaskType();
        if (normalizedTypes.isEmpty()) {
            if (totalUnits != 0 || availableUnits != 0 || !concurrency.isEmpty()) {
                throw invalidCapacity(
                        "Requester-only PONG capacity units and concurrency must be empty/zero."
                );
            }
            return;
        }
        if (totalUnits <= 0) {
            throw invalidCapacity("Executor PONG totalCapacityUnits must be positive.");
        }
        if (availableUnits < 0 || availableUnits > totalUnits) {
            throw invalidCapacity(
                    "PONG availableCapacityUnits must be between zero and totalCapacityUnits."
            );
        }
        Map<String, Integer> normalizedConcurrency = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : concurrency.entrySet()) {
            validateTaskType(entry.getKey(), "PONG concurrency task type");
            String normalized = entry.getKey().trim().toUpperCase(Locale.ROOT);
            if (normalizedConcurrency.containsKey(normalized)) {
                throw invalidCapacity(
                        "PONG contains duplicate normalized concurrency task type " + normalized + "."
                );
            }
            normalizedConcurrency.put(normalized, entry.getValue());
            if (entry.getValue() == null || entry.getValue() <= 0) {
                throw invalidCapacity(
                        "PONG maximum type concurrency must be positive for " + normalized + "."
                );
            }
        }
        if (!normalizedConcurrency.keySet().equals(normalizedTypes)) {
            throw invalidCapacity(
                    "PONG maximum-concurrency task types must exactly match supportedTaskTypes."
            );
        }
    }

    private static void validateTaskAssignment(TaskAssignMessage assignment) {
        validateTaskId(assignment.getTaskId(), "Task id");
        validateJobId(assignment.getJobId(), "Job id");
        validateTaskType(assignment.getTaskType(), "Task type");
        validateAssignmentIdentity(assignment.getAttemptNumber(), assignment.getAssignmentId());
        if (assignment.getLeaseExpiresAtEpochMillis() <= 0L) {
            throw new MessageValidationException(
                    REASON_INVALID_ASSIGNMENT_IDENTITY,
                    "Task assignment leaseExpiresAtEpochMillis must be positive."
            );
        }
        Map<String, Object> payloadEnvelope = new LinkedHashMap<>();
        payloadEnvelope.put("payload", assignment.getPayload());
        payloadEnvelope.put("parameter", assignment.getParameter() == null ? "" : assignment.getParameter());
        validateJsonBytes(payloadEnvelope, PayloadLimits.maxJobPayloadBytes(),
                "Task assignment payload", PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV);
        validatePortablePayloads(
                assignment.getPayload(),
                PayloadLimits.maxInputBytes(),
                PayloadLimits.MAX_INPUT_BYTES_ENV,
                "Task assignment"
        );
    }

    private static void validateTaskResult(TaskResultMessage result) {
        validateTaskId(result.getTaskId(), "Task id");
        validateJobId(result.getJobId(), "Job id");
        validateAssignmentIdentity(result.getAttemptNumber(), result.getAssignmentId());
        if (result.isSuccessful() && result.hasExplicitFailureClassification()) {
            throw new MessageValidationException(
                    REASON_INVALID_TASK_FAILURE_CLASSIFICATION,
                    "Successful task result cannot carry a failure classification."
            );
        }
        Map<String, Object> payloadEnvelope = new LinkedHashMap<>();
        payloadEnvelope.put("resultPayload", result.getResultPayload());
        payloadEnvelope.put("errorMessage", result.getErrorMessage() == null ? "" : result.getErrorMessage());
        validateJsonBytes(payloadEnvelope, PayloadLimits.maxResultBytes(),
                "Task result payload", PayloadLimits.MAX_RESULT_BYTES_ENV);
        validatePortablePayloads(
                result.getResultPayload(),
                PayloadLimits.maxResultBytes(),
                PayloadLimits.MAX_RESULT_BYTES_ENV,
                "Task result"
        );
        if (result.isSuccessful()) {
            validateTaskOutputReferences(result);
        }
    }

    private static void validateTaskOutputReferences(TaskResultMessage result) {
        String expectedKey = TaskFlowObjectKeys.attemptOutputKey(
                result.getJobId(),
                result.getTaskId(),
                result.getAttemptNumber(),
                result.getAssignmentId()
        );
        for (ObjectReference reference : PayloadLimits.objectReferences(result.getResultPayload())) {
            if (!expectedKey.equals(reference.key())) {
                throw new MessageValidationException(
                        REASON_INVALID_TASK_OUTPUT_REFERENCE,
                        "Task result object reference must use the current assignment output key."
                );
            }
        }
    }

    private static void validateJobResult(JobResultMessage result) {
        if (result.isSuccessful() && result.getAdmissionRejection() != null) {
            throw new MessageValidationException(
                    "Successful job result cannot carry an admission rejection."
            );
        }
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
        validatePortablePayloads(
                payloadEnvelope,
                PayloadLimits.maxResultBytes(),
                PayloadLimits.MAX_RESULT_BYTES_ENV,
                "Job result"
        );
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

    private static void validateProtocolCompatibility(Message message) {
        try {
            ProtocolVersions.requireMessageSupported(message.getProtocolVersion(), "Message");
        } catch (IllegalArgumentException e) {
            throw new MessageValidationException(REASON_UNSUPPORTED_PROTOCOL_VERSION, e.getMessage());
        }
        if (message.getProtocolVersion() == ProtocolVersions.CAPACITY_ADVERTISEMENT
                && !(message instanceof PongMessage)) {
            throw new MessageValidationException(
                    REASON_UNSUPPORTED_PROTOCOL_VERSION,
                    message.getType() + " does not support protocolVersion "
                            + ProtocolVersions.CAPACITY_ADVERTISEMENT + "."
            );
        }
        if ((message instanceof TaskAssignMessage || message instanceof TaskResultMessage)
                && message.getProtocolVersion() != ProtocolVersions.ASSIGNMENT_IDENTITY) {
            throw new MessageValidationException(
                    REASON_ASSIGNMENT_PROTOCOL_V2_REQUIRED,
                    message.getType() + " requires protocolVersion "
                            + ProtocolVersions.ASSIGNMENT_IDENTITY
                            + " with assignment identity; received protocolVersion "
                            + message.getProtocolVersion() + "."
            );
        }
    }

    private static void validateCapacityInstanceId(String instanceId) {
        if (instanceId == null || instanceId.isBlank()) {
            throw invalidCapacity("PONG executorInstanceId is required.");
        }
        try {
            String canonical = UUID.fromString(instanceId).toString();
            if (!canonical.equalsIgnoreCase(instanceId)) {
                throw new IllegalArgumentException("non-canonical UUID syntax");
            }
        } catch (IllegalArgumentException e) {
            throw invalidCapacity("PONG executorInstanceId must be a UUID.");
        }
    }

    private static MessageValidationException invalidCapacity(String message) {
        return new MessageValidationException(REASON_INVALID_CAPACITY_ADVERTISEMENT, message);
    }

    private static void validateAssignmentIdentity(int attemptNumber, String assignmentId) {
        if (attemptNumber <= 0) {
            throw new MessageValidationException(
                    REASON_INVALID_ASSIGNMENT_IDENTITY,
                    "Assignment attemptNumber must be positive."
            );
        }
        String candidate;
        try {
            candidate = requireText(assignmentId, "Assignment assignmentId");
        } catch (MessageValidationException e) {
            throw new MessageValidationException(REASON_INVALID_ASSIGNMENT_IDENTITY, e.getMessage());
        }
        try {
            String canonical = UUID.fromString(candidate).toString();
            if (!canonical.equalsIgnoreCase(candidate)) {
                throw new IllegalArgumentException("non-canonical UUID syntax");
            }
        } catch (IllegalArgumentException e) {
            throw new MessageValidationException(
                    REASON_INVALID_ASSIGNMENT_IDENTITY,
                    "Assignment assignmentId must be a UUID."
            );
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

    private static void validatePortablePayloads(Object value,
                                                 long maximumReferenceBytes,
                                                 String referenceLimitName,
                                                 String messageName) {
        long referencedBytes;
        try {
            referencedBytes = PayloadLimits.maximumReferencedPayloadBytes(value);
        } catch (IllegalArgumentException e) {
            throw new MessageValidationException(REASON_INVALID_PAYLOAD_REFERENCE, e.getMessage());
        }
        if (referencedBytes > maximumReferenceBytes) {
            throw new MessageValidationException(
                    REASON_MAX_REFERENCED_PAYLOAD_BYTES,
                    messageName + " object reference exceeds " + referenceLimitName
                            + " (" + maximumReferenceBytes + " bytes): " + referencedBytes
            );
        }

        long inlineBytes;
        try {
            inlineBytes = PayloadLimits.maximumInlinePayloadBytes(value);
        } catch (IllegalArgumentException e) {
            throw new MessageValidationException(REASON_INVALID_INLINE_PAYLOAD, e.getMessage());
        }
        long inlineLimit = PayloadLimits.maxInlinePayloadBytes();
        if (inlineBytes >= inlineLimit && inlineBytes >= 0L) {
            throw new MessageValidationException(
                    REASON_MAX_INLINE_PAYLOAD_BYTES,
                    messageName + " inline file payload must be smaller than "
                            + PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_ENV + " (" + inlineLimit
                            + " bytes): " + inlineBytes
            );
        }
    }
}
