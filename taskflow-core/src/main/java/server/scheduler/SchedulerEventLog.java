package server.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.runtime.TaskFlowClock;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Emits coordinator scheduler events on the documented common schema while
 * retaining the historical {@link TaskScheduler} logger category.
 */
final class SchedulerEventLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskScheduler.class);
    private static final String NO_FAILURE = "NONE";
    private static final Pattern EVENT_NAME = Pattern.compile("[a-z][a-z0-9_]*");
    private static final Pattern FIELD_NAME = Pattern.compile("[a-z][a-z0-9_]*");
    private static final Pattern UNQUOTED_VALUE =
            Pattern.compile("[A-Za-z0-9_./:@+\\-]+");
    private static final Set<String> COMMON_FIELDS = Set.of(
            "event",
            "timestamp",
            "coordinator_instance_id",
            "outcome",
            "failure_reason_code"
    );
    private static final Set<String> SENSITIVE_OR_PAYLOAD_FIELDS = Set.of(
            "requester_token",
            "requester_token_hash",
            "credentials",
            "password",
            "public_key",
            "requester_public_key",
            "private_key",
            "raw_private_key",
            "payload",
            "raw_payload",
            "binary_payload",
            "body"
    );

    private final TaskFlowClock clock;
    private final String coordinatorInstanceId;

    SchedulerEventLog(TaskFlowClock clock, String coordinatorInstanceId) {
        this.clock = Objects.requireNonNull(clock, "clock");
        if (coordinatorInstanceId == null || coordinatorInstanceId.isBlank()) {
            throw new IllegalArgumentException("coordinatorInstanceId is required");
        }
        this.coordinatorInstanceId = coordinatorInstanceId.trim();
    }

    Map<String, Object> fields(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("keyValues must be in pairs");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            String key = String.valueOf(keyValues[i]);
            validateDetailFieldName(key);
            if (out.putIfAbsent(key, keyValues[i + 1]) != null) {
                throw new IllegalArgumentException("Duplicate event field: " + key);
            }
        }
        return out;
    }

    Map<String, Object> assignmentTraceFields(String jobId,
                                              String taskId,
                                              int attemptNumber,
                                              String assignmentId,
                                              String workerId,
                                              Object... additionalFields) {
        Map<String, Object> out = fields(
                "job_id", jobId,
                "task_id", taskId,
                "attempt_number", attemptNumber,
                "assignment_id", assignmentId,
                "worker_id", workerId
        );
        Map<String, Object> extras = fields(additionalFields);
        for (Map.Entry<String, Object> entry : extras.entrySet()) {
            if (out.putIfAbsent(entry.getKey(), entry.getValue()) != null) {
                throw new IllegalArgumentException(
                        "Assignment trace field cannot be replaced: " + entry.getKey()
                );
            }
        }
        return out;
    }

    void info(String event, Map<String, Object> fields) {
        LOGGER.info(formatEvent(event, fields));
    }

    void error(String event, Map<String, Object> fields) {
        LOGGER.error(formatEvent(event, fields));
    }

    private String formatEvent(String event, Map<String, Object> detailFields) {
        if (event == null || !EVENT_NAME.matcher(event).matches()) {
            throw new IllegalArgumentException("Invalid event name: " + event);
        }
        Map<String, Object> checkedDetails = detailFields == null ? Map.of() : detailFields;
        for (String key : checkedDetails.keySet()) {
            validateDetailFieldName(key);
        }
        Classification classification = classify(event, checkedDetails);
        Map<String, Object> common = new LinkedHashMap<>();
        common.put("event", event);
        common.put("timestamp", Instant.ofEpochMilli(clock.nowEpochMillis()));
        common.put("coordinator_instance_id", coordinatorInstanceId);
        common.put("outcome", classification.outcome());
        common.put("failure_reason_code", classification.failureReasonCode());
        common.putAll(checkedDetails);
        return formatFields(common);
    }

    private static Classification classify(String event, Map<String, Object> fields) {
        return switch (event) {
            case "job_started" -> Classification.of(Outcome.ACCEPTED);
            case "job_resumed" -> Classification.of(Outcome.RECOVERED);
            case "job_submission_replayed",
                    "scheduler_durable_transition_replayed" ->
                    Classification.of(Outcome.REPLAYED);
            case "job_submission_conflict" -> Classification.failure(
                    Outcome.CONFLICT,
                    reasonFrom(fields, "submission_outcome", "JOB_SUBMISSION_CONFLICT")
            );
            case "job_admission_rejected" -> Classification.failure(
                    Outcome.REJECTED,
                    reasonFrom(fields, "limit", "ADMISSION_LIMIT_EXCEEDED")
            );
            case "job_start_failed" ->
                    Classification.failure(Outcome.REJECTED, "JOB_START_FAILED");
            case "job_start_failure_requester_missing" ->
                    Classification.failure(Outcome.DEFERRED, "REQUESTER_UNAVAILABLE");
            case "job_start_failure_send_failed" ->
                    Classification.failure(Outcome.DEFERRED, "JOB_START_RESULT_SEND_FAILED");
            case "job_result_requester_missing" ->
                    Classification.failure(Outcome.DEFERRED, "REQUESTER_UNAVAILABLE");
            case "job_result_request_send_failed" ->
                    Classification.failure(Outcome.DEFERRED, "JOB_RESULT_REQUEST_SEND_FAILED");
            case "job_completed" -> booleanField(fields, "success", true)
                    ? Classification.of(Outcome.COMMITTED)
                    : Classification.failure(Outcome.TERMINAL_FAILURE, "JOB_FAILED");
            case "job_failed" ->
                    Classification.failure(Outcome.TERMINAL_FAILURE, "JOB_FAILED");
            case "job_terminal_persistence_deferred" ->
                    Classification.failure(Outcome.DEFERRED, "STORAGE_FAILURE");
            case "job_result_delivery_deferred" ->
                    Classification.failure(Outcome.DEFERRED, "FINAL_RESULT_DELIVERY_FAILED");
            case "job_result_delivery_abandoned" ->
                    Classification.failure(Outcome.ABANDONED, "FINAL_RESULT_DELIVERY_EXHAUSTED");
            case "task_assignment_created" -> Classification.of(Outcome.COMMITTED);
            case "task_dispatch_failed" ->
                    Classification.failure(Outcome.RETRY_SCHEDULED, "DISPATCH_FAILED");
            case "task_result_committed" -> Classification.of(Outcome.COMMITTED);
            case "task_result_stale_rejected" ->
                    Classification.failure(Outcome.STALE_REJECTED, "STALE_ASSIGNMENT");
            case "task_failure_result_ignored" -> Classification.failure(
                    Outcome.IGNORED,
                    reasonFrom(fields, "disposition", "TASK_FAILURE_RESULT_IGNORED")
            );
            case "scheduler_durable_transition_rejected" ->
                    Classification.failure(Outcome.STALE_REJECTED, "STALE_STATE");
            case "task_result_duplicate_ignored" ->
                    Classification.failure(Outcome.DUPLICATE_IGNORED, "DUPLICATE_RESULT");
            case "task_result_not_committed" -> resultNotCommitted(fields);
            case "task_failed" -> taskFailure(
                    fields,
                    reasonFrom(fields, "failure_classification", "TASK_EXECUTION_FAILED")
            );
            case "payload_integrity_failure_committed" -> Classification.failure(
                    Outcome.TERMINAL_FAILURE,
                    "PERMANENT_PAYLOAD_INTEGRITY"
            );
            case "task_timeout" -> taskFailure(fields, "TASK_TIMEOUT");
            case "task_lease_expired" -> taskFailure(fields, "LEASE_EXPIRED");
            case "task_peer_unavailable" -> taskFailure(
                    fields,
                    reasonFrom(fields, "reason_code", "WORKER_UNAVAILABLE")
            );
            case "peer_unavailable_tasks_released" -> Classification.failure(
                    Outcome.RELEASED,
                    reasonFrom(fields, "reason_code", "WORKER_UNAVAILABLE")
            );
            case "scheduler_message_validation_failed" -> Classification.failure(
                    Outcome.REJECTED,
                    reasonFrom(fields, "reason_code", "MESSAGE_VALIDATION_FAILED")
            );
            case "scheduler_message_processing_failed" -> Classification.failure(
                    Outcome.FAILED,
                    reasonFrom(fields, "reason_code", "MESSAGE_PROCESSING_FAILED")
            );
            case "scheduler_delivery_disposed" -> Classification.failure(
                    Outcome.DISPOSED,
                    reasonFrom(fields, "reason_code", "DELIVERY_DISPOSED")
            );
            case "scheduler_message_settlement_failed" -> Classification.failure(
                    Outcome.FAILED,
                    "DELIVERY_SETTLEMENT_FAILED"
            );
            case "scheduler_persistence_failed",
                    "scheduler_pending_outbox_count_failed",
                    "scheduler_overload_outbox_count_failed" ->
                    Classification.failure(Outcome.FAILED, "STORAGE_FAILURE");
            case "broker_outbox_prepare_failed" ->
                    Classification.failure(Outcome.FAILED, "OUTBOX_PREPARE_FAILED");
            case "broker_outbox_publish_deferred" -> Classification.failure(
                    Outcome.DEFERRED,
                    "OUTBOX_PUBLISH_UNCONFIRMED"
            );
            case "broker_outbox_publish_mark_failed" -> Classification.failure(
                    Outcome.DEFERRED,
                    "OUTBOX_PUBLISHED_MARK_FAILED"
            );
            case "broker_outbox_publish_failed" ->
                    Classification.failure(Outcome.DEFERRED, "OUTBOX_PUBLISH_FAILED");
            case "scheduler_metrics" -> Classification.of(Outcome.OBSERVED);
            case "scheduler_overload_started" -> Classification.failure(
                    Outcome.OVERLOADED,
                    reasonFrom(fields, "primary_reason", "SCHEDULER_OVERLOADED")
            );
            case "scheduler_overload_changed" -> overloadChanged(fields);
            case "scheduler_overload_recovered" -> Classification.of(Outcome.RECOVERED);
            default -> throw new IllegalArgumentException(
                    "Scheduler event has no schema classification: " + event
            );
        };
    }

    private static Classification resultNotCommitted(Map<String, Object> fields) {
        String reason = reasonFrom(fields, "commit_outcome", "RESULT_NOT_COMMITTED");
        return "STORAGE_FAILURE".equals(reason)
                ? Classification.failure(Outcome.DEFERRED, reason)
                : Classification.failure(Outcome.REJECTED, reason);
    }

    private static Classification taskFailure(
            Map<String, Object> fields,
            String failureReasonCode
    ) {
        return Classification.failure(
                booleanField(fields, "terminal_failure", false)
                        ? Outcome.TERMINAL_FAILURE
                        : Outcome.RETRY_SCHEDULED,
                failureReasonCode
        );
    }

    private static Classification overloadChanged(Map<String, Object> fields) {
        if (booleanField(fields, "overloaded", false)) {
            return Classification.failure(
                    Outcome.OVERLOADED,
                    reasonFrom(fields, "primary_reason", "SCHEDULER_OVERLOADED")
            );
        }
        if (!booleanField(fields, "pending_outbox_observation_healthy", true)) {
            return Classification.failure(Outcome.FAILED, "STORAGE_FAILURE");
        }
        return Classification.of(Outcome.RECOVERED);
    }

    private static boolean booleanField(
            Map<String, Object> fields,
            String key,
            boolean defaultValue
    ) {
        Object value = fields.get(key);
        return value instanceof Boolean flag ? flag : defaultValue;
    }

    private static String reasonFrom(
            Map<String, Object> fields,
            String key,
            String fallback
    ) {
        Object value = fields.get(key);
        if (value == null || String.valueOf(value).isBlank()) {
            return fallback;
        }
        String normalized = String.valueOf(value)
                .trim()
                .toUpperCase(Locale.ROOT)
                .replaceAll("[^A-Z0-9]+", "_")
                .replaceAll("^_+|_+$", "");
        return normalized.isBlank() ? fallback : normalized;
    }

    private static void validateDetailFieldName(String key) {
        if (key == null || !FIELD_NAME.matcher(key).matches()) {
            throw new IllegalArgumentException("Invalid event field name: " + key);
        }
        if (COMMON_FIELDS.contains(key)) {
            throw new IllegalArgumentException("Common event field cannot be replaced: " + key);
        }
        if (SENSITIVE_OR_PAYLOAD_FIELDS.contains(key)
                || key.contains("token")
                || key.contains("credential")
                || key.contains("password")
                || key.contains("private_key")
                || key.contains("public_key")
                || key.endsWith("_payload")
                || key.endsWith("_body")) {
            throw new IllegalArgumentException(
                    "Sensitive or full-payload event field is prohibited: " + key
            );
        }
    }

    private static String formatFields(Map<String, Object> fields) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (!builder.isEmpty()) {
                builder.append(' ');
            }
            builder.append(entry.getKey())
                    .append('=')
                    .append(formatValue(entry.getValue()));
        }
        return builder.toString();
    }

    private static String formatValue(Object value) {
        String text = String.valueOf(value);
        if (UNQUOTED_VALUE.matcher(text).matches()) {
            return text;
        }
        return '"' + text
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n") + '"';
    }

    private enum Outcome {
        ABANDONED,
        ACCEPTED,
        COMMITTED,
        CONFLICT,
        DEFERRED,
        DISPOSED,
        DUPLICATE_IGNORED,
        FAILED,
        IGNORED,
        OBSERVED,
        OVERLOADED,
        RECOVERED,
        REJECTED,
        RELEASED,
        REPLAYED,
        RETRY_SCHEDULED,
        STALE_REJECTED,
        TERMINAL_FAILURE
    }

    private record Classification(Outcome outcome, String failureReasonCode) {
        private Classification {
            Objects.requireNonNull(outcome, "outcome");
            if (failureReasonCode == null || failureReasonCode.isBlank()) {
                throw new IllegalArgumentException("failureReasonCode is required");
            }
        }

        private static Classification of(Outcome outcome) {
            return new Classification(outcome, NO_FAILURE);
        }

        private static Classification failure(Outcome outcome, String failureReasonCode) {
            return new Classification(outcome, failureReasonCode);
        }
    }
}
