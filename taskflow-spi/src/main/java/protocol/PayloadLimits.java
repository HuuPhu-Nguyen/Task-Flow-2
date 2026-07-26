package protocol;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared client-payload guardrails for plugins that inline task data in messages.
 */
public final class PayloadLimits {
    private static final Gson GSON = new Gson();
    public static final long DEFAULT_MAX_INPUT_BYTES = 32L * 1024L * 1024L;
    public static final int DEFAULT_MAX_TASKS_PER_JOB = 256;
    public static final long DEFAULT_MAX_JOB_PAYLOAD_BYTES = 64L * 1024L * 1024L;
    public static final long DEFAULT_MAX_RESULT_BYTES = 64L * 1024L * 1024L;

    public static final String MAX_INPUT_BYTES_PROPERTY = "taskflow.maxInputBytes";
    public static final String MAX_INPUT_BYTES_ENV = "TASKFLOW_MAX_INPUT_BYTES";
    public static final String MAX_TASKS_PER_JOB_PROPERTY = "taskflow.maxTasksPerJob";
    public static final String MAX_TASKS_PER_JOB_ENV = "TASKFLOW_MAX_TASKS_PER_JOB";
    public static final String MAX_JOB_PAYLOAD_BYTES_PROPERTY = "taskflow.maxJobPayloadBytes";
    public static final String MAX_JOB_PAYLOAD_BYTES_ENV = "TASKFLOW_MAX_JOB_PAYLOAD_BYTES";
    public static final String MAX_RESULT_BYTES_PROPERTY = "taskflow.maxResultBytes";
    public static final String MAX_RESULT_BYTES_ENV = "TASKFLOW_MAX_RESULT_BYTES";

    private PayloadLimits() {
    }

    public static long maxInputBytes() {
        return positiveLong(MAX_INPUT_BYTES_PROPERTY, MAX_INPUT_BYTES_ENV, DEFAULT_MAX_INPUT_BYTES);
    }

    public static int maxTasksPerJob() {
        return positiveInt(MAX_TASKS_PER_JOB_PROPERTY, MAX_TASKS_PER_JOB_ENV, DEFAULT_MAX_TASKS_PER_JOB);
    }

    public static long maxJobPayloadBytes() {
        return positiveLong(MAX_JOB_PAYLOAD_BYTES_PROPERTY, MAX_JOB_PAYLOAD_BYTES_ENV,
                DEFAULT_MAX_JOB_PAYLOAD_BYTES);
    }

    public static long maxResultBytes() {
        return positiveLong(MAX_RESULT_BYTES_PROPERTY, MAX_RESULT_BYTES_ENV, DEFAULT_MAX_RESULT_BYTES);
    }

    public static long base64EncodedLength(long rawBytes) {
        if (rawBytes < 0) {
            throw new IllegalArgumentException("rawBytes must not be negative.");
        }
        if (rawBytes > (Long.MAX_VALUE / 4L - 1L) * 3L) {
            return Long.MAX_VALUE;
        }
        return ((rawBytes + 2L) / 3L) * 4L;
    }

    public static boolean wouldExceed(long currentBytes, long nextBytes, long maxBytes) {
        return nextBytes > maxBytes || currentBytes > maxBytes - nextBytes;
    }

    /**
     * Measures the exact UTF-8 JSON bytes governed by the submitted-job inline
     * payload limit.
     */
    public static long jobPayloadJsonBytes(List<Object> taskPayloads, String parameter) {
        Map<String, Object> payloadEnvelope = new LinkedHashMap<>();
        payloadEnvelope.put("taskPayloads", taskPayloads);
        payloadEnvelope.put("parameter", parameter == null ? "" : parameter);
        return GSON.toJson(payloadEnvelope).getBytes(StandardCharsets.UTF_8).length;
    }

    /**
     * Finds the largest protocol payload reference recursively without
     * depending on any plugin payload type.
     */
    public static long maximumReferencedPayloadBytes(Object value) {
        return maximumReferencedPayloadBytes(GSON.toJsonTree(value));
    }

    private static long maximumReferencedPayloadBytes(JsonElement value) {
        if (value == null || value.isJsonNull() || value.isJsonPrimitive()) {
            return 0L;
        }
        if (value.isJsonArray()) {
            long maximum = 0L;
            for (JsonElement element : value.getAsJsonArray()) {
                maximum = Math.max(maximum, maximumReferencedPayloadBytes(element));
            }
            return maximum;
        }

        JsonObject object = value.getAsJsonObject();
        if (looksLikePayloadReference(object)) {
            return payloadReferenceSize(object);
        }
        long maximum = 0L;
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            maximum = Math.max(maximum, maximumReferencedPayloadBytes(entry.getValue()));
        }
        return maximum;
    }

    private static boolean looksLikePayloadReference(JsonObject object) {
        return object.has("storageType")
                && object.has("location")
                && object.has("sizeBytes")
                && object.has("sha256");
    }

    private static long payloadReferenceSize(JsonObject object) {
        JsonElement storageType = object.get("storageType");
        JsonElement location = object.get("location");
        JsonElement sizeBytes = object.get("sizeBytes");
        JsonElement sha256 = object.get("sha256");
        if (!isString(storageType) || !isString(location) || !isString(sha256)
                || sizeBytes == null || !sizeBytes.isJsonPrimitive()
                || !sizeBytes.getAsJsonPrimitive().isNumber()) {
            throw new IllegalArgumentException("Payload reference metadata is malformed.");
        }

        long size;
        try {
            size = new BigDecimal(sizeBytes.getAsString()).longValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Payload reference sizeBytes must be an integer.",
                    e
            );
        }
        new PayloadReference(
                storageType.getAsString(),
                location.getAsString(),
                size,
                sha256.getAsString()
        );
        return size;
    }

    private static boolean isString(JsonElement value) {
        if (value == null || !value.isJsonPrimitive()) {
            return false;
        }
        JsonPrimitive primitive = value.getAsJsonPrimitive();
        return primitive.isString();
    }

    private static long positiveLong(String propertyName, String envName, long defaultValue) {
        String configured = configuredValue(propertyName, envName);
        if (configured == null) {
            return defaultValue;
        }
        long parsed = Long.parseLong(configured);
        if (parsed <= 0) {
            throw new IllegalArgumentException(envName + " must be positive.");
        }
        return parsed;
    }

    private static int positiveInt(String propertyName, String envName, int defaultValue) {
        String configured = configuredValue(propertyName, envName);
        if (configured == null) {
            return defaultValue;
        }
        int parsed = Integer.parseInt(configured);
        if (parsed <= 0) {
            throw new IllegalArgumentException(envName + " must be positive.");
        }
        return parsed;
    }

    private static String configuredValue(String propertyName, String envName) {
        String configured = System.getProperty(propertyName);
        if (configured == null || configured.isBlank()) {
            configured = System.getenv(envName);
        }
        if (configured == null || configured.isBlank()) {
            return null;
        }
        return configured.trim();
    }
}
