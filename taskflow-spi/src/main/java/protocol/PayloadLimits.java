package protocol;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import objectstore.ObjectReference;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
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
    public static final long DEFAULT_MAX_INLINE_PAYLOAD_BYTES = 8L * 1024L * 1024L;

    public static final String MAX_INPUT_BYTES_PROPERTY = "taskflow.maxInputBytes";
    public static final String MAX_INPUT_BYTES_ENV = "TASKFLOW_MAX_INPUT_BYTES";
    public static final String MAX_TASKS_PER_JOB_PROPERTY = "taskflow.maxTasksPerJob";
    public static final String MAX_TASKS_PER_JOB_ENV = "TASKFLOW_MAX_TASKS_PER_JOB";
    public static final String MAX_JOB_PAYLOAD_BYTES_PROPERTY = "taskflow.maxJobPayloadBytes";
    public static final String MAX_JOB_PAYLOAD_BYTES_ENV = "TASKFLOW_MAX_JOB_PAYLOAD_BYTES";
    public static final String MAX_RESULT_BYTES_PROPERTY = "taskflow.maxResultBytes";
    public static final String MAX_RESULT_BYTES_ENV = "TASKFLOW_MAX_RESULT_BYTES";
    public static final String MAX_INLINE_PAYLOAD_BYTES_PROPERTY = "taskflow.maxInlinePayloadBytes";
    public static final String MAX_INLINE_PAYLOAD_BYTES_ENV = "TASKFLOW_MAX_INLINE_PAYLOAD_BYTES";

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

    /**
     * Returns the exclusive raw-byte ceiling for plugin file payloads carried
     * as Base64. Zero disables file-payload inlining.
     */
    public static long maxInlinePayloadBytes() {
        return nonNegativeLong(
                MAX_INLINE_PAYLOAD_BYTES_PROPERTY,
                MAX_INLINE_PAYLOAD_BYTES_ENV,
                DEFAULT_MAX_INLINE_PAYLOAD_BYTES
        );
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
     * Finds the largest portable object reference recursively without
     * depending on any plugin payload type.
     */
    public static long maximumReferencedPayloadBytes(Object value) {
        long maximum = 0L;
        for (ObjectReference reference : objectReferences(value)) {
            maximum = Math.max(maximum, reference.contentLength());
        }
        return maximum;
    }

    /**
     * Finds every portable object reference recursively without depending on
     * any plugin payload type.
     */
    public static List<ObjectReference> objectReferences(Object value) {
        List<ObjectReference> references = new ArrayList<>();
        collectObjectReferences(GSON.toJsonTree(value), references);
        return List.copyOf(references);
    }

    private static void collectObjectReferences(JsonElement value,
                                                List<ObjectReference> references) {
        if (value == null || value.isJsonNull() || value.isJsonPrimitive()) {
            return;
        }
        if (value.isJsonArray()) {
            for (JsonElement element : value.getAsJsonArray()) {
                collectObjectReferences(element, references);
            }
            return;
        }

        JsonObject object = value.getAsJsonObject();
        if (looksLikeLegacyPayloadReference(object)) {
            throw new IllegalArgumentException(
                    "Local filesystem payload references are not supported."
            );
        }
        if (looksLikeObjectReference(object)) {
            references.add(parseObjectReference(object));
            return;
        }
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            collectObjectReferences(entry.getValue(), references);
        }
    }

    /**
     * Returns the largest decoded {@code base64Data} field recursively, or
     * {@code -1} when no inline file payload is present.
     */
    public static long maximumInlinePayloadBytes(Object value) {
        return maximumInlinePayloadBytes(GSON.toJsonTree(value));
    }

    private static long maximumInlinePayloadBytes(JsonElement value) {
        if (value == null || value.isJsonNull() || value.isJsonPrimitive()) {
            return -1L;
        }
        if (value.isJsonArray()) {
            long maximum = -1L;
            for (JsonElement element : value.getAsJsonArray()) {
                maximum = Math.max(maximum, maximumInlinePayloadBytes(element));
            }
            return maximum;
        }

        JsonObject object = value.getAsJsonObject();
        long maximum = inlinePayloadSize(object);
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            maximum = Math.max(maximum, maximumInlinePayloadBytes(entry.getValue()));
        }
        return maximum;
    }

    private static long inlinePayloadSize(JsonObject object) {
        if (!object.has("base64Data") || object.get("base64Data").isJsonNull()) {
            return -1L;
        }
        JsonElement data = object.get("base64Data");
        if (!isString(data)) {
            throw new IllegalArgumentException("Inline payload Base64 data is malformed.");
        }
        try {
            return Base64.getDecoder().decode(data.getAsString()).length;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Inline payload Base64 data is malformed.", e);
        }
    }

    private static boolean looksLikeLegacyPayloadReference(JsonObject object) {
        return object.has("storageType")
                && object.has("location")
                && object.has("sizeBytes")
                && object.has("sha256");
    }

    private static boolean looksLikeObjectReference(JsonObject object) {
        return object.has("key")
                && object.has("contentLength")
                && object.has("sha256")
                && object.has("contentType");
    }

    private static ObjectReference parseObjectReference(JsonObject object) {
        JsonElement key = object.get("key");
        JsonElement contentLength = object.get("contentLength");
        JsonElement sha256 = object.get("sha256");
        JsonElement contentType = object.get("contentType");
        if (!isString(key) || !isString(sha256) || !isString(contentType)
                || contentLength == null || !contentLength.isJsonPrimitive()
                || !contentLength.getAsJsonPrimitive().isNumber()) {
            throw new IllegalArgumentException("Object reference metadata is malformed.");
        }

        long size;
        try {
            size = new BigDecimal(contentLength.getAsString()).longValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Object reference contentLength must be an integer.",
                    e
            );
        }
        return new ObjectReference(
                key.getAsString(),
                size,
                sha256.getAsString(),
                contentType.getAsString()
        );
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

    private static long nonNegativeLong(String propertyName, String envName, long defaultValue) {
        String configured = configuredValue(propertyName, envName);
        if (configured == null) {
            return defaultValue;
        }
        long parsed = Long.parseLong(configured);
        if (parsed < 0) {
            throw new IllegalArgumentException(envName + " must not be negative.");
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
