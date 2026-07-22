package protocol;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Produces the durable, versioned fingerprint for one logical job submission.
 *
 * <p>The job id is the idempotency key and is deliberately not part of the
 * fingerprint. Volatile envelope fields such as message time, routing node id,
 * and signature are also excluded. Payload-array order remains significant,
 * while JSON object member order and equivalent numeric spellings do not.</p>
 */
public final class JobSubmissionHasher {
    private static final String HASH_VERSION = "taskflow-job-submission-v1";
    private static final String HASH_PREFIX = "v1:";
    private static final Gson GSON = new Gson();
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    private JobSubmissionHasher() {
    }

    public static String hash(JobSubmitMessage submission,
                              String requesterTokenHash,
                              String requesterIdentityKey) {
        Objects.requireNonNull(submission, "submission");
        if (!RequesterTokens.hasTokenHash(requesterTokenHash)) {
            throw new IllegalArgumentException("Requester token hash is required.");
        }

        MessageDigest request = sha256();
        updateText(request, HASH_VERSION);
        updateText(request, requesterTokenHash);
        updateText(request, value(requesterIdentityKey));
        updateText(request, normalizeTaskType(submission.getTaskType()));
        updateText(request, normalizeParameter(submission.getParameter()));

        List<Object> payloads = Objects.requireNonNull(
                submission.getTaskPayloads(),
                "submission.taskPayloads"
        );
        updateInt(request, payloads.size());
        for (Object payload : payloads) {
            byte[] payloadDigest = canonicalPayloadDigest(payload);
            updateInt(request, payloadDigest.length);
            request.update(payloadDigest);
        }
        return HASH_PREFIX + ENCODER.encodeToString(request.digest());
    }

    private static byte[] canonicalPayloadDigest(Object payload) {
        MessageDigest digest = sha256();
        updateJson(digest, GSON.toJsonTree(payload));
        return digest.digest();
    }

    private static void updateJson(MessageDigest digest, JsonElement element) {
        if (element == null || element.isJsonNull()) {
            digest.update((byte) 'N');
            return;
        }
        if (element.isJsonArray()) {
            digest.update((byte) 'A');
            updateInt(digest, element.getAsJsonArray().size());
            element.getAsJsonArray().forEach(child -> updateJson(digest, child));
            return;
        }
        if (element.isJsonObject()) {
            digest.update((byte) 'O');
            JsonObject object = element.getAsJsonObject();
            List<Map.Entry<String, JsonElement>> entries = new ArrayList<>(object.entrySet());
            entries.sort(Comparator.comparing(Map.Entry::getKey));
            updateInt(digest, entries.size());
            for (Map.Entry<String, JsonElement> entry : entries) {
                updateText(digest, entry.getKey());
                updateJson(digest, entry.getValue());
            }
            return;
        }

        JsonPrimitive primitive = element.getAsJsonPrimitive();
        if (primitive.isBoolean()) {
            digest.update((byte) 'B');
            digest.update(primitive.getAsBoolean() ? (byte) 1 : (byte) 0);
        } else if (primitive.isNumber()) {
            digest.update((byte) 'D');
            updateText(digest, canonicalNumber(primitive));
        } else {
            digest.update((byte) 'S');
            updateText(digest, primitive.getAsString());
        }
    }

    private static String canonicalNumber(JsonPrimitive primitive) {
        BigDecimal number = new BigDecimal(primitive.getAsString());
        if (number.signum() == 0) {
            return "0";
        }
        return number.stripTrailingZeros().toString();
    }

    private static String normalizeTaskType(String taskType) {
        return value(taskType).trim().toUpperCase(Locale.ROOT);
    }

    private static String normalizeParameter(String parameter) {
        return value(parameter);
    }

    private static void updateText(MessageDigest digest, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        updateInt(digest, bytes.length);
        digest.update(bytes);
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }
}
