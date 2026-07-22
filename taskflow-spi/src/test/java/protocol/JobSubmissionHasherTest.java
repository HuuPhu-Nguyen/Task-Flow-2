package protocol;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class JobSubmissionHasherTest {
    private static final Gson GSON = new Gson();
    private static final String TOKEN_HASH = RequesterTokens.hashToken("owner-token");

    @Test
    void equivalentJsonHasStableHashAcrossSerializationAndMapOrder() {
        Map<String, Object> firstPayload = new LinkedHashMap<>();
        firstPayload.put("z", List.of(1, 2.0, -0.0));
        firstPayload.put("a", Map.of("enabled", true, "value", 1_000));

        Map<String, Object> reorderedPayload = new LinkedHashMap<>();
        reorderedPayload.put("a", Map.of("value", 1e3, "enabled", true));
        reorderedPayload.put("z", List.of(1.0, 2, 0));

        JobSubmitMessage direct = submission("text_analysis", List.of(firstPayload), "café");
        JobSubmitMessage reordered = submission("TEXT_ANALYSIS", List.of(reorderedPayload), "café");
        JobSubmitMessage roundTripped = GSON.fromJson(GSON.toJson(reordered), JobSubmitMessage.class);

        String expected = JobSubmissionHasher.hash(direct, TOKEN_HASH, "owner-public-key");
        assertEquals(expected, JobSubmissionHasher.hash(reordered, TOKEN_HASH, "owner-public-key"));
        assertEquals(expected, JobSubmissionHasher.hash(roundTripped, TOKEN_HASH, "owner-public-key"));
    }

    @Test
    void ownerPayloadOrderAndExactParameterRemainPartOfFingerprint() {
        JobSubmitMessage original = submission("TEST_TASK", List.of("a", "b"), "mode=fast");

        assertNotEquals(
                JobSubmissionHasher.hash(original, TOKEN_HASH, "owner-public-key"),
                JobSubmissionHasher.hash(original, RequesterTokens.hashToken("other-token"), "owner-public-key")
        );
        assertNotEquals(
                JobSubmissionHasher.hash(original, TOKEN_HASH, "owner-public-key"),
                JobSubmissionHasher.hash(original, TOKEN_HASH, "other-public-key")
        );
        assertNotEquals(
                JobSubmissionHasher.hash(original, TOKEN_HASH, "owner-public-key"),
                JobSubmissionHasher.hash(submission("TEST_TASK", List.of("b", "a"), "mode=fast"),
                        TOKEN_HASH, "owner-public-key")
        );
        assertNotEquals(
                JobSubmissionHasher.hash(original, TOKEN_HASH, "owner-public-key"),
                JobSubmissionHasher.hash(submission("TEST_TASK", List.of("a", "b"), "mode=fast "),
                        TOKEN_HASH, "owner-public-key")
        );
    }

    @Test
    void idempotencyKeyAndVolatileEnvelopeFieldsAreExcludedFromRequestHash() {
        JobSubmitMessage first = submission("TEST_TASK", List.of("payload"), "parameter");
        JobSubmitMessage reissued = new JobSubmitMessage(
                "different-route",
                "2030-01-01T00:00:00Z",
                "different-idempotency-key",
                "test_task",
                List.of("payload"),
                "parameter",
                "owner-token",
                "owner-public-key",
                "different-signature"
        );

        assertEquals(
                JobSubmissionHasher.hash(first, TOKEN_HASH, "owner-public-key"),
                JobSubmissionHasher.hash(reissued, TOKEN_HASH, "owner-public-key")
        );
    }

    private static JobSubmitMessage submission(String taskType, List<Object> payloads, String parameter) {
        return new JobSubmitMessage(
                "requester-route",
                "2026-07-23T00:00:00Z",
                "job-idempotency-key",
                taskType,
                payloads,
                parameter,
                "owner-token"
        );
    }
}
