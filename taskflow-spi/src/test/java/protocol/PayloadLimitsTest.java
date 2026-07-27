package protocol;

import objectstore.ObjectReference;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PayloadLimitsTest {

    @Test
    void exposesConservativeInlinePayloadDefaults() {
        assertEquals(32L * 1024L * 1024L, PayloadLimits.DEFAULT_MAX_INPUT_BYTES);
        assertEquals(256, PayloadLimits.DEFAULT_MAX_TASKS_PER_JOB);
        assertEquals(64L * 1024L * 1024L, PayloadLimits.DEFAULT_MAX_JOB_PAYLOAD_BYTES);
        assertEquals(64L * 1024L * 1024L, PayloadLimits.DEFAULT_MAX_RESULT_BYTES);
        assertEquals(8L * 1024L * 1024L, PayloadLimits.DEFAULT_MAX_INLINE_PAYLOAD_BYTES);
    }

    @Test
    void systemPropertiesOverrideDefaults() {
        System.setProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY, "123");
        System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, "7");
        System.setProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY, "456");
        System.setProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY, "789");
        System.setProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY, "12");
        try {
            assertEquals(123, PayloadLimits.maxInputBytes());
            assertEquals(7, PayloadLimits.maxTasksPerJob());
            assertEquals(456, PayloadLimits.maxJobPayloadBytes());
            assertEquals(789, PayloadLimits.maxResultBytes());
            assertEquals(12, PayloadLimits.maxInlinePayloadBytes());
        } finally {
            System.clearProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY);
            System.clearProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
            System.clearProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY);
            System.clearProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY);
            System.clearProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void rejectsNonPositiveProperties() {
        System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, "0");
        try {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    PayloadLimits::maxTasksPerJob);

            assertTrue(error.getMessage().contains(PayloadLimits.MAX_TASKS_PER_JOB_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
        }
    }

    @Test
    void estimatesBase64EncodedLength() {
        assertEquals(0, PayloadLimits.base64EncodedLength(0));
        assertEquals(4, PayloadLimits.base64EncodedLength(1));
        assertEquals(4, PayloadLimits.base64EncodedLength(2));
        assertEquals(4, PayloadLimits.base64EncodedLength(3));
        assertEquals(8, PayloadLimits.base64EncodedLength(4));
    }

    @Test
    void detectsPayloadLimitOverflow() {
        assertTrue(PayloadLimits.wouldExceed(7, 4, 10));
        assertTrue(PayloadLimits.wouldExceed(Long.MAX_VALUE - 1, 10, Long.MAX_VALUE));
    }

    @Test
    void measuresNestedObjectReferencesWithoutPluginTypes() {
        ObjectReference small = reference(10L, "a");
        Map<String, Object> serializedLarge = Map.of(
                "key", TaskFlowObjectKeys.objectKey("inputs", "b"),
                "contentLength", 25L,
                "sha256", "b".repeat(64),
                "contentType", "application/octet-stream"
        );

        long maximum = PayloadLimits.maximumReferencedPayloadBytes(Map.of(
                "first", List.of(Map.of("nested", small)),
                "second", Map.of("deeper", List.of(serializedLarge))
        ));

        assertEquals(25L, maximum);
    }

    @Test
    void rejectsMalformedReferenceSizeMetadata() {
        Map<String, Object> malformed = Map.of(
                "key", TaskFlowObjectKeys.objectKey("inputs", "b"),
                "contentLength", 1.5,
                "sha256", "b".repeat(64),
                "contentType", "application/octet-stream"
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> PayloadLimits.maximumReferencedPayloadBytes(malformed)
        );
    }

    @Test
    void rejectsLegacyFilesystemReferences() {
        Map<String, Object> legacy = Map.of(
                "storageType", "local-file",
                "location", "payloads/input.bin",
                "sizeBytes", 10L,
                "sha256", "a".repeat(64)
        );

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> PayloadLimits.maximumReferencedPayloadBytes(legacy)
        );

        assertTrue(error.getMessage().contains("Local filesystem"));
    }

    @Test
    void measuresDecodedInlinePayloadBytesAndRejectsMalformedBase64() {
        Map<String, Object> payload = Map.of(
                "fileName", "sample.png",
                "base64Data", Base64.getEncoder().encodeToString(new byte[] {1, 2, 3})
        );

        assertEquals(3L, PayloadLimits.maximumInlinePayloadBytes(payload));
        assertEquals(-1L, PayloadLimits.maximumInlinePayloadBytes(Map.of("text", "value")));
        assertThrows(IllegalArgumentException.class, () ->
                PayloadLimits.maximumInlinePayloadBytes(Map.of("base64Data", "%%%")));
    }

    private static ObjectReference reference(long sizeBytes, String digestCharacter) {
        return new ObjectReference(
                TaskFlowObjectKeys.objectKey("inputs", "input"),
                sizeBytes,
                digestCharacter.repeat(64),
                "application/octet-stream"
        );
    }
}
