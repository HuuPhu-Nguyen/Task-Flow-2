package client;

import org.junit.jupiter.api.Test;

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
    }

    @Test
    void systemPropertiesOverrideDefaults() {
        System.setProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY, "123");
        System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, "7");
        System.setProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY, "456");
        System.setProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY, "789");
        try {
            assertEquals(123, PayloadLimits.maxInputBytes());
            assertEquals(7, PayloadLimits.maxTasksPerJob());
            assertEquals(456, PayloadLimits.maxJobPayloadBytes());
            assertEquals(789, PayloadLimits.maxResultBytes());
        } finally {
            System.clearProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY);
            System.clearProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
            System.clearProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY);
            System.clearProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY);
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
}
