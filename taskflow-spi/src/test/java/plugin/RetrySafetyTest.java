package plugin;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetrySafetyTest {
    @Test
    void onlyUnsafeProcessorsDisableAutomaticRetries() {
        assertTrue(RetrySafety.PURE.permitsAutomaticRetry());
        assertTrue(RetrySafety.IDEMPOTENT.permitsAutomaticRetry());
        assertTrue(RetrySafety.REQUIRES_IDEMPOTENCY_KEY.permitsAutomaticRetry());
        assertFalse(RetrySafety.UNSAFE_TO_RETRY.permitsAutomaticRetry());
    }
}
