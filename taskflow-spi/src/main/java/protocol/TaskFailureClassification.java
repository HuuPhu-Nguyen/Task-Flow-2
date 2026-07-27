package protocol;

/**
 * Coordinator retry policy carried by an unsuccessful task result.
 */
public enum TaskFailureClassification {
    RETRYABLE,
    PERMANENT_PAYLOAD_INTEGRITY;

    public boolean retryable() {
        return this == RETRYABLE;
    }
}
