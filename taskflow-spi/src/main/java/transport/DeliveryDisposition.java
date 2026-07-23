package transport;

/**
 * The domain-aware outcome of handling one consumed broker delivery.
 *
 * <p>Assignment generation, task retry count, and broker delivery disposition
 * are deliberately separate concepts.</p>
 */
public enum DeliveryDisposition {
    ACK_SUCCESS,
    ACK_DUPLICATE_OR_STALE,
    RETRY_TRANSIENT,
    REJECT_INVALID,
    QUARANTINE_POISON;

    public boolean acknowledges() {
        return this == ACK_SUCCESS || this == ACK_DUPLICATE_OR_STALE;
    }

    public boolean retries() {
        return this == RETRY_TRANSIENT;
    }

    public boolean rejects() {
        return this == REJECT_INVALID || this == QUARANTINE_POISON;
    }
}
