package transport;

/**
 * Marks a broker delivery failure whose cause is temporary infrastructure
 * unavailability rather than invalid input or deterministic processing.
 */
public final class TransientDeliveryException extends RuntimeException {
    public static final String DEFAULT_REASON_CODE = "transient_infrastructure_failure";

    private final String reasonCode;

    public TransientDeliveryException(String message) {
        this(DEFAULT_REASON_CODE, message, null);
    }

    public TransientDeliveryException(String message, Throwable cause) {
        this(DEFAULT_REASON_CODE, message, cause);
    }

    public TransientDeliveryException(String reasonCode, String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = reasonCode == null || reasonCode.isBlank()
                ? DEFAULT_REASON_CODE
                : reasonCode;
    }

    public String reasonCode() {
        return reasonCode;
    }
}
