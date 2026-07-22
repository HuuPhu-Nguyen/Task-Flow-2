package protocol;

public class MessageValidationException extends IllegalArgumentException {
    public static final String DEFAULT_REASON_CODE = "message_validation_failed";

    private final String reasonCode;

    public MessageValidationException(String message) {
        this(DEFAULT_REASON_CODE, message);
    }

    public MessageValidationException(String reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode == null || reasonCode.isBlank()
                ? DEFAULT_REASON_CODE
                : reasonCode;
    }

    public String reasonCode() {
        return reasonCode;
    }
}
