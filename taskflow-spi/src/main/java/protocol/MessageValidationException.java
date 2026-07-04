package protocol;

public class MessageValidationException extends IllegalArgumentException {
    public MessageValidationException(String message) {
        super(message);
    }
}
