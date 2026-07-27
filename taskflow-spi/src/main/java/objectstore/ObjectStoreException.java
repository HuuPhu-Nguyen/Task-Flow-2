package objectstore;

import java.io.IOException;
import java.util.Objects;

/**
 * Infrastructure-independent failure classification for object-store calls.
 */
public class ObjectStoreException extends IOException {
    public enum Reason {
        NOT_FOUND,
        ALREADY_EXISTS,
        INVALID_METADATA,
        PAYLOAD_INTEGRITY,
        STORAGE_FAILURE
    }

    private final Reason reason;

    public ObjectStoreException(Reason reason, String message) {
        super(message);
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ObjectStoreException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public Reason reason() {
        return reason;
    }
}
