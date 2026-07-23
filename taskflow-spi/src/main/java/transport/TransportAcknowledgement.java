package transport;

import java.util.Objects;

public interface TransportAcknowledgement {
    default void settle(DeliveryDisposition disposition) throws Exception {
        switch (Objects.requireNonNull(disposition, "disposition")) {
            case ACK_SUCCESS, ACK_DUPLICATE_OR_STALE -> ack();
            case RETRY_TRANSIENT -> requeue();
            case REJECT_INVALID, QUARANTINE_POISON -> reject();
        }
    }

    void ack() throws Exception;

    void requeue() throws Exception;

    void reject() throws Exception;

    default void defer() {
    }
}
