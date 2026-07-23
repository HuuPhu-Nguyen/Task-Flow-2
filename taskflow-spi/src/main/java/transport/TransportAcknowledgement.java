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

    /**
     * Settles a delivery while carrying a stable, machine-readable reason when
     * the transport supports settlement metadata.
     */
    default void settle(DeliveryDisposition disposition, String reasonCode) throws Exception {
        settle(disposition);
    }

    void ack() throws Exception;

    void requeue() throws Exception;

    void reject() throws Exception;

    default void defer() {
    }
}
