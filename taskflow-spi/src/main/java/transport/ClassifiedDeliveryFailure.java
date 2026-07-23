package transport;

import java.util.Objects;

/**
 * Stable broker disposition and reason code derived from a handler failure.
 */
public record ClassifiedDeliveryFailure(
        DeliveryDisposition disposition,
        String reasonCode
) {
    public ClassifiedDeliveryFailure {
        Objects.requireNonNull(disposition, "disposition");
        if (disposition.acknowledges()) {
            throw new IllegalArgumentException("A handler failure cannot map to an acknowledgement disposition.");
        }
        if (reasonCode == null || reasonCode.isBlank()) {
            throw new IllegalArgumentException("reasonCode is required.");
        }
    }
}
