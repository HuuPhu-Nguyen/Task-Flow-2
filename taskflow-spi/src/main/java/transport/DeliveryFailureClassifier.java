package transport;

import protocol.MessageValidationException;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Maps handler exceptions to one broker delivery disposition.
 */
public final class DeliveryFailureClassifier {
    private static final String INVALID_REASON_CODE = "invalid_delivery";
    private static final String INTERRUPTED_REASON_CODE = "delivery_interrupted";
    private static final String CANCELLED_REASON_CODE = "delivery_cancelled";
    private static final String TIMEOUT_REASON_CODE = "delivery_timeout";
    private static final String TRANSIENT_REASON_CODE = "transient_infrastructure_failure";
    private static final String POISON_REASON_CODE = "deterministic_processing_failure";

    private DeliveryFailureClassifier() {
    }

    public static ClassifiedDeliveryFailure classify(Throwable failure) {
        Throwable classified = unwrap(Objects.requireNonNull(failure, "failure"));
        if (classified instanceof TransientDeliveryException transientFailure) {
            return new ClassifiedDeliveryFailure(
                    DeliveryDisposition.RETRY_TRANSIENT,
                    transientFailure.reasonCode()
            );
        }
        if (classified instanceof MessageValidationException validationFailure) {
            return new ClassifiedDeliveryFailure(
                    DeliveryDisposition.REJECT_INVALID,
                    validationFailure.reasonCode()
            );
        }
        if (classified instanceof IllegalArgumentException) {
            return new ClassifiedDeliveryFailure(
                    DeliveryDisposition.REJECT_INVALID,
                    INVALID_REASON_CODE
            );
        }
        if (classified instanceof InterruptedException) {
            return new ClassifiedDeliveryFailure(
                    DeliveryDisposition.RETRY_TRANSIENT,
                    INTERRUPTED_REASON_CODE
            );
        }
        if (classified instanceof CancellationException
                || classified instanceof RejectedExecutionException) {
            return new ClassifiedDeliveryFailure(
                    DeliveryDisposition.RETRY_TRANSIENT,
                    CANCELLED_REASON_CODE
            );
        }
        if (classified instanceof TimeoutException) {
            return new ClassifiedDeliveryFailure(
                    DeliveryDisposition.RETRY_TRANSIENT,
                    TIMEOUT_REASON_CODE
            );
        }
        if (classified instanceof IOException) {
            return new ClassifiedDeliveryFailure(
                    DeliveryDisposition.RETRY_TRANSIENT,
                    TRANSIENT_REASON_CODE
            );
        }
        return new ClassifiedDeliveryFailure(
                DeliveryDisposition.QUARANTINE_POISON,
                POISON_REASON_CODE
        );
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null
                && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

}
