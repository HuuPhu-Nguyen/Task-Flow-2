package transport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import protocol.MessageValidationException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeliveryFailureClassifierTest {
    @Test
    void dispositionContractContainsExactlyTheFiveBrokerOutcomes() {
        assertEquals(
                List.of(
                        "ACK_SUCCESS",
                        "ACK_DUPLICATE_OR_STALE",
                        "RETRY_TRANSIENT",
                        "REJECT_INVALID",
                        "QUARANTINE_POISON"
                ),
                Arrays.stream(DeliveryDisposition.values()).map(Enum::name).toList()
        );
    }

    @ParameterizedTest
    @MethodSource("failureCategories")
    void everyExceptionCategoryMapsToOneDisposition(Throwable failure,
                                                    DeliveryDisposition expectedDisposition,
                                                    String expectedReasonCode) {
        ClassifiedDeliveryFailure classified = DeliveryFailureClassifier.classify(failure);

        assertEquals(expectedDisposition, classified.disposition());
        assertEquals(expectedReasonCode, classified.reasonCode());
    }

    @Test
    void asynchronousWrapperDoesNotHideTheUnderlyingFailureCategory() {
        ClassifiedDeliveryFailure classified = DeliveryFailureClassifier.classify(
                new CompletionException(new IOException("broker unavailable"))
        );

        assertEquals(DeliveryDisposition.RETRY_TRANSIENT, classified.disposition());
        assertEquals("transient_infrastructure_failure", classified.reasonCode());
    }

    @Test
    void checkedAsynchronousWrapperDoesNotHideTheUnderlyingFailureCategory() {
        ClassifiedDeliveryFailure classified = DeliveryFailureClassifier.classify(
                new ExecutionException(new MessageValidationException("invalid_async_delivery", "invalid"))
        );

        assertEquals(DeliveryDisposition.REJECT_INVALID, classified.disposition());
        assertEquals("invalid_async_delivery", classified.reasonCode());
    }

    @Test
    void failureClassificationCannotPretendAnExceptionWasAcknowledged() {
        assertThrows(IllegalArgumentException.class, () -> new ClassifiedDeliveryFailure(
                DeliveryDisposition.ACK_SUCCESS,
                "not-a-failure"
        ));
        assertThrows(IllegalArgumentException.class, () -> new ClassifiedDeliveryFailure(
                DeliveryDisposition.ACK_DUPLICATE_OR_STALE,
                "not-a-failure"
        ));
    }

    private static Stream<Arguments> failureCategories() {
        return Stream.of(
                Arguments.of(
                        new MessageValidationException("unsupported_protocol_version", "unsupported"),
                        DeliveryDisposition.REJECT_INVALID,
                        "unsupported_protocol_version"
                ),
                Arguments.of(
                        new IllegalArgumentException("invalid route"),
                        DeliveryDisposition.REJECT_INVALID,
                        "invalid_delivery"
                ),
                Arguments.of(
                        new TransientDeliveryException(
                                "sqlite_temporarily_unavailable",
                                "database unavailable",
                                null
                        ),
                        DeliveryDisposition.RETRY_TRANSIENT,
                        "sqlite_temporarily_unavailable"
                ),
                Arguments.of(
                        new IOException("connection reset"),
                        DeliveryDisposition.RETRY_TRANSIENT,
                        "transient_infrastructure_failure"
                ),
                Arguments.of(
                        new TimeoutException("confirm timed out"),
                        DeliveryDisposition.RETRY_TRANSIENT,
                        "delivery_timeout"
                ),
                Arguments.of(
                        new InterruptedException("shutdown"),
                        DeliveryDisposition.RETRY_TRANSIENT,
                        "delivery_interrupted"
                ),
                Arguments.of(
                        new CancellationException("executor shutdown"),
                        DeliveryDisposition.RETRY_TRANSIENT,
                        "delivery_cancelled"
                ),
                Arguments.of(
                        new RejectedExecutionException("executor saturated"),
                        DeliveryDisposition.RETRY_TRANSIENT,
                        "delivery_cancelled"
                ),
                Arguments.of(
                        new Exception("unclassified checked processing failure"),
                        DeliveryDisposition.QUARANTINE_POISON,
                        "deterministic_processing_failure"
                ),
                Arguments.of(
                        new IllegalStateException("deterministic invariant failure"),
                        DeliveryDisposition.QUARANTINE_POISON,
                        "deterministic_processing_failure"
                )
        );
    }
}
