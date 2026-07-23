package transport;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransportAcknowledgementTest {
    @ParameterizedTest
    @EnumSource(DeliveryDisposition.class)
    void everyDispositionMapsToExactlyOnePrimitiveBrokerSettlement(DeliveryDisposition disposition)
            throws Exception {
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();

        acknowledgement.settle(disposition);

        assertEquals(disposition.acknowledges() ? 1 : 0, acknowledgement.ackCount);
        assertEquals(disposition.retries() ? 1 : 0, acknowledgement.requeueCount);
        assertEquals(disposition.rejects() ? 1 : 0, acknowledgement.rejectCount);
        assertEquals(1, acknowledgement.ackCount
                + acknowledgement.requeueCount
                + acknowledgement.rejectCount);
    }

    private static final class RecordingAcknowledgement implements TransportAcknowledgement {
        private int ackCount;
        private int requeueCount;
        private int rejectCount;

        @Override
        public void ack() {
            ackCount++;
        }

        @Override
        public void requeue() {
            requeueCount++;
        }

        @Override
        public void reject() {
            rejectCount++;
        }
    }
}
