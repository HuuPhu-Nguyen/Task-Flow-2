package server.scheduler;

import org.junit.jupiter.api.Test;
import protocol.PongMessage;
import server.model.MessageEnvelope;
import transport.DeliveryDisposition;
import transport.InboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerMailboxTest {

    @Test
    void createsBoundedMailboxFromSchedulerConfig() {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(
                java.util.Map.of("TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY", "1")
        );
        BlockingQueue<MessageEnvelope> mailbox = SchedulerMailbox.create(config);

        assertTrue(SchedulerMailbox.offer(mailbox, new MessageEnvelope(heartbeat(), "peer-1")));
        assertFalse(SchedulerMailbox.offer(mailbox, new MessageEnvelope(heartbeat(), "peer-2")));
    }

    @Test
    void acceptedBrokerDeliveryIsDeferredAndQueuedForSchedulerAck() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = SchedulerMailbox.create(
                SchedulerConfig.fromEnvironment(java.util.Map.of("TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY", "1"))
        );
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        InboundTransportMessage delivery = new InboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-1",
                heartbeat(),
                acknowledgement
        );

        assertTrue(SchedulerMailbox.offerBrokerDelivery(mailbox, delivery));

        assertEquals(1, acknowledgement.deferCount());
        assertEquals(0, acknowledgement.requeueCount());
        MessageEnvelope queued = mailbox.take();
        assertSame(delivery.message(), queued.message());
        assertSame(acknowledgement, queued.acknowledgement());
    }

    @Test
    void fullMailboxRequeuesBrokerDeliveryWithoutAcceptingEnvelope() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = SchedulerMailbox.create(
                SchedulerConfig.fromEnvironment(java.util.Map.of("TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY", "1"))
        );
        assertTrue(SchedulerMailbox.offer(mailbox, new MessageEnvelope(heartbeat(), "peer-existing")));

        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        InboundTransportMessage delivery = new InboundTransportMessage(
                TransportRoute.TASK_RESULT,
                "peer-1",
                heartbeat(),
                acknowledgement
        );

        assertFalse(SchedulerMailbox.offerBrokerDelivery(mailbox, delivery));

        assertEquals(1, acknowledgement.deferCount());
        assertEquals(1, acknowledgement.requeueCount());
        assertEquals(DeliveryDisposition.RETRY_TRANSIENT, acknowledgement.disposition());
        assertEquals(1, mailbox.size());
    }

    @Test
    void repeatedBrokerOverflowRequeuesDeliveriesWithoutReplacingAcceptedWork() throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = SchedulerMailbox.create(
                SchedulerConfig.fromEnvironment(java.util.Map.of("TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY", "1"))
        );
        MessageEnvelope accepted = new MessageEnvelope(heartbeat(), "peer-accepted");
        assertTrue(SchedulerMailbox.offer(mailbox, accepted));

        RecordingAcknowledgement firstOverflow = new RecordingAcknowledgement();
        RecordingAcknowledgement secondOverflow = new RecordingAcknowledgement();

        assertFalse(SchedulerMailbox.offerBrokerDelivery(mailbox, brokerDelivery("peer-overflow-1", firstOverflow)));
        assertFalse(SchedulerMailbox.offerBrokerDelivery(mailbox, brokerDelivery("peer-overflow-2", secondOverflow)));

        assertEquals(1, firstOverflow.deferCount());
        assertEquals(1, firstOverflow.requeueCount());
        assertEquals(DeliveryDisposition.RETRY_TRANSIENT, firstOverflow.disposition());
        assertEquals(1, secondOverflow.deferCount());
        assertEquals(1, secondOverflow.requeueCount());
        assertEquals(DeliveryDisposition.RETRY_TRANSIENT, secondOverflow.disposition());
        assertEquals(1, mailbox.size());
        assertSame(accepted, mailbox.take());
    }

    private static InboundTransportMessage brokerDelivery(String peerId, TransportAcknowledgement acknowledgement) {
        return new InboundTransportMessage(
                TransportRoute.TASK_RESULT,
                peerId,
                heartbeat(),
                acknowledgement
        );
    }

    private static PongMessage heartbeat() {
        return new PongMessage("peer", Instant.now().toString(), List.of("TEST_TASK"));
    }

    private static class RecordingAcknowledgement implements TransportAcknowledgement {
        private final AtomicInteger deferCount = new AtomicInteger();
        private final AtomicInteger requeueCount = new AtomicInteger();
        private final AtomicReference<DeliveryDisposition> disposition = new AtomicReference<>();

        @Override
        public void settle(DeliveryDisposition requestedDisposition) throws Exception {
            disposition.compareAndSet(null, requestedDisposition);
            TransportAcknowledgement.super.settle(requestedDisposition);
        }

        @Override
        public void ack() {
        }

        @Override
        public void requeue() {
            requeueCount.incrementAndGet();
        }

        @Override
        public void reject() {
        }

        @Override
        public void defer() {
            deferCount.incrementAndGet();
        }

        int deferCount() {
            return deferCount.get();
        }

        int requeueCount() {
            return requeueCount.get();
        }

        DeliveryDisposition disposition() {
            return disposition.get();
        }
    }
}
