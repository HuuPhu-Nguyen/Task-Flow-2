package server.scheduler;

import server.model.MessageEnvelope;
import transport.InboundTransportMessage;
import transport.TransportAcknowledgement;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class SchedulerMailbox {
    private SchedulerMailbox() {
    }

    public static BlockingQueue<MessageEnvelope> create(SchedulerConfig config) {
        SchedulerConfig effectiveConfig = config == null ? SchedulerConfig.defaults() : config;
        return new LinkedBlockingQueue<>(effectiveConfig.inboundQueueCapacity());
    }

    public static boolean offer(BlockingQueue<MessageEnvelope> mailbox, MessageEnvelope envelope) {
        return mailbox.offer(envelope);
    }

    public static boolean offerBrokerDelivery(BlockingQueue<MessageEnvelope> mailbox,
                                              InboundTransportMessage delivery) throws Exception {
        TransportAcknowledgement acknowledgement = delivery.acknowledgement();
        if (acknowledgement != null) {
            acknowledgement.defer();
        }

        boolean queued = mailbox.offer(new MessageEnvelope(
                delivery.message(),
                delivery.fromNodeId(),
                acknowledgement
        ));
        if (!queued && acknowledgement != null) {
            acknowledgement.requeue();
        }
        return queued;
    }
}
