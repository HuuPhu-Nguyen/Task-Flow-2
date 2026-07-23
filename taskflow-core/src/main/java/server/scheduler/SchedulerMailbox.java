package server.scheduler;

import server.model.MessageEnvelope;
import transport.DeliveryDisposition;
import transport.InboundTransportMessage;
import transport.TransportAcknowledgement;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Objects;

public final class SchedulerMailbox {
    public enum BrokerOfferOutcome {
        QUEUED,
        MAILBOX_FULL_RETRY,
        INTAKE_STOPPED_UNACKNOWLEDGED
    }

    private SchedulerMailbox() {
    }

    public static BlockingQueue<MessageEnvelope> create(SchedulerConfig config) {
        SchedulerConfig effectiveConfig = config == null ? SchedulerConfig.defaults() : config;
        return new LinkedBlockingQueue<>(effectiveConfig.inboundQueueCapacity());
    }

    public static boolean offer(BlockingQueue<MessageEnvelope> mailbox, MessageEnvelope envelope) {
        return mailbox.offer(envelope);
    }

    public static void put(BlockingQueue<MessageEnvelope> mailbox, MessageEnvelope envelope)
            throws InterruptedException {
        mailbox.put(envelope);
    }

    public static BrokerIngress brokerIngress(BlockingQueue<MessageEnvelope> mailbox) {
        return new BrokerIngress(mailbox);
    }

    /**
     * Serializes the shutdown intake edge with broker delivery admission. Once
     * stopped, a delivery is deferred but deliberately left unsettled so closing
     * the broker channel returns ownership to RabbitMQ.
     */
    public static final class BrokerIngress {
        private final BlockingQueue<MessageEnvelope> mailbox;
        private boolean accepting = true;

        private BrokerIngress(BlockingQueue<MessageEnvelope> mailbox) {
            this.mailbox = Objects.requireNonNull(mailbox, "mailbox");
        }

        public BrokerOfferOutcome offer(InboundTransportMessage delivery) throws Exception {
            Objects.requireNonNull(delivery, "delivery");
            TransportAcknowledgement acknowledgement = delivery.acknowledgement();
            if (acknowledgement != null) {
                acknowledgement.defer();
            }

            BrokerOfferOutcome outcome;
            synchronized (this) {
                if (!accepting) {
                    outcome = BrokerOfferOutcome.INTAKE_STOPPED_UNACKNOWLEDGED;
                } else {
                    boolean queued = mailbox.offer(new MessageEnvelope(
                            delivery.message(),
                            delivery.fromNodeId(),
                            acknowledgement
                    ));
                    outcome = queued
                            ? BrokerOfferOutcome.QUEUED
                            : BrokerOfferOutcome.MAILBOX_FULL_RETRY;
                }
            }

            if (outcome == BrokerOfferOutcome.MAILBOX_FULL_RETRY && acknowledgement != null) {
                acknowledgement.settle(
                        DeliveryDisposition.RETRY_TRANSIENT,
                        "scheduler_mailbox_full"
                );
            }
            return outcome;
        }

        public synchronized void stopIntake() {
            accepting = false;
        }

        public synchronized boolean isAccepting() {
            return accepting;
        }

        public int queueDepth() {
            return mailbox.size();
        }
    }
}
