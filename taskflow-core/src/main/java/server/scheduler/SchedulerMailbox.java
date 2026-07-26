package server.scheduler;

import server.model.MessageEnvelope;
import transport.DeliveryDisposition;
import transport.InboundTransportMessage;
import transport.TransportAcknowledgement;

import java.util.concurrent.BlockingQueue;
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
        return new SchedulerPriorityMailbox(effectiveConfig.inboundQueueCapacity());
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

    public static DepthSnapshot depthSnapshot(BlockingQueue<MessageEnvelope> mailbox) {
        Objects.requireNonNull(mailbox, "mailbox");
        if (mailbox instanceof SchedulerPriorityMailbox priorityMailbox) {
            return priorityMailbox.depthSnapshot();
        }
        int depth = mailbox.size();
        long observedCapacity = (long) depth + mailbox.remainingCapacity();
        int capacity = (int) Math.min(Integer.MAX_VALUE, observedCapacity);
        return new DepthSnapshot(depth, capacity, 0, 0, false);
    }

    public record DepthSnapshot(
            int submissionDepth,
            int submissionCapacity,
            int taskResultDepth,
            int taskResultCapacity,
            boolean taskResultReserveSaturated
    ) {
        public DepthSnapshot {
            if (submissionDepth < 0 || taskResultDepth < 0) {
                throw new IllegalArgumentException("mailbox depths must not be negative");
            }
            if (submissionCapacity <= 0 || taskResultCapacity < 0) {
                throw new IllegalArgumentException("mailbox capacities are invalid");
            }
            if (submissionDepth > submissionCapacity
                    || taskResultDepth > taskResultCapacity) {
                throw new IllegalArgumentException("mailbox depth exceeds capacity");
            }
            if (taskResultReserveSaturated && taskResultCapacity == 0) {
                throw new IllegalArgumentException(
                        "a mailbox without a result reserve cannot report reserve saturation"
                );
            }
        }

        public int totalDepth() {
            return Math.addExact(submissionDepth, taskResultDepth);
        }
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

        public DepthSnapshot depthSnapshot() {
            return SchedulerMailbox.depthSnapshot(mailbox);
        }
    }
}
