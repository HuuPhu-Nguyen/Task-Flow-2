package transport.rabbitmq;

import com.rabbitmq.client.Channel;
import transport.DeliveryDisposition;
import transport.TransportAcknowledgement;

import java.util.Objects;

class RabbitMqAcknowledgement implements TransportAcknowledgement {
    private final Channel channel;
    private final long deliveryTag;
    private final RabbitMqDeliveryRetry deliveryRetry;
    private boolean settled;
    private boolean deferred;
    private DeliveryDisposition settledDisposition;
    private String settledReasonCode;

    RabbitMqAcknowledgement(Channel channel,
                            long deliveryTag,
                            RabbitMqDeliveryRetry deliveryRetry) {
        this.channel = channel;
        this.deliveryTag = deliveryTag;
        this.deliveryRetry = Objects.requireNonNull(deliveryRetry, "deliveryRetry");
    }

    @Override
    public synchronized void ack() throws Exception {
        settle(DeliveryDisposition.ACK_SUCCESS);
    }

    @Override
    public synchronized void requeue() throws Exception {
        settle(DeliveryDisposition.RETRY_TRANSIENT);
    }

    @Override
    public synchronized void reject() throws Exception {
        settle(DeliveryDisposition.REJECT_INVALID);
    }

    @Override
    public synchronized void settle(DeliveryDisposition disposition) throws Exception {
        settle(disposition, null);
    }

    @Override
    public synchronized void settle(DeliveryDisposition disposition, String reasonCode) throws Exception {
        DeliveryDisposition required = Objects.requireNonNull(disposition, "disposition");
        String effectiveReason = reasonCode == null || reasonCode.isBlank()
                ? required.name().toLowerCase(java.util.Locale.ROOT)
                : reasonCode;
        if (required.acknowledges()) {
            settleOnce(required, effectiveReason, () -> channel.basicAck(deliveryTag, false));
        } else if (required.retries() || required == DeliveryDisposition.QUARANTINE_POISON) {
            settleOnce(required, effectiveReason, () -> {
                boolean published = deliveryRetry.publishRetryOrQuarantine(required, effectiveReason);
                if (published) {
                    channel.basicAck(deliveryTag, false);
                } else {
                    channel.basicReject(deliveryTag, false);
                }
            });
        } else {
            settleOnce(required, effectiveReason, () -> channel.basicReject(deliveryTag, false));
        }
    }

    @Override
    public synchronized void defer() {
        if (!settled) {
            deferred = true;
        }
    }

    synchronized boolean isSettled() {
        return settled;
    }

    synchronized boolean isDeferred() {
        return deferred;
    }

    synchronized DeliveryDisposition settledDisposition() {
        return settledDisposition;
    }

    synchronized String settledReasonCode() {
        return settledReasonCode;
    }

    private void settleOnce(DeliveryDisposition disposition,
                            String reasonCode,
                            SettlementOperation operation) throws Exception {
        if (settled) {
            return;
        }
        synchronized (channel) {
            operation.run();
        }
        settled = true;
        deferred = false;
        settledDisposition = disposition;
        settledReasonCode = reasonCode;
    }

    @FunctionalInterface
    private interface SettlementOperation {
        void run() throws Exception;
    }
}
