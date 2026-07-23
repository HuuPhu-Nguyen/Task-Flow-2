package transport.rabbitmq;

import com.rabbitmq.client.Channel;
import transport.DeliveryDisposition;
import transport.TransportAcknowledgement;

import java.util.Objects;

class RabbitMqAcknowledgement implements TransportAcknowledgement {
    private final Channel channel;
    private final long deliveryTag;
    private boolean settled;
    private boolean deferred;
    private DeliveryDisposition settledDisposition;

    RabbitMqAcknowledgement(Channel channel, long deliveryTag) {
        this.channel = channel;
        this.deliveryTag = deliveryTag;
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
        DeliveryDisposition required = Objects.requireNonNull(disposition, "disposition");
        if (required.acknowledges()) {
            settleOnce(required, () -> channel.basicAck(deliveryTag, false));
        } else if (required.retries()) {
            settleOnce(required, () -> channel.basicNack(deliveryTag, false, true));
        } else {
            settleOnce(required, () -> channel.basicReject(deliveryTag, false));
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

    private void settleOnce(DeliveryDisposition disposition, SettlementOperation operation) throws Exception {
        if (settled) {
            return;
        }
        synchronized (channel) {
            operation.run();
        }
        settled = true;
        deferred = false;
        settledDisposition = disposition;
    }

    @FunctionalInterface
    private interface SettlementOperation {
        void run() throws Exception;
    }
}
