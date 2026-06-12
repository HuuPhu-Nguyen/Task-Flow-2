package transport.rabbitmq;

import com.rabbitmq.client.Channel;
import transport.TransportAcknowledgement;

class RabbitMqAcknowledgement implements TransportAcknowledgement {
    private final Channel channel;
    private final long deliveryTag;
    private boolean settled;
    private boolean deferred;

    RabbitMqAcknowledgement(Channel channel, long deliveryTag) {
        this.channel = channel;
        this.deliveryTag = deliveryTag;
    }

    @Override
    public synchronized void ack() throws Exception {
        settle(() -> channel.basicAck(deliveryTag, false));
    }

    @Override
    public synchronized void requeue() throws Exception {
        settle(() -> channel.basicNack(deliveryTag, false, true));
    }

    @Override
    public synchronized void reject() throws Exception {
        settle(() -> channel.basicReject(deliveryTag, false));
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

    private void settle(SettlementOperation operation) throws Exception {
        if (settled) {
            return;
        }
        synchronized (channel) {
            operation.run();
        }
        settled = true;
        deferred = false;
    }

    @FunctionalInterface
    private interface SettlementOperation {
        void run() throws Exception;
    }
}
