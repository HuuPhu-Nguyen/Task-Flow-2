package transport.rabbitmq;

import com.rabbitmq.client.Channel;
import transport.TransportAcknowledgement;

import java.util.concurrent.atomic.AtomicBoolean;

class RabbitMqAcknowledgement implements TransportAcknowledgement {
    private final Channel channel;
    private final long deliveryTag;
    private final AtomicBoolean settled = new AtomicBoolean(false);

    RabbitMqAcknowledgement(Channel channel, long deliveryTag) {
        this.channel = channel;
        this.deliveryTag = deliveryTag;
    }

    @Override
    public void ack() throws Exception {
        if (settled.compareAndSet(false, true)) {
            synchronized (channel) {
                channel.basicAck(deliveryTag, false);
            }
        }
    }

    @Override
    public void requeue() throws Exception {
        if (settled.compareAndSet(false, true)) {
            synchronized (channel) {
                channel.basicNack(deliveryTag, false, true);
            }
        }
    }

    @Override
    public void reject() throws Exception {
        if (settled.compareAndSet(false, true)) {
            synchronized (channel) {
                channel.basicReject(deliveryTag, false);
            }
        }
    }

    boolean isSettled() {
        return settled.get();
    }
}
