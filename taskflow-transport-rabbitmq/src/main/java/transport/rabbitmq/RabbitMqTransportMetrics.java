package transport.rabbitmq;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-lifetime broker delivery counters owned by one transport instance.
 */
public final class RabbitMqTransportMetrics {
    private final AtomicLong redeliveriesTotal = new AtomicLong();
    private final AtomicLong quarantinedTotal = new AtomicLong();

    void recordRedelivery() {
        redeliveriesTotal.incrementAndGet();
    }

    void recordQuarantined() {
        quarantinedTotal.incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(redeliveriesTotal.get(), quarantinedTotal.get());
    }

    public record Snapshot(long redeliveriesTotal, long quarantinedTotal) {
        public Snapshot {
            if (redeliveriesTotal < 0L || quarantinedTotal < 0L) {
                throw new IllegalArgumentException(
                        "RabbitMQ metric counters must not be negative"
                );
            }
        }
    }
}
