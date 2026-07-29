package server.rabbitmq;

import server.db.DatabaseManager;
import server.scheduler.BrokerOutboxPublisher;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/**
 * Test-source access to the production outbox replay batch for TF-0708.
 */
public final class RecoveryOutboxProbe {
    private static final long RETRY_PARK_NANOS =
            TimeUnit.MILLISECONDS.toNanos(10L);

    private RecoveryOutboxProbe() {
    }

    public static ReplayResult drain(
            DatabaseManager database,
            BrokerOutboxPublisher publisher,
            int batchSize,
            long timeoutSeconds,
            BooleanSupplier deliveriesComplete
    ) {
        long startedAt = System.nanoTime();
        long deadline = startedAt
                + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        int published = 0;
        int iterations = 0;
        try (RabbitMqOutboxReplayer replayer = new RabbitMqOutboxReplayer(
                database,
                publisher,
                batchSize,
                1_000L
        )) {
            while (System.nanoTime() < deadline) {
                iterations++;
                int replayed = replayer.replayOnce();
                published += replayed;
                long pending = database.countPendingBrokerOutbox().count();
                if (pending == 0L && deliveriesComplete.getAsBoolean()) {
                    return new ReplayResult(
                            System.nanoTime() - startedAt,
                            published,
                            iterations
                    );
                }
                if (replayed == 0) {
                    LockSupport.parkNanos(RETRY_PARK_NANOS);
                }
            }
        }
        throw new IllegalStateException(
                "Timed out draining recovery outbox; pending="
                        + database.countPendingBrokerOutbox().count()
                        + ", published="
                        + published
                        + ", iterations="
                        + iterations
        );
    }

    public record ReplayResult(
            long durationNanos,
            int publishedRows,
            int iterations
    ) {
        public ReplayResult {
            if (durationNanos <= 0L
                    || publishedRows < 0
                    || iterations < 1) {
                throw new IllegalArgumentException(
                        "Recovery replay measurements are invalid."
                );
            }
        }
    }
}
