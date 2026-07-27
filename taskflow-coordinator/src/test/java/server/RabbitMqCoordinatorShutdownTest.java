package server;

import org.junit.jupiter.api.Test;
import transport.BrokerTransport;
import transport.OutboundTransportMessage;
import transport.TransportMessageHandler;
import transport.TransportRoute;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqCoordinatorShutdownTest {
    @Test
    void operationsEndpointAndOrphanCollectorStopBeforeSharedDatabaseCloses() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingTransport transport = new RecordingTransport(events, false);
        Thread schedulerThread = new Thread(() -> {
        });

        RabbitMqCoordinatorShutdown shutdown = new RabbitMqCoordinatorShutdown(
                () -> events.add("stop-intake"),
                transport,
                List.of(),
                () -> events.add("stop-monitor"),
                () -> events.add("close-operations-endpoint"),
                () -> events.add("close-orphan-gc"),
                () -> events.add("close-outbox"),
                () -> events.add("request-drain"),
                schedulerThread,
                () -> events.add("close-database")
        );

        shutdown.shutdown();

        assertTrue(events.indexOf("close-operations-endpoint") >= 0);
        assertTrue(events.indexOf("close-operations-endpoint") < events.indexOf("close-database"));
        assertTrue(events.indexOf("close-orphan-gc") >= 0);
        assertTrue(events.indexOf("close-orphan-gc") < events.indexOf("close-database"));
    }


    @Test
    void stopsIntakeCancelsConsumersDrainsSchedulerThenClosesTransportAndDatabase() {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch drainRequested = new CountDownLatch(1);
        Thread schedulerThread = new Thread(() -> {
            await(drainRequested);
            events.add("scheduler-drained");
        }, "coordinator-shutdown-drain-test");
        schedulerThread.start();
        RecordingTransport transport = new RecordingTransport(events);
        RabbitMqCoordinatorShutdown shutdown = new RabbitMqCoordinatorShutdown(
                () -> events.add("stop-intake"),
                transport,
                List.of("job-consumer", "result-consumer"),
                () -> events.add("stop-monitor"),
                () -> events.add("close-outbox"),
                () -> {
                    events.add("request-drain");
                    drainRequested.countDown();
                },
                schedulerThread,
                () -> events.add("close-database"),
                2_000L
        );

        shutdown.shutdown();
        shutdown.shutdown();

        assertFalse(schedulerThread.isAlive());
        assertEquals(List.of(
                "stop-intake",
                "cancel:job-consumer",
                "cancel:result-consumer",
                "stop-monitor",
                "close-outbox",
                "request-drain",
                "scheduler-drained",
                "close-transport",
                "close-database"
        ), events);
    }

    @Test
    void drainTimeoutInterruptsSchedulerAndClosesTransportBeforeDatabase() {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch schedulerStarted = new CountDownLatch(1);
        Thread schedulerThread = new Thread(() -> {
            schedulerStarted.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                events.add("scheduler-interrupted");
                Thread.currentThread().interrupt();
            }
        }, "coordinator-shutdown-timeout-test");
        schedulerThread.start();
        await(schedulerStarted);
        RecordingTransport transport = new RecordingTransport(events);
        RabbitMqCoordinatorShutdown shutdown = new RabbitMqCoordinatorShutdown(
                () -> events.add("stop-intake"),
                transport,
                List.of("job-consumer"),
                () -> events.add("stop-monitor"),
                () -> events.add("close-outbox"),
                () -> events.add("request-drain"),
                schedulerThread,
                () -> events.add("close-database"),
                25L
        );

        shutdown.shutdown();

        assertFalse(schedulerThread.isAlive());
        assertTrue(events.indexOf("cancel:job-consumer") < events.indexOf("request-drain"));
        assertTrue(events.indexOf("scheduler-interrupted") < events.indexOf("close-database"));
        assertTrue(events.indexOf("close-transport") < events.indexOf("close-database"));
    }

    @Test
    void auxiliaryShutdownFailureDefersDatabaseClose() {
        List<String> events = new CopyOnWriteArrayList<>();
        Thread schedulerThread = new Thread(() -> {
        }, "coordinator-shutdown-already-stopped-scheduler");
        RecordingTransport transport = new RecordingTransport(events);
        RabbitMqCoordinatorShutdown shutdown = new RabbitMqCoordinatorShutdown(
                () -> events.add("stop-intake"),
                transport,
                List.of("job-consumer"),
                () -> events.add("stop-monitor"),
                () -> {
                    events.add("close-outbox-failed");
                    throw new IllegalStateException("outbox thread still running");
                },
                () -> events.add("request-drain"),
                schedulerThread,
                () -> events.add("close-database"),
                25L
        );

        shutdown.shutdown();

        assertTrue(events.contains("close-transport"));
        assertTrue(events.contains("close-outbox-failed"));
        assertFalse(events.contains("close-database"));
    }

    @Test
    void transportShutdownFailureDefersDatabaseClose() {
        List<String> events = new CopyOnWriteArrayList<>();
        Thread schedulerThread = new Thread(() -> {
        }, "coordinator-shutdown-transport-failure-scheduler");
        RecordingTransport transport = new RecordingTransport(events, true);
        RabbitMqCoordinatorShutdown shutdown = new RabbitMqCoordinatorShutdown(
                () -> events.add("stop-intake"),
                transport,
                List.of("job-consumer"),
                () -> events.add("stop-monitor"),
                () -> events.add("close-outbox"),
                () -> events.add("request-drain"),
                schedulerThread,
                () -> events.add("close-database"),
                25L
        );

        shutdown.shutdown();

        assertTrue(events.contains("close-transport"));
        assertFalse(events.contains("close-database"));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Test synchronization was interrupted.", e);
        }
    }

    private static final class RecordingTransport implements BrokerTransport {
        private final List<String> events;
        private final boolean failClose;

        private RecordingTransport(List<String> events) {
            this(events, false);
        }

        private RecordingTransport(List<String> events, boolean failClose) {
            this.events = events;
            this.failClose = failClose;
        }

        @Override
        public void declareTopology() {
        }

        @Override
        public boolean publish(OutboundTransportMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String subscribe(TransportRoute route, TransportMessageHandler handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancel(String consumerTag) {
            events.add("cancel:" + consumerTag);
        }

        @Override
        public void close() {
            events.add("close-transport");
            if (failClose) {
                throw new IllegalStateException("transport callbacks may still be active");
            }
        }
    }
}
