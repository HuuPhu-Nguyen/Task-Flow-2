package server;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import protocol.JobSubmitMessage;
import server.model.MessageEnvelope;
import server.scheduler.SchedulerMailbox;
import transport.DeliveryDisposition;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqMessageCodec;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;
import transport.rabbitmq.RabbitMqTopology;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RabbitMqCoordinatorShutdownLiveIntegrationTest {
    private static final String LIVE_TEST_PROPERTY = "taskflow.rabbitmq.live";
    private static final String LIVE_TEST_ENV = "TASKFLOW_RABBITMQ_LIVE_TEST";

    @Test
    void shutdownDrainsAcceptedDeliveryAndReturnsPostStopDeliveryToRabbitMq() throws Exception {
        Assumptions.assumeTrue(liveTestEnabled(),
                "Set -D" + LIVE_TEST_PROPERTY + "=true or " + LIVE_TEST_ENV + "=true to run live RabbitMQ tests.");

        String token = "shutdown-ownership-" + UUID.randomUUID().toString().replace("-", "");
        String peerId = "peer-" + token;
        String acceptedJobId = "accepted-" + token;
        String postStopJobId = "post-stop-" + token;
        RabbitMqTransportConfig config = liveConfig(token);
        RabbitMqTopology topology = new RabbitMqTopology(config);
        RabbitMqTransport coordinatorTransport = null;
        RabbitMqTransport publisherTransport = null;
        Thread schedulerThread = null;
        Thread shutdownThread = null;

        try {
            cleanup(config, topology, peerId);
            coordinatorTransport = new RabbitMqTransport(config);
            publisherTransport = new RabbitMqTransport(config);
            coordinatorTransport.declareTopology();

            BlockingQueue<MessageEnvelope> mailbox = new LinkedBlockingQueue<>(1);
            SchedulerMailbox.BrokerIngress ingress = SchedulerMailbox.brokerIngress(mailbox);
            CountDownLatch acceptedDeliveryQueued = new CountDownLatch(1);
            CountDownLatch intakeStopped = new CountDownLatch(1);
            CountDownLatch postStopDeliveryObserved = new CountDownLatch(1);
            CountDownLatch drainRequested = new CountDownLatch(1);
            CountDownLatch acceptedDeliveryDrained = new CountDownLatch(1);
            AtomicReference<Throwable> handlerFailure = new AtomicReference<>();
            AtomicReference<Throwable> schedulerFailure = new AtomicReference<>();
            AtomicReference<Throwable> shutdownFailure = new AtomicReference<>();
            AtomicBoolean databaseClosed = new AtomicBoolean();
            List<String> events = new CopyOnWriteArrayList<>();

            String consumerTag = coordinatorTransport.subscribe(
                    TransportRoute.JOB_SUBMIT,
                    delivery -> {
                        try {
                            JobSubmitMessage submit = assertInstanceOf(
                                    JobSubmitMessage.class,
                                    delivery.message()
                            );
                            SchedulerMailbox.BrokerOfferOutcome outcome = ingress.offer(delivery);
                            if (acceptedJobId.equals(submit.getJobId())) {
                                assertEquals(SchedulerMailbox.BrokerOfferOutcome.QUEUED, outcome);
                                events.add("accepted-queued");
                                acceptedDeliveryQueued.countDown();
                            } else if (postStopJobId.equals(submit.getJobId())) {
                                assertEquals(
                                        SchedulerMailbox.BrokerOfferOutcome.INTAKE_STOPPED_UNACKNOWLEDGED,
                                        outcome
                                );
                                events.add("post-stop-unacknowledged");
                                postStopDeliveryObserved.countDown();
                            } else {
                                fail("Unexpected live shutdown test job: " + submit.getJobId());
                            }
                        } catch (Throwable e) {
                            handlerFailure.compareAndSet(null, e);
                            throw e;
                        }
                    }
            );

            schedulerThread = new Thread(() -> {
                try {
                    assertTrue(
                            drainRequested.await(10, TimeUnit.SECONDS),
                            "Timed out waiting for the coordinator drain request."
                    );
                    MessageEnvelope accepted = mailbox.poll(10, TimeUnit.SECONDS);
                    assertNotNull(accepted, "Shutdown did not retain the accepted mailbox delivery.");
                    JobSubmitMessage submit = assertInstanceOf(
                            JobSubmitMessage.class,
                            accepted.message()
                    );
                    assertEquals(acceptedJobId, submit.getJobId());
                    assertNotNull(accepted.acknowledgement());
                    accepted.acknowledgement().settle(
                            DeliveryDisposition.ACK_SUCCESS,
                            "shutdown_drain_complete"
                    );
                    events.add("accepted-drained");
                    acceptedDeliveryDrained.countDown();
                } catch (Throwable e) {
                    schedulerFailure.compareAndSet(null, e);
                }
            }, "rabbitmq-live-shutdown-drain-scheduler");
            schedulerThread.start();

            Thread schedulerToDrain = schedulerThread;
            RabbitMqCoordinatorShutdown shutdown = new RabbitMqCoordinatorShutdown(
                    () -> {
                        ingress.stopIntake();
                        events.add("intake-stopped");
                        intakeStopped.countDown();
                        try {
                            if (!postStopDeliveryObserved.await(10, TimeUnit.SECONDS)) {
                                shutdownFailure.compareAndSet(
                                        null,
                                        new AssertionError(
                                                "Timed out waiting for a post-stop broker delivery."
                                        )
                                );
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            shutdownFailure.compareAndSet(null, e);
                        }
                    },
                    coordinatorTransport,
                    List.of(consumerTag),
                    () -> events.add("monitor-stopped"),
                    null,
                    () -> {
                        events.add("drain-requested");
                        drainRequested.countDown();
                    },
                    schedulerToDrain,
                    () -> {
                        databaseClosed.set(true);
                        events.add("database-closed");
                    },
                    2_000L
            );

            assertTrue(publisherTransport.publish(new OutboundTransportMessage(
                    TransportRoute.JOB_SUBMIT,
                    peerId,
                    jobSubmission(peerId, acceptedJobId)
            )));
            await(acceptedDeliveryQueued, handlerFailure, "accepted pre-shutdown delivery");

            shutdownThread = new Thread(shutdown, "rabbitmq-live-coordinator-shutdown");
            shutdownThread.start();
            await(intakeStopped, shutdownFailure, "shutdown intake stop");

            assertTrue(publisherTransport.publish(new OutboundTransportMessage(
                    TransportRoute.JOB_SUBMIT,
                    peerId,
                    jobSubmission(peerId, postStopJobId)
            )));
            await(
                    postStopDeliveryObserved,
                    handlerFailure,
                    "post-stop unacknowledged delivery"
            );

            shutdownThread.join(10_000L);
            assertFalse(shutdownThread.isAlive(), "Coordinator shutdown did not finish.");
            assertNoFailure(shutdownFailure);
            assertNoFailure(schedulerFailure);
            await(
                    acceptedDeliveryDrained,
                    schedulerFailure,
                    "accepted delivery drain acknowledgement"
            );
            assertFalse(schedulerThread.isAlive());
            assertTrue(databaseClosed.get());
            assertTrue(events.indexOf("intake-stopped") < events.indexOf("drain-requested"));
            assertTrue(events.indexOf("accepted-drained") < events.indexOf("database-closed"));
            coordinatorTransport = null;

            CountDownLatch brokerOwnershipRestored = new CountDownLatch(1);
            AtomicReference<Throwable> redeliveryFailure = new AtomicReference<>();
            try (Connection connection = connectionFactory(config)
                    .newConnection("taskflow-rabbitmq-live-shutdown-redelivery");
                 Channel channel = connection.createChannel()) {
                channel.basicConsume(
                        topology.queueName(TransportRoute.JOB_SUBMIT),
                        false,
                        (tag, delivery) -> {
                            try {
                                assertTrue(
                                        delivery.getEnvelope().isRedeliver(),
                                        "Post-stop unacknowledged delivery was not marked redelivered."
                                );
                                InboundTransportMessage decoded = new RabbitMqMessageCodec().decode(
                                        delivery.getBody(),
                                        TransportRoute.JOB_SUBMIT,
                                        null
                                );
                                JobSubmitMessage submit = assertInstanceOf(
                                        JobSubmitMessage.class,
                                        decoded.message()
                                );
                                assertEquals(postStopJobId, submit.getJobId());
                                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                            } catch (Throwable e) {
                                redeliveryFailure.compareAndSet(null, e);
                            } finally {
                                brokerOwnershipRestored.countDown();
                            }
                        },
                        tag -> {
                        }
                );
                await(
                        brokerOwnershipRestored,
                        redeliveryFailure,
                        "post-stop broker ownership restoration"
                );
                assertEquals(
                        0L,
                        channel.queueDeclarePassive(
                                topology.queueName(TransportRoute.JOB_SUBMIT)
                        ).getMessageCount()
                );
            }
        } finally {
            if (shutdownThread != null && shutdownThread.isAlive()) {
                shutdownThread.interrupt();
                shutdownThread.join(2_000L);
            }
            if (schedulerThread != null && schedulerThread.isAlive()) {
                schedulerThread.interrupt();
                schedulerThread.join(2_000L);
            }
            closeQuietly(coordinatorTransport);
            closeQuietly(publisherTransport);
            cleanup(config, topology, peerId);
        }
    }

    private static JobSubmitMessage jobSubmission(String peerId, String jobId) {
        return new JobSubmitMessage(
                peerId,
                Instant.now().toString(),
                jobId,
                "ACKNOWLEDGEMENT_TEST_TASK",
                List.of("payload"),
                "",
                "token-" + jobId
        );
    }

    private static RabbitMqTransportConfig liveConfig(String token) {
        RabbitMqTransportConfig base = RabbitMqTransportConfig.fromEnvironment();
        String name = "taskflow.live.shutdown." + token;
        return new RabbitMqTransportConfig(
                base.host(),
                base.port(),
                base.username(),
                base.password(),
                base.virtualHost(),
                name + ".exchange",
                name,
                false,
                2,
                base.publisherConfirmTimeoutMillis(),
                true,
                name + ".dlx",
                name + ".dlq",
                "dead-letter",
                base.retryDelaysMillis()
        );
    }

    private static boolean liveTestEnabled() {
        if (Boolean.getBoolean(LIVE_TEST_PROPERTY)) {
            return true;
        }
        return "true".equalsIgnoreCase(System.getenv(LIVE_TEST_ENV));
    }

    private static ConnectionFactory connectionFactory(RabbitMqTransportConfig config) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        factory.setAutomaticRecoveryEnabled(false);
        return factory;
    }

    private static void await(CountDownLatch latch,
                              AtomicReference<Throwable> failure,
                              String description) throws InterruptedException {
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for " + description);
        assertNoFailure(failure);
    }

    private static void assertNoFailure(AtomicReference<Throwable> failure) {
        Throwable error = failure.get();
        if (error != null) {
            fail(error);
        }
    }

    private static void cleanup(RabbitMqTransportConfig config,
                                RabbitMqTopology topology,
                                String peerId) {
        for (TransportRoute route : TransportRoute.values()) {
            deleteQueue(config, topology.queueName(route));
            deleteQueue(config, topology.peerQueueName(route, peerId));
        }
        for (int retryStage = 1; retryStage <= topology.retryStageCount(); retryStage++) {
            deleteQueue(config, topology.retryQueueName(retryStage));
            deleteExchange(config, topology.retryExchangeName(retryStage));
        }
        deleteQueue(config, topology.deadLetterQuarantineQueueName());
        deleteQueue(config, config.deadLetterQueueName());
        if (!topology.quarantineExchangeName().equals(config.deadLetterExchangeName())) {
            deleteExchange(config, topology.quarantineExchangeName());
        }
        deleteExchange(config, config.exchangeName());
        deleteExchange(config, config.deadLetterExchangeName());
    }

    private static void deleteQueue(RabbitMqTransportConfig config, String queueName) {
        try (Connection connection = connectionFactory(config)
                .newConnection("taskflow-rabbitmq-live-shutdown-cleanup");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(queueName);
        } catch (Exception ignored) {
        }
    }

    private static void deleteExchange(RabbitMqTransportConfig config, String exchangeName) {
        try (Connection connection = connectionFactory(config)
                .newConnection("taskflow-rabbitmq-live-shutdown-cleanup");
             Channel channel = connection.createChannel()) {
            channel.exchangeDelete(exchangeName);
        } catch (Exception ignored) {
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }
}
