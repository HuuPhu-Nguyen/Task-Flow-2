package transport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import protocol.JobResultMessage;
import protocol.PingMessage;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reusable black-box broker contract for a {@link BrokerTransport} adapter.
 *
 * <p>The adapter binding owns infrastructure lifecycle, topology inspection,
 * and bounded waiting. Behavioral methods remain unchanged across adapters.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BrokerTransportContractTest {
    private static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration STATE_TIMEOUT = Duration.ofSeconds(10);
    private static final List<Long> SHORT_RETRY_DELAYS = List.of(200L, 350L);
    private static final String NODE_ID = "broker-contract";

    private BrokerHarness harness;

    /**
     * Keeps infrastructure-backed contracts opt-in in the normal fast reactor.
     */
    protected abstract boolean contractEnabled();

    /**
     * Starts the adapter's isolated real-broker harness.
     */
    protected abstract BrokerHarness startHarness() throws Exception;

    @BeforeAll
    protected final void startContractHarness() throws Exception {
        Assumptions.assumeTrue(
                contractEnabled(),
                "Enable the live broker contract to start its managed broker."
        );
        harness = startHarness();
    }

    @AfterAll
    protected final void stopContractHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    protected void publisherConfirmsAcceptedPublication() throws Exception {
        try (BrokerSession session = session("publisher-confirms");
             BrokerTransport transport = session.openTransport()) {
            transport.declareTopology();
            CountDownLatch delivered = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            transport.subscribe(TransportRoute.HEARTBEAT, delivery ->
                    captureDelivery(delivery, "confirmed", delivered, failure));

            assertTrue(
                    transport.publish(heartbeat("confirmed")),
                    "Publish success must mean the broker confirmed acceptance."
            );
            await(delivered, failure, "confirmed publication");
            session.awaitQueueState(
                    TransportRoute.HEARTBEAT,
                    0L,
                    0L,
                    STATE_TIMEOUT
            );
        }
    }

    @Test
    protected void unroutablePeerPublicationIsDetected() throws Exception {
        try (BrokerSession session = session("unroutable");
             BrokerTransport transport = session.openTransport()) {
            transport.declareTopology();

            assertFalse(
                    transport.publishToPeer(
                            TransportRoute.JOB_RESULT,
                            "missing-peer",
                            jobResult("unroutable")
                    ),
                    "A mandatory peer publish without a bound endpoint must fail."
            );
        }
    }

    @Test
    protected void manualAcknowledgementOwnsDeliverySettlement()
            throws Exception {
        try (BrokerSession session = session("manual-ack");
             BrokerTransport transport = session.openTransport()) {
            transport.declareTopology();
            CountDownLatch delivered = new CountDownLatch(1);
            AtomicReference<TransportAcknowledgement> acknowledgement =
                    new AtomicReference<>();
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                assertHeartbeat(delivery, "manual-ack");
                delivery.acknowledgement().defer();
                acknowledgement.set(delivery.acknowledgement());
                delivered.countDown();
            });

            assertTrue(transport.publish(heartbeat("manual-ack")));
            await(delivered, null, "deferred delivery");
            session.awaitQueueState(
                    TransportRoute.HEARTBEAT,
                    0L,
                    1L,
                    STATE_TIMEOUT
            );

            assertNotNull(acknowledgement.get());
            acknowledgement.get().ack();
            session.awaitQueueState(
                    TransportRoute.HEARTBEAT,
                    0L,
                    0L,
                    STATE_TIMEOUT
            );
        }
    }

    @Test
    protected void consumerDeathRedeliversUnacknowledgedMessage()
            throws Exception {
        try (BrokerSession session = session("consumer-death")) {
            BrokerTransport firstConsumer = session.openTransport();
            firstConsumer.declareTopology();
            CountDownLatch firstDelivery = new CountDownLatch(1);
            firstConsumer.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                assertHeartbeat(delivery, "consumer-death");
                delivery.acknowledgement().defer();
                firstDelivery.countDown();
            });

            assertTrue(firstConsumer.publish(heartbeat("consumer-death")));
            await(firstDelivery, null, "first unacknowledged delivery");
            session.awaitQueueState(
                    TransportRoute.HEARTBEAT,
                    0L,
                    1L,
                    STATE_TIMEOUT
            );
            firstConsumer.close();

            try (BrokerTransport replacementConsumer =
                         session.openTransport()) {
                CountDownLatch redelivered = new CountDownLatch(1);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                replacementConsumer.subscribe(
                        TransportRoute.HEARTBEAT,
                        delivery -> captureDelivery(
                                delivery,
                                "consumer-death",
                                redelivered,
                                failure
                        )
                );
                await(redelivered, failure, "redelivery after consumer death");
                session.awaitQueueState(
                        TransportRoute.HEARTBEAT,
                        0L,
                        0L,
                        STATE_TIMEOUT
                );
                assertTrue(
                        session.redeliveryCount(replacementConsumer) >= 1L,
                        "The replacement consumer must observe broker redelivery."
                );
            }
        }
    }

    @Test
    protected void delayedRetryUsesBoundedConfiguredStages() throws Exception {
        try (BrokerSession session = session(
                "bounded-retry",
                SHORT_RETRY_DELAYS
        ); BrokerTransport transport = session.openTransport()) {
            transport.declareTopology();
            CountDownLatch deliveries = new CountDownLatch(2);
            AtomicInteger deliveryCount = new AtomicInteger();
            AtomicLong firstAt = new AtomicLong();
            AtomicLong secondAt = new AtomicLong();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                try {
                    assertHeartbeat(delivery, "bounded-retry");
                    int current = deliveryCount.incrementAndGet();
                    if (current == 1) {
                        firstAt.set(System.nanoTime());
                        delivery.acknowledgement().settle(
                                DeliveryDisposition.RETRY_TRANSIENT,
                                "contract_transient"
                        );
                    } else if (current == 2) {
                        secondAt.set(System.nanoTime());
                        delivery.acknowledgement().ack();
                    } else {
                        throw new AssertionError(
                                "Unexpected retry delivery " + current
                        );
                    }
                } catch (Throwable error) {
                    failure.compareAndSet(null, error);
                } finally {
                    deliveries.countDown();
                }
            });

            assertTrue(transport.publish(heartbeat("bounded-retry")));
            await(deliveries, failure, "bounded delayed retry");
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    secondAt.get() - firstAt.get()
            );
            assertTrue(
                    elapsedMillis >= SHORT_RETRY_DELAYS.getFirst() - 35L,
                    "Retry arrived before the configured delay: "
                            + elapsedMillis + " ms"
            );
            assertEquals(2, deliveryCount.get());
            session.awaitQueueState(
                    TransportRoute.HEARTBEAT,
                    0L,
                    0L,
                    STATE_TIMEOUT
            );
            assertEquals(0L, session.quarantineCount());
        }
    }

    @Test
    protected void retryExhaustionQuarantinesExactlyOnce() throws Exception {
        try (BrokerSession session = session(
                "retry-exhaustion",
                SHORT_RETRY_DELAYS
        ); BrokerTransport transport = session.openTransport()) {
            transport.declareTopology();
            int maximumAttempts = session.maxDeliveryAttempts();
            CountDownLatch deliveries = new CountDownLatch(maximumAttempts);
            AtomicInteger deliveryCount = new AtomicInteger();
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                assertHeartbeat(delivery, "retry-exhaustion");
                deliveryCount.incrementAndGet();
                delivery.acknowledgement().settle(
                        DeliveryDisposition.QUARANTINE_POISON,
                        "contract_poison"
                );
                deliveries.countDown();
            });

            assertTrue(transport.publish(heartbeat("retry-exhaustion")));
            await(deliveries, null, "bounded poison attempts");
            QuarantinedDelivery quarantined =
                    session.awaitSingleQuarantined(STATE_TIMEOUT);

            assertEquals(maximumAttempts, deliveryCount.get());
            assertEquals(maximumAttempts, quarantined.deliveryAttempt());
            assertEquals("contract_poison", quarantined.reasonCode());
            assertEquals(
                    DeliveryDisposition.QUARANTINE_POISON,
                    quarantined.disposition()
            );
            assertEquals(
                    TransportRoute.HEARTBEAT,
                    quarantined.originalRoute()
            );
            assertEquals(1L, session.quarantineCount());
        }
    }

    @Test
    protected void duplicateDeliveriesAcceptDuplicateClassification()
            throws Exception {
        try (BrokerSession session = session("duplicate-tolerance");
             BrokerTransport transport = session.openTransport()) {
            transport.declareTopology();
            CountDownLatch deliveries = new CountDownLatch(2);
            AtomicInteger deliveryCount = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            transport.subscribe(TransportRoute.HEARTBEAT, delivery -> {
                try {
                    assertHeartbeat(delivery, "same-logical-event");
                    delivery.acknowledgement().settle(
                            DeliveryDisposition.ACK_DUPLICATE_OR_STALE,
                            "contract_duplicate"
                    );
                    deliveryCount.incrementAndGet();
                } catch (Throwable error) {
                    failure.compareAndSet(null, error);
                } finally {
                    deliveries.countDown();
                }
            });

            OutboundTransportMessage duplicate =
                    heartbeat("same-logical-event");
            assertTrue(transport.publish(duplicate));
            assertTrue(transport.publish(duplicate));
            await(deliveries, failure, "two duplicate logical deliveries");

            assertEquals(2, deliveryCount.get());
            session.awaitQueueState(
                    TransportRoute.HEARTBEAT,
                    0L,
                    0L,
                    STATE_TIMEOUT
            );
            assertEquals(0L, session.quarantineCount());
        }
    }

    @Test
    protected void establishedTransportReconnectsAfterBrokerRestart()
            throws Exception {
        try (BrokerSession session = session("reconnect");
             BrokerTransport transport = session.openTransport()) {
            transport.declareTopology();
            CountDownLatch delivered = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            transport.subscribe(TransportRoute.HEARTBEAT, delivery ->
                    captureDelivery(delivery, "after-restart", delivered, failure));

            harness.stopBroker();
            harness.startBroker();
            session.awaitTransportRecovered(transport, DELIVERY_TIMEOUT);
            assertTrue(
                    session.publishEventually(
                            transport,
                            heartbeat("after-restart"),
                            DELIVERY_TIMEOUT
                    )
            );
            await(delivered, failure, "delivery after broker restart");
        }
    }

    @Test
    protected void durableTopologyAndPersistentMessageSurviveBrokerRestart()
            throws Exception {
        try (BrokerSession session = session("durability")) {
            try (BrokerTransport transport = session.openTransport()) {
                transport.declareTopology();
                session.assertSharedTopologyDurable();

                try (BrokerTransport peer = session.openTransport()) {
                    peer.subscribePeer(
                            TransportRoute.JOB_RESULT,
                            "ephemeral-peer",
                            delivery -> delivery.acknowledgement().ack()
                    );
                    session.assertPeerEndpointEphemeral(
                            TransportRoute.JOB_RESULT,
                            "ephemeral-peer"
                    );
                }

                assertTrue(transport.publish(heartbeat("durable-message")));
                session.awaitQueueState(
                        TransportRoute.HEARTBEAT,
                        1L,
                        0L,
                        STATE_TIMEOUT
                );
            }

            harness.stopBroker();
            harness.startBroker();
            session.assertSharedTopologyDurable();
            session.awaitQueueState(
                    TransportRoute.HEARTBEAT,
                    1L,
                    0L,
                    STATE_TIMEOUT
            );

            try (BrokerTransport recovered = session.openTransport()) {
                CountDownLatch delivered = new CountDownLatch(1);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                recovered.subscribe(TransportRoute.HEARTBEAT, delivery ->
                        captureDelivery(
                                delivery,
                                "durable-message",
                                delivered,
                                failure
                        )
                );
                await(delivered, failure, "persistent message after restart");
            }
        }
    }

    private BrokerSession session(String scenario) throws Exception {
        return session(scenario, SHORT_RETRY_DELAYS);
    }

    private BrokerSession session(String scenario,
                                  List<Long> retryDelays) throws Exception {
        return harness.openSession(scenario, retryDelays);
    }

    private static OutboundTransportMessage heartbeat(String marker) {
        return new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                NODE_ID,
                new PingMessage(NODE_ID, marker)
        );
    }

    private static OutboundTransportMessage jobResult(String marker) {
        return new OutboundTransportMessage(
                TransportRoute.JOB_RESULT,
                NODE_ID,
                new JobResultMessage(
                        NODE_ID,
                        marker,
                        "job-" + marker,
                        "TEST_TASK",
                        true,
                        List.of()
                )
        );
    }

    private static void captureDelivery(
            InboundTransportMessage delivery,
            String marker,
            CountDownLatch delivered,
            AtomicReference<Throwable> failure
    ) {
        try {
            assertHeartbeat(delivery, marker);
            delivery.acknowledgement().ack();
        } catch (Throwable error) {
            failure.compareAndSet(null, error);
        } finally {
            delivered.countDown();
        }
    }

    private static void assertHeartbeat(
            InboundTransportMessage delivery,
            String marker
    ) {
        assertEquals(TransportRoute.HEARTBEAT, delivery.route());
        assertEquals(NODE_ID, delivery.fromNodeId());
        PingMessage ping = assertInstanceOf(
                PingMessage.class,
                delivery.message()
        );
        assertEquals(NODE_ID, ping.getNodeId());
        assertEquals(marker, ping.getTime());
    }

    private static void await(
            CountDownLatch latch,
            AtomicReference<Throwable> failure,
            String description
    ) throws Exception {
        if (!latch.await(DELIVERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            fail("Timed out waiting for " + description);
        }
        if (failure != null && failure.get() != null) {
            fail(description + " failed", failure.get());
        }
    }

    /**
     * Adapter-owned lifecycle for one real broker.
     */
    protected interface BrokerHarness extends AutoCloseable {
        BrokerSession openSession(String scenario,
                                  List<Long> retryDelays) throws Exception;

        void stopBroker() throws Exception;

        void startBroker() throws Exception;

        @Override
        void close() throws Exception;
    }

    /**
     * Adapter-owned binding for one isolated topology namespace.
     */
    protected interface BrokerSession extends AutoCloseable {
        BrokerTransport openTransport() throws Exception;

        int maxDeliveryAttempts();

        void awaitQueueState(TransportRoute route,
                             long ready,
                             long unacknowledged,
                             Duration timeout) throws Exception;

        long quarantineCount() throws Exception;

        QuarantinedDelivery awaitSingleQuarantined(Duration timeout)
                throws Exception;

        long redeliveryCount(BrokerTransport transport) throws Exception;

        void awaitTransportRecovered(BrokerTransport transport,
                                     Duration timeout) throws Exception;

        boolean publishEventually(BrokerTransport transport,
                                  OutboundTransportMessage message,
                                  Duration timeout) throws Exception;

        void assertSharedTopologyDurable() throws Exception;

        void assertPeerEndpointEphemeral(TransportRoute route,
                                         String peerNodeId) throws Exception;

        @Override
        void close() throws Exception;
    }

    /**
     * Adapter-neutral quarantine evidence exposed by the binding.
     */
    protected record QuarantinedDelivery(
            int deliveryAttempt,
            String reasonCode,
            DeliveryDisposition disposition,
            TransportRoute originalRoute
    ) {
        public QuarantinedDelivery {
        }
    }
}
