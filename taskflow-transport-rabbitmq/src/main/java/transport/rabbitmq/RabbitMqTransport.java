package transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.Return;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.MessageValidationException;
import transport.BrokerTransport;
import transport.ClassifiedDeliveryFailure;
import transport.DeliveryDisposition;
import transport.DeliveryFailureClassifier;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportMessageHandler;
import transport.TransportRoute;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RabbitMqTransport implements BrokerTransport {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqTransport.class);
    private static final long MANDATORY_RETURN_WAIT_MILLIS = 250L;
    private static final int PERSISTENT_DELIVERY_MODE = 2;

    private final RabbitMqTransportConfig config;
    private final RabbitMqTopology topology;
    private final RabbitMqMessageCodec codec;
    private final RabbitMqRecoveryPolicy recoveryPolicy;
    private final Connection connection;
    private final Channel channel;
    private final RabbitMqTransportMetrics metrics = new RabbitMqTransportMetrics();
    private final Map<String, CompletableFuture<Return>> mandatoryReturns = new ConcurrentHashMap<>();
    private final Map<String, Channel> consumerChannels = new ConcurrentHashMap<>();
    private final AtomicBoolean connectionUsable = new AtomicBoolean();

    public RabbitMqTransport(RabbitMqTransportConfig config) throws Exception {
        this(config, RabbitMqRecoveryPolicy.defaults());
    }

    public RabbitMqTransport(RabbitMqTransportConfig config,
                             RabbitMqRecoveryPolicy recoveryPolicy) throws Exception {
        this(config, new RabbitMqMessageCodec(), recoveryPolicy);
    }

    public RabbitMqTransport(RabbitMqTransportConfig config, RabbitMqMessageCodec codec) throws Exception {
        this(config, codec, RabbitMqRecoveryPolicy.defaults());
    }

    public RabbitMqTransport(RabbitMqTransportConfig config,
                             RabbitMqMessageCodec codec,
                             RabbitMqRecoveryPolicy recoveryPolicy) throws Exception {
        this(config, codec, recoveryPolicy, openResources(config, recoveryPolicy));
    }

    private RabbitMqTransport(RabbitMqTransportConfig config,
                              RabbitMqMessageCodec codec,
                              RabbitMqRecoveryPolicy recoveryPolicy,
                              RabbitMqConnectionResources resources) throws Exception {
        this(config, codec, recoveryPolicy, resources.connection(), resources.channel());
    }

    RabbitMqTransport(RabbitMqTransportConfig config,
                      RabbitMqMessageCodec codec,
                      Connection connection,
                      Channel channel) throws Exception {
        this(
                config,
                codec,
                RabbitMqRecoveryPolicy.defaults(),
                connection,
                channel
        );
    }

    RabbitMqTransport(RabbitMqTransportConfig config,
                      RabbitMqMessageCodec codec,
                      RabbitMqRecoveryPolicy recoveryPolicy,
                      Connection connection,
                      Channel channel) throws Exception {
        this.config = config;
        this.topology = new RabbitMqTopology(config);
        this.codec = codec;
        this.recoveryPolicy = recoveryPolicy;
        this.connection = connection;
        this.channel = channel;
        try {
            synchronized (channel) {
                this.channel.basicQos(config.prefetchCount());
                this.channel.confirmSelect();
                this.channel.addReturnListener(this::handleReturnedMessage);
            }
            registerRecoveryObservers();
            connectionUsable.set(true);
        } catch (Exception e) {
            closeAfterStartupFailure(connection, channel, e);
            throw e;
        }
        LOGGER.info("event=rabbitmq_connected host={} port={} vhost={} exchange={} durable={} prefetch={} publisher_confirm_timeout_ms={} dead_letter_enabled={} retry_delays_ms={} max_delivery_attempts={} connection_timeout_ms={} recovery_initial_delay_ms={} recovery_max_delay_ms={} recovery_backoff_multiplier={}",
                config.host(),
                config.port(),
                config.virtualHost(),
                config.exchangeName(),
                config.durable(),
                config.prefetchCount(),
                config.publisherConfirmTimeoutMillis(),
                config.deadLetterEnabled(),
                config.retryDelaysMillis(),
                config.maxDeliveryAttempts(),
                recoveryPolicy.connectionTimeoutMillis(),
                recoveryPolicy.initialRetryDelayMillis(),
                recoveryPolicy.maxRetryDelayMillis(),
                recoveryPolicy.backoffMultiplier());
    }

    private static RabbitMqConnectionResources openResources(
            RabbitMqTransportConfig config,
            RabbitMqRecoveryPolicy recoveryPolicy
    ) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setConnectionTimeout(recoveryPolicy.connectionTimeoutMillis());
        factory.setHandshakeTimeout(recoveryPolicy.connectionTimeoutMillis());
        factory.setRecoveryDelayHandler(recoveryAttempts -> {
            int failedAttempts = Math.max(1, recoveryAttempts);
            long delayMillis = recoveryPolicy.retryDelayMillis(failedAttempts);
            LOGGER.warn(
                    "event=rabbitmq_connection_recovery_retry_scheduled attempt={} delay_ms={} host={} port={}",
                    failedAttempts,
                    delayMillis,
                    config.host(),
                    config.port()
            );
            return delayMillis;
        });
        Connection connection = factory.newConnection("taskflow-rabbitmq-transport");
        try {
            return new RabbitMqConnectionResources(connection, connection.createChannel());
        } catch (Exception e) {
            try {
                connection.close();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    private void registerRecoveryObservers() {
        AtomicInteger recoveryCycle = new AtomicInteger();
        AtomicLong recoveryStartedNanos = new AtomicLong();
        connection.addShutdownListener(cause -> {
            connectionUsable.set(false);
            if (cause.isInitiatedByApplication()) {
                LOGGER.info(
                        "event=rabbitmq_connection_closed host={} port={} hard_error={} reason={}",
                        config.host(),
                        config.port(),
                        cause.isHardError(),
                        cause.getReason()
                );
            } else {
                LOGGER.warn(
                        "event=rabbitmq_connection_interrupted host={} port={} hard_error={} reason={}",
                        config.host(),
                        config.port(),
                        cause.isHardError(),
                        cause.getReason()
                );
            }
        });
        if (connection instanceof Recoverable recoverable) {
            recoverable.addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecoveryStarted(Recoverable ignored) {
                    connectionUsable.set(false);
                    recoveryStartedNanos.set(System.nanoTime());
                    LOGGER.warn(
                            "event=rabbitmq_connection_recovery_started cycle={} host={} port={} "
                                    + "initial_delay_ms={} max_delay_ms={} backoff_multiplier={}",
                            recoveryCycle.incrementAndGet(),
                            config.host(),
                            config.port(),
                            recoveryPolicy.initialRetryDelayMillis(),
                            recoveryPolicy.maxRetryDelayMillis(),
                            recoveryPolicy.backoffMultiplier()
                    );
                }

                @Override
                public void handleTopologyRecoveryStarted(Recoverable ignored) {
                    connectionUsable.set(false);
                    LOGGER.info(
                            "event=rabbitmq_topology_recovery_started cycle={} host={} port={}",
                            recoveryCycle.get(),
                            config.host(),
                            config.port()
                    );
                }

                @Override
                public void handleRecovery(Recoverable ignored) {
                    connectionUsable.set(true);
                    long startedNanos = recoveryStartedNanos.getAndSet(0L);
                    long elapsedMillis = startedNanos == 0L
                            ? 0L
                            : (System.nanoTime() - startedNanos) / 1_000_000L;
                    LOGGER.info(
                            "event=rabbitmq_connection_recovery_completed cycle={} elapsed_ms={} "
                                    + "host={} port={} topology_recovered=true",
                            recoveryCycle.get(),
                            elapsedMillis,
                            config.host(),
                            config.port()
                    );
                }
            });
        }
    }

    private static void closeAfterStartupFailure(Connection connection, Channel channel, Exception startupFailure) {
        try {
            channel.close();
        } catch (Exception closeFailure) {
            startupFailure.addSuppressed(closeFailure);
        }
        try {
            connection.close();
        } catch (Exception closeFailure) {
            startupFailure.addSuppressed(closeFailure);
        }
    }

    @Override
    public void declareTopology() throws Exception {
        synchronized (channel) {
            RabbitMqTopologyDeclarer.declare(channel, topology);
        }
        LOGGER.info("event=rabbitmq_topology_declared exchange={} durable={} dead_letter_enabled={} dead_letter_exchange={} dead_letter_queue={} quarantine_exchange={} quarantine_queue={} retry_delays_ms={} max_delivery_attempts={}",
                topology.exchangeName(),
                topology.durable(),
                topology.deadLetterEnabled(),
                topology.deadLetterEnabled() ? topology.deadLetterExchangeName() : "",
                topology.deadLetterEnabled() ? topology.deadLetterQueueName() : "",
                topology.quarantineExchangeName(),
                topology.deadLetterQuarantineQueueName(),
                topology.retryDelaysMillis(),
                topology.maxDeliveryAttempts());
    }

    @Override
    public boolean publish(OutboundTransportMessage message) throws Exception {
        return publish(message, message.route().routingKey(), false);
    }

    @Override
    public boolean publishToPeer(TransportRoute route,
                                 String peerNodeId,
                                 OutboundTransportMessage message) throws Exception {
        return publish(message, topology.peerRoutingKey(route, peerNodeId), true);
    }

    private boolean publish(OutboundTransportMessage message, String routingKey, boolean mandatory) throws Exception {
        byte[] body = codec.encode(message);
        String messageId = UUID.randomUUID().toString();
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .messageId(messageId)
                .contentType("application/json")
                .contentEncoding(StandardCharsets.UTF_8.name())
                .deliveryMode(PERSISTENT_DELIVERY_MODE)
                .timestamp(Date.from(Instant.now()))
                .headers(Map.of(
                        RabbitMqRetryHeaders.DELIVERY_ATTEMPT,
                        1,
                        RabbitMqRetryHeaders.ORIGINAL_ROUTING_KEY,
                        routingKey,
                        RabbitMqRetryHeaders.ORIGINAL_EXCHANGE,
                        topology.exchangeName(),
                        RabbitMqRetryHeaders.ORIGINAL_MESSAGE_ID,
                        messageId
                ))
                .build();
        CompletableFuture<Return> returned = mandatory ? new CompletableFuture<>() : null;
        if (mandatory) {
            mandatoryReturns.put(messageId, returned);
        }
        try {
            synchronized (channel) {
                channel.basicPublish(topology.exchangeName(), routingKey, mandatory, properties, body);
                if (!waitForPublisherConfirm(message, routingKey)) {
                    return false;
                }
            }
            if (!mandatory) {
                return true;
            }
            try {
                Return returnedMessage = returned.get(MANDATORY_RETURN_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                LOGGER.warn("event=rabbitmq_publish_unroutable route={} routing_key={} reply_code={} reply_text={}",
                        message.route().name(),
                        returnedMessage.getRoutingKey(),
                        returnedMessage.getReplyCode(),
                        returnedMessage.getReplyText());
                return false;
            } catch (TimeoutException expectedWhenRouted) {
                return true;
            }
        } finally {
            if (mandatory) {
                mandatoryReturns.remove(messageId);
            }
        }
    }

    private boolean waitForPublisherConfirm(OutboundTransportMessage message, String routingKey) throws IOException {
        try {
            boolean confirmed = channel.waitForConfirms(config.publisherConfirmTimeoutMillis());
            if (!confirmed) {
                LOGGER.warn("event=rabbitmq_publish_not_confirmed route={} routing_key={} timeout_ms={}",
                        message.route().name(),
                        routingKey,
                        config.publisherConfirmTimeoutMillis());
            }
            return confirmed;
        } catch (TimeoutException e) {
            LOGGER.warn("event=rabbitmq_publish_confirm_timeout route={} routing_key={} timeout_ms={}",
                    message.route().name(),
                    routingKey,
                    config.publisherConfirmTimeoutMillis());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for RabbitMQ publisher confirm", e);
        }
    }

    private void handleReturnedMessage(Return returned) {
        String messageId = returned.getProperties() == null ? null : returned.getProperties().getMessageId();
        if (messageId != null) {
            CompletableFuture<Return> pending = mandatoryReturns.remove(messageId);
            if (pending != null) {
                pending.complete(returned);
                return;
            }
        }
        LOGGER.warn("event=rabbitmq_publish_returned_unmatched routing_key={} reply_code={} reply_text={}",
                returned.getRoutingKey(),
                returned.getReplyCode(),
                returned.getReplyText());
    }

    @Override
    public String subscribe(TransportRoute route, TransportMessageHandler handler) throws Exception {
        String queueName = topology.queueName(route);
        return consume(queueName, route, handler, channel, null);
    }

    /**
     * Creates one route-local consumer channel so its prefetch cannot reduce
     * credit for other consumers on this transport.
     */
    public String subscribe(TransportRoute route,
                            int prefetchCount,
                            TransportMessageHandler handler) throws Exception {
        if (prefetchCount <= 0) {
            throw new IllegalArgumentException("prefetchCount must be positive");
        }
        Channel consumerChannel = connection.createChannel();
        if (consumerChannel == null) {
            throw new IOException("RabbitMQ did not create a route-local consumer channel.");
        }
        boolean subscribed = false;
        try {
            synchronized (consumerChannel) {
                consumerChannel.basicQos(prefetchCount);
                String consumerTag = stableConsumerTag(route);
                String actualTag = consume(
                        topology.queueName(route),
                        route,
                        handler,
                        consumerChannel,
                        consumerTag
                );
                subscribed = true;
                LOGGER.info(
                        "event=rabbitmq_route_prefetch_applied route={} prefetch={} consumer_tag={}",
                        route.name(),
                        prefetchCount,
                        actualTag
                );
                return actualTag;
            }
        } finally {
            if (!subscribed) {
                try {
                    consumerChannel.close();
                } catch (Exception closeFailure) {
                    LOGGER.warn(
                            "event=rabbitmq_route_consumer_channel_close_failed route={} error={}",
                            route.name(),
                            closeFailure.getMessage()
                    );
                }
            }
        }
    }

    @Override
    public String subscribePeer(TransportRoute route,
                                String peerNodeId,
                                TransportMessageHandler handler) throws Exception {
        declarePeerEndpoint(route, peerNodeId);
        return consume(topology.peerQueueName(route, peerNodeId), route, handler, channel, null);
    }

    public void declarePeerEndpoint(TransportRoute route, String peerNodeId) throws Exception {
        synchronized (channel) {
            String queueName = topology.peerQueueName(route, peerNodeId);
            channel.queueDeclare(queueName, false, true, true, topology.queueArguments());
            channel.queueBind(queueName, topology.exchangeName(), topology.peerRoutingKey(route, peerNodeId));
        }
        LOGGER.debug("event=rabbitmq_peer_endpoint_declared route={} peer_id={}",
                route.name(), peerNodeId);
    }

    private String consume(String queueName,
                           TransportRoute route,
                           TransportMessageHandler handler,
                           Channel consumerChannel,
                           String requestedConsumerTag) throws Exception {
        synchronized (consumerChannel) {
            com.rabbitmq.client.DeliverCallback deliverCallback = (tag, delivery) -> {
                RabbitMqDeliveryRetry retry = new RabbitMqDeliveryRetry(
                        config,
                        topology,
                        delivery,
                        this::publishSettlementMessage,
                        metrics
                );
                if (delivery.getEnvelope().isRedeliver() || retry.deliveryAttempt() > 1) {
                    metrics.recordRedelivery();
                }
                RabbitMqAcknowledgement acknowledgement = new RabbitMqAcknowledgement(
                        consumerChannel,
                        delivery.getEnvelope().getDeliveryTag(),
                        retry
                );
                InboundTransportMessage message;
                try {
                    message = codec.decode(delivery.getBody(), route, acknowledgement);
                } catch (Exception e) {
                    String reasonCode = e instanceof MessageValidationException validation
                            ? validation.reasonCode()
                            : "message_decode_failed";
                    LOGGER.warn("event=rabbitmq_delivery_decode_failed route={} queue={} reason_code={} disposition={} error={}",
                            route.name(),
                            queueName,
                            reasonCode,
                            DeliveryDisposition.REJECT_INVALID,
                            e.getMessage(),
                            e);
                    settleDelivery(
                            acknowledgement,
                            DeliveryDisposition.REJECT_INVALID,
                            reasonCode
                    );
                    return;
                }

                try {
                    handler.handle(message);
                    if (!acknowledgement.isSettled() && !acknowledgement.isDeferred()) {
                        settleDelivery(
                                acknowledgement,
                                DeliveryDisposition.ACK_SUCCESS,
                                "handler_completed"
                        );
                    }
                } catch (Exception e) {
                    preserveInterrupt(e);
                    ClassifiedDeliveryFailure failure = DeliveryFailureClassifier.classify(e);
                    LOGGER.warn("event=rabbitmq_delivery_handler_failed route={} queue={} reason_code={} disposition={} delivery_attempt={} max_delivery_attempts={} error={}",
                            route.name(),
                            queueName,
                            failure.reasonCode(),
                            failure.disposition(),
                            retry.deliveryAttempt(),
                            config.maxDeliveryAttempts(),
                            e.getMessage(),
                            e);
                    settleDelivery(
                            acknowledgement,
                            failure.disposition(),
                            failure.reasonCode()
                    );
                }
            };
            com.rabbitmq.client.CancelCallback cancelCallback = tag -> {
                LOGGER.warn("event=rabbitmq_consumer_cancelled route={} queue={} consumer_tag={}",
                        route.name(), queueName, tag);
            };
            String consumerTag = requestedConsumerTag == null
                    ? consumerChannel.basicConsume(
                            queueName,
                            false,
                            deliverCallback,
                            cancelCallback
                    )
                    : consumerChannel.basicConsume(
                            queueName,
                            false,
                            requestedConsumerTag,
                            deliverCallback,
                            cancelCallback
                    );
            consumerChannels.put(consumerTag, consumerChannel);
            LOGGER.info("event=rabbitmq_consumer_started route={} queue={} consumer_tag={}",
                    route.name(), queueName, consumerTag);
            return consumerTag;
        }
    }

    private void settleDelivery(RabbitMqAcknowledgement acknowledgement,
                                DeliveryDisposition disposition,
                                String reasonCode) throws IOException {
        if (acknowledgement.isSettled()) {
            return;
        }
        try {
            acknowledgement.settle(disposition, reasonCode);
        } catch (Exception ackError) {
            preserveInterrupt(ackError);
            throw new IOException(
                    "Failed to settle RabbitMQ delivery as " + disposition,
                    ackError
            );
        }
    }

    private boolean publishSettlementMessage(String exchange,
                                             String routingKey,
                                             AMQP.BasicProperties properties,
                                             byte[] body) throws Exception {
        String messageId = properties.getMessageId();
        CompletableFuture<Return> returned = new CompletableFuture<>();
        mandatoryReturns.put(messageId, returned);
        try {
            synchronized (channel) {
                channel.basicPublish(exchange, routingKey, true, properties, body);
                if (!waitForSettlementPublisherConfirm(exchange, routingKey)) {
                    return false;
                }
            }
            try {
                Return returnedMessage = returned.get(MANDATORY_RETURN_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                LOGGER.error("event=rabbitmq_settlement_publish_unroutable exchange={} routing_key={} reply_code={} reply_text={}",
                        exchange,
                        returnedMessage.getRoutingKey(),
                        returnedMessage.getReplyCode(),
                        returnedMessage.getReplyText());
                return false;
            } catch (TimeoutException expectedWhenRouted) {
                return true;
            }
        } finally {
            mandatoryReturns.remove(messageId);
        }
    }

    private boolean waitForSettlementPublisherConfirm(String exchange,
                                                      String routingKey) throws IOException {
        try {
            boolean confirmed = channel.waitForConfirms(config.publisherConfirmTimeoutMillis());
            if (!confirmed) {
                LOGGER.error("event=rabbitmq_settlement_publish_not_confirmed exchange={} routing_key={} timeout_ms={}",
                        exchange,
                        routingKey,
                        config.publisherConfirmTimeoutMillis());
            }
            return confirmed;
        } catch (TimeoutException e) {
            LOGGER.error("event=rabbitmq_settlement_publish_confirm_timeout exchange={} routing_key={} timeout_ms={}",
                    exchange,
                    routingKey,
                    config.publisherConfirmTimeoutMillis());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for RabbitMQ settlement publisher confirm", e);
        }
    }

    private static void preserveInterrupt(Exception error) {
        if (error instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void cancel(String consumerTag) throws Exception {
        Channel consumerChannel = consumerChannels.getOrDefault(consumerTag, channel);
        synchronized (consumerChannel) {
            consumerChannel.basicCancel(consumerTag);
        }
        consumerChannels.remove(consumerTag, consumerChannel);
    }

    @Override
    public void close() throws Exception {
        connectionUsable.set(false);
        try {
            channel.close();
        } finally {
            consumerChannels.clear();
            connection.close();
        }
    }

    public RabbitMqTransportMetrics.Snapshot metricsSnapshot() {
        return metrics.snapshot();
    }

    /**
     * Returns whether the recovered connection, publisher channel, and all
     * currently owned consumer channels are usable now.
     */
    public boolean connectionUsable() {
        if (!connectionUsable.get() || !connection.isOpen() || !channel.isOpen()) {
            return false;
        }
        return consumerChannels.values().stream().allMatch(Channel::isOpen);
    }

    private static String stableConsumerTag(TransportRoute route) {
        return "taskflow-"
                + route.name().toLowerCase(java.util.Locale.ROOT).replace('_', '-')
                + "-"
                + UUID.randomUUID();
    }

    private record RabbitMqConnectionResources(Connection connection, Channel channel) {}
}
