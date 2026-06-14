package transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Return;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transport.BrokerTransport;
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

public class RabbitMqTransport implements BrokerTransport {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqTransport.class);
    private static final long MANDATORY_RETURN_WAIT_MILLIS = 250L;

    private final RabbitMqTransportConfig config;
    private final RabbitMqTopology topology;
    private final RabbitMqMessageCodec codec;
    private final Connection connection;
    private final Channel channel;
    private final Map<String, CompletableFuture<Return>> mandatoryReturns = new ConcurrentHashMap<>();

    public RabbitMqTransport(RabbitMqTransportConfig config) throws Exception {
        this(config, new RabbitMqMessageCodec());
    }

    public RabbitMqTransport(RabbitMqTransportConfig config, RabbitMqMessageCodec codec) throws Exception {
        this(config, codec, openResources(config));
    }

    private RabbitMqTransport(RabbitMqTransportConfig config,
                              RabbitMqMessageCodec codec,
                              RabbitMqConnectionResources resources) throws Exception {
        this(config, codec, resources.connection(), resources.channel());
    }

    RabbitMqTransport(RabbitMqTransportConfig config,
                      RabbitMqMessageCodec codec,
                      Connection connection,
                      Channel channel) throws Exception {
        this.config = config;
        this.topology = new RabbitMqTopology(config);
        this.codec = codec;
        this.connection = connection;
        this.channel = channel;
        synchronized (channel) {
            this.channel.basicQos(config.prefetchCount());
            this.channel.confirmSelect();
            this.channel.addReturnListener(this::handleReturnedMessage);
        }
        LOGGER.info("event=rabbitmq_connected host={} port={} vhost={} exchange={} durable={} prefetch={} publisher_confirm_timeout_ms={} dead_letter_enabled={} requeue_on_handler_failure={}",
                config.host(),
                config.port(),
                config.virtualHost(),
                config.exchangeName(),
                config.durable(),
                config.prefetchCount(),
                config.publisherConfirmTimeoutMillis(),
                config.deadLetterEnabled(),
                config.requeueOnHandlerFailure());
    }

    private static RabbitMqConnectionResources openResources(RabbitMqTransportConfig config) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        factory.setAutomaticRecoveryEnabled(true);
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

    @Override
    public void declareTopology() throws Exception {
        synchronized (channel) {
            if (topology.deadLetterEnabled()) {
                channel.exchangeDeclare(topology.deadLetterExchangeName(), BuiltinExchangeType.DIRECT, topology.durable());
                channel.queueDeclare(topology.deadLetterQueueName(), topology.durable(), false, false, null);
                channel.queueBind(
                        topology.deadLetterQueueName(),
                        topology.deadLetterExchangeName(),
                        topology.deadLetterRoutingKey()
                );
            }
            Map<String, Object> queueArguments = topology.queueArguments();
            channel.exchangeDeclare(topology.exchangeName(), BuiltinExchangeType.DIRECT, topology.durable());
            for (TransportRoute route : TransportRoute.values()) {
                String queueName = topology.queueName(route);
                channel.queueDeclare(queueName, topology.durable(), false, false, queueArguments);
                channel.queueBind(queueName, topology.exchangeName(), route.routingKey());
            }
        }
        LOGGER.info("event=rabbitmq_topology_declared exchange={} durable={} dead_letter_enabled={} dead_letter_exchange={} dead_letter_queue={}",
                topology.exchangeName(),
                topology.durable(),
                topology.deadLetterEnabled(),
                topology.deadLetterEnabled() ? topology.deadLetterExchangeName() : "",
                topology.deadLetterEnabled() ? topology.deadLetterQueueName() : "");
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
                .deliveryMode(config.durable() ? 2 : 1)
                .timestamp(Date.from(Instant.now()))
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
        return consume(queueName, route, handler);
    }

    @Override
    public String subscribePeer(TransportRoute route,
                                String peerNodeId,
                                TransportMessageHandler handler) throws Exception {
        declarePeerEndpoint(route, peerNodeId);
        return consume(topology.peerQueueName(route, peerNodeId), route, handler);
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

    private String consume(String queueName, TransportRoute route, TransportMessageHandler handler) throws Exception {
        synchronized (channel) {
            String consumerTag = channel.basicConsume(queueName, false, (tag, delivery) -> {
                RabbitMqAcknowledgement acknowledgement =
                        new RabbitMqAcknowledgement(channel, delivery.getEnvelope().getDeliveryTag());
                InboundTransportMessage message;
                try {
                    message = codec.decode(delivery.getBody(), route, acknowledgement);
                } catch (Exception e) {
                    LOGGER.warn("event=rabbitmq_delivery_decode_failed route={} queue={} action=reject error={}",
                            route.name(), queueName, e.getMessage(), e);
                    rejectFailedDelivery(acknowledgement);
                    return;
                }

                try {
                    handler.handle(message);
                    if (!acknowledgement.isSettled() && !acknowledgement.isDeferred()) {
                        acknowledgement.ack();
                    }
                } catch (Exception e) {
                    String action = config.requeueOnHandlerFailure() ? "requeue" : "reject";
                    LOGGER.warn("event=rabbitmq_delivery_handler_failed route={} queue={} action={} error={}",
                            route.name(), queueName, action, e.getMessage(), e);
                    settleHandlerFailure(acknowledgement);
                }
            }, tag -> {
                LOGGER.warn("event=rabbitmq_consumer_cancelled route={} queue={} consumer_tag={}",
                        route.name(), queueName, tag);
            });
            LOGGER.info("event=rabbitmq_consumer_started route={} queue={} consumer_tag={}",
                    route.name(), queueName, consumerTag);
            return consumerTag;
        }
    }

    private void settleHandlerFailure(RabbitMqAcknowledgement acknowledgement) throws IOException {
        if (acknowledgement.isSettled()) {
            return;
        }
        try {
            if (config.requeueOnHandlerFailure()) {
                acknowledgement.requeue();
            } else {
                acknowledgement.reject();
            }
        } catch (Exception ackError) {
            throw new IOException("Failed to settle RabbitMQ delivery after handler failure", ackError);
        }
    }

    private void rejectFailedDelivery(RabbitMqAcknowledgement acknowledgement) throws IOException {
        if (acknowledgement.isSettled()) {
            return;
        }
        try {
            acknowledgement.reject();
        } catch (Exception ackError) {
            throw new IOException("Failed to reject RabbitMQ delivery", ackError);
        }
    }

    @Override
    public void cancel(String consumerTag) throws Exception {
        synchronized (channel) {
            channel.basicCancel(consumerTag);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            channel.close();
        } finally {
            connection.close();
        }
    }

    private record RabbitMqConnectionResources(Connection connection, Channel channel) {}
}
