package transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import transport.BrokerTransport;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportMessageHandler;
import transport.TransportRoute;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;

public class RabbitMqTransport implements BrokerTransport {
    private final RabbitMqTransportConfig config;
    private final RabbitMqTopology topology;
    private final RabbitMqMessageCodec codec;
    private final Connection connection;
    private final Channel channel;

    public RabbitMqTransport(RabbitMqTransportConfig config) throws Exception {
        this(config, new RabbitMqMessageCodec());
    }

    public RabbitMqTransport(RabbitMqTransportConfig config, RabbitMqMessageCodec codec) throws Exception {
        this.config = config;
        this.topology = new RabbitMqTopology(config);
        this.codec = codec;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        factory.setAutomaticRecoveryEnabled(true);
        this.connection = factory.newConnection("taskflow-rabbitmq-transport");
        this.channel = connection.createChannel();
        this.channel.basicQos(config.prefetchCount());
    }

    @Override
    public void declareTopology() throws Exception {
        synchronized (channel) {
            channel.exchangeDeclare(topology.exchangeName(), BuiltinExchangeType.DIRECT, topology.durable());
            for (TransportRoute route : TransportRoute.values()) {
                String queueName = topology.queueName(route);
                channel.queueDeclare(queueName, topology.durable(), false, false, null);
                channel.queueBind(queueName, topology.exchangeName(), route.routingKey());
            }
        }
    }

    @Override
    public void publish(OutboundTransportMessage message) throws Exception {
        byte[] body = codec.encode(message);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .contentEncoding(StandardCharsets.UTF_8.name())
                .deliveryMode(config.durable() ? 2 : 1)
                .timestamp(Date.from(Instant.now()))
                .build();
        synchronized (channel) {
            channel.basicPublish(topology.exchangeName(), message.route().routingKey(), properties, body);
        }
    }

    @Override
    public String subscribe(TransportRoute route, TransportMessageHandler handler) throws Exception {
        String queueName = topology.queueName(route);
        return channel.basicConsume(queueName, false, (consumerTag, delivery) -> {
            RabbitMqAcknowledgement acknowledgement =
                    new RabbitMqAcknowledgement(channel, delivery.getEnvelope().getDeliveryTag());
            try {
                InboundTransportMessage message = codec.decode(delivery.getBody(), route, acknowledgement);
                handler.handle(message);
                if (!acknowledgement.isSettled()) {
                    acknowledgement.ack();
                }
            } catch (Exception e) {
                if (!acknowledgement.isSettled()) {
                    try {
                        acknowledgement.requeue();
                    } catch (Exception ackError) {
                        throw new IOException("Failed to requeue RabbitMQ delivery", ackError);
                    }
                }
            }
        }, consumerTag -> {
        });
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
}
