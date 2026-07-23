package transport.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import transport.TransportRoute;

import java.io.IOException;
import java.util.Map;

final class RabbitMqTopologyDeclarer {
    private RabbitMqTopologyDeclarer() {
    }

    static void declare(Channel channel, RabbitMqTopology topology) throws IOException {
        channel.exchangeDeclare(topology.exchangeName(), BuiltinExchangeType.DIRECT, topology.durable());

        if (topology.deadLetterEnabled()) {
            channel.exchangeDeclare(
                    topology.deadLetterExchangeName(),
                    BuiltinExchangeType.DIRECT,
                    topology.durable()
            );
            channel.queueDeclare(
                    topology.deadLetterQueueName(),
                    topology.durable(),
                    false,
                    false,
                    null
            );
            channel.queueBind(
                    topology.deadLetterQueueName(),
                    topology.deadLetterExchangeName(),
                    topology.deadLetterRoutingKey()
            );
        }

        if (!topology.deadLetterEnabled()) {
            channel.exchangeDeclare(
                    topology.quarantineExchangeName(),
                    BuiltinExchangeType.DIRECT,
                    topology.durable()
            );
        }
        channel.queueDeclare(
                topology.deadLetterQuarantineQueueName(),
                topology.durable(),
                false,
                false,
                null
        );
        channel.queueBind(
                topology.deadLetterQuarantineQueueName(),
                topology.quarantineExchangeName(),
                topology.deadLetterQuarantineRoutingKey()
        );

        for (int retryStage = 1; retryStage <= topology.retryStageCount(); retryStage++) {
            String retryExchange = topology.retryExchangeName(retryStage);
            String retryQueue = topology.retryQueueName(retryStage);
            channel.exchangeDeclare(retryExchange, BuiltinExchangeType.TOPIC, topology.durable());
            channel.queueDeclare(
                    retryQueue,
                    topology.durable(),
                    false,
                    false,
                    topology.retryQueueArguments(retryStage)
            );
            channel.queueBind(retryQueue, retryExchange, "#");
        }

        Map<String, Object> queueArguments = topology.queueArguments();
        for (TransportRoute route : TransportRoute.values()) {
            String queueName = topology.queueName(route);
            channel.queueDeclare(queueName, topology.durable(), false, false, queueArguments);
            channel.queueBind(queueName, topology.exchangeName(), route.routingKey());
        }
    }
}
