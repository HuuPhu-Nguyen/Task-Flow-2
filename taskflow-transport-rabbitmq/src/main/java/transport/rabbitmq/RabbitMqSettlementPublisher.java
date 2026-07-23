package transport.rabbitmq;

import com.rabbitmq.client.AMQP;

@FunctionalInterface
interface RabbitMqSettlementPublisher {
    boolean publish(String exchange,
                    String routingKey,
                    AMQP.BasicProperties properties,
                    byte[] body) throws Exception;
}
