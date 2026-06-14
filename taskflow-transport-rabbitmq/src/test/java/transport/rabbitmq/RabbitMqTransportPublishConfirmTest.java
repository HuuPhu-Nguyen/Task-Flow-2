package transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ReturnCallback;
import org.junit.jupiter.api.Test;
import protocol.PingMessage;
import transport.OutboundTransportMessage;
import transport.TransportRoute;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqTransportPublishConfirmTest {

    @Test
    void enablesPublisherConfirmModeOnStartup() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();

        try (RabbitMqTransport ignored = transport(fakeChannel)) {
            assertTrue(fakeChannel.confirmSelected);
            assertEquals(3, fakeChannel.prefetchCount);
        }
    }

    @Test
    void startupFailureClosesPartiallyOpenedRabbitMqResources() {
        FakeChannel fakeChannel = new FakeChannel();
        fakeChannel.failConfirmSelect = true;
        Channel channel = fakeChannel.proxy();
        FakeConnection fakeConnection = new FakeConnection(channel);

        IOException error = assertThrows(IOException.class, () -> new RabbitMqTransport(
                config(),
                new RabbitMqMessageCodec(),
                fakeConnection.proxy(),
                channel
        ));

        assertEquals("confirm-select failed", error.getMessage());
        assertEquals(1, fakeChannel.closeCount);
        assertEquals(1, fakeConnection.closeCount);
    }

    @Test
    void publishReturnsTrueAfterBrokerConfirm() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();

        try (RabbitMqTransport transport = transport(fakeChannel)) {
            assertTrue(transport.publish(message(TransportRoute.HEARTBEAT)));
        }

        assertEquals(TransportRoute.HEARTBEAT.routingKey(), fakeChannel.routingKey);
        assertFalse(fakeChannel.mandatory);
        assertEquals(25L, fakeChannel.confirmTimeoutMillis);
        assertNotNull(fakeChannel.body);
        assertTrue(new String(fakeChannel.body, StandardCharsets.UTF_8).contains("\"route\":\"HEARTBEAT\""));
    }

    @Test
    void publishReturnsFalseWhenBrokerDoesNotConfirm() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        fakeChannel.confirmResult = false;

        try (RabbitMqTransport transport = transport(fakeChannel)) {
            assertFalse(transport.publish(message(TransportRoute.HEARTBEAT)));
        }
    }

    @Test
    void publishReturnsFalseWhenConfirmTimesOut() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        fakeChannel.confirmTimeout = true;

        try (RabbitMqTransport transport = transport(fakeChannel)) {
            assertFalse(transport.publish(message(TransportRoute.HEARTBEAT)));
        }
    }

    @Test
    void publishThrowsWhenBrokerChannelFailsDuringPublish() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        fakeChannel.failPublish = true;

        try (RabbitMqTransport transport = transport(fakeChannel)) {
            IOException error = assertThrows(IOException.class,
                    () -> transport.publish(message(TransportRoute.HEARTBEAT)));

            assertEquals("broker publish failed", error.getMessage());
        }
    }

    @Test
    void peerPublishReturnsFalseWhenMandatoryMessageIsReturned() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        fakeChannel.returnMandatoryPublish = true;

        try (RabbitMqTransport transport = transport(fakeChannel)) {
            assertFalse(transport.publishToPeer(
                    TransportRoute.JOB_RESULT,
                    "peer-1",
                    message(TransportRoute.JOB_RESULT)
            ));
        }

        assertTrue(fakeChannel.mandatory);
        assertEquals("jobs.result.peer-1", fakeChannel.routingKey);
    }

    private static RabbitMqTransport transport(FakeChannel fakeChannel) throws Exception {
        Channel channel = fakeChannel.proxy();
        return new RabbitMqTransport(
                config(),
                new RabbitMqMessageCodec(),
                connection(channel),
                channel
        );
    }

    private static RabbitMqTransportConfig config() {
        RabbitMqTransportConfig defaults = RabbitMqTransportConfig.localDefaults();
        return new RabbitMqTransportConfig(
                defaults.host(),
                defaults.port(),
                defaults.username(),
                defaults.password(),
                defaults.virtualHost(),
                defaults.exchangeName(),
                defaults.queuePrefix(),
                defaults.durable(),
                defaults.prefetchCount(),
                25L,
                defaults.deadLetterEnabled(),
                defaults.deadLetterExchangeName(),
                defaults.deadLetterQueueName(),
                defaults.deadLetterRoutingKey(),
                defaults.requeueOnHandlerFailure()
        );
    }

    private static Connection connection(Channel channel) {
        return new FakeConnection(channel).proxy();
    }

    private static OutboundTransportMessage message(TransportRoute route) {
        return new OutboundTransportMessage(route, "peer-1", new PingMessage("peer-1", "now"));
    }

    private static Object defaultValue(Class<?> returnType) {
        if (returnType == void.class) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0F;
        }
        if (returnType == double.class) {
            return 0D;
        }
        return null;
    }

    private static final class FakeChannel implements InvocationHandler {
        private boolean confirmSelected;
        private int prefetchCount;
        private boolean confirmResult = true;
        private boolean confirmTimeout;
        private boolean returnMandatoryPublish;
        private boolean failConfirmSelect;
        private boolean failPublish;
        private String routingKey;
        private boolean mandatory;
        private byte[] body;
        private long confirmTimeoutMillis;
        private int closeCount;
        private ReturnCallback returnCallback;

        private Channel proxy() {
            return (Channel) Proxy.newProxyInstance(
                    Channel.class.getClassLoader(),
                    new Class<?>[] { Channel.class },
                    this
            );
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return switch (method.getName()) {
                case "basicQos" -> {
                    prefetchCount = (Integer) args[0];
                    yield null;
                }
                case "confirmSelect" -> {
                    if (failConfirmSelect) {
                        throw new IOException("confirm-select failed");
                    }
                    confirmSelected = true;
                    yield null;
                }
                case "addReturnListener" -> {
                    if (args[0] instanceof ReturnCallback callback) {
                        returnCallback = callback;
                    }
                    yield null;
                }
                case "basicPublish" -> {
                    routingKey = (String) args[1];
                    mandatory = (Boolean) args[2];
                    AMQP.BasicProperties properties = (AMQP.BasicProperties) args[3];
                    body = (byte[]) args[4];
                    if (failPublish) {
                        throw new IOException("broker publish failed");
                    }
                    if (returnMandatoryPublish && returnCallback != null) {
                        returnCallback.handle(new Return(
                                312,
                                "NO_ROUTE",
                                (String) args[0],
                                routingKey,
                                properties,
                                body
                        ));
                    }
                    yield null;
                }
                case "waitForConfirms" -> {
                    confirmTimeoutMillis = (Long) args[0];
                    if (confirmTimeout) {
                        throw new TimeoutException("confirm timed out");
                    }
                    yield confirmResult;
                }
                case "close" -> {
                    closeCount++;
                    yield null;
                }
                case "isOpen" -> true;
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class FakeConnection implements InvocationHandler {
        private final Channel channel;
        private int closeCount;

        private FakeConnection(Channel channel) {
            this.channel = channel;
        }

        private Connection proxy() {
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[] { Connection.class },
                    this
            );
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            return switch (method.getName()) {
                case "createChannel" -> channel;
                case "close" -> {
                    closeCount++;
                    yield null;
                }
                case "isOpen" -> true;
                default -> defaultValue(method.getReturnType());
            };
        }
    }
}
