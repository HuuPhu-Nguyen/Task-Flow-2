package transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ReturnCallback;
import org.junit.jupiter.api.Test;
import protocol.PongMessage;
import transport.OutboundTransportMessage;
import transport.TransportRoute;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqDlqClientTest {
    @Test
    void declareTopologyIncludesDeadLetterQuarantineQueue() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();

        try (RabbitMqDlqClient client = client(fakeChannel)) {
            client.declareTopology();
        }

        assertTrue(fakeChannel.declaredQueues.contains("taskflow.dead-letter"));
        assertTrue(fakeChannel.declaredQueues.contains("taskflow.dead-letter.quarantine"));
        assertTrue(fakeChannel.bindings.contains("taskflow.dead-letter.quarantine|taskflow.dead-letter.exchange|dead-letter.quarantine"));
    }

    @Test
    void inspectReportsDeadLetterMetadataAndRequeuesDelivery() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        byte[] body = heartbeatBody();
        fakeChannel.deliveries.add(response(7L, body, properties("msg-1", "heartbeats", 2L, Map.of())));

        List<RabbitMqDlqMessage> messages;
        try (RabbitMqDlqClient client = client(fakeChannel)) {
            messages = client.inspect(1);
        }

        assertEquals(1, messages.size());
        RabbitMqDlqMessage message = messages.getFirst();
        assertEquals("msg-1", message.messageId());
        assertEquals("taskflow.exchange", message.originalExchange());
        assertEquals("heartbeats", message.originalRoutingKey());
        assertEquals("taskflow.heartbeats", message.deadLetterQueue());
        assertEquals("rejected", message.deadLetterReason());
        assertEquals(2L, message.deadLetterCount());
        assertEquals(TransportRoute.HEARTBEAT, message.inferredRoute());
        assertTrue(message.redrivable());
        assertArrayEquals(body, message.body());
        assertEquals(List.of(7L), fakeChannel.nackedTags);
        assertEquals(List.of(true), fakeChannel.nackRequeueFlags);
        assertTrue(fakeChannel.ackedTags.isEmpty());
    }

    @Test
    void redrivePublishesToOriginalRouteAndAcknowledgesDlqEntryAfterConfirm() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        byte[] body = heartbeatBody();
        fakeChannel.deliveries.add(response(
                8L,
                body,
                properties("msg-redrive", "heartbeats", 1L, Map.of("x-taskflow-redrive-count", 1))
        ));

        RabbitMqDlqDecisionResult result;
        try (RabbitMqDlqClient client = client(fakeChannel)) {
            result = client.redriveNext();
        }

        assertEquals(RabbitMqDlqDecisionResult.Status.REDRIVEN, result.status());
        assertEquals("taskflow.exchange", fakeChannel.publishedExchange);
        assertEquals("heartbeats", fakeChannel.publishedRoutingKey);
        assertTrue(fakeChannel.publishedMandatory);
        assertArrayEquals(body, fakeChannel.publishedBody);
        assertEquals(List.of(8L), fakeChannel.ackedTags);
        assertTrue(fakeChannel.nackedTags.isEmpty());
        assertEquals(25L, fakeChannel.confirmTimeoutMillis);
        assertEquals("redrive", fakeChannel.publishedProperties.getHeaders().get("x-taskflow-dlq-decision"));
        assertEquals(2, fakeChannel.publishedProperties.getHeaders().get("x-taskflow-redrive-count"));
        assertEquals("heartbeats", fakeChannel.publishedProperties.getHeaders().get("x-taskflow-original-routing-key"));
    }

    @Test
    void redriveRequeuesMessageWhenPublishIsReturnedUnroutable() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        fakeChannel.returnMandatoryPublish = true;
        fakeChannel.deliveries.add(response(9L, heartbeatBody(), properties("msg-unroutable", "heartbeats", 1L, Map.of())));

        RabbitMqDlqDecisionResult result;
        try (RabbitMqDlqClient client = client(fakeChannel)) {
            result = client.redriveNext();
        }

        assertEquals(RabbitMqDlqDecisionResult.Status.PUBLISH_NOT_CONFIRMED, result.status());
        assertTrue(fakeChannel.ackedTags.isEmpty());
        assertEquals(List.of(9L), fakeChannel.nackedTags);
        assertEquals(List.of(true), fakeChannel.nackRequeueFlags);
    }

    @Test
    void redriveLeavesMalformedPoisonMessageInDlq() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        byte[] body = "{not valid json".getBytes(StandardCharsets.UTF_8);
        fakeChannel.deliveries.add(response(10L, body, properties("msg-poison", "heartbeats", 1L, Map.of())));

        RabbitMqDlqDecisionResult result;
        try (RabbitMqDlqClient client = client(fakeChannel)) {
            result = client.redriveNext();
        }

        assertEquals(RabbitMqDlqDecisionResult.Status.NOT_REDRIVABLE, result.status());
        assertNotNull(result.message());
        assertFalse(result.message().redrivable());
        assertTrue(result.message().nonRedrivableReason().contains("not a valid TaskFlow broker envelope"));
        assertTrue(fakeChannel.publishedBody == null);
        assertTrue(fakeChannel.ackedTags.isEmpty());
        assertEquals(List.of(10L), fakeChannel.nackedTags);
        assertEquals(List.of(true), fakeChannel.nackRequeueFlags);
    }

    @Test
    void quarantinePublishesToQuarantineQueueAndAcknowledgesDlqEntry() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        byte[] body = "{not valid json".getBytes(StandardCharsets.UTF_8);
        fakeChannel.deliveries.add(response(11L, body, properties("msg-quarantine", "heartbeats", 1L, Map.of())));

        RabbitMqDlqDecisionResult result;
        try (RabbitMqDlqClient client = client(fakeChannel)) {
            result = client.quarantineNext();
        }

        assertEquals(RabbitMqDlqDecisionResult.Status.QUARANTINED, result.status());
        assertEquals("taskflow.dead-letter.exchange", fakeChannel.publishedExchange);
        assertEquals("dead-letter.quarantine", fakeChannel.publishedRoutingKey);
        assertTrue(fakeChannel.publishedMandatory);
        assertArrayEquals(body, fakeChannel.publishedBody);
        assertEquals("quarantine", fakeChannel.publishedProperties.getHeaders().get("x-taskflow-dlq-decision"));
        assertEquals(List.of(11L), fakeChannel.ackedTags);
    }

    @Test
    void discardAcknowledgesWithoutPublishing() throws Exception {
        FakeChannel fakeChannel = new FakeChannel();
        fakeChannel.deliveries.add(response(12L, heartbeatBody(), properties("msg-discard", "heartbeats", 1L, Map.of())));

        RabbitMqDlqDecisionResult result;
        try (RabbitMqDlqClient client = client(fakeChannel)) {
            result = client.discardNext();
        }

        assertEquals(RabbitMqDlqDecisionResult.Status.DISCARDED, result.status());
        assertEquals(List.of(12L), fakeChannel.ackedTags);
        assertTrue(fakeChannel.nackedTags.isEmpty());
        assertTrue(fakeChannel.publishedBody == null);
    }

    @Test
    void rejectsInvalidInspectionCount() throws Exception {
        try (RabbitMqDlqClient client = client(new FakeChannel())) {
            assertThrows(IllegalArgumentException.class, () -> client.inspect(0));
        }
    }

    private static RabbitMqDlqClient client(FakeChannel fakeChannel) throws Exception {
        Channel channel = fakeChannel.proxy();
        return new RabbitMqDlqClient(
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

    private static byte[] heartbeatBody() {
        return new RabbitMqMessageCodec().encode(new OutboundTransportMessage(
                TransportRoute.HEARTBEAT,
                "peer-1",
                new PongMessage("peer-1", Instant.EPOCH.toString(), List.of("TEXT_ANALYSIS"))
        ));
    }

    private static AMQP.BasicProperties properties(String messageId,
                                                   String routingKey,
                                                   long count,
                                                   Map<String, Object> extraHeaders) {
        Map<String, Object> death = new LinkedHashMap<>();
        death.put("exchange", "taskflow.exchange");
        death.put("queue", "taskflow.heartbeats");
        death.put("reason", "rejected");
        death.put("count", count);
        death.put("time", Date.from(Instant.EPOCH));
        death.put("routing-keys", List.of(routingKey));

        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("x-death", List.of(death));
        headers.putAll(extraHeaders);

        return new AMQP.BasicProperties.Builder()
                .messageId(messageId)
                .contentType("application/json")
                .headers(headers)
                .build();
    }

    private static GetResponse response(long deliveryTag,
                                        byte[] body,
                                        AMQP.BasicProperties properties) {
        return new GetResponse(
                new Envelope(deliveryTag, false, "taskflow.dead-letter.exchange", "dead-letter"),
                properties,
                body,
                0
        );
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
        private final Queue<GetResponse> deliveries = new ArrayDeque<>();
        private final List<String> declaredQueues = new ArrayList<>();
        private final List<String> bindings = new ArrayList<>();
        private final List<Long> ackedTags = new ArrayList<>();
        private final List<Long> nackedTags = new ArrayList<>();
        private final List<Boolean> nackRequeueFlags = new ArrayList<>();
        private boolean confirmResult = true;
        private boolean confirmTimeout;
        private boolean returnMandatoryPublish;
        private String publishedExchange;
        private String publishedRoutingKey;
        private boolean publishedMandatory;
        private AMQP.BasicProperties publishedProperties;
        private byte[] publishedBody;
        private long confirmTimeoutMillis;
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
                case "confirmSelect" -> null;
                case "addReturnListener" -> {
                    if (args[0] instanceof ReturnCallback callback) {
                        returnCallback = callback;
                    }
                    yield null;
                }
                case "exchangeDeclare" -> null;
                case "queueDeclare" -> {
                    declaredQueues.add((String) args[0]);
                    yield null;
                }
                case "queueBind" -> {
                    bindings.add(args[0] + "|" + args[1] + "|" + args[2]);
                    yield null;
                }
                case "basicGet" -> deliveries.poll();
                case "basicAck" -> {
                    ackedTags.add((Long) args[0]);
                    yield null;
                }
                case "basicNack" -> {
                    nackedTags.add((Long) args[0]);
                    nackRequeueFlags.add((Boolean) args[2]);
                    yield null;
                }
                case "basicPublish" -> {
                    publishedExchange = (String) args[0];
                    publishedRoutingKey = (String) args[1];
                    publishedMandatory = (Boolean) args[2];
                    publishedProperties = (AMQP.BasicProperties) args[3];
                    publishedBody = (byte[]) args[4];
                    if (returnMandatoryPublish && returnCallback != null) {
                        returnCallback.handle(new Return(
                                312,
                                "NO_ROUTE",
                                publishedExchange,
                                publishedRoutingKey,
                                publishedProperties,
                                publishedBody
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
                case "close" -> null;
                case "isOpen" -> true;
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class FakeConnection implements InvocationHandler {
        private final Channel channel;

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
                case "close" -> null;
                case "isOpen" -> true;
                default -> defaultValue(method.getReturnType());
            };
        }
    }
}
