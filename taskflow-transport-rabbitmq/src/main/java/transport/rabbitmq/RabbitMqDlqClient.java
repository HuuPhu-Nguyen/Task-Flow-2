package transport.rabbitmq;

import com.google.gson.JsonParser;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Return;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transport.InboundTransportMessage;
import transport.TransportRoute;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RabbitMqDlqClient implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqDlqClient.class);
    private static final long MANDATORY_RETURN_WAIT_MILLIS = 250L;
    private static final String HEADER_X_DEATH = "x-death";
    private static final String HEADER_REDRIVE_COUNT = "x-taskflow-redrive-count";
    private static final String HEADER_DECISION = "x-taskflow-dlq-decision";
    private static final String HEADER_DECISION_AT = "x-taskflow-dlq-decision-at";
    private static final String HEADER_ORIGINAL_ROUTING_KEY = "x-taskflow-original-routing-key";
    private static final String DECISION_REDRIVE = "redrive";
    private static final String DECISION_QUARANTINE = "quarantine";

    private final RabbitMqTransportConfig config;
    private final RabbitMqTopology topology;
    private final RabbitMqMessageCodec codec;
    private final Connection connection;
    private final Channel channel;
    private final Map<String, CompletableFuture<Return>> mandatoryReturns = new ConcurrentHashMap<>();

    public RabbitMqDlqClient(RabbitMqTransportConfig config) throws Exception {
        this(config, new RabbitMqMessageCodec(), openResources(config));
    }

    private RabbitMqDlqClient(RabbitMqTransportConfig config,
                              RabbitMqMessageCodec codec,
                              RabbitMqConnectionResources resources) throws Exception {
        this(config, codec, resources.connection(), resources.channel());
    }

    RabbitMqDlqClient(RabbitMqTransportConfig config,
                      RabbitMqMessageCodec codec,
                      Connection connection,
                      Channel channel) throws Exception {
        this.config = config;
        this.topology = new RabbitMqTopology(config);
        this.codec = codec;
        this.connection = connection;
        this.channel = channel;
        try {
            synchronized (channel) {
                this.channel.confirmSelect();
                this.channel.addReturnListener(this::handleReturnedMessage);
            }
        } catch (Exception e) {
            closeAfterStartupFailure(connection, channel, e);
            throw e;
        }
    }

    public void declareTopology() throws Exception {
        if (!topology.deadLetterEnabled()) {
            throw new IllegalStateException("RabbitMQ dead-lettering is disabled.");
        }
        synchronized (channel) {
            channel.exchangeDeclare(topology.exchangeName(), BuiltinExchangeType.DIRECT, topology.durable());
            Map<String, Object> queueArguments = topology.queueArguments();
            for (TransportRoute route : TransportRoute.values()) {
                String queueName = topology.queueName(route);
                channel.queueDeclare(queueName, topology.durable(), false, false, queueArguments);
                channel.queueBind(queueName, topology.exchangeName(), route.routingKey());
            }
            channel.exchangeDeclare(topology.deadLetterExchangeName(), BuiltinExchangeType.DIRECT, topology.durable());
            channel.queueDeclare(topology.deadLetterQueueName(), topology.durable(), false, false, null);
            channel.queueBind(
                    topology.deadLetterQueueName(),
                    topology.deadLetterExchangeName(),
                    topology.deadLetterRoutingKey()
            );
            channel.queueDeclare(topology.deadLetterQuarantineQueueName(), topology.durable(), false, false, null);
            channel.queueBind(
                    topology.deadLetterQuarantineQueueName(),
                    topology.deadLetterExchangeName(),
                    topology.deadLetterQuarantineRoutingKey()
            );
        }
        LOGGER.info("event=rabbitmq_dlq_topology_declared dlq_queue={} quarantine_queue={}",
                topology.deadLetterQueueName(),
                topology.deadLetterQuarantineQueueName());
    }

    public List<RabbitMqDlqMessage> inspect(int maxMessages) throws Exception {
        validateMaxMessages(maxMessages);
        List<PulledDlqMessage> pulled = new ArrayList<>();
        try {
            synchronized (channel) {
                for (int i = 0; i < maxMessages; i++) {
                    GetResponse response = channel.basicGet(topology.deadLetterQueueName(), false);
                    if (response == null) {
                        break;
                    }
                    pulled.add(new PulledDlqMessage(response, toMessage(response)));
                }
            }
            return pulled.stream()
                    .map(PulledDlqMessage::message)
                    .toList();
        } finally {
            requeuePulledMessages(pulled);
        }
    }

    public List<RabbitMqDlqDecisionResult> redrive(int maxMessages) throws Exception {
        validateMaxMessages(maxMessages);
        return applyDecision(maxMessages, this::redriveNext);
    }

    public RabbitMqDlqDecisionResult redriveNext() throws Exception {
        return withNextDelivery(DECISION_REDRIVE, this::redriveDelivery);
    }

    public List<RabbitMqDlqDecisionResult> quarantine(int maxMessages) throws Exception {
        validateMaxMessages(maxMessages);
        return applyDecision(maxMessages, this::quarantineNext);
    }

    public RabbitMqDlqDecisionResult quarantineNext() throws Exception {
        return withNextDelivery(DECISION_QUARANTINE, this::quarantineDelivery);
    }

    public List<RabbitMqDlqDecisionResult> discard(int maxMessages) throws Exception {
        validateMaxMessages(maxMessages);
        return applyDecision(maxMessages, this::discardNext);
    }

    public RabbitMqDlqDecisionResult discardNext() throws Exception {
        return withNextDelivery("discard", (response, message) -> {
            synchronized (channel) {
                channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
            }
            LOGGER.info("event=rabbitmq_dlq_discarded message_id={} original_routing_key={}",
                    message.messageId(),
                    message.originalRoutingKey());
            return new RabbitMqDlqDecisionResult(
                    "discard",
                    RabbitMqDlqDecisionResult.Status.DISCARDED,
                    message,
                    "DLQ message discarded"
            );
        });
    }

    @Override
    public void close() throws Exception {
        try {
            channel.close();
        } finally {
            connection.close();
        }
    }

    private RabbitMqDlqDecisionResult redriveDelivery(GetResponse response,
                                                      RabbitMqDlqMessage message) throws Exception {
        if (!message.redrivable()) {
            requeue(response);
            LOGGER.warn("event=rabbitmq_dlq_redrive_rejected status=not_redrivable message_id={} original_routing_key={} reason={}",
                    message.messageId(),
                    message.originalRoutingKey(),
                    message.nonRedrivableReason());
            return new RabbitMqDlqDecisionResult(
                    DECISION_REDRIVE,
                    RabbitMqDlqDecisionResult.Status.NOT_REDRIVABLE,
                    message,
                    message.nonRedrivableReason()
            );
        }
        String routingKey = redriveRoutingKey(message);
        AMQP.BasicProperties properties = propertiesWithDecision(response, message, DECISION_REDRIVE);
        boolean published;
        try {
            published = publishAndConfirm(config.exchangeName(), routingKey, properties, response.getBody());
        } catch (Exception publishError) {
            requeue(response);
            throw publishError;
        }
        if (!published) {
            requeue(response);
            LOGGER.warn("event=rabbitmq_dlq_redrive_deferred message_id={} original_routing_key={}",
                    message.messageId(),
                    routingKey);
            return new RabbitMqDlqDecisionResult(
                    DECISION_REDRIVE,
                    RabbitMqDlqDecisionResult.Status.PUBLISH_NOT_CONFIRMED,
                    message,
                    "Redrive publish was not confirmed or was unroutable"
            );
        }
        ack(response);
        LOGGER.info("event=rabbitmq_dlq_redriven message_id={} original_routing_key={} redrive_count={}",
                message.messageId(),
                routingKey,
                message.redriveCount() + 1);
        return new RabbitMqDlqDecisionResult(
                DECISION_REDRIVE,
                RabbitMqDlqDecisionResult.Status.REDRIVEN,
                message,
                "Redriven to " + routingKey
        );
    }

    private RabbitMqDlqDecisionResult quarantineDelivery(GetResponse response,
                                                         RabbitMqDlqMessage message) throws Exception {
        AMQP.BasicProperties properties = propertiesWithDecision(response, message, DECISION_QUARANTINE);
        boolean published;
        try {
            published = publishAndConfirm(
                    topology.deadLetterExchangeName(),
                    topology.deadLetterQuarantineRoutingKey(),
                    properties,
                    response.getBody()
            );
        } catch (Exception publishError) {
            requeue(response);
            throw publishError;
        }
        if (!published) {
            requeue(response);
            LOGGER.warn("event=rabbitmq_dlq_quarantine_deferred message_id={} quarantine_queue={}",
                    message.messageId(),
                    topology.deadLetterQuarantineQueueName());
            return new RabbitMqDlqDecisionResult(
                    DECISION_QUARANTINE,
                    RabbitMqDlqDecisionResult.Status.PUBLISH_NOT_CONFIRMED,
                    message,
                    "Quarantine publish was not confirmed or was unroutable"
            );
        }
        ack(response);
        LOGGER.info("event=rabbitmq_dlq_quarantined message_id={} quarantine_queue={}",
                message.messageId(),
                topology.deadLetterQuarantineQueueName());
        return new RabbitMqDlqDecisionResult(
                DECISION_QUARANTINE,
                RabbitMqDlqDecisionResult.Status.QUARANTINED,
                message,
                "Quarantined to " + topology.deadLetterQuarantineQueueName()
        );
    }

    private RabbitMqDlqDecisionResult withNextDelivery(String decision,
                                                       DlqDeliveryOperation operation) throws Exception {
        GetResponse response;
        synchronized (channel) {
            response = channel.basicGet(topology.deadLetterQueueName(), false);
        }
        if (response == null) {
            return new RabbitMqDlqDecisionResult(
                    decision,
                    RabbitMqDlqDecisionResult.Status.EMPTY,
                    null,
                    "DLQ is empty"
            );
        }
        RabbitMqDlqMessage message = toMessage(response);
        return operation.apply(response, message);
    }

    private List<RabbitMqDlqDecisionResult> applyDecision(int maxMessages,
                                                          DlqDecisionSupplier supplier) throws Exception {
        List<RabbitMqDlqDecisionResult> results = new ArrayList<>();
        for (int i = 0; i < maxMessages; i++) {
            RabbitMqDlqDecisionResult result = supplier.get();
            results.add(result);
            if (result.status() == RabbitMqDlqDecisionResult.Status.EMPTY) {
                break;
            }
        }
        return List.copyOf(results);
    }

    private RabbitMqDlqMessage toMessage(GetResponse response) {
        AMQP.BasicProperties properties = properties(response);
        Map<String, Object> headers = copyHeaders(properties.getHeaders());
        XDeath xDeath = firstXDeath(headers);
        String originalRoutingKey = firstNonBlank(
                firstRoutingKey(xDeath),
                stringValue(headers.get(HEADER_ORIGINAL_ROUTING_KEY))
        );
        TransportRoute routeFromRoutingKey = topology.routeForRoutingKey(originalRoutingKey);
        DecodeResult decodeResult = decode(response.getBody(), routeFromRoutingKey);
        TransportRoute inferredRoute = routeFromRoutingKey == null ? decodeResult.route() : routeFromRoutingKey;
        String nonRedrivableReason = nonRedrivableReason(originalRoutingKey, routeFromRoutingKey, decodeResult);
        return new RabbitMqDlqMessage(
                properties.getMessageId(),
                properties.getContentType(),
                xDeath == null ? null : xDeath.exchange(),
                originalRoutingKey,
                xDeath == null ? topology.deadLetterQueueName() : xDeath.queue(),
                xDeath == null ? "" : xDeath.reason(),
                xDeath == null ? 0L : xDeath.count(),
                xDeath == null ? null : xDeath.firstTime(),
                xDeath == null ? null : xDeath.lastTime(),
                redriveCount(headers),
                inferredRoute,
                nonRedrivableReason == null,
                nonRedrivableReason,
                response.getBody(),
                headers
        );
    }

    private DecodeResult decode(byte[] body, TransportRoute routeFromRoutingKey) {
        TransportRoute explicitRoute = explicitRoute(body);
        TransportRoute fallbackRoute = routeFromRoutingKey == null
                ? (explicitRoute == null ? TransportRoute.HEARTBEAT : explicitRoute)
                : routeFromRoutingKey;
        try {
            InboundTransportMessage decoded = codec.decode(body, fallbackRoute, null);
            return new DecodeResult(decoded.route(), explicitRoute != null, null);
        } catch (Exception e) {
            return new DecodeResult(null, explicitRoute != null, e.getMessage());
        }
    }

    private TransportRoute explicitRoute(byte[] body) {
        try {
            var root = JsonParser.parseString(new String(body, java.nio.charset.StandardCharsets.UTF_8))
                    .getAsJsonObject();
            if (!root.has("route")) {
                return null;
            }
            return TransportRoute.valueOf(root.get("route").getAsString());
        } catch (Exception ignored) {
            return null;
        }
    }

    private String nonRedrivableReason(String originalRoutingKey,
                                       TransportRoute routeFromRoutingKey,
                                       DecodeResult decodeResult) {
        if (decodeResult.error() != null) {
            return "Message body is not a valid TaskFlow broker envelope: " + decodeResult.error();
        }
        if (originalRoutingKey != null && routeFromRoutingKey == null) {
            return "Original routing key is not a TaskFlow route: " + originalRoutingKey;
        }
        if (routeFromRoutingKey == null && !decodeResult.explicitRoute()) {
            return "No original routing key or explicit envelope route could be inferred for redrive";
        }
        if (routeFromRoutingKey != null && decodeResult.route() != routeFromRoutingKey) {
            return "Envelope route " + decodeResult.route() + " does not match original routing key "
                    + originalRoutingKey;
        }
        if (routeFromRoutingKey == null && decodeResult.route() == null) {
            return "No TaskFlow route could be inferred for redrive";
        }
        return null;
    }

    private String redriveRoutingKey(RabbitMqDlqMessage message) {
        if (message.originalRoutingKey() != null && !message.originalRoutingKey().isBlank()) {
            return message.originalRoutingKey();
        }
        return message.inferredRoute().routingKey();
    }

    private AMQP.BasicProperties propertiesWithDecision(GetResponse response,
                                                        RabbitMqDlqMessage message,
                                                        String decision) {
        AMQP.BasicProperties properties = properties(response);
        Map<String, Object> headers = copyHeaders(properties.getHeaders());
        headers.put(HEADER_DECISION, decision);
        headers.put(HEADER_DECISION_AT, Instant.now().toString());
        if (message.originalRoutingKey() != null && !message.originalRoutingKey().isBlank()) {
            headers.put(HEADER_ORIGINAL_ROUTING_KEY, message.originalRoutingKey());
        }
        if (DECISION_REDRIVE.equals(decision)) {
            headers.put(HEADER_REDRIVE_COUNT, message.redriveCount() + 1);
        }
        String messageId = properties.getMessageId();
        if (messageId == null || messageId.isBlank()) {
            messageId = UUID.randomUUID().toString();
        }
        return properties.builder()
                .messageId(messageId)
                .headers(headers)
                .build();
    }

    private boolean publishAndConfirm(String exchange,
                                      String routingKey,
                                      AMQP.BasicProperties properties,
                                      byte[] body) throws Exception {
        String messageId = properties.getMessageId();
        CompletableFuture<Return> returned = new CompletableFuture<>();
        mandatoryReturns.put(messageId, returned);
        try {
            synchronized (channel) {
                channel.basicPublish(exchange, routingKey, true, properties, body);
                if (!waitForPublisherConfirm(exchange, routingKey)) {
                    return false;
                }
            }
            try {
                Return returnedMessage = returned.get(MANDATORY_RETURN_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                LOGGER.warn("event=rabbitmq_dlq_publish_unroutable exchange={} routing_key={} reply_code={} reply_text={}",
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

    private boolean waitForPublisherConfirm(String exchange, String routingKey) throws IOException {
        try {
            boolean confirmed = channel.waitForConfirms(config.publisherConfirmTimeoutMillis());
            if (!confirmed) {
                LOGGER.warn("event=rabbitmq_dlq_publish_not_confirmed exchange={} routing_key={} timeout_ms={}",
                        exchange,
                        routingKey,
                        config.publisherConfirmTimeoutMillis());
            }
            return confirmed;
        } catch (TimeoutException e) {
            LOGGER.warn("event=rabbitmq_dlq_publish_confirm_timeout exchange={} routing_key={} timeout_ms={}",
                    exchange,
                    routingKey,
                    config.publisherConfirmTimeoutMillis());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for RabbitMQ DLQ publisher confirm", e);
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
        LOGGER.warn("event=rabbitmq_dlq_publish_returned_unmatched routing_key={} reply_code={} reply_text={}",
                returned.getRoutingKey(),
                returned.getReplyCode(),
                returned.getReplyText());
    }

    private void requeuePulledMessages(List<PulledDlqMessage> pulled) throws Exception {
        synchronized (channel) {
            for (int i = pulled.size() - 1; i >= 0; i--) {
                long deliveryTag = pulled.get(i).response().getEnvelope().getDeliveryTag();
                channel.basicNack(deliveryTag, false, true);
            }
        }
    }

    private void ack(GetResponse response) throws IOException {
        synchronized (channel) {
            channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
        }
    }

    private void requeue(GetResponse response) throws IOException {
        synchronized (channel) {
            channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
        }
    }

    private XDeath firstXDeath(Map<String, Object> headers) {
        Object raw = headers.get(HEADER_X_DEATH);
        if (!(raw instanceof List<?> deaths) || deaths.isEmpty()) {
            return null;
        }
        Object first = deaths.get(0);
        if (!(first instanceof Map<?, ?> fields)) {
            return null;
        }
        Map<String, Object> stringFields = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : fields.entrySet()) {
            if (entry.getKey() != null) {
                stringFields.put(entry.getKey().toString(), entry.getValue());
            }
        }
        return new XDeath(
                stringValue(stringFields.get("exchange")),
                stringValue(stringFields.get("queue")),
                stringValue(stringFields.get("reason")),
                longValue(stringFields.get("count")),
                instantValue(firstNonNull(stringFields.get("first-time"), stringFields.get("time"))),
                instantValue(firstNonNull(stringFields.get("last-time"), stringFields.get("time"))),
                routingKeys(stringFields.get("routing-keys"))
        );
    }

    private String firstRoutingKey(XDeath xDeath) {
        if (xDeath == null || xDeath.routingKeys().isEmpty()) {
            return null;
        }
        return firstNonBlank(xDeath.routingKeys().toArray(String[]::new));
    }

    private List<String> routingKeys(Object value) {
        if (!(value instanceof List<?> rawValues)) {
            String single = stringValue(value);
            return single == null ? List.of() : List.of(single);
        }
        List<String> routingKeys = new ArrayList<>();
        for (Object rawValue : rawValues) {
            String routingKey = stringValue(rawValue);
            if (routingKey != null && !routingKey.isBlank()) {
                routingKeys.add(routingKey);
            }
        }
        return List.copyOf(routingKeys);
    }

    private int redriveCount(Map<String, Object> headers) {
        Object value = headers.get(HEADER_REDRIVE_COUNT);
        if (value instanceof Number number) {
            return number.intValue();
        }
        String text = stringValue(value);
        if (text == null || text.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private AMQP.BasicProperties properties(GetResponse response) {
        return response.getProps() == null ? new AMQP.BasicProperties() : response.getProps();
    }

    private Map<String, Object> copyHeaders(Map<String, Object> headers) {
        if (headers == null || headers.isEmpty()) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                copy.put(entry.getKey(), entry.getValue());
            }
        }
        return copy;
    }

    private static RabbitMqConnectionResources openResources(RabbitMqTransportConfig config) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());
        factory.setVirtualHost(config.virtualHost());
        factory.setAutomaticRecoveryEnabled(true);
        Connection connection = factory.newConnection("taskflow-rabbitmq-dlq");
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

    private static void validateMaxMessages(int maxMessages) {
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("maxMessages must be positive");
        }
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static Object firstNonNull(Object... values) {
        if (values == null) {
            return null;
        }
        for (Object value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    private static long longValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        String text = stringValue(value);
        if (text == null || text.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ignored) {
            return 0L;
        }
    }

    private static Instant instantValue(Object value) {
        if (value instanceof Date date) {
            return date.toInstant();
        }
        String text = stringValue(value);
        if (text == null || text.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(text);
        } catch (Exception ignored) {
            return null;
        }
    }

    private record RabbitMqConnectionResources(Connection connection, Channel channel) {
    }

    private record PulledDlqMessage(GetResponse response, RabbitMqDlqMessage message) {
    }

    private record DecodeResult(TransportRoute route, boolean explicitRoute, String error) {
    }

    private record XDeath(
            String exchange,
            String queue,
            String reason,
            long count,
            Instant firstTime,
            Instant lastTime,
            List<String> routingKeys
    ) {
    }

    @FunctionalInterface
    private interface DlqDeliveryOperation {
        RabbitMqDlqDecisionResult apply(GetResponse response, RabbitMqDlqMessage message) throws Exception;
    }

    @FunctionalInterface
    private interface DlqDecisionSupplier {
        RabbitMqDlqDecisionResult get() throws Exception;
    }
}
