package transport.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transport.DeliveryDisposition;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

final class RabbitMqDeliveryRetry {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqDeliveryRetry.class);

    private final RabbitMqTransportConfig config;
    private final RabbitMqTopology topology;
    private final Delivery delivery;
    private final RabbitMqSettlementPublisher publisher;
    private final RabbitMqTransportMetrics metrics;

    RabbitMqDeliveryRetry(RabbitMqTransportConfig config,
                          RabbitMqTopology topology,
                          Delivery delivery,
                          RabbitMqSettlementPublisher publisher) {
        this(config, topology, delivery, publisher, new RabbitMqTransportMetrics());
    }

    RabbitMqDeliveryRetry(RabbitMqTransportConfig config,
                          RabbitMqTopology topology,
                          Delivery delivery,
                          RabbitMqSettlementPublisher publisher,
                          RabbitMqTransportMetrics metrics) {
        this.config = config;
        this.topology = topology;
        this.delivery = delivery;
        this.publisher = publisher;
        this.metrics = metrics;
    }

    int deliveryAttempt() {
        return RabbitMqRetryHeaders.deliveryAttempt(properties().getHeaders());
    }

    boolean publishRetryOrQuarantine(DeliveryDisposition disposition,
                                     String reasonCode) throws Exception {
        int deliveryAttempt = deliveryAttempt();
        if (deliveryAttempt < config.maxDeliveryAttempts()) {
            return publishRetry(deliveryAttempt, disposition, normalizeReason(reasonCode, disposition));
        }
        return publishQuarantine(deliveryAttempt, disposition, normalizeReason(reasonCode, disposition));
    }

    private boolean publishRetry(int deliveryAttempt,
                                 DeliveryDisposition disposition,
                                 String reasonCode) throws Exception {
        int retryStage = deliveryAttempt;
        long delayMillis = config.retryDelaysMillis().get(retryStage - 1);
        Map<String, Object> headers = failureHeaders(disposition, reasonCode);
        headers.put(RabbitMqRetryHeaders.DELIVERY_ATTEMPT, deliveryAttempt + 1);
        headers.put(RabbitMqRetryHeaders.RETRY_DELAY_MILLIS, delayMillis);
        headers.put(RabbitMqRetryHeaders.RETRY_SCHEDULED_AT, Instant.now().toString());
        headers.remove(RabbitMqRetryHeaders.QUARANTINED_AT);
        headers.remove(RabbitMqRetryHeaders.RETRY_EXHAUSTED);

        String routingKey = originalRoutingKey(headers);
        boolean published = publisher.publish(
                topology.retryExchangeName(retryStage),
                routingKey,
                republishedProperties(headers),
                delivery.getBody()
        );
        if (published) {
            LOGGER.warn("event=rabbitmq_delivery_retry_scheduled routing_key={} reason_code={} disposition={} delivery_attempt={} next_delivery_attempt={} max_delivery_attempts={} delay_ms={} retry_queue={}",
                    routingKey,
                    reasonCode,
                    disposition,
                    deliveryAttempt,
                    deliveryAttempt + 1,
                    config.maxDeliveryAttempts(),
                    delayMillis,
                    topology.retryQueueName(retryStage));
        } else {
            LOGGER.error("event=rabbitmq_delivery_retry_publish_failed routing_key={} reason_code={} disposition={} delivery_attempt={} max_delivery_attempts={} retry_queue={}",
                    routingKey,
                    reasonCode,
                    disposition,
                    deliveryAttempt,
                    config.maxDeliveryAttempts(),
                    topology.retryQueueName(retryStage));
        }
        return published;
    }

    private boolean publishQuarantine(int deliveryAttempt,
                                      DeliveryDisposition disposition,
                                      String reasonCode) throws Exception {
        Map<String, Object> headers = failureHeaders(disposition, reasonCode);
        headers.put(RabbitMqRetryHeaders.DELIVERY_ATTEMPT, deliveryAttempt);
        headers.put(RabbitMqRetryHeaders.QUARANTINED_AT, Instant.now().toString());
        headers.put(RabbitMqRetryHeaders.RETRY_EXHAUSTED, true);

        String routingKey = originalRoutingKey(headers);
        boolean published = publisher.publish(
                topology.quarantineExchangeName(),
                topology.deadLetterQuarantineRoutingKey(),
                republishedProperties(headers),
                delivery.getBody()
        );
        if (published) {
            metrics.recordQuarantined();
            LOGGER.error("event=rabbitmq_delivery_quarantined routing_key={} reason_code={} disposition={} delivery_attempt={} max_delivery_attempts={} quarantine_queue={}",
                    routingKey,
                    reasonCode,
                    disposition,
                    deliveryAttempt,
                    config.maxDeliveryAttempts(),
                    topology.deadLetterQuarantineQueueName());
        } else {
            LOGGER.error("event=rabbitmq_delivery_quarantine_publish_failed routing_key={} reason_code={} disposition={} delivery_attempt={} max_delivery_attempts={} quarantine_queue={}",
                    routingKey,
                    reasonCode,
                    disposition,
                    deliveryAttempt,
                    config.maxDeliveryAttempts(),
                    topology.deadLetterQuarantineQueueName());
        }
        return published;
    }

    private Map<String, Object> failureHeaders(DeliveryDisposition disposition, String reasonCode) {
        Map<String, Object> headers = RabbitMqRetryHeaders.copy(properties().getHeaders());
        headers.put(RabbitMqRetryHeaders.ORIGINAL_ROUTING_KEY, delivery.getEnvelope().getRoutingKey());
        headers.put(RabbitMqRetryHeaders.ORIGINAL_EXCHANGE, delivery.getEnvelope().getExchange());
        String messageId = properties().getMessageId();
        if (messageId != null && !messageId.isBlank()) {
            headers.putIfAbsent(RabbitMqRetryHeaders.ORIGINAL_MESSAGE_ID, messageId);
        }
        headers.putIfAbsent(RabbitMqRetryHeaders.FIRST_FAILURE_REASON, reasonCode);
        headers.put(RabbitMqRetryHeaders.FAILURE_REASON, reasonCode);
        headers.put(RabbitMqRetryHeaders.FAILURE_DISPOSITION, disposition.name());
        return headers;
    }

    private AMQP.BasicProperties republishedProperties(Map<String, Object> headers) {
        return properties().builder()
                .messageId(UUID.randomUUID().toString())
                .headers(headers)
                .expiration(null)
                .build();
    }

    private String originalRoutingKey(Map<String, Object> headers) {
        String original = RabbitMqRetryHeaders.stringValue(
                headers,
                RabbitMqRetryHeaders.ORIGINAL_ROUTING_KEY
        );
        if (original == null || original.isBlank()) {
            return delivery.getEnvelope().getRoutingKey();
        }
        return original;
    }

    private AMQP.BasicProperties properties() {
        return delivery.getProperties() == null
                ? new AMQP.BasicProperties()
                : delivery.getProperties();
    }

    private static String normalizeReason(String reasonCode, DeliveryDisposition disposition) {
        if (reasonCode == null || reasonCode.isBlank()) {
            return disposition.name().toLowerCase(java.util.Locale.ROOT);
        }
        return reasonCode;
    }
}
