package transport.rabbitmq;

import java.util.LinkedHashMap;
import java.util.Map;

final class RabbitMqRetryHeaders {
    static final String DELIVERY_ATTEMPT = "x-taskflow-delivery-attempt";
    static final String ORIGINAL_ROUTING_KEY = "x-taskflow-original-routing-key";
    static final String ORIGINAL_EXCHANGE = "x-taskflow-original-exchange";
    static final String ORIGINAL_MESSAGE_ID = "x-taskflow-original-message-id";
    static final String FAILURE_REASON = "x-taskflow-failure-reason";
    static final String FIRST_FAILURE_REASON = "x-taskflow-first-failure-reason";
    static final String FAILURE_DISPOSITION = "x-taskflow-failure-disposition";
    static final String RETRY_DELAY_MILLIS = "x-taskflow-retry-delay-ms";
    static final String RETRY_SCHEDULED_AT = "x-taskflow-retry-scheduled-at";
    static final String QUARANTINED_AT = "x-taskflow-quarantined-at";
    static final String RETRY_EXHAUSTED = "x-taskflow-retry-exhausted";

    private RabbitMqRetryHeaders() {
    }

    static int deliveryAttempt(Map<String, Object> headers) {
        if (headers == null) {
            return 1;
        }
        Object raw = headers.get(DELIVERY_ATTEMPT);
        if (raw instanceof Number number) {
            return positiveOrFirst(number.intValue());
        }
        if (raw != null) {
            try {
                return positiveOrFirst(Integer.parseInt(raw.toString()));
            } catch (NumberFormatException ignored) {
                return 1;
            }
        }
        return 1;
    }

    static String stringValue(Map<String, Object> headers, String name) {
        if (headers == null) {
            return null;
        }
        Object raw = headers.get(name);
        return raw == null ? null : raw.toString();
    }

    static Map<String, Object> copy(Map<String, Object> headers) {
        Map<String, Object> copy = new LinkedHashMap<>();
        if (headers == null) {
            return copy;
        }
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                copy.put(entry.getKey(), entry.getValue());
            }
        }
        return copy;
    }

    private static int positiveOrFirst(int value) {
        return value <= 0 ? 1 : value;
    }
}
