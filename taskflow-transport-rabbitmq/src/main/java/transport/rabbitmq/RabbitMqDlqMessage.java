package transport.rabbitmq;

import transport.TransportRoute;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public record RabbitMqDlqMessage(
        String messageId,
        String contentType,
        String originalExchange,
        String originalRoutingKey,
        String deadLetterQueue,
        String deadLetterReason,
        long deadLetterCount,
        Instant firstDeadLetteredAt,
        Instant lastDeadLetteredAt,
        int redriveCount,
        TransportRoute inferredRoute,
        boolean redrivable,
        String nonRedrivableReason,
        byte[] body,
        Map<String, Object> headers
) {
    public RabbitMqDlqMessage {
        body = body == null ? new byte[0] : body.clone();
        headers = copyHeaders(headers);
    }

    @Override
    public byte[] body() {
        return body.clone();
    }

    public String bodyAsUtf8() {
        return new String(body, StandardCharsets.UTF_8);
    }

    public String bodyPreview(int maxCharacters) {
        if (maxCharacters <= 0) {
            return "";
        }
        String compact = bodyAsUtf8()
                .replace('\r', ' ')
                .replace('\n', ' ')
                .trim();
        if (compact.length() <= maxCharacters) {
            return compact;
        }
        return compact.substring(0, maxCharacters) + "...";
    }

    private static Map<String, Object> copyHeaders(Map<String, Object> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                copy.put(entry.getKey(), entry.getValue());
            }
        }
        return Map.copyOf(copy);
    }
}
