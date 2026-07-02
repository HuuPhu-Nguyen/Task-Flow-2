package transport.rabbitmq;

public record RabbitMqDlqDecisionResult(
        String decision,
        Status status,
        RabbitMqDlqMessage message,
        String detail
) {
    public enum Status {
        EMPTY,
        REDRIVEN,
        QUARANTINED,
        DISCARDED,
        NOT_REDRIVABLE,
        PUBLISH_NOT_CONFIRMED
    }
}
