package transport.rabbitmq;

public record RabbitMqQueueStatus(
        String role,
        String queueName,
        long messageCount,
        long consumerCount,
        boolean available,
        String error
) {
    public RabbitMqQueueStatus {
        role = role == null || role.isBlank() ? "UNKNOWN" : role.trim();
        queueName = queueName == null ? "" : queueName.trim();
        messageCount = Math.max(0L, messageCount);
        consumerCount = Math.max(0L, consumerCount);
        error = error == null ? "" : error.trim();
    }
}
