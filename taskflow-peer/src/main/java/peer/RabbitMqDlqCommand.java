package peer;

import transport.rabbitmq.RabbitMqDlqClient;
import transport.rabbitmq.RabbitMqDlqDecisionResult;
import transport.rabbitmq.RabbitMqDlqMessage;

import java.io.PrintStream;
import java.util.List;
import java.util.Locale;

final class RabbitMqDlqCommand {
    private static final int DEFAULT_COUNT = 1;
    private static final int BODY_PREVIEW_CHARACTERS = 240;

    private RabbitMqDlqCommand() {
    }

    static boolean isCommand(String[] args) {
        return args.length > 0 && "dlq".equalsIgnoreCase(args[0]);
    }

    static void run(RabbitMqDlqClient client, String[] args, PrintStream out) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException(usage());
        }
        String action = args[1].toLowerCase(Locale.ROOT);
        int count = args.length >= 3 ? parseCount(args[2]) : DEFAULT_COUNT;
        switch (action) {
            case "inspect" -> printMessages(client.inspect(count), "DLQ", out);
            case "inspect-quarantine" ->
                    printMessages(client.inspectQuarantine(count), "Quarantine queue", out);
            case "redrive" -> printResults(client.redrive(count), out);
            case "redrive-quarantine" -> printResults(client.redriveQuarantine(count), out);
            case "quarantine" -> printResults(client.quarantine(count), out);
            case "discard" -> printResults(client.discard(count), out);
            default -> throw new IllegalArgumentException(usage());
        }
    }

    static String usage() {
        return """
                Usage: peer.PeerNode dlq <inspect|inspect-quarantine|redrive|redrive-quarantine|quarantine|discard> [count]
                Examples:
                  .\\mvnw.cmd -pl taskflow-peer exec:java -Dexec.args="dlq inspect 5"
                  .\\mvnw.cmd -pl taskflow-peer exec:java -Dexec.args="dlq redrive 1"
                  .\\mvnw.cmd -pl taskflow-peer exec:java -Dexec.args="dlq inspect-quarantine 5"
                  .\\mvnw.cmd -pl taskflow-peer exec:java -Dexec.args="dlq redrive-quarantine 1"
                """;
    }

    private static void printMessages(List<RabbitMqDlqMessage> messages,
                                      String queueLabel,
                                      PrintStream out) {
        if (messages.isEmpty()) {
            out.println(queueLabel + " is empty.");
            return;
        }
        for (int i = 0; i < messages.size(); i++) {
            printMessage(i + 1, messages.get(i), out);
        }
    }

    private static void printResults(List<RabbitMqDlqDecisionResult> results, PrintStream out) {
        for (RabbitMqDlqDecisionResult result : results) {
            out.printf("decision=%s status=%s detail=\"%s\"%n",
                    result.decision(),
                    result.status(),
                    result.detail());
            if (result.message() != null) {
                printMessage(1, result.message(), out);
            }
        }
    }

    private static void printMessage(int index, RabbitMqDlqMessage message, PrintStream out) {
        out.printf(
                "message[%d] id=%s route=%s originalRoutingKey=%s deadLetterQueue=%s reason=%s deadLetterCount=%d redriveCount=%d deliveryAttempt=%d failureReason=%s failureDisposition=%s redrivable=%s%n",
                index,
                value(message.messageId()),
                value(message.inferredRoute()),
                value(message.originalRoutingKey()),
                value(message.deadLetterQueue()),
                value(message.deadLetterReason()),
                message.deadLetterCount(),
                message.redriveCount(),
                message.deliveryAttempt(),
                value(message.failureReason()),
                value(message.failureDisposition()),
                message.redrivable()
        );
        if (!message.redrivable()) {
            out.printf("message[%d] nonRedrivableReason=\"%s\"%n", index, message.nonRedrivableReason());
        }
        out.printf("message[%d] bodyPreview=%s%n", index, message.bodyPreview(BODY_PREVIEW_CHARACTERS));
    }

    private static int parseCount(String raw) {
        try {
            int count = Integer.parseInt(raw);
            if (count <= 0) {
                throw new IllegalArgumentException("count must be positive");
            }
            return count;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("count must be a positive integer", e);
        }
    }

    private static String value(Object value) {
        return value == null ? "" : value.toString();
    }
}
