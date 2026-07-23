package server;

/**
 * RabbitMQ-only coordinator entry point.
 */
public final class TaskCoordinatorServer {

    private TaskCoordinatorServer() {
    }

    public static void main(String[] args) throws Exception {
        if (isHelpRequested(args)) {
            System.out.println(usage());
            return;
        }
        if (StatusCommand.isCommand(args)) {
            StatusCommand.run(args, System.out);
            return;
        }
        RabbitMqTaskCoordinatorServer.main(args);
    }

    static boolean isHelpRequested(String[] args) {
        return args != null
                && args.length > 0
                && ("--help".equals(args[0]) || "-h".equals(args[0]));
    }

    static String usage() {
        return String.join(System.lineSeparator(),
                "TaskFlow coordinator: sole authority for scheduling, leases, retries, durable state transitions, authoritative result commitment, and job completion.",
                "RabbitMQ is the sole supported runtime transport.",
                "java -jar taskflow-coordinator-<version>-coordinator-runtime.jar",
                "java -jar taskflow-coordinator-<version>-coordinator-runtime.jar status [summary|jobs|peers|outbox|queues|dlq] [count]");
    }
}
