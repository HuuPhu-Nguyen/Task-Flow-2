package peer;

/**
 * RabbitMQ-only command-line participant entry point.
 *
 * <p>The artifact and class retain the historical {@code peer} compatibility
 * name, but the process may enable the requester role, executor role, or both.</p>
 */
public final class PeerNode {

    private PeerNode() {
    }

    public static void main(String[] args) throws Exception {
        if (isHelpRequested(args)) {
            System.out.println(usage());
            return;
        }
        RabbitMqPeerNode.main(args);
    }

    static boolean isHelpRequested(String[] args) {
        return args != null
                && args.length > 0
                && ("--help".equals(args[0]) || "-h".equals(args[0]));
    }

    static String usage() {
        return String.join(System.lineSeparator(),
                "TaskFlow RabbitMQ participant runtime (taskflow-peer compatibility artifact): enable the requester role, executor role, or both.",
                "java -jar taskflow-peer-<version>-submitter-runtime.jar submit <task-type> <parameter> <file> [file...] (requester role)",
                "java -jar taskflow-peer-<version>-executor-runtime.jar (executor role)",
                "java -jar taskflow-peer-<version>-combined-runtime.jar [submit <task-type> <parameter> <file> [file...]] (requester + executor)",
                "java -jar taskflow-peer-<version>-combined-runtime.jar dlq <inspect|redrive|quarantine|discard> [count]");
    }
}
