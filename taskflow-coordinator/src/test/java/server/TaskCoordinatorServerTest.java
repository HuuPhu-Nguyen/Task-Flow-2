package server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskCoordinatorServerTest {

    @Test
    void usageDescribesCoordinatorRuntimePackage() {
        String usage = TaskCoordinatorServer.usage();

        assertTrue(usage.contains("sole authority for scheduling"));
        assertTrue(usage.contains("authoritative result commitment"));
        assertTrue(usage.contains("RabbitMQ is the sole supported runtime transport"));
        assertTrue(usage.contains("taskflow-coordinator-<version>-coordinator-runtime.jar"));
        assertTrue(usage.contains("status [summary|jobs|peers|outbox|queues|dlq] [count]"));
        assertFalse(usage.contains("TASKFLOW_TRANSPORT"));
        assertFalse(usage.contains("TCP"));
    }

    @Test
    void helpDetectionDoesNotStartTheBrokerRuntime() {
        assertTrue(TaskCoordinatorServer.isHelpRequested(new String[] {"--help"}));
        assertTrue(TaskCoordinatorServer.isHelpRequested(new String[] {"-h"}));
        assertFalse(TaskCoordinatorServer.isHelpRequested(new String[0]));
    }
}
