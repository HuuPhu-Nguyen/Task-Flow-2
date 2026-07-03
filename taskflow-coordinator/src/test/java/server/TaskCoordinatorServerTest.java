package server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskCoordinatorServerTest {

    @Test
    void usageDescribesCoordinatorRuntimePackage() {
        String usage = TaskCoordinatorServer.usage();

        assertTrue(usage.contains("taskflow-coordinator-<version>-coordinator-runtime.jar"));
        assertTrue(usage.contains("TASKFLOW_TRANSPORT=tcp"));
        assertTrue(usage.contains("TASKFLOW_TRANSPORT=rabbitmq"));
        assertTrue(usage.contains("status [summary|jobs|peers|outbox|queues|dlq] [count]"));
    }
}
