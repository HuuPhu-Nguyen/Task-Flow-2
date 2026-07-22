package server;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskCoordinatorServerTest {

    @Test
    void usageDescribesCoordinatorRuntimePackage() {
        String usage = TaskCoordinatorServer.usage();

        assertTrue(usage.contains("sole authority for scheduling"));
        assertTrue(usage.contains("authoritative result commitment"));
        assertTrue(usage.contains("taskflow-coordinator-<version>-coordinator-runtime.jar"));
        assertTrue(usage.contains("TASKFLOW_TRANSPORT=tcp"));
        assertTrue(usage.contains("TASKFLOW_TRANSPORT=rabbitmq"));
        assertTrue(usage.contains("status [summary|jobs|peers|outbox|queues|dlq] [count]"));
    }

    @Test
    void unsetTransportSelectsRabbitMqByDefault() {
        assertTrue(TaskCoordinatorServer.isRabbitMqTransportSelected(Map.of()));
        assertTrue(TaskCoordinatorServer.isRabbitMqTransportSelected(Map.of("TASKFLOW_TRANSPORT", " ")));
        assertTrue(TaskCoordinatorServer.isRabbitMqTransportSelected(Map.of("TASKFLOW_TRANSPORT", "rabbitmq")));
        assertFalse(TaskCoordinatorServer.isRabbitMqTransportSelected(Map.of("TASKFLOW_TRANSPORT", "tcp")));
    }

    @Test
    void unknownTransportIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> TaskCoordinatorServer.isRabbitMqTransportSelected(Map.of("TASKFLOW_TRANSPORT", "udp")));
    }
}
