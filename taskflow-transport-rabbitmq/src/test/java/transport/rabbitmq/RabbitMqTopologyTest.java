package transport.rabbitmq;

import org.junit.jupiter.api.Test;
import transport.TransportRoute;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RabbitMqTopologyTest {
    @Test
    void mapsRoutesToStableQueueNames() {
        RabbitMqTopology topology = new RabbitMqTopology(RabbitMqTransportConfig.localDefaults());

        assertEquals("taskflow.jobs", topology.queueName(TransportRoute.JOB_SUBMIT));
        assertEquals("taskflow.tasks", topology.queueName(TransportRoute.TASK_ASSIGN));
        assertEquals("taskflow.task-results", topology.queueName(TransportRoute.TASK_RESULT));
        assertEquals("taskflow.job-results", topology.queueName(TransportRoute.JOB_RESULT));
        assertEquals("taskflow.heartbeats", topology.queueName(TransportRoute.HEARTBEAT));
    }

    @Test
    void exposesRoutingKeysForBrokerBindings() {
        assertEquals("jobs.submit", TransportRoute.JOB_SUBMIT.routingKey());
        assertEquals("tasks.assign", TransportRoute.TASK_ASSIGN.routingKey());
        assertEquals("tasks.result", TransportRoute.TASK_RESULT.routingKey());
        assertEquals("jobs.result", TransportRoute.JOB_RESULT.routingKey());
        assertEquals("heartbeats", TransportRoute.HEARTBEAT.routingKey());
    }
}
