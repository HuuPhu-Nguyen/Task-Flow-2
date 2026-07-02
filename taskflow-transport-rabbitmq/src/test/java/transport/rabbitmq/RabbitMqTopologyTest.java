package transport.rabbitmq;

import org.junit.jupiter.api.Test;
import transport.TransportRoute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void mapsPeerRoutesToPeerSpecificQueuesAndRoutingKeys() {
        RabbitMqTopology topology = new RabbitMqTopology(RabbitMqTransportConfig.localDefaults());

        assertEquals("taskflow.peer.peer_1.task-assign",
                topology.peerQueueName(TransportRoute.TASK_ASSIGN, "peer.1"));
        assertEquals("tasks.assign.peer_1",
                topology.peerRoutingKey(TransportRoute.TASK_ASSIGN, "peer.1"));
        assertEquals("taskflow.peer.peer_1.job-result",
                topology.peerQueueName(TransportRoute.JOB_RESULT, "peer.1"));
        assertEquals("jobs.result.peer_1",
                topology.peerRoutingKey(TransportRoute.JOB_RESULT, "peer.1"));
    }

    @Test
    void exposesDeadLetterTopologyAndQueueArguments() {
        RabbitMqTopology topology = new RabbitMqTopology(RabbitMqTransportConfig.localDefaults());

        assertTrue(topology.deadLetterEnabled());
        assertEquals("taskflow.dead-letter.exchange", topology.deadLetterExchangeName());
        assertEquals("taskflow.dead-letter", topology.deadLetterQueueName());
        assertEquals("dead-letter", topology.deadLetterRoutingKey());
        assertEquals("taskflow.dead-letter.quarantine", topology.deadLetterQuarantineQueueName());
        assertEquals("dead-letter.quarantine", topology.deadLetterQuarantineRoutingKey());
        assertEquals("taskflow.dead-letter.exchange",
                topology.queueArguments().get("x-dead-letter-exchange"));
        assertEquals("dead-letter",
                topology.queueArguments().get("x-dead-letter-routing-key"));
    }

    @Test
    void mapsNormalAndPeerRoutingKeysBackToRoutes() {
        RabbitMqTopology topology = new RabbitMqTopology(RabbitMqTransportConfig.localDefaults());

        assertEquals(TransportRoute.HEARTBEAT, topology.routeForRoutingKey("heartbeats"));
        assertEquals(TransportRoute.JOB_RESULT, topology.routeForRoutingKey("jobs.result.peer_1"));
        assertEquals(TransportRoute.TASK_ASSIGN, topology.routeForRoutingKey("tasks.assign.peer_1"));
        assertEquals(null, topology.routeForRoutingKey("unknown.route"));
    }

    @Test
    void omitsDeadLetterArgumentsWhenDisabled() {
        RabbitMqTransportConfig defaults = RabbitMqTransportConfig.localDefaults();
        RabbitMqTransportConfig config = new RabbitMqTransportConfig(
                defaults.host(),
                defaults.port(),
                defaults.username(),
                defaults.password(),
                defaults.virtualHost(),
                defaults.exchangeName(),
                defaults.queuePrefix(),
                defaults.durable(),
                defaults.prefetchCount(),
                defaults.publisherConfirmTimeoutMillis(),
                false,
                "",
                "",
                "",
                defaults.requeueOnHandlerFailure()
        );
        RabbitMqTopology topology = new RabbitMqTopology(config);

        assertFalse(topology.deadLetterEnabled());
        assertTrue(topology.queueArguments().isEmpty());
    }
}
