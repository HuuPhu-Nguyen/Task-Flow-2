package peer;

import peer.engine.PeerExecutionEngine;
import protocol.Message;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRuntimeDefaults;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.util.concurrent.ExecutionException;

public class RabbitMqPeerNode {

    public static void main(String[] args) throws Exception {
        String nodeId = RabbitMqRuntimeDefaults.WORKER_POOL_NODE_ID;
        RabbitMqTransport transport = new RabbitMqTransport(RabbitMqTransportConfig.fromEnvironment());
        transport.declareTopology();

        PeerExecutionEngine engine = new PeerExecutionEngine(nodeId);
        System.out.println("Registered task processors: " + engine.getRegisteredTaskTypes());

        transport.subscribe(TransportRoute.TASK_ASSIGN,
                delivery -> handleTaskAssignment(transport, engine, delivery));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                transport.close();
            } catch (Exception ignored) {
            }
        }));

        System.out.println("RabbitMqPeerNode consuming task assignments.");
        Thread.currentThread().join();
    }

    private static void handleTaskAssignment(RabbitMqTransport transport,
                                             PeerExecutionEngine engine,
                                             InboundTransportMessage delivery) throws Exception {
        Message message = delivery.message();
        if (!(message instanceof TaskAssignMessage task)) {
            delivery.acknowledgement().reject();
            return;
        }

        try {
            TaskResultMessage result = engine.executeTask(task).get();
            transport.publish(new OutboundTransportMessage(
                    TransportRoute.TASK_RESULT,
                    result.getNodeId(),
                    result
            ));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throw new RuntimeException("Worker execution failed before result publication.", e);
        }
    }
}
