package server;

import protocol.Message;
import protocol.TaskResultMessage;
import server.db.DatabaseManager;
import server.model.MessageEnvelope;
import server.rabbitmq.RabbitMqSchedulerOutput;
import server.registry.InMemoryPeerRegistry;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.scheduler.TaskScheduler;
import transport.InboundTransportMessage;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqRuntimeDefaults;
import transport.rabbitmq.RabbitMqTransport;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RabbitMqTaskCoordinatorServer {

    public static void main(String[] args) throws Exception {
        RabbitMqTransport transport = new RabbitMqTransport(RabbitMqTransportConfig.fromEnvironment());
        transport.declareTopology();

        BlockingQueue<MessageEnvelope> inboundMailbox = new LinkedBlockingQueue<>();
        PeerRegistry registry = new InMemoryPeerRegistry();
        registry.register(RabbitMqRuntimeDefaults.WORKER_POOL_NODE_ID,
                new PeerInfo(RabbitMqRuntimeDefaults.WORKER_POOL_NODE_ID));

        DatabaseManager db = null;
        try {
            db = new DatabaseManager();
            System.out.println("Database initialized: " + DatabaseManager.DB_PATH);
        } catch (Exception e) {
            System.err.println("Warning: could not open database, history will not be persisted: " + e.getMessage());
        }

        TaskScheduler schedulerLogic = new TaskScheduler(
                inboundMailbox,
                registry,
                db,
                new RabbitMqSchedulerOutput(transport)
        );
        Thread schedulerThread = new Thread(schedulerLogic, "rabbitmq-task-scheduler");

        transport.subscribe(TransportRoute.JOB_SUBMIT,
                delivery -> enqueueForScheduler(inboundMailbox, delivery));
        transport.subscribe(TransportRoute.TASK_RESULT,
                delivery -> enqueueForScheduler(inboundMailbox, normalizeWorkerPoolResult(delivery)));

        DatabaseManager finalDb = db;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down RabbitMQ coordinator...");
            schedulerThread.interrupt();
            if (finalDb != null) {
                finalDb.close();
            }
            try {
                transport.close();
            } catch (Exception ignored) {
            }
        }));

        schedulerThread.start();
        System.out.println("RabbitMqTaskCoordinatorServer listening on RabbitMQ routes.");
        Thread.currentThread().join();
    }

    private static void enqueueForScheduler(BlockingQueue<MessageEnvelope> inboundMailbox,
                                            InboundTransportMessage delivery) throws InterruptedException {
        inboundMailbox.put(new MessageEnvelope(delivery.message(), delivery.fromNodeId()));
    }

    private static InboundTransportMessage normalizeWorkerPoolResult(InboundTransportMessage delivery) {
        Message message = delivery.message();
        if (!(message instanceof TaskResultMessage result)) {
            return delivery;
        }
        TaskResultMessage normalized = new TaskResultMessage(
                RabbitMqRuntimeDefaults.WORKER_POOL_NODE_ID,
                result.getTime(),
                result.getTaskId(),
                result.getJobId(),
                result.getResultPayload(),
                result.isSuccessful(),
                result.getErrorMessage()
        );
        return new InboundTransportMessage(
                delivery.route(),
                RabbitMqRuntimeDefaults.WORKER_POOL_NODE_ID,
                normalized,
                delivery.acknowledgement()
        );
    }
}
