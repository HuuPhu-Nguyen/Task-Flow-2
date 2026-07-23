package gui;

import org.junit.jupiter.api.Test;
import peer.engine.AssignmentExecution;
import protocol.JobResultMessage;
import protocol.Message;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.BrokerTransport;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportMessageHandler;
import transport.TransportRoute;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqCoordinatorConnectionTest {
    @Test
    void startDeclaresTopologySubscribesPeerQueuesAndSendsHeartbeat() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingListener listener = new RecordingListener();
        AtomicReference<RabbitMqTransportConfig> config = new AtomicReference<>();
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                new FakeWorkerRuntime(),
                config::set);

        connection.start();

        assertTrue(listener.awaitConnected());
        assertTrue(connection.isOpen());
        assertSame(connection, listener.connectedConnection.get());
        assertEquals("broker.example", config.get().host());
        assertEquals(5679, config.get().port());
        assertTrue(transport.topologyDeclared);
        assertEquals("peer-1", transport.peerSubscriptions.get(TransportRoute.TASK_ASSIGN));
        assertEquals("peer-1", transport.peerSubscriptions.get(TransportRoute.JOB_RESULT));
        OutboundTransportMessage heartbeat = onlyPublished(transport, TransportRoute.HEARTBEAT);
        assertEquals("peer-1", heartbeat.fromNodeId());
        PongMessage pong = assertInstanceOf(PongMessage.class, heartbeat.message());
        assertEquals(List.of("TEXT_ANALYSIS"), pong.getSupportedTaskTypes());

        connection.close();

        assertFalse(connection.isOpen());
        assertEquals(List.of("TASK_ASSIGN-tag", "JOB_RESULT-tag"), transport.cancelledTags);
        assertTrue(transport.closed);
    }

    @Test
    void taskAssignmentExecutesPublishesTaskResultAndAcks() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingListener listener = new RecordingListener();
        FakeWorkerRuntime workerRuntime = new FakeWorkerRuntime();
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                workerRuntime,
                config -> {
                });
        connection.start();
        assertTrue(listener.awaitConnected());
        transport.published.clear();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        TaskAssignMessage assignment = taskAssignment("peer-1");

        transport.deliverToPeer(TransportRoute.TASK_ASSIGN, assignment, acknowledgement);

        assertSame(assignment, workerRuntime.assignedTask.get());
        OutboundTransportMessage result = onlyPublished(transport, TransportRoute.TASK_RESULT);
        assertEquals("peer-1", result.fromNodeId());
        TaskResultMessage taskResult = assertInstanceOf(TaskResultMessage.class, result.message());
        assertEquals("task-1", taskResult.getTaskId());
        assertEquals(assignment.getAttemptNumber(), taskResult.getAttemptNumber());
        assertEquals(assignment.getAssignmentId(), taskResult.getAssignmentId());
        assertTrue(acknowledgement.deferred);
        assertTrue(acknowledgement.acked);
        assertFalse(acknowledgement.requeued);
        connection.close();
    }

    @Test
    void taskResultPublishFailureRequeuesAssignment() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingListener listener = new RecordingListener();
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                new FakeWorkerRuntime(),
                config -> {
                });
        connection.start();
        assertTrue(listener.awaitConnected());
        transport.failTaskResultPublish = true;
        transport.published.clear();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();

        transport.deliverToPeer(TransportRoute.TASK_ASSIGN, taskAssignment("peer-1"), acknowledgement);

        assertTrue(acknowledgement.deferred);
        assertTrue(acknowledgement.requeued);
        assertFalse(acknowledgement.acked);
        connection.close();
    }

    @Test
    void taskExecutionFailureRequeuesAssignment() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingListener listener = new RecordingListener();
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                new FailingWorkerRuntime(),
                config -> {
                });
        connection.start();
        assertTrue(listener.awaitConnected());
        transport.published.clear();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();

        transport.deliverToPeer(TransportRoute.TASK_ASSIGN, taskAssignment("peer-1"), acknowledgement);

        assertTrue(acknowledgement.deferred);
        assertTrue(acknowledgement.requeued);
        assertFalse(acknowledgement.acked);
        assertEquals(List.of(), transport.published);
        connection.close();
    }

    @Test
    void duplicateRunningAssignmentIsAckedWithoutPublishing() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingListener listener = new RecordingListener();
        DeduplicationWorkerRuntime workerRuntime = new DeduplicationWorkerRuntime(
                new AssignmentExecution(
                        AssignmentExecution.Disposition.DUPLICATE_RUNNING,
                        new CompletableFuture<>()
                )
        );
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                workerRuntime,
                config -> {
                });
        connection.start();
        assertTrue(listener.awaitConnected());
        transport.published.clear();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();

        transport.deliverToPeer(TransportRoute.TASK_ASSIGN, taskAssignment("peer-1"), acknowledgement);

        assertTrue(acknowledgement.deferred);
        assertTrue(acknowledgement.acked);
        assertFalse(acknowledgement.requeued);
        assertEquals(List.of(), transport.published);
        connection.close();
    }

    @Test
    void duplicateCompletedAssignmentRepublishesCachedResult() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingListener listener = new RecordingListener();
        TaskResultMessage cachedResult = new TaskResultMessage(
                "peer-1",
                Instant.EPOCH.toString(),
                "task-1",
                "job-1",
                1,
                "550e8400-e29b-41d4-a716-446655440000",
                "done",
                true,
                null
        );
        DeduplicationWorkerRuntime workerRuntime = new DeduplicationWorkerRuntime(
                new AssignmentExecution(
                        AssignmentExecution.Disposition.DUPLICATE_COMPLETED,
                        CompletableFuture.completedFuture(cachedResult)
                )
        );
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                workerRuntime,
                config -> {
                });
        connection.start();
        assertTrue(listener.awaitConnected());
        transport.published.clear();
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();

        transport.deliverToPeer(TransportRoute.TASK_ASSIGN, taskAssignment("peer-1"), acknowledgement);

        OutboundTransportMessage published = onlyPublished(transport, TransportRoute.TASK_RESULT);
        assertSame(cachedResult, published.message());
        assertTrue(acknowledgement.deferred);
        assertTrue(acknowledgement.acked);
        assertFalse(acknowledgement.requeued);
        connection.close();
    }

    @Test
    void assignmentForDifferentPeerIsIgnoredAndAcked() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingListener listener = new RecordingListener();
        FakeWorkerRuntime workerRuntime = new FakeWorkerRuntime();
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                workerRuntime,
                config -> {
                });
        connection.start();
        assertTrue(listener.awaitConnected());
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();

        transport.deliverToPeer(TransportRoute.TASK_ASSIGN, taskAssignment("other-peer"), acknowledgement);

        assertTrue(acknowledgement.acked);
        assertFalse(acknowledgement.deferred);
        assertNull(workerRuntime.assignedTask.get());
        connection.close();
    }

    @Test
    void jobResultRoutesToListenerAndAcks() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingListener listener = new RecordingListener();
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                new FakeWorkerRuntime(),
                config -> {
                });
        connection.start();
        assertTrue(listener.awaitConnected());
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        JobResultMessage result = new JobResultMessage(
                "coordinator",
                Instant.EPOCH.toString(),
                "job-1",
                "TEXT_ANALYSIS",
                true,
                List.of("done"));

        transport.deliverToPeer(TransportRoute.JOB_RESULT, result, acknowledgement);

        assertSame(result, listener.jobResult.get());
        assertTrue(acknowledgement.acked);
        connection.close();
    }

    @Test
    void malformedJobResultIsRejected() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        RecordingListener listener = new RecordingListener();
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                new FakeWorkerRuntime(),
                config -> {
                });
        connection.start();
        assertTrue(listener.awaitConnected());
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();

        transport.deliverToPeer(
                TransportRoute.JOB_RESULT,
                new PongMessage("coordinator", Instant.EPOCH.toString()),
                acknowledgement);

        assertTrue(acknowledgement.rejected);
        connection.close();
    }

    @Test
    void startupFailureReportsConnectionFailure() throws Exception {
        RecordingListener listener = new RecordingListener();
        RabbitMqCoordinatorConnection connection = new RabbitMqCoordinatorConnection(
                "peer-1",
                "broker.example",
                5679,
                new FakeWorkerRuntime(),
                listener,
                config -> {
                    throw new IllegalStateException("broker down");
                },
                TimeUnit.MINUTES.toMillis(10));

        connection.start();

        assertTrue(listener.awaitConnectionFailed());
        assertEquals("broker down", listener.connectionFailure.get());
        assertFalse(connection.isOpen());
    }

    @Test
    void startupHeartbeatPublishFailureReportsConnectionFailureAndClosesTransport() throws Exception {
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        transport.failHeartbeatPublish = true;
        RecordingListener listener = new RecordingListener();
        RabbitMqCoordinatorConnection connection = newConnection(
                transport,
                listener,
                new FakeWorkerRuntime(),
                config -> {
                });

        connection.start();

        assertTrue(listener.awaitConnectionFailed());
        assertEquals("Heartbeat publish was not confirmed.", listener.connectionFailure.get());
        assertFalse(connection.isOpen());
        assertTrue(transport.closed);
        assertEquals(List.of("TASK_ASSIGN-tag", "JOB_RESULT-tag"), transport.cancelledTags);
    }

    private static RabbitMqCoordinatorConnection newConnection(RecordingBrokerTransport transport,
                                                              RecordingListener listener,
                                                              GuiWorkerRuntime workerRuntime,
                                                              java.util.function.Consumer<RabbitMqTransportConfig> configCapture) {
        return new RabbitMqCoordinatorConnection(
                "peer-1",
                "broker.example",
                5679,
                workerRuntime,
                listener,
                config -> {
                    configCapture.accept(config);
                    return transport;
                },
                TimeUnit.MINUTES.toMillis(10));
    }

    private static TaskAssignMessage taskAssignment(String peerId) {
        return new TaskAssignMessage(
                peerId,
                Instant.EPOCH.toString(),
                "task-1",
                "job-1",
                "TEXT_ANALYSIS",
                1,
                "550e8400-e29b-41d4-a716-446655440000",
                1_780_000_000_000L,
                "payload",
                "summary");
    }

    private static OutboundTransportMessage onlyPublished(RecordingBrokerTransport transport, TransportRoute route) {
        List<OutboundTransportMessage> matching = transport.published.stream()
                .filter(message -> message.route() == route)
                .toList();
        assertEquals(1, matching.size());
        return matching.getFirst();
    }

    private static final class RecordingBrokerTransport implements BrokerTransport {
        private final List<OutboundTransportMessage> published = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final List<String> cancelledTags = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final Map<TransportRoute, String> peerSubscriptions = new EnumMap<>(TransportRoute.class);
        private final Map<TransportRoute, TransportMessageHandler> peerHandlers = new EnumMap<>(TransportRoute.class);
        private boolean topologyDeclared;
        private boolean closed;
        private boolean failTaskResultPublish;
        private boolean failHeartbeatPublish;

        @Override
        public void declareTopology() {
            topologyDeclared = true;
        }

        @Override
        public boolean publish(OutboundTransportMessage message) {
            published.add(message);
            if (failTaskResultPublish && message.route() == TransportRoute.TASK_RESULT) {
                return false;
            }
            return !(failHeartbeatPublish && message.route() == TransportRoute.HEARTBEAT);
        }

        @Override
        public String subscribe(TransportRoute route, TransportMessageHandler handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String subscribePeer(TransportRoute route, String peerNodeId, TransportMessageHandler handler) {
            peerSubscriptions.put(route, peerNodeId);
            peerHandlers.put(route, handler);
            return route.name() + "-tag";
        }

        void deliverToPeer(TransportRoute route, Message message, TransportAcknowledgement acknowledgement)
                throws Exception {
            TransportMessageHandler handler = peerHandlers.get(route);
            if (handler == null) {
                throw new IllegalStateException("No handler for " + route);
            }
            handler.handle(new InboundTransportMessage(
                    route,
                    "coordinator",
                    message,
                    acknowledgement));
        }

        @Override
        public void cancel(String consumerTag) {
            cancelledTags.add(consumerTag);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class RecordingAcknowledgement implements TransportAcknowledgement {
        private boolean acked;
        private boolean requeued;
        private boolean rejected;
        private boolean deferred;

        @Override
        public void ack() {
            acked = true;
        }

        @Override
        public void requeue() {
            requeued = true;
        }

        @Override
        public void reject() {
            rejected = true;
        }

        @Override
        public void defer() {
            deferred = true;
        }
    }

    private static final class FakeWorkerRuntime implements GuiWorkerRuntime {
        private final AtomicReference<TaskAssignMessage> assignedTask = new AtomicReference<>();

        @Override
        public Set<String> supportedTaskTypes() {
            return Set.of("TEXT_ANALYSIS");
        }

        @Override
        public CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task) {
            assignedTask.set(task);
            return CompletableFuture.completedFuture(new TaskResultMessage(
                    "peer-1",
                    Instant.EPOCH.toString(),
                    task.getTaskId(),
                    task.getJobId(),
                    task.getAttemptNumber(),
                    task.getAssignmentId(),
                    "done",
                    true,
                    null));
        }

        @Override
        public void shutdown() {
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }

    private static final class FailingWorkerRuntime implements GuiWorkerRuntime {
        @Override
        public Set<String> supportedTaskTypes() {
            return Set.of("TEXT_ANALYSIS");
        }

        @Override
        public CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task) {
            return CompletableFuture.failedFuture(new IllegalStateException("processor failed"));
        }

        @Override
        public void shutdown() {
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }

    private static final class DeduplicationWorkerRuntime implements GuiWorkerRuntime {
        private final AssignmentExecution execution;

        private DeduplicationWorkerRuntime(AssignmentExecution execution) {
            this.execution = execution;
        }

        @Override
        public Set<String> supportedTaskTypes() {
            return Set.of("TEXT_ANALYSIS");
        }

        @Override
        public CompletableFuture<TaskResultMessage> executeTask(TaskAssignMessage task) {
            throw new AssertionError("duplicate assignment must not start execution");
        }

        @Override
        public AssignmentExecution executeAssignment(TaskAssignMessage task) {
            return execution;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }

    private static final class RecordingListener implements CoordinatorConnectionListener {
        private final CountDownLatch connected = new CountDownLatch(1);
        private final CountDownLatch connectionFailed = new CountDownLatch(1);
        private final AtomicReference<CoordinatorConnection> connectedConnection = new AtomicReference<>();
        private final AtomicReference<String> connectionFailure = new AtomicReference<>();
        private final AtomicReference<JobResultMessage> jobResult = new AtomicReference<>();

        @Override
        public void onConnected(CoordinatorConnection connection) {
            connectedConnection.set(connection);
            connected.countDown();
        }

        @Override
        public void onConnectionFailed(CoordinatorConnection connection, String error) {
            connectionFailure.set(error);
            connectionFailed.countDown();
        }

        @Override
        public void onDisconnected(CoordinatorConnection connection, String message) {
        }

        @Override
        public void onJobResult(CoordinatorConnection connection, JobResultMessage result) {
            jobResult.set(result);
        }

        private boolean awaitConnected() throws InterruptedException {
            return connected.await(2, TimeUnit.SECONDS);
        }

        private boolean awaitConnectionFailed() throws InterruptedException {
            return connectionFailed.await(2, TimeUnit.SECONDS);
        }
    }
}
