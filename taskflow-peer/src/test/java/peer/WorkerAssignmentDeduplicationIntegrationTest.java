package peer;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import plugin.TaskResourceProfile;
import peer.engine.AssignmentCacheConfig;
import peer.engine.PeerExecutionEngine;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.BrokerTransport;
import transport.DeliveryDisposition;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportMessageHandler;
import transport.TransportRoute;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerAssignmentDeduplicationIntegrationTest {
    private static final String PEER_ID = "peer-1";

    @Test
    void invalidAssignmentRoutePayloadIsRejectedWithoutRetry() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine(
                PEER_ID,
                new AssignmentCacheConfig(8, TimeUnit.MINUTES.toMillis(1))
        );
        RecordingAcknowledgement acknowledgement = new RecordingAcknowledgement();
        try {
            RabbitMqPeerNode.handleTaskAssignment(
                    PEER_ID,
                    new RecordingBrokerTransport(),
                    engine,
                    new InboundTransportMessage(
                            TransportRoute.TASK_ASSIGN,
                            "coordinator",
                            new PongMessage("coordinator", Instant.EPOCH.toString(), java.util.List.of()),
                            acknowledgement
                    )
            );

            assertTrue(acknowledgement.rejected.get());
            assertFalse(acknowledgement.requeued.get());
            assertEquals(DeliveryDisposition.REJECT_INVALID, acknowledgement.disposition.get());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void assignmentIdentityCollisionIsQuarantinedAsDeterministicPoison() throws Exception {
        PeerExecutionEngine engine = new PeerExecutionEngine(
                PEER_ID,
                new AssignmentCacheConfig(8, TimeUnit.MINUTES.toMillis(1))
        );
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        try {
            engine.registerProcessor(
                    "TEST",
                    TaskResourceProfile.ofCapacityUnits(1),
                    task -> "done"
            );
            TaskAssignMessage original = taskAssignment("task-1");
            RecordingAcknowledgement originalAck = new RecordingAcknowledgement();
            RabbitMqPeerNode.handleTaskAssignment(
                    PEER_ID,
                    transport,
                    engine,
                    delivery(original, originalAck)
            );
            assertTrue(originalAck.awaitAck());

            TaskAssignMessage collision = taskAssignment("task-2");
            RecordingAcknowledgement collisionAck = new RecordingAcknowledgement();
            RabbitMqPeerNode.handleTaskAssignment(
                    PEER_ID,
                    transport,
                    engine,
                    delivery(collision, collisionAck)
            );

            assertTrue(collisionAck.rejected.get());
            assertFalse(collisionAck.requeued.get());
            assertEquals(DeliveryDisposition.QUARANTINE_POISON, collisionAck.disposition.get());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void duplicateRunningAssignmentExecutesOnce() throws Exception {
        CountDownLatch processorEntered = new CountDownLatch(1);
        CountDownLatch releaseProcessor = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        PeerExecutionEngine engine = new PeerExecutionEngine(
                PEER_ID,
                new AssignmentCacheConfig(8, TimeUnit.MINUTES.toMillis(1))
        );
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        try {
            engine.registerProcessor("TEST", TaskResourceProfile.ofCapacityUnits(1), task -> {
                invocations.incrementAndGet();
                processorEntered.countDown();
                if (!releaseProcessor.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("processor release timed out");
                }
                return "done";
            });
            RecordingAcknowledgement originalAck = new RecordingAcknowledgement();
            RecordingAcknowledgement duplicateAck = new RecordingAcknowledgement();
            TaskAssignMessage assignment = taskAssignment("task-1");

            RabbitMqPeerNode.handleTaskAssignment(
                    PEER_ID,
                    transport,
                    engine,
                    delivery(assignment, originalAck)
            );
            assertTrue(processorEntered.await(2, TimeUnit.SECONDS));
            RabbitMqPeerNode.handleTaskAssignment(
                    PEER_ID,
                    transport,
                    engine,
                    delivery(assignment, duplicateAck)
            );

            assertTrue(duplicateAck.awaitAck());
            assertTrue(duplicateAck.deferred.get());
            assertFalse(duplicateAck.requeued.get());
            assertFalse(duplicateAck.rejected.get());
            assertEquals(DeliveryDisposition.ACK_DUPLICATE_OR_STALE, duplicateAck.disposition.get());
            assertFalse(originalAck.awaitAck(100, TimeUnit.MILLISECONDS));
            assertEquals(1, invocations.get());
            assertNull(transport.results.poll(100, TimeUnit.MILLISECONDS));

            releaseProcessor.countDown();

            assertTrue(originalAck.awaitAck());
            OutboundTransportMessage result = transport.results.poll(2, TimeUnit.SECONDS);
            assertNotNull(result);
            assertEquals(TransportRoute.TASK_RESULT, result.route());
            assertEquals(DeliveryDisposition.ACK_SUCCESS, originalAck.disposition.get());
            assertEquals(1, invocations.get());
            assertEquals(0, transport.results.size());
        } finally {
            releaseProcessor.countDown();
            engine.shutdown();
        }
    }

    @Test
    void duplicateCompletedAssignmentRepublishesSameResult() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        PeerExecutionEngine engine = new PeerExecutionEngine(
                PEER_ID,
                new AssignmentCacheConfig(8, TimeUnit.MINUTES.toMillis(1))
        );
        RecordingBrokerTransport transport = new RecordingBrokerTransport();
        try {
            engine.registerProcessor("TEST", TaskResourceProfile.ofCapacityUnits(1), task -> {
                invocations.incrementAndGet();
                return "done";
            });
            TaskAssignMessage assignment = taskAssignment("task-1");
            RecordingAcknowledgement originalAck = new RecordingAcknowledgement();
            RecordingAcknowledgement duplicateAck = new RecordingAcknowledgement();

            RabbitMqPeerNode.handleTaskAssignment(
                    PEER_ID,
                    transport,
                    engine,
                    delivery(assignment, originalAck)
            );
            assertTrue(originalAck.awaitAck());
            assertEquals(DeliveryDisposition.ACK_SUCCESS, originalAck.disposition.get());
            OutboundTransportMessage firstPublish = transport.results.poll(2, TimeUnit.SECONDS);
            assertNotNull(firstPublish);

            RabbitMqPeerNode.handleTaskAssignment(
                    PEER_ID,
                    transport,
                    engine,
                    delivery(assignment, duplicateAck)
            );
            assertTrue(duplicateAck.awaitAck());
            OutboundTransportMessage duplicatePublish = transport.results.poll(2, TimeUnit.SECONDS);

            assertNotNull(duplicatePublish);
            assertSame(firstPublish.message(), duplicatePublish.message());
            assertEquals(
                    new Gson().toJson(firstPublish.message()),
                    new Gson().toJson(duplicatePublish.message())
            );
            TaskResultMessage firstResult = (TaskResultMessage) firstPublish.message();
            TaskResultMessage duplicateResult = (TaskResultMessage) duplicatePublish.message();
            assertEquals(assignment.getTaskId(), duplicateResult.getTaskId());
            assertEquals(assignment.getAttemptNumber(), duplicateResult.getAttemptNumber());
            assertEquals(assignment.getAssignmentId(), duplicateResult.getAssignmentId());
            assertEquals(firstResult.getTime(), duplicateResult.getTime());
            assertEquals(1, invocations.get());
            assertTrue(duplicateAck.deferred.get());
            assertFalse(duplicateAck.requeued.get());
            assertFalse(duplicateAck.rejected.get());
            assertEquals(DeliveryDisposition.ACK_DUPLICATE_OR_STALE, duplicateAck.disposition.get());
        } finally {
            engine.shutdown();
        }
    }

    private static InboundTransportMessage delivery(TaskAssignMessage assignment,
                                                    TransportAcknowledgement acknowledgement) {
        return new InboundTransportMessage(
                TransportRoute.TASK_ASSIGN,
                "coordinator",
                assignment,
                acknowledgement
        );
    }

    private static TaskAssignMessage taskAssignment(String taskId) {
        return new TaskAssignMessage(
                PEER_ID,
                Instant.EPOCH.toString(),
                taskId,
                "job-1",
                "TEST",
                1,
                "550e8400-e29b-41d4-a716-446655440000",
                1_780_000_000_000L,
                "payload",
                "parameter"
        );
    }

    private static final class RecordingBrokerTransport implements BrokerTransport {
        private final BlockingQueue<OutboundTransportMessage> results = new LinkedBlockingQueue<>();

        @Override
        public void declareTopology() {
        }

        @Override
        public boolean publish(OutboundTransportMessage message) {
            results.add(message);
            return true;
        }

        @Override
        public String subscribe(TransportRoute route, TransportMessageHandler handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancel(String consumerTag) {
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingAcknowledgement implements TransportAcknowledgement {
        private final CountDownLatch acknowledged = new CountDownLatch(1);
        private final AtomicBoolean deferred = new AtomicBoolean();
        private final AtomicBoolean requeued = new AtomicBoolean();
        private final AtomicBoolean rejected = new AtomicBoolean();
        private final AtomicReference<DeliveryDisposition> disposition = new AtomicReference<>();

        @Override
        public void settle(DeliveryDisposition requestedDisposition) throws Exception {
            disposition.compareAndSet(null, requestedDisposition);
            TransportAcknowledgement.super.settle(requestedDisposition);
        }

        @Override
        public void ack() {
            acknowledged.countDown();
        }

        @Override
        public void requeue() {
            requeued.set(true);
        }

        @Override
        public void reject() {
            rejected.set(true);
        }

        @Override
        public void defer() {
            deferred.set(true);
        }

        private boolean awaitAck() throws InterruptedException {
            return awaitAck(2, TimeUnit.SECONDS);
        }

        private boolean awaitAck(long timeout, TimeUnit unit) throws InterruptedException {
            return acknowledged.await(timeout, unit);
        }
    }
}
