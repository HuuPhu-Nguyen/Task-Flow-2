package server.handler;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import protocol.JobSubmitMessage;
import protocol.Message;
import protocol.PeerDisconnectedMessage;
import protocol.TaskResultMessage;
import server.model.MessageEnvelope;
import server.registry.InMemoryPeerRegistry;
import server.scheduler.SchedulerConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerHandlerTest {
    private final Gson gson = new Gson();

    @Test
    void tcpJobSubmissionsWaitForSchedulerMailboxCapacity() throws Exception {
        JobSubmitMessage submit = new JobSubmitMessage(
                "client-1",
                Instant.EPOCH.toString(),
                "job-1",
                "TEST_TASK",
                List.of("payload"),
                "parameter"
        );

        assertTcpMessageWaitsForMailboxCapacity(submit, envelope -> {
            JobSubmitMessage queued = assertInstanceOf(JobSubmitMessage.class, envelope.message());
            assertEquals("job-1", queued.getJobId());
        });
    }

    @Test
    void tcpTaskResultsWaitForSchedulerMailboxCapacity() throws Exception {
        TaskResultMessage result = new TaskResultMessage(
                "peer-1",
                Instant.EPOCH.toString(),
                "task-1",
                "job-1",
                "result",
                true,
                null
        );

        assertTcpMessageWaitsForMailboxCapacity(result, envelope -> {
            TaskResultMessage queued = assertInstanceOf(TaskResultMessage.class, envelope.message());
            assertEquals("task-1", queued.getTaskId());
        });
    }

    private void assertTcpMessageWaitsForMailboxCapacity(Message message,
                                                         Consumer<MessageEnvelope> queuedAssertion) throws Exception {
        BlockingQueue<MessageEnvelope> mailbox = new ArrayBlockingQueue<>(1);
        MessageEnvelope existingEnvelope = new MessageEnvelope(
                new PeerDisconnectedMessage("existing-peer", Instant.EPOCH.toString(), "test"),
                "existing-peer"
        );
        assertTrue(mailbox.offer(existingEnvelope));

        Thread handlerThread = null;
        try (ServerSocket server = new ServerSocket(0);
             Socket client = new Socket("localhost", server.getLocalPort());
             Socket accepted = server.accept();
             BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
             PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {
            client.setSoTimeout(2_000);
            handlerThread = new Thread(
                    new PeerHandler(
                            accepted,
                            new InMemoryPeerRegistry(),
                            mailbox,
                            SchedulerConfig.defaults()
                    ),
                    "peer-handler-mailbox-backpressure-test"
            );
            handlerThread.start();

            assertNotNull(in.readLine());
            out.println(gson.toJson(message));

            Thread.sleep(150);
            assertSame(existingEnvelope, mailbox.peek());

            assertSame(existingEnvelope, mailbox.take());
            MessageEnvelope queuedEnvelope = mailbox.poll(2, TimeUnit.SECONDS);
            assertNotNull(queuedEnvelope);
            assertFalse(queuedEnvelope.fromNodeId().isBlank());
            queuedAssertion.accept(queuedEnvelope);
        } finally {
            if (handlerThread != null) {
                handlerThread.interrupt();
                handlerThread.join(2_000);
                assertFalse(handlerThread.isAlive());
            }
        }
    }
}
