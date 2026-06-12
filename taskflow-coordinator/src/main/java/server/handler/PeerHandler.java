package server.handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.concurrent.BlockingQueue;

import com.google.gson.Gson;

import messaging.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.Message;
import protocol.MessageType;
import protocol.PingMessage;
import protocol.PongMessage;
import protocol.TaskResultMessage;
import server.model.MessageEnvelope;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.scheduler.SchedulerConfig;
import server.transport.TcpPeerConnection;
import transport.TransportConnection;

public class PeerHandler implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeerHandler.class);

    private static final long HEARTBEAT_INTERVAL_MILLIS = 30_000;
    private static final int SOCKET_POLL_TIMEOUT_MILLIS = 1_000;

    private final Socket socket;
    private final PeerRegistry registry;
    private final BlockingQueue<MessageEnvelope> mailbox;
    private final SchedulerConfig schedulerConfig;

    public PeerHandler(Socket socket,
                       PeerRegistry registry,
                       BlockingQueue<MessageEnvelope> mailbox) {
        this(socket, registry, mailbox, SchedulerConfig.defaults());
    }

    public PeerHandler(Socket socket,
                       PeerRegistry registry,
                       BlockingQueue<MessageEnvelope> mailbox,
                       SchedulerConfig schedulerConfig) {
        this.socket = socket;
        this.registry = registry;
        this.mailbox = mailbox;
        this.schedulerConfig = schedulerConfig == null ? SchedulerConfig.defaults() : schedulerConfig;
    }

    @Override
    public void run() {
        Gson gson = new Gson();
        MessageFactory factory = createFactory(gson);

        String nodeId = socket.getRemoteSocketAddress().toString();

        try {
            socket.setSoTimeout(SOCKET_POLL_TIMEOUT_MILLIS);
        } catch (IOException e) {
            LOGGER.warn("event=peer_socket_timeout_config_failed peer_id={} error={}",
                    nodeId, e.getMessage(), e);
            cleanup(nodeId);
            return;
        }

        try (
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            TransportConnection connection = new TcpPeerConnection(nodeId, socket, out, gson);
            registry.register(nodeId, new PeerInfo(nodeId, connection, schedulerConfig));

            LOGGER.info("event=peer_handler_started peer_id={}", nodeId);
            long nextHeartbeatAt = 0L;
            ArrayDeque<Long> outstandingPings = new ArrayDeque<>();
            while (connection.isOpen()) {
                long now = System.currentTimeMillis();
                if (now >= nextHeartbeatAt) {
                    PingMessage ping = new PingMessage(nodeId, Instant.now().toString());
                    connection.send(ping);
                    outstandingPings.addLast(System.currentTimeMillis());
                    while (outstandingPings.size() > 5) {
                        outstandingPings.pollFirst();
                    }
                    nextHeartbeatAt = now + HEARTBEAT_INTERVAL_MILLIS;
                }

                try {
                    String incomingJson = in.readLine();

                    if (incomingJson == null) {
                        LOGGER.info("event=peer_connection_closed peer_id={}", nodeId);
                        break;
                    }

                    if (incomingJson.trim().isEmpty()) {
                        continue;
                    }

                    Message message = factory.fromJson(incomingJson);

                    if (message instanceof PongMessage) {
                        long sentAt = outstandingPings.isEmpty()
                                ? System.currentTimeMillis()
                                : outstandingPings.pollFirst();
                        long rtt = Math.max(1L, System.currentTimeMillis() - sentAt);
                        registry.updateHeartbeat(nodeId);
                        // UPDATE PEER INFO
                        PeerInfo info = registry.get(nodeId);
                        if (info != null) {
                            info.updateLatency(rtt);
                            info.setSupportedTaskTypes(((PongMessage) message).getSupportedTaskTypes());
                        }
                        continue;
                    }

                    mailbox.put(new MessageEnvelope(message, nodeId));

                } catch (SocketTimeoutException ignored) {
                    // Normal polling timeout
                } catch (SocketException e) {
                    LOGGER.info("event=peer_connection_reset peer_id={} error={}", nodeId, e.getMessage());
                    break;
                } catch (IOException e) {
                    LOGGER.warn("event=peer_io_error peer_id={} error={}", nodeId, e.getMessage(), e);
                    break;
                } catch (Exception e) {
                    LOGGER.warn("event=peer_message_processing_failed peer_id={} error={}",
                            nodeId, e.getMessage(), e);
                }
            }

        } catch (IOException e) {
            LOGGER.warn("event=peer_connection_failed peer_id={} error={}", nodeId, e.getMessage(), e);
        } finally {
            cleanup(nodeId);
        }
    }

    private void cleanup(String nodeId) {
        registry.remove(nodeId);

        try {
            socket.close();
        } catch (IOException ignored) {
        }

        LOGGER.info("event=peer_disconnected peer_id={}", nodeId);
    }

    private MessageFactory createFactory(Gson gson) {
        MessageFactory factory = new MessageFactory();

        factory.register(MessageType.PONG,
                json -> gson.fromJson(json, PongMessage.class));

        factory.register(MessageType.TASK_RESULT,
                json -> gson.fromJson(json, TaskResultMessage.class));

        factory.register(protocol.MessageType.JOB_SUBMIT,
                json -> gson.fromJson(json, protocol.JobSubmitMessage.class));

        return factory;
    }
}
