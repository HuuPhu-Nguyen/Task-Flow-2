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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.google.gson.Gson;

import messaging.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.Message;
import protocol.MessageType;
import protocol.PeerIdentity;
import protocol.PeerDisconnectedMessage;
import protocol.PingMessage;
import protocol.PongMessage;
import protocol.TaskResultMessage;
import server.model.MessageEnvelope;
import server.registry.PeerInfo;
import server.registry.PeerRegistry;
import server.registry.PeerTransport;
import server.scheduler.SchedulerConfig;
import server.scheduler.SchedulerMailbox;
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

        String connectionId = PeerIdentity.generated("TCP_CONN");
        String nodeId = connectionId;
        boolean registered = false;

        try {
            socket.setSoTimeout(SOCKET_POLL_TIMEOUT_MILLIS);
        } catch (IOException e) {
            LOGGER.warn("event=peer_socket_timeout_config_failed connection_id={} error={}",
                    nodeId, e.getMessage(), e);
            cleanup(nodeId, false);
            return;
        }

        try (
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            TransportConnection connection = new TcpPeerConnection(connectionId, socket, out, gson);

            LOGGER.info("event=peer_handler_started connection_id={}", connectionId);
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
                        PongMessage pong = (PongMessage) message;
                        if (!registered) {
                            String peerId = resolvePeerId(pong.getNodeId(), connectionId);
                            TransportConnection peerConnection = new TcpPeerConnection(peerId, socket, out, gson);
                            if (!registerPeer(peerId, peerConnection, pong.getSupportedTaskTypes())) {
                                nodeId = peerId;
                                break;
                            }
                            nodeId = peerId;
                            connection = peerConnection;
                            registered = true;
                        } else if (!nodeId.equals(resolvePeerId(pong.getNodeId(), connectionId))) {
                            LOGGER.warn("event=peer_identity_mismatch peer_id={} message_peer_id={}",
                                    nodeId, pong.getNodeId());
                            break;
                        }
                        long sentAt = outstandingPings.isEmpty()
                                ? System.currentTimeMillis()
                                : outstandingPings.pollFirst();
                        long rtt = Math.max(1L, System.currentTimeMillis() - sentAt);
                        PeerInfo info = registry.get(nodeId);
                        if (info != null) {
                            info.updateLatency(rtt);
                            registry.updateHeartbeat(nodeId, pong.getSupportedTaskTypes());
                        }
                        continue;
                    }

                    if (!registered) {
                        String peerId = resolvePeerId(message.getNodeId(), connectionId);
                        TransportConnection peerConnection = new TcpPeerConnection(peerId, socket, out, gson);
                        if (!registerPeer(peerId, peerConnection, List.of())) {
                            nodeId = peerId;
                            break;
                        }
                        nodeId = peerId;
                        connection = peerConnection;
                        registered = true;
                    } else if (message.getNodeId() != null
                            && !message.getNodeId().isBlank()
                            && !nodeId.equals(resolvePeerId(message.getNodeId(), connectionId))) {
                        LOGGER.warn("event=peer_identity_mismatch peer_id={} message_peer_id={}",
                                nodeId, message.getNodeId());
                        break;
                    }

                    SchedulerMailbox.put(mailbox, new MessageEnvelope(message, nodeId));

                } catch (SocketTimeoutException ignored) {
                    // Normal polling timeout
                } catch (SocketException e) {
                    LOGGER.info("event=peer_connection_reset peer_id={} error={}", nodeId, e.getMessage());
                    break;
                } catch (IOException e) {
                    LOGGER.warn("event=peer_io_error peer_id={} error={}", nodeId, e.getMessage(), e);
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    LOGGER.warn("event=peer_message_processing_failed peer_id={} error={}",
                            nodeId, e.getMessage(), e);
                }
            }

        } catch (IOException e) {
            LOGGER.warn("event=peer_connection_failed peer_id={} error={}", nodeId, e.getMessage(), e);
        } finally {
            cleanup(nodeId, registered);
        }
    }

    private boolean registerPeer(String nodeId,
                                 TransportConnection connection,
                                 Collection<String> supportedTaskTypes) {
        PeerInfo peer = new PeerInfo(nodeId, connection, schedulerConfig, supportedTaskTypes, PeerTransport.TCP);
        if (!registry.registerIfAbsent(nodeId, peer)) {
            LOGGER.warn("event=peer_connection_rejected peer_id={} reason=duplicate_peer_id", nodeId);
            return false;
        }
        LOGGER.info("event=peer_identity_registered peer_id={}", nodeId);
        return true;
    }

    private String resolvePeerId(String candidatePeerId, String fallbackPeerId) {
        if (candidatePeerId != null && !candidatePeerId.isBlank()) {
            return PeerIdentity.require(candidatePeerId);
        }
        return PeerIdentity.require(fallbackPeerId);
    }

    private void cleanup(String nodeId, boolean registered) {
        if (!registered) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            LOGGER.info("event=peer_disconnected peer_id={} registered=false", nodeId);
            return;
        }
        registry.remove(nodeId);
        notifySchedulerPeerDisconnected(nodeId, "tcp_disconnect");

        try {
            socket.close();
        } catch (IOException ignored) {
        }

        LOGGER.info("event=peer_disconnected peer_id={}", nodeId);
    }

    private void notifySchedulerPeerDisconnected(String nodeId, String reason) {
        boolean queued = SchedulerMailbox.offer(mailbox, new MessageEnvelope(
                new PeerDisconnectedMessage(nodeId, Instant.now().toString(), reason),
                nodeId
        ));
        if (!queued) {
            LOGGER.error("event=peer_disconnect_event_dropped peer_id={} reason={}", nodeId, reason);
        }
    }

    private MessageFactory createFactory(Gson gson) {
        MessageFactory factory = new MessageFactory();

        factory.register(MessageType.PONG,
                json -> gson.fromJson(json, PongMessage.class));

        factory.register(MessageType.TASK_RESULT,
                json -> gson.fromJson(json, TaskResultMessage.class));

        factory.register(protocol.MessageType.JOB_SUBMIT,
                json -> gson.fromJson(json, protocol.JobSubmitMessage.class));

        factory.register(protocol.MessageType.JOB_RESULT_REQUEST,
                json -> gson.fromJson(json, protocol.JobResultRequestMessage.class));

        return factory;
    }
}
