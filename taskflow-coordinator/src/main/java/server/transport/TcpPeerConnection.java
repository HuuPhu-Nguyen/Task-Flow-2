package server.transport;

import com.google.gson.Gson;
import messaging.SafeJsonWriter;
import protocol.Message;
import protocol.PeerIdentity;
import transport.TransportConnection;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class TcpPeerConnection implements TransportConnection {
    private final String nodeId;
    private final Socket socket;
    private final PrintWriter out;
    private final Gson gson;

    public TcpPeerConnection(String nodeId, Socket socket, PrintWriter out, Gson gson) {
        this.nodeId = PeerIdentity.require(nodeId);
        this.socket = socket;
        this.out = out;
        this.gson = gson;
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public boolean isOpen() {
        return socket.isConnected() && !socket.isClosed();
    }

    @Override
    public boolean send(Message message) {
        if (!isOpen()) {
            return false;
        }
        return SafeJsonWriter.send(out, gson, message);
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
