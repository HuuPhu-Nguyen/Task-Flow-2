package server.transport;

import com.google.gson.Gson;
import protocol.Message;
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
        this.nodeId = nodeId;
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
    public void send(Message message) {
        synchronized (out) {
            out.println(gson.toJson(message));
        }
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
