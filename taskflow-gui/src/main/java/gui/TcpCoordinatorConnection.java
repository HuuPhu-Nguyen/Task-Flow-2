package gui;

import com.google.gson.Gson;
import messaging.MessageDispatcher;
import messaging.MessageFactory;
import messaging.handlers.PingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.JobResultMessage;
import protocol.Message;
import protocol.MessageType;
import protocol.PingMessage;
import protocol.TaskAssignMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

final class TcpCoordinatorConnection implements StartableCoordinatorConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpCoordinatorConnection.class);

    interface Listener {
        void onConnected(CoordinatorConnection connection);

        void onConnectionFailed(CoordinatorConnection connection, String error);

        void onDisconnected(CoordinatorConnection connection, String message);

        void onJobResult(CoordinatorConnection connection, JobResultMessage result);
    }

    private final String host;
    private final int port;
    private final GuiWorkerRuntime workerRuntime;
    private final Listener listener;
    private final Gson gson = new Gson();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile Socket socket;
    private volatile PrintWriter writer;
    private volatile Thread thread;
    private volatile boolean connected;

    TcpCoordinatorConnection(String host, int port, GuiWorkerRuntime workerRuntime, Listener listener) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.workerRuntime = Objects.requireNonNull(workerRuntime, "workerRuntime");
        this.listener = Objects.requireNonNull(listener, "listener");
    }

    @Override
    public void start() {
        Thread networkThread = new Thread(this::run, "gui-tcp-connection");
        networkThread.setDaemon(true);
        this.thread = networkThread;
        networkThread.start();
    }

    @Override
    public PrintWriter writer() {
        return writer;
    }

    @Override
    public boolean isOpen() {
        Socket currentSocket = socket;
        return !closed.get()
                && currentSocket != null
                && currentSocket.isConnected()
                && !currentSocket.isClosed()
                && writer != null;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closeResources();
        Thread currentThread = thread;
        if (currentThread != null) {
            currentThread.interrupt();
        }
    }

    private void run() {
        try (Socket openedSocket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(openedSocket.getInputStream()))) {
            socket = openedSocket;
            writer = new PrintWriter(openedSocket.getOutputStream(), true);
            connected = true;
            listener.onConnected(this);

            MessageFactory factory = createFactory();
            MessageDispatcher dispatcher = createDispatcher(writer);

            String line;
            while (!closed.get() && (line = in.readLine()) != null) {
                try {
                    if (line.trim().isEmpty()) {
                        continue;
                    }
                    Message message = factory.fromJson(line);
                    dispatcher.dispatch(message, writer);
                } catch (Exception messageError) {
                    LOGGER.warn("event=gui_message_processing_failed error={}",
                            messageError.getMessage(), messageError);
                }
            }

            if (!closed.get()) {
                listener.onDisconnected(this, "Coordinator connection closed.");
            }
        } catch (IOException e) {
            if (!closed.get()) {
                if (connected) {
                    listener.onDisconnected(this, "Connection to coordinator was lost: " + e.getMessage());
                } else {
                    listener.onConnectionFailed(this, e.getMessage());
                }
            }
        } finally {
            closeResources();
        }
    }

    private void closeResources() {
        PrintWriter currentWriter = writer;
        if (currentWriter != null) {
            currentWriter.close();
        }
        Socket currentSocket = socket;
        if (currentSocket != null) {
            try {
                currentSocket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private MessageFactory createFactory() {
        MessageFactory factory = new MessageFactory();
        factory.register(MessageType.PING, json -> gson.fromJson(json, PingMessage.class));
        factory.register(MessageType.TASK_ASSIGN, json -> gson.fromJson(json, TaskAssignMessage.class));
        factory.register(MessageType.JOB_RESULT, json -> gson.fromJson(json, JobResultMessage.class));
        return factory;
    }

    private MessageDispatcher createDispatcher(PrintWriter out) {
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(MessageType.PING, new PingHandler(workerRuntime::supportedTaskTypes));
        dispatcher.register(MessageType.TASK_ASSIGN, (message, writer) ->
                workerRuntime.submitTask((TaskAssignMessage) message, out));
        dispatcher.register(MessageType.JOB_RESULT, (message, writer) ->
                listener.onJobResult(this, (JobResultMessage) message));
        return dispatcher;
    }
}
