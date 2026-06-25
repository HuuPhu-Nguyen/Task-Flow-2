package gui;

import protocol.JobResultMessage;

import java.util.Objects;

final class GuiCoordinatorConnectionService {
    interface Listener {
        void onConnected();

        void onConnectionFailed(String error);

        void onDisconnected(String message);

        void onJobResult(JobResultMessage result);
    }

    interface ConnectionFactory {
        StartableCoordinatorConnection create(
                String host,
                int port,
                GuiWorkerRuntime workerRuntime,
                TcpCoordinatorConnection.Listener listener);
    }

    private final GuiWorkerRuntime workerRuntime;
    private final ConnectionFactory connectionFactory;

    private volatile CoordinatorConnection currentConnection;
    private volatile boolean stopping;

    GuiCoordinatorConnectionService(GuiWorkerRuntime workerRuntime) {
        this(workerRuntime, TcpCoordinatorConnection::new);
    }

    GuiCoordinatorConnectionService(GuiWorkerRuntime workerRuntime, ConnectionFactory connectionFactory) {
        this.workerRuntime = Objects.requireNonNull(workerRuntime, "workerRuntime");
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
    }

    void start(String host, int port, Listener listener) {
        Objects.requireNonNull(listener, "listener");
        stopping = false;
        TcpCoordinatorConnection.Listener connectionListener = new TcpCoordinatorConnection.Listener() {
            @Override
            public void onConnected(CoordinatorConnection connection) {
                listener.onConnected();
            }

            @Override
            public void onConnectionFailed(CoordinatorConnection connection, String error) {
                clear(connection, false);
                listener.onConnectionFailed(error);
            }

            @Override
            public void onDisconnected(CoordinatorConnection connection, String message) {
                clear(connection, false);
                if (!stopping) {
                    listener.onDisconnected(message);
                }
            }

            @Override
            public void onJobResult(CoordinatorConnection connection, JobResultMessage result) {
                listener.onJobResult(result);
            }
        };

        StartableCoordinatorConnection connection = connectionFactory.create(
                host,
                port,
                workerRuntime,
                connectionListener);
        currentConnection = connection;
        connection.start();
    }

    CoordinatorConnection currentConnection() {
        return currentConnection;
    }

    boolean isCurrent(CoordinatorConnection connection) {
        return currentConnection == connection;
    }

    void clear(CoordinatorConnection connection, boolean closeConnection) {
        if (connection == null) {
            return;
        }
        if (currentConnection == connection) {
            currentConnection = null;
        }
        if (closeConnection) {
            connection.close();
        }
    }

    void stop() {
        stopping = true;
        CoordinatorConnection connection = currentConnection;
        if (connection != null) {
            clear(connection, true);
        }
    }
}
