package gui;

import protocol.JobResultMessage;

interface CoordinatorConnectionListener {
    void onConnected(CoordinatorConnection connection);

    void onConnectionFailed(CoordinatorConnection connection, String error);

    void onDisconnected(CoordinatorConnection connection, String message);

    void onJobResult(CoordinatorConnection connection, JobResultMessage result);
}
