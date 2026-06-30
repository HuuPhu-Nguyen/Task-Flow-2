package gui;

import java.io.PrintWriter;

final class TestCoordinatorConnection implements CoordinatorConnection {
    private final PrintWriter writer;
    private boolean open = true;

    TestCoordinatorConnection(PrintWriter writer) {
        this.writer = writer;
    }

    @Override
    public PrintWriter writer() {
        return writer;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
        if (writer != null) {
            writer.close();
        }
    }
}
