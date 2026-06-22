package gui;

import java.io.PrintWriter;

interface CoordinatorConnection extends AutoCloseable {
    PrintWriter writer();

    boolean isOpen();

    @Override
    void close();
}
