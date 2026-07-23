package gui;

interface CoordinatorConnection extends AutoCloseable {
    boolean isOpen();

    @Override
    void close();
}
