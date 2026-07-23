package gui;

final class TestCoordinatorConnection implements CoordinatorConnection {
    private boolean open = true;

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
    }
}
