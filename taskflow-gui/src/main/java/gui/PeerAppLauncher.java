package gui;

public final class PeerAppLauncher {

    private PeerAppLauncher() {
    }

    public static void main(String[] args) {
        if (GuiUsage.isHelpRequested(args)) {
            System.out.println(GuiUsage.usage());
            return;
        }
        PeerApp.main(args);
    }
}
