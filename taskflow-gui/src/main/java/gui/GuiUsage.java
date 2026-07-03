package gui;

final class GuiUsage {

    private GuiUsage() {
    }

    static boolean isHelpRequested(String[] args) {
        return args != null
                && args.length > 0
                && ("--help".equals(args[0]) || "-h".equals(args[0]));
    }

    static String usage() {
        return String.join(System.lineSeparator(),
                "TASKFLOW_TRANSPORT=tcp java --module-path <lib> --add-modules javafx.controls,javafx.fxml -cp \"taskflow-gui-<version>.jar;<lib>/*\" gui.PeerAppLauncher",
                "TASKFLOW_TRANSPORT=rabbitmq java --module-path <lib> --add-modules javafx.controls,javafx.fxml -cp \"taskflow-gui-<version>.jar;<lib>/*\" gui.PeerAppLauncher");
    }
}
