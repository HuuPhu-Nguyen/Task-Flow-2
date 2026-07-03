package gui;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerAppLauncherTest {

    @Test
    void usageDescribesGuiClasspathPackage() {
        String usage = GuiUsage.usage();

        assertTrue(usage.contains("taskflow-gui-<version>.jar"));
        assertTrue(usage.contains("gui.PeerAppLauncher"));
        assertTrue(usage.contains("javafx.controls,javafx.fxml"));
        assertTrue(usage.contains("TASKFLOW_TRANSPORT=tcp"));
        assertTrue(usage.contains("TASKFLOW_TRANSPORT=rabbitmq"));
    }
}
