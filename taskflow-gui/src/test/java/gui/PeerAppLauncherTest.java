package gui;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerAppLauncherTest {

    @Test
    void usageDescribesGuiParticipantRolesAndClasspathPackage() {
        String usage = GuiUsage.usage();

        assertTrue(usage.contains("JavaFX participant runtime"));
        assertTrue(usage.contains("requester role"));
        assertTrue(usage.contains("executor role"));
        assertTrue(usage.contains("taskflow-gui-<version>.jar"));
        assertTrue(usage.contains("gui.PeerAppLauncher"));
        assertTrue(usage.contains("javafx.controls,javafx.fxml"));
        assertTrue(usage.contains("TASKFLOW_TRANSPORT=tcp"));
        assertTrue(usage.contains("TASKFLOW_TRANSPORT=rabbitmq"));
    }
}
