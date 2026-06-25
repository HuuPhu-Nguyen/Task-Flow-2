package gui;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BooleanSupplier;

final class GuiInputStagingService {
    private final Path inputRoot;
    private final Path outputRoot;

    static GuiInputStagingService forSession(String sessionId) {
        Objects.requireNonNull(sessionId, "sessionId");
        return new GuiInputStagingService(
                Paths.get("java/in_" + sessionId),
                Paths.get("java/out_" + sessionId));
    }

    GuiInputStagingService(Path inputRoot, Path outputRoot) {
        this.inputRoot = Objects.requireNonNull(inputRoot, "inputRoot");
        this.outputRoot = Objects.requireNonNull(outputRoot, "outputRoot");
    }

    void prepareDirectories() throws IOException {
        FileUtils.prepareDirectories(inputRoot.toString(), outputRoot.toString());
    }

    List<InputStaging.StagedInput> stageFiles(List<Path> sources, BooleanSupplier cancelled) throws IOException {
        return InputStaging.stageFiles(sources, inputRoot, cancelled);
    }

    List<Path> stagedInputFiles() throws IOException {
        return InputStaging.stagedInputFiles(inputRoot);
    }

    void clearStagedInputs() throws IOException {
        InputStaging.clear(inputRoot);
    }

    static String fileChooserPattern(String extension) {
        if (extension == null || extension.isBlank()) {
            return "*.*";
        }
        String value = extension.trim().toLowerCase(Locale.ROOT);
        if (value.startsWith("*.")) {
            return value;
        }
        if (value.startsWith(".")) {
            return "*" + value;
        }
        return "*." + value;
    }
}
