package gui;

import protocol.RequesterTokens;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

final class FileGuiRequesterTokenStore implements GuiRequesterTokenStore {
    private static final String STORE_ENV = "TASKFLOW_GUI_REQUESTER_TOKEN_STORE";
    private static final Set<PosixFilePermission> OWNER_FILE_PERMISSIONS = EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE
    );
    private static final Set<PosixFilePermission> OWNER_DIRECTORY_PERMISSIONS = EnumSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE
    );

    private final Path path;

    FileGuiRequesterTokenStore(Path path) {
        this.path = Objects.requireNonNull(path, "path");
    }

    static FileGuiRequesterTokenStore openDefault() {
        String configured = System.getenv(STORE_ENV);
        if (configured != null && !configured.isBlank()) {
            return new FileGuiRequesterTokenStore(Path.of(configured.trim()));
        }
        return new FileGuiRequesterTokenStore(Path.of(
                System.getProperty("user.home"),
                ".taskflow",
                "gui-requester-tokens.properties"
        ));
    }

    @Override
    public synchronized String createTokenForJob(String jobId) {
        String normalizedJobId = requireJobId(jobId);
        Properties tokens = load();
        String token = RequesterTokens.newToken();
        tokens.setProperty(normalizedJobId, token);
        save(tokens);
        return token;
    }

    @Override
    public synchronized Optional<String> tokenForJob(String jobId) {
        String normalizedJobId = requireJobId(jobId);
        String token = load().getProperty(normalizedJobId);
        if (!RequesterTokens.hasToken(token)) {
            return Optional.empty();
        }
        return Optional.of(token);
    }

    @Override
    public synchronized void forgetToken(String jobId) {
        String normalizedJobId = requireJobId(jobId);
        Properties tokens = load();
        if (tokens.remove(normalizedJobId) != null) {
            save(tokens);
        }
    }

    private Properties load() {
        Properties tokens = new Properties();
        if (!Files.exists(path)) {
            return tokens;
        }
        try (InputStream input = Files.newInputStream(path)) {
            tokens.load(input);
            return tokens;
        } catch (IOException e) {
            throw new IllegalStateException("Could not read requester token store.", e);
        }
    }

    private void save(Properties tokens) {
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
                restrictPermissions(parent, OWNER_DIRECTORY_PERMISSIONS);
            }
            try (OutputStream output = Files.newOutputStream(path)) {
                tokens.store(output, "TaskFlow GUI requester tokens");
            }
            restrictPermissions(path, OWNER_FILE_PERMISSIONS);
        } catch (IOException e) {
            throw new IllegalStateException("Could not write requester token store.", e);
        }
    }

    private static String requireJobId(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("jobId is required.");
        }
        return jobId.trim();
    }

    private static void restrictPermissions(Path path, Set<PosixFilePermission> permissions) {
        try {
            Files.setPosixFilePermissions(path, permissions);
        } catch (IOException | UnsupportedOperationException ignored) {
        }
    }
}
