package protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class LocalPayloadStorage {
    public static final long DEFAULT_EXTERNAL_PAYLOAD_THRESHOLD_BYTES = 8L * 1024L * 1024L;
    public static final String PAYLOAD_STORAGE_DIR_PROPERTY = "taskflow.payloadStorageDir";
    public static final String PAYLOAD_STORAGE_DIR_ENV = "TASKFLOW_PAYLOAD_STORAGE_DIR";
    public static final String EXTERNAL_PAYLOAD_THRESHOLD_BYTES_PROPERTY =
            "taskflow.externalPayloadThresholdBytes";
    public static final String EXTERNAL_PAYLOAD_THRESHOLD_BYTES_ENV =
            "TASKFLOW_EXTERNAL_PAYLOAD_THRESHOLD_BYTES";

    private static final int BUFFER_SIZE = 8192;

    private LocalPayloadStorage() {
    }

    public static Optional<Path> storageRoot() {
        String configured = configuredValue(PAYLOAD_STORAGE_DIR_PROPERTY, PAYLOAD_STORAGE_DIR_ENV);
        if (configured == null) {
            return Optional.empty();
        }
        return Optional.of(Path.of(configured).toAbsolutePath().normalize());
    }

    public static long externalPayloadThresholdBytes() {
        String configured = configuredValue(
                EXTERNAL_PAYLOAD_THRESHOLD_BYTES_PROPERTY,
                EXTERNAL_PAYLOAD_THRESHOLD_BYTES_ENV
        );
        if (configured == null) {
            return DEFAULT_EXTERNAL_PAYLOAD_THRESHOLD_BYTES;
        }
        long parsed = Long.parseLong(configured);
        if (parsed < 0) {
            throw new IllegalArgumentException(EXTERNAL_PAYLOAD_THRESHOLD_BYTES_ENV + " must not be negative.");
        }
        return parsed;
    }

    public static boolean shouldExternalize(long sizeBytes) {
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must not be negative.");
        }
        return storageRoot().isPresent() && sizeBytes >= externalPayloadThresholdBytes();
    }

    public static PayloadReference storeFile(Path source, String fileName) throws IOException {
        Path input = Objects.requireNonNull(source, "source").toAbsolutePath().normalize();
        if (!Files.isRegularFile(input)) {
            throw new IOException("Payload source file does not exist: " + input);
        }

        StoredPayloadTarget target = newTarget(fileName);
        MessageDigest digest = sha256();
        long sizeBytes = 0L;
        try (InputStream in = Files.newInputStream(input);
             OutputStream out = Files.newOutputStream(target.path())) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                if (read == 0) {
                    continue;
                }
                digest.update(buffer, 0, read);
                out.write(buffer, 0, read);
                sizeBytes += read;
            }
        }
        return new PayloadReference(
                PayloadReference.LOCAL_FILE,
                target.location(),
                sizeBytes,
                HexFormat.of().formatHex(digest.digest())
        );
    }

    public static PayloadReference storeBytes(String fileName, byte[] data) throws IOException {
        byte[] bytes = Objects.requireNonNull(data, "data");
        StoredPayloadTarget target = newTarget(fileName);
        Files.write(target.path(), bytes);
        return new PayloadReference(
                PayloadReference.LOCAL_FILE,
                target.location(),
                bytes.length,
                HexFormat.of().formatHex(sha256().digest(bytes))
        );
    }

    public static byte[] read(PayloadReference reference, long maxBytes) throws IOException {
        Objects.requireNonNull(reference, "reference");
        if (!reference.isLocalFile()) {
            throw new IOException("Unsupported payload reference storage type: " + reference.storageType());
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive.");
        }

        Path path = resolve(reference);
        if (!Files.isRegularFile(path)) {
            throw new IOException("Payload reference file does not exist: " + reference.location());
        }
        long sizeBytes = Files.size(path);
        if (sizeBytes != reference.sizeBytes()) {
            throw new IOException("Payload reference size mismatch for " + reference.location());
        }
        if (sizeBytes > maxBytes) {
            throw new IOException("Payload reference exceeds configured limit (" + maxBytes
                    + " bytes): " + reference.location());
        }

        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length != reference.sizeBytes()) {
            throw new IOException("Payload reference size mismatch for " + reference.location());
        }
        if (bytes.length > maxBytes) {
            throw new IOException("Payload reference exceeds configured limit (" + maxBytes
                    + " bytes): " + reference.location());
        }
        String actualHash = HexFormat.of().formatHex(sha256().digest(bytes));
        if (!actualHash.equalsIgnoreCase(reference.sha256())) {
            throw new IOException("Payload reference checksum mismatch for " + reference.location());
        }
        return bytes;
    }

    public static boolean delete(PayloadReference reference) throws IOException {
        Objects.requireNonNull(reference, "reference");
        if (!reference.isLocalFile()) {
            throw new IOException("Unsupported payload reference storage type: " + reference.storageType());
        }
        Path path = resolve(reference);
        return Files.deleteIfExists(path);
    }

    private static StoredPayloadTarget newTarget(String fileName) throws IOException {
        Path root = requireStorageRoot();
        Files.createDirectories(root);

        String safeFileName = SafeFileNames.sanitize(fileName, "payload.bin");
        String location = UUID.randomUUID() + "/" + safeFileName;
        Path path = root.resolve(location).normalize();
        if (!path.startsWith(root)) {
            throw new IOException("Refusing to store payload outside " + PAYLOAD_STORAGE_DIR_ENV);
        }
        Files.createDirectories(path.getParent());
        return new StoredPayloadTarget(path, location);
    }

    private static Path resolve(PayloadReference reference) throws IOException {
        Path root = requireStorageRoot();
        Path path = root.resolve(reference.location()).normalize();
        if (!path.startsWith(root)) {
            throw new IOException("Refusing to read payload outside " + PAYLOAD_STORAGE_DIR_ENV
                    + ": " + reference.location());
        }
        return path;
    }

    private static Path requireStorageRoot() throws IOException {
        return storageRoot().orElseThrow(() -> new IOException(PAYLOAD_STORAGE_DIR_ENV
                + " is required for local payload references."));
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String configuredValue(String propertyName, String envName) {
        String configured = System.getProperty(propertyName);
        if (configured == null || configured.isBlank()) {
            configured = System.getenv(envName);
        }
        if (configured == null || configured.isBlank()) {
            return null;
        }
        return configured.trim();
    }

    private record StoredPayloadTarget(Path path, String location) {
    }
}
