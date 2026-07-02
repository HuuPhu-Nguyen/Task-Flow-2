package protocol;

import java.util.Locale;

public record PayloadReference(String storageType, String location, long sizeBytes, String sha256) {
    public static final String LOCAL_FILE = "local-file";

    public PayloadReference {
        if (storageType == null || storageType.isBlank()) {
            throw new IllegalArgumentException("Payload reference storageType is required.");
        }
        if (location == null || location.isBlank()) {
            throw new IllegalArgumentException("Payload reference location is required.");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("Payload reference sizeBytes must not be negative.");
        }
        if (sha256 == null || sha256.isBlank()) {
            throw new IllegalArgumentException("Payload reference sha256 is required.");
        }
        storageType = storageType.trim();
        location = location.trim().replace('\\', '/');
        sha256 = sha256.trim().toLowerCase(Locale.ROOT);
        if (!sha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Payload reference sha256 must be a hex SHA-256 digest.");
        }
    }

    public boolean isLocalFile() {
        return LOCAL_FILE.equals(storageType);
    }
}
