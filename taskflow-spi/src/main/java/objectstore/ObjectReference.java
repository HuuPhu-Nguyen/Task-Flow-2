package objectstore;

import java.util.Locale;

/**
 * Portable metadata for one TaskFlow-owned object.
 */
public record ObjectReference(
        String key,
        long contentLength,
        String sha256,
        String contentType
) {
    public ObjectReference {
        key = TaskFlowObjectKeys.requireObjectKey(key);
        if (contentLength < 0L) {
            throw new IllegalArgumentException("contentLength must not be negative.");
        }
        if (sha256 == null || sha256.isBlank()) {
            throw new IllegalArgumentException("sha256 is required.");
        }
        sha256 = sha256.trim().toLowerCase(Locale.ROOT);
        if (!sha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("sha256 must be a 64-character hexadecimal digest.");
        }
        if (contentType == null || contentType.isBlank()) {
            throw new IllegalArgumentException("contentType is required.");
        }
        contentType = contentType.trim();
        if (contentType.indexOf('\r') >= 0 || contentType.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("contentType must not contain line breaks.");
        }
    }
}
