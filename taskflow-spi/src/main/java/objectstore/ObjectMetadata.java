package objectstore;

/**
 * Integrity metadata plus the last-modification time of one stored object.
 *
 * <p>TaskFlow output objects are never overwritten, so an S3-compatible
 * object's last-modified timestamp is also its creation timestamp. Garbage
 * collection uses this timestamp rather than assignment age so a stale
 * executor that uploads late still receives the complete safety window.</p>
 */
public record ObjectMetadata(
        ObjectReference reference,
        long lastModifiedAtEpochMillis
) {
    public ObjectMetadata {
        if (reference == null) {
            throw new IllegalArgumentException("reference is required.");
        }
        if (lastModifiedAtEpochMillis < 0L) {
            throw new IllegalArgumentException(
                    "lastModifiedAtEpochMillis must not be negative."
            );
        }
    }

    public String key() {
        return reference.key();
    }
}
