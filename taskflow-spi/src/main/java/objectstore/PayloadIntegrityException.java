package objectstore;

/**
 * Permanent failure raised when object bytes do not match their portable
 * reference metadata.
 */
public final class PayloadIntegrityException extends ObjectStoreException {
    public enum Mismatch {
        LENGTH,
        SHA256
    }

    private final Mismatch mismatch;
    private final String objectKey;
    private final long expectedLength;
    private final long actualLength;
    private final String expectedSha256;
    private final String actualSha256;

    private PayloadIntegrityException(Mismatch mismatch,
                                      ObjectReference reference,
                                      long actualLength,
                                      String actualSha256) {
        super(
                Reason.PAYLOAD_INTEGRITY,
                message(mismatch, reference, actualLength, actualSha256)
        );
        this.mismatch = mismatch;
        this.objectKey = reference.key();
        this.expectedLength = reference.contentLength();
        this.actualLength = actualLength;
        this.expectedSha256 = reference.sha256();
        this.actualSha256 = actualSha256;
    }

    public static PayloadIntegrityException lengthMismatch(ObjectReference reference,
                                                           long actualLength) {
        return new PayloadIntegrityException(Mismatch.LENGTH, reference, actualLength, null);
    }

    public static PayloadIntegrityException sha256Mismatch(ObjectReference reference,
                                                           long actualLength,
                                                           String actualSha256) {
        return new PayloadIntegrityException(
                Mismatch.SHA256,
                reference,
                actualLength,
                actualSha256
        );
    }

    public Mismatch mismatch() {
        return mismatch;
    }

    public String objectKey() {
        return objectKey;
    }

    public long expectedLength() {
        return expectedLength;
    }

    public long actualLength() {
        return actualLength;
    }

    public String expectedSha256() {
        return expectedSha256;
    }

    public String actualSha256() {
        return actualSha256;
    }

    private static String message(Mismatch mismatch,
                                  ObjectReference reference,
                                  long actualLength,
                                  String actualSha256) {
        if (mismatch == Mismatch.LENGTH) {
            return "Object payload length mismatch for key '" + reference.key()
                    + "': expected " + reference.contentLength()
                    + " bytes but received " + actualLength + ".";
        }
        return "Object payload SHA-256 mismatch for key '" + reference.key()
                + "': expected " + reference.sha256()
                + " but received " + actualSha256 + ".";
    }
}
