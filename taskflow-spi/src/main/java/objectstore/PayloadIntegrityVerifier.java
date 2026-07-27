package objectstore;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Streams object bytes through exact length and SHA-256 verification.
 */
public final class PayloadIntegrityVerifier {
    private static final int COPY_BUFFER_BYTES = 64 * 1024;

    private PayloadIntegrityVerifier() {
    }

    /**
     * Returns all content only after its exact length and digest match the
     * supplied reference. The caller retains ownership of {@code content}.
     */
    public static byte[] readVerified(InputStream content,
                                      ObjectReference reference,
                                      long maxBytes) throws IOException {
        VerifyingInputStream verified = verifyingStream(content, reference, maxBytes);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[COPY_BUFFER_BYTES];
        int read;
        while ((read = verified.read(buffer)) != -1) {
            output.write(buffer, 0, read);
        }
        verified.verifyComplete();
        return output.toByteArray();
    }

    /**
     * Wraps a caller-owned stream so bytes are counted and hashed as its
     * consumer reads them. Call
     * {@link VerifyingInputStream#verifyEndOfStream()} after consumers that
     * stop at a declared content length.
     */
    public static VerifyingInputStream verifyingStream(InputStream content,
                                                       ObjectReference reference,
                                                       long maxBytes) {
        Objects.requireNonNull(content, "content");
        Objects.requireNonNull(reference, "reference");
        if (maxBytes < 0L) {
            throw new IllegalArgumentException("maxBytes must not be negative.");
        }
        if (reference.contentLength() > maxBytes) {
            throw new IllegalArgumentException(
                    "Object reference contentLength exceeds the verification bound."
            );
        }
        return new VerifyingInputStream(content, reference, maxBytes);
    }

    public static final class VerifyingInputStream extends FilterInputStream {
        private final ObjectReference reference;
        private final long maxBytes;
        private final MessageDigest digest;
        private long bytesRead;
        private boolean verified;
        private String actualSha256;

        private VerifyingInputStream(InputStream content,
                                     ObjectReference reference,
                                     long maxBytes) {
            super(content);
            this.reference = reference;
            this.maxBytes = maxBytes;
            this.digest = sha256Digest();
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value == -1) {
                verifyComplete();
                return -1;
            }
            byte[] oneByte = {(byte) value};
            record(oneByte, 0, 1);
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int read = super.read(buffer, offset, length);
            if (read == -1) {
                verifyComplete();
                return -1;
            }
            record(buffer, offset, read);
            return read;
        }

        /**
         * Verifies bytes already consumed by a bounded-length reader.
         */
        public void verifyComplete() throws PayloadIntegrityException {
            if (verified) {
                return;
            }
            if (bytesRead != reference.contentLength()) {
                throw PayloadIntegrityException.lengthMismatch(reference, bytesRead);
            }
            actualSha256 = HexFormat.of().formatHex(digest.digest());
            if (!MessageDigest.isEqual(
                    reference.sha256().getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                    actualSha256.getBytes(java.nio.charset.StandardCharsets.US_ASCII)
            )) {
                throw PayloadIntegrityException.sha256Mismatch(
                        reference,
                        bytesRead,
                        actualSha256
                );
            }
            verified = true;
        }

        /**
         * Probes the caller-owned stream for an extension before verifying.
         * Use this after a consumer that may stop at the declared length.
         */
        public void verifyEndOfStream() throws IOException {
            int extra = in.read();
            if (extra != -1) {
                byte[] oneByte = {(byte) extra};
                record(oneByte, 0, 1);
            }
            verifyComplete();
        }

        public long bytesRead() {
            return bytesRead;
        }

        private void record(byte[] buffer, int offset, int length)
                throws PayloadIntegrityException {
            long next = bytesRead + length;
            if (next < bytesRead || next > maxBytes
                    || next > reference.contentLength()) {
                bytesRead = next;
                throw PayloadIntegrityException.lengthMismatch(reference, bytesRead);
            }
            digest.update(buffer, offset, length);
            bytesRead = next;
        }

        private static MessageDigest sha256Digest() {
            try {
                return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 must be available.", e);
            }
        }
    }
}
