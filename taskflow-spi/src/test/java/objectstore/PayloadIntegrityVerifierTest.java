package objectstore;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PayloadIntegrityVerifierTest {
    @Test
    void returnsBytesOnlyAfterExactLengthAndDigestVerification() throws Exception {
        byte[] content = "verified content".getBytes(StandardCharsets.UTF_8);
        ObjectReference reference = reference(content);

        byte[] verified = PayloadIntegrityVerifier.readVerified(
                new ByteArrayInputStream(content),
                reference,
                content.length
        );

        assertArrayEquals(content, verified);
    }

    @Test
    void rejectsTruncatedAndExtendedContentWithObservedLengths() {
        byte[] expected = "expected".getBytes(StandardCharsets.UTF_8);
        byte[] truncated = "expecte".getBytes(StandardCharsets.UTF_8);
        byte[] extended = "expected!".getBytes(StandardCharsets.UTF_8);
        ObjectReference reference = reference(expected);

        PayloadIntegrityException shortFailure = assertThrows(
                PayloadIntegrityException.class,
                () -> PayloadIntegrityVerifier.readVerified(
                        new ByteArrayInputStream(truncated),
                        reference,
                        expected.length
                )
        );
        PayloadIntegrityException longFailure = assertThrows(
                PayloadIntegrityException.class,
                () -> PayloadIntegrityVerifier.readVerified(
                        new ByteArrayInputStream(extended),
                        reference,
                        extended.length
                )
        );

        assertEquals(PayloadIntegrityException.Mismatch.LENGTH, shortFailure.mismatch());
        assertEquals(truncated.length, shortFailure.actualLength());
        assertEquals(PayloadIntegrityException.Mismatch.LENGTH, longFailure.mismatch());
        assertEquals(extended.length, longFailure.actualLength());
    }

    @Test
    void rejectsSameLengthCorruptionWithObservedDigest() {
        byte[] expected = "expected".getBytes(StandardCharsets.UTF_8);
        byte[] corrupt = "corrupt!".getBytes(StandardCharsets.UTF_8);
        ObjectReference reference = reference(expected);

        PayloadIntegrityException failure = assertThrows(
                PayloadIntegrityException.class,
                () -> PayloadIntegrityVerifier.readVerified(
                        new ByteArrayInputStream(corrupt),
                        reference,
                        corrupt.length
                )
        );

        assertEquals(PayloadIntegrityException.Mismatch.SHA256, failure.mismatch());
        assertEquals(reference.sha256(), failure.expectedSha256());
        assertEquals(sha256(corrupt), failure.actualSha256());
        assertFalse(failure.expectedSha256().equals(failure.actualSha256()));
    }

    @Test
    void boundedConsumerCanVerifyAfterReadingExactlyTheDeclaredLength() throws Exception {
        byte[] content = "stream me".getBytes(StandardCharsets.UTF_8);
        ObjectReference reference = reference(content);
        PayloadIntegrityVerifier.VerifyingInputStream stream =
                PayloadIntegrityVerifier.verifyingStream(
                        new ByteArrayInputStream(content),
                        reference,
                        content.length
                );

        assertArrayEquals(content, stream.readNBytes(content.length));
        stream.verifyEndOfStream();
        assertEquals(content.length, stream.bytesRead());
    }

    private static ObjectReference reference(byte[] content) {
        return new ObjectReference(
                TaskFlowObjectKeys.objectKey("inputs", "integrity-test"),
                content.length,
                sha256(content),
                "application/octet-stream"
        );
    }

    private static String sha256(byte[] content) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(content)
            );
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 must be available.", e);
        }
    }
}
