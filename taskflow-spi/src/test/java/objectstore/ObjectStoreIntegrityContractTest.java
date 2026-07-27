package objectstore;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ObjectStoreIntegrityContractTest {

    @Test
    void corruptPayloadIsRejectedBeforeProcessorInvocation() throws Exception {
        byte[] expected = "immutable-payload".getBytes(StandardCharsets.UTF_8);
        byte[] corrupt = expected.clone();
        corrupt[corrupt.length - 1] ^= 1;
        ObjectReference reference = new ObjectReference(
                TaskFlowObjectKeys.objectKey("inputs", "integrity-contract"),
                expected.length,
                HexFormat.of().formatHex(
                        MessageDigest.getInstance("SHA-256").digest(expected)
                ),
                "application/octet-stream"
        );
        AtomicInteger processorInvocations = new AtomicInteger();

        PayloadIntegrityException failure = assertThrows(
                PayloadIntegrityException.class,
                () -> verifyThenProcess(
                        new ByteArrayInputStream(corrupt),
                        reference,
                        processorInvocations::incrementAndGet
                )
        );

        assertEquals(PayloadIntegrityException.Mismatch.SHA256, failure.mismatch());
        assertEquals(0, processorInvocations.get());
    }

    private static void verifyThenProcess(ByteArrayInputStream content,
                                          ObjectReference reference,
                                          Runnable processor) throws Exception {
        PayloadIntegrityVerifier.readVerified(
                content,
                reference,
                reference.contentLength()
        );
        processor.run();
    }
}
