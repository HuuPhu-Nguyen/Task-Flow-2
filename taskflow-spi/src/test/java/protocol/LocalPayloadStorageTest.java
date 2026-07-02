package protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalPayloadStorageTest {
    @TempDir
    Path tempDir;

    @Test
    void storesReadsAndDeletesLocalPayloadReference() throws Exception {
        withStorageRoot(() -> {
            byte[] data = new byte[] { 1, 2, 3, 4 };

            PayloadReference reference = LocalPayloadStorage.storeBytes("../sample.bin", data);

            assertEquals(PayloadReference.LOCAL_FILE, reference.storageType());
            assertEquals(data.length, reference.sizeBytes());
            assertFalse(reference.location().contains(".."));
            assertArrayEquals(data, LocalPayloadStorage.read(reference, 10));
            assertTrue(LocalPayloadStorage.delete(reference));
            assertFalse(LocalPayloadStorage.delete(reference));
        });
    }

    @Test
    void leavesInlinePayloadsDefaultWhenStorageRootIsMissing() {
        System.clearProperty(LocalPayloadStorage.PAYLOAD_STORAGE_DIR_PROPERTY);
        System.setProperty(LocalPayloadStorage.EXTERNAL_PAYLOAD_THRESHOLD_BYTES_PROPERTY, "0");
        try {
            assertFalse(LocalPayloadStorage.shouldExternalize(10));
        } finally {
            System.clearProperty(LocalPayloadStorage.EXTERNAL_PAYLOAD_THRESHOLD_BYTES_PROPERTY);
        }
    }

    @Test
    void rejectsReferenceReadWithoutStorageRoot() throws Exception {
        System.setProperty(LocalPayloadStorage.PAYLOAD_STORAGE_DIR_PROPERTY,
                tempDir.resolve("payloads").toString());
        PayloadReference reference;
        try {
            reference = LocalPayloadStorage.storeBytes("sample.bin", new byte[] { 1, 2, 3 });
        } finally {
            System.clearProperty(LocalPayloadStorage.PAYLOAD_STORAGE_DIR_PROPERTY);
        }

        IOException error = assertThrows(IOException.class,
                () -> LocalPayloadStorage.read(reference, 10));

        assertTrue(error.getMessage().contains(LocalPayloadStorage.PAYLOAD_STORAGE_DIR_ENV));
    }

    @Test
    void rejectsChecksumMismatch() throws Exception {
        withStorageRoot(() -> {
            byte[] data = new byte[] { 1, 2, 3 };
            PayloadReference reference = LocalPayloadStorage.storeBytes("sample.bin", data);
            Files.write(tempDir.resolve("payloads").resolve(reference.location()), new byte[] { 9, 9, 9 });

            IOException error = assertThrows(IOException.class,
                    () -> LocalPayloadStorage.read(reference, 10));

            assertTrue(error.getMessage().contains("checksum mismatch"));
        });
    }

    @Test
    void rejectsReferencesOutsideStorageRoot() throws Exception {
        withStorageRoot(() -> {
            PayloadReference reference = new PayloadReference(
                    PayloadReference.LOCAL_FILE,
                    "../escape.bin",
                    1,
                    "0".repeat(64)
            );

            IOException error = assertThrows(IOException.class,
                    () -> LocalPayloadStorage.read(reference, 10));

            assertTrue(error.getMessage().contains("outside"));
        });
    }

    @Test
    void rejectsUnsupportedStorageType() throws Exception {
        PayloadReference reference = new PayloadReference(
                "s3",
                "payload.bin",
                1,
                "0".repeat(64)
        );

        IOException readError = assertThrows(IOException.class,
                () -> LocalPayloadStorage.read(reference, 10));
        IOException deleteError = assertThrows(IOException.class,
                () -> LocalPayloadStorage.delete(reference));

        assertTrue(readError.getMessage().contains("Unsupported payload reference storage type"));
        assertTrue(deleteError.getMessage().contains("Unsupported payload reference storage type"));
    }

    private void withStorageRoot(ThrowingRunnable operation) throws Exception {
        System.setProperty(LocalPayloadStorage.PAYLOAD_STORAGE_DIR_PROPERTY,
                tempDir.resolve("payloads").toString());
        try {
            operation.run();
        } finally {
            System.clearProperty(LocalPayloadStorage.PAYLOAD_STORAGE_DIR_PROPERTY);
            System.clearProperty(LocalPayloadStorage.EXTERNAL_PAYLOAD_THRESHOLD_BYTES_PROPERTY);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
