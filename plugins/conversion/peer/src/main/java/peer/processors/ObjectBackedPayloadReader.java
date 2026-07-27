package peer.processors;

import conversion.model.FilePayload;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreProvider;
import objectstore.PayloadIntegrityVerifier;
import protocol.PayloadLimits;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

final class ObjectBackedPayloadReader {
    private ObjectBackedPayloadReader() {
    }

    static byte[] readInput(FilePayload payload,
                            String taskName,
                            ObjectStoreProvider objectStoreProvider) throws IOException {
        if (payload == null || (!payload.hasInlineData() && !payload.hasObjectReference())) {
            throw new IOException(taskName + " task has no input data.");
        }
        if (payload.hasInlineData() == payload.hasObjectReference()) {
            throw new IOException(taskName
                    + " task must contain exactly one of Base64 data or an object reference: "
                    + payload.fileName());
        }

        long maxBytes = PayloadLimits.maxInputBytes();
        if (payload.hasObjectReference()) {
            ObjectReference reference = payload.objectReference();
            if (reference.contentLength() > maxBytes) {
                throw tooLarge(payload.fileName(), maxBytes);
            }
            try (ObjectStore objectStore = objectStoreProvider.open();
                 InputStream content = objectStore.get(reference.key())) {
                return PayloadIntegrityVerifier.readVerified(
                        content,
                        reference,
                        maxBytes
                );
            }
        }

        byte[] rawBytes;
        try {
            rawBytes = Base64.getDecoder().decode(payload.base64Data());
        } catch (IllegalArgumentException e) {
            throw new IOException(
                    taskName + " task payload is not valid Base64: " + payload.fileName(),
                    e
            );
        }
        if (rawBytes.length >= PayloadLimits.maxInlinePayloadBytes()) {
            throw new IOException("Inline file payload must be smaller than "
                    + PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_ENV + " ("
                    + PayloadLimits.maxInlinePayloadBytes() + " bytes): "
                    + payload.fileName());
        }
        if (rawBytes.length > maxBytes) {
            throw tooLarge(payload.fileName(), maxBytes);
        }
        return rawBytes;
    }

    private static IOException tooLarge(String fileName, long maxBytes) {
        return new IOException("Input payload exceeds " + PayloadLimits.MAX_INPUT_BYTES_ENV
                + " (" + maxBytes + " bytes): " + fileName);
    }
}
