package conversion.model;

import protocol.PayloadReference;

/**
 * File payload model owned by the conversion plugin.
 */
public record FilePayload(String fileName, String base64Data, PayloadReference payloadReference) {
    public FilePayload(String fileName, String base64Data) {
        this(fileName, base64Data, null);
    }

    public boolean hasInlineData() {
        return base64Data != null && !base64Data.isBlank();
    }

    public boolean hasPayloadReference() {
        return payloadReference != null;
    }
}
