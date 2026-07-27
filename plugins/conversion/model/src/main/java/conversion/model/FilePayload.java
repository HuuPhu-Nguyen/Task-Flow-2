package conversion.model;

import objectstore.ObjectReference;

/**
 * File payload model owned by the conversion plugin.
 */
public record FilePayload(String fileName, String base64Data, ObjectReference objectReference) {
    public FilePayload(String fileName, String base64Data) {
        this(fileName, base64Data, null);
    }

    public boolean hasInlineData() {
        return base64Data != null && !base64Data.isBlank();
    }

    public boolean hasObjectReference() {
        return objectReference != null;
    }
}
