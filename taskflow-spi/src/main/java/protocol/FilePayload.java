package protocol;

/**
 * Stores the Base64 string version of the file for transit.
 */
public record FilePayload(String fileName, String base64Data) {}