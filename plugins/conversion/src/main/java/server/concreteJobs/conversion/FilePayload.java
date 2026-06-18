package server.concreteJobs.conversion;

/**
 * File payload model owned by the conversion plugin.
 */
public record FilePayload(String fileName, String base64Data) {
}
