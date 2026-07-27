package peer.processors;

import conversion.model.FilePayload;
import objectstore.ObjectReference;
import objectstore.ObjectStore;
import objectstore.ObjectStoreException;
import objectstore.ObjectStoreProvider;
import objectstore.PayloadIntegrityException;
import objectstore.TaskFlowObjectKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.PayloadLimits;
import protocol.TaskAssignMessage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Applies the shared inline threshold and immutable assignment-output policy.
 */
final class ConversionOutputPublisher {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ConversionOutputPublisher.class);
    private static final int DIGEST_BUFFER_BYTES = 64 * 1024;

    private ConversionOutputPublisher() {
    }

    static FilePayload publish(TaskAssignMessage task,
                               String fileName,
                               String contentType,
                               byte[] content,
                               ObjectStoreProvider objectStoreProvider) throws IOException {
        Objects.requireNonNull(content, "content");
        if (content.length < PayloadLimits.maxInlinePayloadBytes()) {
            return new FilePayload(
                    fileName,
                    Base64.getEncoder().encodeToString(content)
            );
        }
        ObjectReference reference = reference(
                task,
                content.length,
                sha256(content),
                contentType
        );
        try (InputStream input = new ByteArrayInputStream(content)) {
            return publishReference(
                    task,
                    fileName,
                    reference,
                    input,
                    objectStoreProvider
            );
        }
    }

    static FilePayload publish(TaskAssignMessage task,
                               String fileName,
                               String contentType,
                               Path content,
                               ObjectStoreProvider objectStoreProvider) throws IOException {
        Objects.requireNonNull(content, "content");
        long contentLength = Files.size(content);
        if (contentLength < PayloadLimits.maxInlinePayloadBytes()) {
            return new FilePayload(
                    fileName,
                    Base64.getEncoder().encodeToString(Files.readAllBytes(content))
            );
        }
        ObjectReference reference = reference(
                task,
                contentLength,
                sha256(content),
                contentType
        );
        try (InputStream input = Files.newInputStream(content)) {
            return publishReference(
                    task,
                    fileName,
                    reference,
                    input,
                    objectStoreProvider
            );
        }
    }

    private static FilePayload publishReference(TaskAssignMessage task,
                                                String fileName,
                                                ObjectReference reference,
                                                InputStream content,
                                                ObjectStoreProvider objectStoreProvider)
            throws IOException {
        String outcome;
        ObjectReference published;
        try (ObjectStore objectStore = objectStoreProvider.open()) {
            try {
                published = objectStore.putIfAbsent(reference, content);
                outcome = "CREATED";
            } catch (ObjectStoreException e) {
                if (e.reason() != ObjectStoreException.Reason.ALREADY_EXISTS) {
                    throw e;
                }
                published = requireMatchingExisting(objectStore.stat(reference.key()), reference);
                outcome = "REUSED";
            }
        }
        LOGGER.info(
                "event=attempt_output_staged job_id={} task_id={} attempt_number={} "
                        + "assignment_id={} object_key={} content_length={} outcome={}",
                task.getJobId(),
                task.getTaskId(),
                task.getAttemptNumber(),
                task.getAssignmentId(),
                published.key(),
                published.contentLength(),
                outcome
        );
        return new FilePayload(fileName, null, published);
    }

    private static ObjectReference requireMatchingExisting(ObjectReference existing,
                                                           ObjectReference expected)
            throws PayloadIntegrityException {
        if (existing.contentLength() != expected.contentLength()) {
            throw PayloadIntegrityException.lengthMismatch(
                    expected,
                    existing.contentLength()
            );
        }
        if (!existing.sha256().equals(expected.sha256())) {
            throw PayloadIntegrityException.sha256Mismatch(
                    expected,
                    existing.contentLength(),
                    existing.sha256()
            );
        }
        return existing;
    }

    private static ObjectReference reference(TaskAssignMessage task,
                                             long contentLength,
                                             String sha256,
                                             String contentType) {
        return new ObjectReference(
                TaskFlowObjectKeys.attemptOutputKey(
                        task.getJobId(),
                        task.getTaskId(),
                        task.getAttemptNumber(),
                        task.getAssignmentId()
                ),
                contentLength,
                sha256,
                contentType
        );
    }

    private static String sha256(byte[] content) {
        return HexFormat.of().formatHex(sha256Digest().digest(content));
    }

    private static String sha256(Path content) throws IOException {
        MessageDigest digest = sha256Digest();
        try (InputStream input = Files.newInputStream(content)) {
            byte[] buffer = new byte[DIGEST_BUFFER_BYTES];
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 must be available.", e);
        }
    }
}
