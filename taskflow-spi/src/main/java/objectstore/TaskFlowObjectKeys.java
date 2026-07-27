package objectstore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Generates and validates keys inside TaskFlow's controlled object namespace.
 */
public final class TaskFlowObjectKeys {
    public static final String ROOT_PREFIX = "taskflow/";

    private static final int MAX_KEY_BYTES = 1024;
    private static final int MAX_SEGMENT_LENGTH = 255;
    private static final Pattern SAFE_SEGMENT = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");

    private TaskFlowObjectKeys() {
    }

    public static String objectKey(String... segments) {
        List<String> validated = validatedSegments(segments);
        if (validated.isEmpty()) {
            throw new IllegalArgumentException("At least one object-key segment is required.");
        }
        return requireObjectKey(ROOT_PREFIX + String.join("/", validated));
    }

    public static String prefix(String... segments) {
        List<String> validated = validatedSegments(segments);
        if (validated.isEmpty()) {
            return ROOT_PREFIX;
        }
        return requirePrefix(ROOT_PREFIX + String.join("/", validated) + "/");
    }

    /**
     * Returns the immutable output key owned by one exact assignment attempt.
     */
    public static String attemptOutputKey(String jobId,
                                          String taskId,
                                          int attemptNumber,
                                          String assignmentId) {
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("Attempt number must be positive.");
        }
        String canonicalAssignmentId;
        try {
            canonicalAssignmentId = UUID.fromString(
                    requireText(assignmentId, "Assignment id")
            ).toString();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Assignment id must be a canonical UUID.", e);
        }
        if (!canonicalAssignmentId.equalsIgnoreCase(assignmentId)) {
            throw new IllegalArgumentException("Assignment id must be a canonical UUID.");
        }
        return objectKey(
                "jobs",
                requireText(jobId, "Job id"),
                "tasks",
                requireText(taskId, "Task id"),
                "attempts",
                Integer.toString(attemptNumber),
                canonicalAssignmentId,
                "output"
        );
    }

    public static String requireObjectKey(String key) {
        String candidate = requireText(key, "Object key");
        if (candidate.endsWith("/")) {
            throw new IllegalArgumentException("Object key must identify an object, not a prefix.");
        }
        validateControlledName(candidate, false);
        return candidate;
    }

    public static String requirePrefix(String prefix) {
        String candidate = requireText(prefix, "Object prefix");
        if (!candidate.endsWith("/")) {
            throw new IllegalArgumentException("Object prefix must end with '/'.");
        }
        validateControlledName(candidate, true);
        return candidate;
    }

    public static String requireStartAfter(String prefix, String startAfter) {
        String validatedPrefix = requirePrefix(prefix);
        String validatedKey = requireObjectKey(startAfter);
        if (!validatedKey.startsWith(validatedPrefix)) {
            throw new IllegalArgumentException("startAfter must be inside the requested TaskFlow prefix.");
        }
        return validatedKey;
    }

    private static void validateControlledName(String candidate, boolean prefix) {
        if (!candidate.startsWith(ROOT_PREFIX)) {
            throw new IllegalArgumentException("Object names must remain inside '" + ROOT_PREFIX + "'.");
        }
        if (candidate.indexOf('\\') >= 0 || candidate.contains("//")) {
            throw new IllegalArgumentException("Object names must use non-empty '/'-separated segments.");
        }
        if (candidate.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("Object name exceeds " + MAX_KEY_BYTES + " UTF-8 bytes.");
        }

        String relative = candidate.substring(ROOT_PREFIX.length());
        if (prefix && relative.endsWith("/")) {
            relative = relative.substring(0, relative.length() - 1);
        }
        if (relative.isEmpty()) {
            if (prefix) {
                return;
            }
            throw new IllegalArgumentException("Object key must contain a segment after the TaskFlow prefix.");
        }
        for (String segment : relative.split("/", -1)) {
            validateSegment(segment);
        }
    }

    private static List<String> validatedSegments(String[] segments) {
        Objects.requireNonNull(segments, "segments");
        List<String> validated = new ArrayList<>(segments.length);
        for (String segment : segments) {
            String candidate = requireText(segment, "Object-key segment");
            validateSegment(candidate);
            validated.add(candidate);
        }
        return validated;
    }

    private static void validateSegment(String segment) {
        if (segment.length() > MAX_SEGMENT_LENGTH) {
            throw new IllegalArgumentException(
                    "Object-key segment exceeds " + MAX_SEGMENT_LENGTH + " characters."
            );
        }
        if (".".equals(segment) || "..".equals(segment) || !SAFE_SEGMENT.matcher(segment).matches()) {
            throw new IllegalArgumentException("Unsafe TaskFlow object-key segment: " + segment);
        }
    }

    private static String requireText(String value, String description) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(description + " is required.");
        }
        String candidate = value.trim();
        if (!candidate.equals(value)) {
            throw new IllegalArgumentException(description + " must not contain surrounding whitespace.");
        }
        for (int index = 0; index < candidate.length(); index++) {
            if (Character.isISOControl(candidate.charAt(index))) {
                throw new IllegalArgumentException(description + " must not contain control characters.");
            }
        }
        return candidate;
    }
}
