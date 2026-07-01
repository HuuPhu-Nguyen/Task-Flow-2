package protocol;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class PeerIdentity {
    public static final String PEER_ID_ENV = "TASKFLOW_PEER_ID";

    private static final String DEFAULT_PREFIX = "PEER";
    private static final int MAX_PEER_ID_LENGTH = 80;

    private PeerIdentity() {
    }

    public static String configuredOrGenerated(Map<String, String> environment, String fallbackPrefix) {
        Objects.requireNonNull(environment, "environment");
        String configured = environment.get(PEER_ID_ENV);
        if (configured != null && !configured.isBlank()) {
            return require(configured);
        }
        return generated(fallbackPrefix);
    }

    public static String configuredOrGenerated(String fallbackPrefix) {
        return configuredOrGenerated(System.getenv(), fallbackPrefix);
    }

    public static String generated(String fallbackPrefix) {
        String prefix = sanitize(fallbackPrefix);
        if (prefix.isBlank()) {
            prefix = DEFAULT_PREFIX;
        }
        return truncate(prefix + "_" + UUID.randomUUID());
    }

    public static String require(String peerId) {
        String sanitized = sanitize(peerId);
        if (sanitized.isBlank()) {
            throw new IllegalArgumentException("Peer id is required.");
        }
        return sanitized;
    }

    public static String sanitize(String peerId) {
        if (peerId == null) {
            return "";
        }
        String trimmed = peerId.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        StringBuilder safe = new StringBuilder(trimmed.length());
        boolean previousWasSeparator = false;
        for (int i = 0; i < trimmed.length(); i++) {
            char ch = trimmed.charAt(i);
            if (isSafe(ch)) {
                safe.append(ch);
                previousWasSeparator = false;
            } else if (!previousWasSeparator) {
                safe.append('_');
                previousWasSeparator = true;
            }
        }
        return trimSeparators(truncate(safe.toString()));
    }

    private static boolean isSafe(char ch) {
        return (ch >= 'a' && ch <= 'z')
                || (ch >= 'A' && ch <= 'Z')
                || (ch >= '0' && ch <= '9')
                || ch == '-'
                || ch == '_';
    }

    private static String truncate(String value) {
        return value.length() <= MAX_PEER_ID_LENGTH
                ? value
                : value.substring(0, MAX_PEER_ID_LENGTH);
    }

    private static String trimSeparators(String value) {
        int start = 0;
        int end = value.length();
        while (start < end && isSeparator(value.charAt(start))) {
            start++;
        }
        while (end > start && isSeparator(value.charAt(end - 1))) {
            end--;
        }
        return value.substring(start, end);
    }

    private static boolean isSeparator(char ch) {
        return ch == '-' || ch == '_';
    }
}
