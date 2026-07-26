package protocol;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Objects;

public final class ProtocolVersions {
    public static final String FIELD_NAME = "protocolVersion";
    public static final int LEGACY = 0;
    public static final int VERSION_1 = 1;
    public static final int ASSIGNMENT_IDENTITY = 2;
    public static final int CAPACITY_ADVERTISEMENT = 3;
    public static final int ENVELOPE_CURRENT = ASSIGNMENT_IDENTITY;
    public static final int CURRENT = ASSIGNMENT_IDENTITY;
    public static final int MAX_MESSAGE_SUPPORTED = CAPACITY_ADVERTISEMENT;
    public static final int MIN_SUPPORTED = LEGACY;

    private ProtocolVersions() {
    }

    public static int read(JsonObject object, String context) {
        Objects.requireNonNull(object, "object");
        JsonElement versionElement = object.get(FIELD_NAME);
        if (versionElement == null || versionElement.isJsonNull()) {
            return LEGACY;
        }
        if (!versionElement.isJsonPrimitive()) {
            throw invalidVersion(context, null);
        }
        String rawVersion;
        try {
            rawVersion = versionElement.getAsString();
        } catch (UnsupportedOperationException | IllegalStateException e) {
            throw invalidVersion(context, e);
        }
        if (rawVersion == null || rawVersion.isBlank() || !rawVersion.trim().matches("[+-]?\\d+")) {
            throw invalidVersion(context, null);
        }
        try {
            return Integer.parseInt(rawVersion.trim());
        } catch (NumberFormatException e) {
            throw invalidVersion(context, e);
        }
    }

    public static int requireSupported(JsonObject object, String context) {
        int version = read(object, context);
        requireSupported(version, context);
        return version;
    }

    public static int normalizeSupportedVersion(JsonObject object, String context) {
        int version = requireSupported(object, context);
        object.addProperty(FIELD_NAME, version);
        return version;
    }

    public static int normalizeSupportedMessageVersion(JsonObject object, String context) {
        int version = read(object, context);
        requireMessageSupported(version, context);
        object.addProperty(FIELD_NAME, version);
        return version;
    }

    public static void requireSupported(int version, String context) {
        if (version < MIN_SUPPORTED || version > CURRENT) {
            throw new IllegalArgumentException(label(context)
                    + " uses unsupported TaskFlow protocolVersion " + version
                    + "; supported versions are " + supportedRange() + ".");
        }
    }

    public static void requireMessageSupported(int version, String context) {
        if (version < MIN_SUPPORTED || version > MAX_MESSAGE_SUPPORTED) {
            throw new IllegalArgumentException(label(context)
                    + " uses unsupported TaskFlow protocolVersion " + version
                    + "; supported message versions are " + MIN_SUPPORTED
                    + " through " + MAX_MESSAGE_SUPPORTED + ".");
        }
    }

    public static String supportedRange() {
        return MIN_SUPPORTED + " through " + CURRENT;
    }

    private static IllegalArgumentException invalidVersion(String context, Exception cause) {
        String message = label(context) + " protocolVersion must be an integer.";
        return cause == null ? new IllegalArgumentException(message) : new IllegalArgumentException(message, cause);
    }

    private static String label(String context) {
        return context == null || context.isBlank() ? "Message" : context;
    }
}
