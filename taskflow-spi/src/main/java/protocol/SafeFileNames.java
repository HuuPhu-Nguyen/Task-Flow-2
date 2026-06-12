package protocol;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;

public final class SafeFileNames {
    public static final String DEFAULT_OUTPUT_FILE_NAME = "taskflow-output";

    private static final Pattern UNSAFE_FILE_NAME_CHARS = Pattern.compile("[\\p{Cntrl}\\\\/:*?\"<>|]+");
    private static final Pattern RESERVED_WINDOWS_NAME = Pattern.compile(
            "(?i)^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\\..*)?$"
    );

    private SafeFileNames() {
    }

    public static String sanitize(String fileName) {
        return sanitize(fileName, DEFAULT_OUTPUT_FILE_NAME);
    }

    public static String sanitize(String fileName, String fallback) {
        String safeFallback = fallbackName(fallback);
        if (fileName == null || fileName.isBlank()) {
            return safeFallback;
        }

        String normalized = fileName.replace('\\', '/');
        int slash = normalized.lastIndexOf('/');
        String baseName = slash >= 0 ? normalized.substring(slash + 1) : normalized;
        String safeName = UNSAFE_FILE_NAME_CHARS.matcher(baseName).replaceAll("_").trim();

        while (safeName.endsWith(".") || safeName.endsWith(" ")) {
            safeName = safeName.substring(0, safeName.length() - 1);
        }

        if (safeName.isBlank() || ".".equals(safeName) || "..".equals(safeName)) {
            return safeFallback;
        }
        if (RESERVED_WINDOWS_NAME.matcher(safeName).matches()) {
            return "_" + safeName;
        }
        return safeName;
    }

    public static Path safeOutputPath(Path outputDir, String fileName) throws IOException {
        return safeOutputPath(outputDir, fileName, DEFAULT_OUTPUT_FILE_NAME);
    }

    public static Path safeOutputPath(Path outputDir, String fileName, String fallback) throws IOException {
        Path root = Objects.requireNonNull(outputDir, "outputDir").toAbsolutePath().normalize();
        Path outputPath = root.resolve(sanitize(fileName, fallback)).normalize();
        if (!outputPath.startsWith(root)) {
            throw new IOException("Refusing to write outside the selected folder: " + fileName);
        }
        return outputPath;
    }

    private static String fallbackName(String fallback) {
        if (fallback == null || fallback.isBlank()) {
            return DEFAULT_OUTPUT_FILE_NAME;
        }
        String normalized = fallback.replace('\\', '/');
        int slash = normalized.lastIndexOf('/');
        String baseName = slash >= 0 ? normalized.substring(slash + 1) : normalized;
        String safeFallback = UNSAFE_FILE_NAME_CHARS.matcher(baseName).replaceAll("_").trim();
        while (safeFallback.endsWith(".") || safeFallback.endsWith(" ")) {
            safeFallback = safeFallback.substring(0, safeFallback.length() - 1);
        }
        if (safeFallback.isBlank() || ".".equals(safeFallback) || "..".equals(safeFallback)) {
            return DEFAULT_OUTPUT_FILE_NAME;
        }
        if (RESERVED_WINDOWS_NAME.matcher(safeFallback).matches()) {
            return "_" + safeFallback;
        }
        return safeFallback;
    }
}
