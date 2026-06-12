package gui;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.stream.Stream;

public class FileUtils {
    public static void prepareDirectories(String... folderPaths) throws IOException {
        for (String pathStr : folderPaths) {
            Path path = Paths.get(pathStr);
            if (Files.exists(path)) {
                try (Stream<Path> entries = Files.walk(path)) {
                    entries.sorted(Comparator.reverseOrder())
                            .filter(p -> !p.equals(path))
                            .forEach(FileUtils::deletePath);
                } catch (UncheckedIOException e) {
                    throw e.getCause();
                }
            } else {
                Files.createDirectories(path);
            }
        }
    }

    private static void deletePath(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
