package gui;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;

public class FileUtils {
    public static void prepareDirectories(String... folderPaths) throws IOException {
        for (String pathStr : folderPaths) {
            Path path = Paths.get(pathStr);
            if (Files.exists(path)) {
                Files.walk(path)
                        .sorted(Comparator.reverseOrder())
                        .filter(p -> !p.equals(path))
                        .map(Path::toFile)
                        .forEach(File::delete);
            } else {
                Files.createDirectories(path);
            }
        }
    }
}
