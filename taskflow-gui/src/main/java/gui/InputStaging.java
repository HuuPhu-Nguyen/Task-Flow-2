package gui;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Stream;

final class InputStaging {
    private static final Pattern SLOT_DIRECTORY = Pattern.compile("\\d{6}");

    private InputStaging() {
    }

    static List<StagedInput> stageFiles(List<Path> sourcePaths, Path stagingRoot) throws IOException {
        if (sourcePaths == null || sourcePaths.isEmpty()) {
            return List.of();
        }

        Path root = stagingRoot.toAbsolutePath().normalize();
        Files.createDirectories(root);

        int nextSlot = nextSlot(root);
        List<StagedInput> stagedInputs = new ArrayList<>();
        for (Path sourcePath : sourcePaths) {
            Path source = sourcePath.toAbsolutePath().normalize();
            if (!Files.isRegularFile(source)) {
                throw new IOException("Input file does not exist: " + source);
            }

            String displayName = displayName(source);
            Path slotDir = root.resolve(String.format(Locale.ROOT, "%06d", nextSlot++));
            Files.createDirectories(slotDir);
            Path stagedPath = slotDir.resolve(displayName);
            Files.copy(source, stagedPath, StandardCopyOption.REPLACE_EXISTING);
            stagedInputs.add(new StagedInput(source, stagedPath, displayName));
        }
        return stagedInputs;
    }

    static List<Path> stagedInputFiles(Path stagingRoot) throws IOException {
        Path root = stagingRoot.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            return List.of();
        }
        try (Stream<Path> paths = Files.walk(root, 2)) {
            return paths.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(path -> root.relativize(path).toString()))
                    .toList();
        }
    }

    static void clear(Path stagingRoot) throws IOException {
        Path root = stagingRoot.toAbsolutePath().normalize();
        if (!Files.exists(root)) {
            Files.createDirectories(root);
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder())
                    .filter(path -> !path.equals(root))
                    .forEach(InputStaging::deletePath);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static int nextSlot(Path root) throws IOException {
        int maxSlot = 0;
        try (Stream<Path> paths = Files.list(root)) {
            for (Path path : paths.filter(Files::isDirectory).toList()) {
                String name = path.getFileName().toString();
                if (SLOT_DIRECTORY.matcher(name).matches()) {
                    maxSlot = Math.max(maxSlot, Integer.parseInt(name));
                }
            }
        }
        return maxSlot + 1;
    }

    private static String displayName(Path source) {
        Path fileName = source.getFileName();
        if (fileName == null || fileName.toString().isBlank()) {
            return "input";
        }
        return fileName.toString();
    }

    private static void deletePath(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    record StagedInput(Path sourcePath, Path stagedPath, String displayName) {
    }
}
