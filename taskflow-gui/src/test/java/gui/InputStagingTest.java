package gui;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class InputStagingTest {
    @TempDir
    Path tempDir;

    @Test
    void duplicateBasenamesAreStagedAsDistinctInputsWithOriginalDisplayNames() throws Exception {
        Path firstSource = tempDir.resolve("first").resolve("same-name.txt");
        Path secondSource = tempDir.resolve("second").resolve("same-name.txt");
        Files.createDirectories(firstSource.getParent());
        Files.createDirectories(secondSource.getParent());
        Files.writeString(firstSource, "first", StandardCharsets.UTF_8);
        Files.writeString(secondSource, "second", StandardCharsets.UTF_8);

        Path stagingRoot = tempDir.resolve("staging");
        List<InputStaging.StagedInput> staged = InputStaging.stageFiles(
                List.of(firstSource, secondSource),
                stagingRoot
        );

        assertEquals(2, staged.size());
        assertNotEquals(staged.get(0).stagedPath(), staged.get(1).stagedPath());
        assertEquals("same-name.txt", staged.get(0).displayName());
        assertEquals("same-name.txt", staged.get(1).displayName());
        assertEquals("same-name.txt", staged.get(0).stagedPath().getFileName().toString());
        assertEquals("same-name.txt", staged.get(1).stagedPath().getFileName().toString());
        assertEquals("000001", staged.get(0).stagedPath().getParent().getFileName().toString());
        assertEquals("000002", staged.get(1).stagedPath().getParent().getFileName().toString());
        assertEquals("first", Files.readString(staged.get(0).stagedPath(), StandardCharsets.UTF_8));
        assertEquals("second", Files.readString(staged.get(1).stagedPath(), StandardCharsets.UTF_8));
        assertIterableEquals(
                staged.stream().map(InputStaging.StagedInput::stagedPath).toList(),
                InputStaging.stagedInputFiles(stagingRoot)
        );
    }

    @Test
    void laterUploadsContinueDeterministicSlotSequence() throws Exception {
        Path firstSource = tempDir.resolve("first.txt");
        Path secondSource = tempDir.resolve("second.txt");
        Files.writeString(firstSource, "first", StandardCharsets.UTF_8);
        Files.writeString(secondSource, "second", StandardCharsets.UTF_8);

        Path stagingRoot = tempDir.resolve("staging");
        InputStaging.stageFiles(List.of(firstSource), stagingRoot);
        List<InputStaging.StagedInput> later = InputStaging.stageFiles(List.of(secondSource), stagingRoot);

        assertEquals("000002", later.getFirst().stagedPath().getParent().getFileName().toString());
        assertEquals(List.of(
                stagingRoot.resolve("000001").resolve("first.txt").toAbsolutePath().normalize(),
                stagingRoot.resolve("000002").resolve("second.txt").toAbsolutePath().normalize()
        ), InputStaging.stagedInputFiles(stagingRoot));
    }
}
