package protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SafeFileNamesTest {

    @TempDir
    Path tempDir;

    @Test
    void sanitizeKeepsOnlyBaseNameAndRemovesUnsafeCharacters() {
        assertEquals("report_final_.png",
                SafeFileNames.sanitize("../nested/report:final?.png"));
        assertEquals("image.jpg",
                SafeFileNames.sanitize("C:\\users\\student\\image.jpg"));
    }

    @Test
    void sanitizeUsesFallbackForBlankDotAndReservedNames() {
        assertEquals("result.bin", SafeFileNames.sanitize("..", "result.bin"));
        assertEquals("result.bin", SafeFileNames.sanitize("   ", "result.bin"));
        assertEquals("_CON.txt", SafeFileNames.sanitize("CON.txt", "result.bin"));
        assertEquals("taskflow-output", SafeFileNames.sanitize("..", "../"));
    }

    @Test
    void safeOutputPathStaysInsideOutputDirectory() throws Exception {
        Path outputPath = SafeFileNames.safeOutputPath(tempDir, "../escape.txt", "result.bin");

        assertTrue(outputPath.startsWith(tempDir.toAbsolutePath().normalize()));
        assertEquals("escape.txt", outputPath.getFileName().toString());
    }
}
