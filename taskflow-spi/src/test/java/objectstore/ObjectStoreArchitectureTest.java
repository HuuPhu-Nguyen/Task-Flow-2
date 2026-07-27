package objectstore;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectStoreArchitectureTest {
    @Test
    void frameworkPortAndCoordinatorStateRemainIndependentFromMinioSdk() throws IOException {
        Path repository = repositoryRoot();
        for (String relativeRoot : List.of(
                "taskflow-spi/src/main/java",
                "taskflow-core/src/main/java",
                "taskflow-persistence-sqlite/src/main/java",
                "taskflow-coordinator/src/main/java"
        )) {
            try (var sources = Files.walk(repository.resolve(relativeRoot))) {
                for (Path source : sources.filter(path -> path.toString().endsWith(".java")).toList()) {
                    assertFalse(
                            Files.readString(source).contains("io.minio"),
                            source + " must not depend on the MinIO SDK"
                    );
                }
            }
        }

        String adapter = Files.readString(
                repository.resolve(
                        "taskflow-objectstore-minio/src/main/java/objectstore/minio/MinioObjectStore.java"
                )
        );
        assertTrue(adapter.contains("implements ObjectStore"));
        assertTrue(adapter.contains("import io.minio.MinioClient;"));

        String adapterPom = Files.readString(
                repository.resolve("taskflow-objectstore-minio/pom.xml")
        );
        assertFalse(adapterPom.contains("taskflow-persistence-sqlite"));
        assertFalse(adapterPom.contains("taskflow-core"));
        for (String stateOwnerPom : List.of(
                "taskflow-persistence-sqlite/pom.xml",
                "taskflow-coordinator/pom.xml"
        )) {
            assertFalse(
                    Files.readString(repository.resolve(stateOwnerPom))
                            .contains("taskflow-objectstore-minio"),
                    stateOwnerPom + " must not make MinIO an authority for coordinator state"
            );
        }
    }

    private static Path repositoryRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isDirectory(current.resolve("taskflow-spi"))
                    && Files.isDirectory(current.resolve("taskflow-core"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Could not locate the TaskFlow repository root.");
    }
}
