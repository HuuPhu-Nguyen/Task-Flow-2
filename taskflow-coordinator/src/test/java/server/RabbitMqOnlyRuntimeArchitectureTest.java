package server;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitMqOnlyRuntimeArchitectureTest {
    private static final List<String> RUNTIME_MODULES = List.of(
            "taskflow-spi",
            "taskflow-core",
            "taskflow-coordinator",
            "taskflow-peer",
            "taskflow-gui"
    );

    @Test
    void defaultBuildAndRuntimePackagesDeclareOnlyRabbitMqTransport() throws IOException {
        Path root = repositoryRoot();
        String reactor = Files.readString(root.resolve("pom.xml"));
        assertTrue(reactor.contains("<module>taskflow-transport-rabbitmq</module>"));
        assertFalse(reactor.toLowerCase().contains("transport-tcp"));

        for (String module : List.of(
                "taskflow-coordinator",
                "taskflow-peer",
                "taskflow-gui"
        )) {
            String pom = Files.readString(root.resolve(module).resolve("pom.xml"));
            assertTrue(pom.contains("<artifactId>taskflow-transport-rabbitmq</artifactId>"),
                    module + " must package the RabbitMQ transport");
            assertFalse(pom.toLowerCase().contains("transport-tcp"),
                    module + " must not package a TCP transport");
        }
    }

    @Test
    void runtimeSourcesContainNoLegacySocketTransportOrSelector() throws IOException {
        Path root = repositoryRoot();
        for (String module : RUNTIME_MODULES) {
            Path sourceRoot = root.resolve(module).resolve("src/main");
            if (!Files.isDirectory(sourceRoot)) {
                continue;
            }
            try (Stream<Path> sources = Files.walk(sourceRoot)) {
                for (Path source : sources.filter(Files::isRegularFile).toList()) {
                    String content = Files.readString(source);
                    assertFalse(content.contains("TASKFLOW_TRANSPORT"),
                            source + " contains the removed transport selector");
                    assertFalse(content.contains("java.net.ServerSocket"),
                            source + " contains a server-socket transport");
                    assertFalse(content.contains("java.net.Socket"),
                            source + " contains a socket transport");
                    assertFalse(source.getFileName().toString().startsWith("Tcp"),
                            source + " is a legacy TCP runtime type");
                }
            }
        }
    }

    @Test
    void runtimeConfigurationContainsNoTransportSelector() throws IOException {
        Path root = repositoryRoot();
        assertNoSelector(root.resolve("Dockerfile"));
        assertNoSelector(root.resolve("docker-compose.yml"));

        for (String directory : List.of("scripts", ".github/workflows")) {
            Path configRoot = root.resolve(directory);
            if (!Files.isDirectory(configRoot)) {
                continue;
            }
            try (Stream<Path> files = Files.walk(configRoot)) {
                for (Path file : files.filter(Files::isRegularFile).toList()) {
                    assertNoSelector(file);
                }
            }
        }
    }

    private static void assertNoSelector(Path file) throws IOException {
        if (Files.isRegularFile(file)) {
            assertFalse(Files.readString(file).contains("TASKFLOW_TRANSPORT"),
                    file + " contains the removed transport selector");
        }
    }

    private static Path repositoryRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isDirectory(current.resolve("taskflow-coordinator"))
                    && Files.isDirectory(current.resolve("taskflow-transport-rabbitmq"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException(
                "Could not find repository root from " + System.getProperty("user.dir")
        );
    }
}
