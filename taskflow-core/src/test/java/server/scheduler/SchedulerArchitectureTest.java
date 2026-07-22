package server.scheduler;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerArchitectureTest {
    private static final List<String> FOCUSED_COMPONENTS = List.of(
            "AssignmentService.java",
            "ResultCommitService.java",
            "LeaseService.java",
            "JobCompletionService.java",
            "RecoveryService.java",
            "SchedulerLoop.java"
    );

    @Test
    void schedulerFacadeAndLoopCannotOwnTransitionEffects() throws IOException {
        Path schedulerRoot = schedulerSourceRoot();
        String facade = read(schedulerRoot, "TaskScheduler.java");
        String loop = read(schedulerRoot, "SchedulerLoop.java");

        for (String forbidden : List.of(
                "TaskAssignMessage",
                "TaskResultMessage",
                "MessageValidator",
                "aggregateResultPayload(",
                "applyCommittedResult(",
                "markAssigned(",
                "failAttemptBy(",
                "commitTaskResult(",
                "isLeaseExpired("
        )) {
            assertFalse(facade.contains(forbidden), "TaskScheduler owns forbidden effect: " + forbidden);
            assertFalse(loop.contains(forbidden), "SchedulerLoop owns forbidden effect: " + forbidden);
        }

        for (String forbiddenDependency : List.of(
                "server.db.",
                "server.job.",
                "protocol.",
                "com.rabbitmq",
                "transport.rabbitmq",
                "java.sql",
                "javafx"
        )) {
            assertFalse(loop.contains(forbiddenDependency),
                    "SchedulerLoop crosses dependency boundary: " + forbiddenDependency);
        }
    }

    @Test
    void focusedServicesOwnDistinctRuntimeResponsibilitiesThroughPorts() throws IOException {
        Path schedulerRoot = schedulerSourceRoot();
        for (String component : FOCUSED_COMPONENTS) {
            assertTrue(Files.isRegularFile(schedulerRoot.resolve(component)), component + " is required");
        }

        assertTrue(read(schedulerRoot, "AssignmentService.java")
                .contains("createTaskAssignmentAndEnqueueBrokerOutbox("));
        assertTrue(read(schedulerRoot, "ResultCommitService.java").contains("commitTaskResult("));
        assertTrue(read(schedulerRoot, "LeaseService.java").contains("leaseExpired("));
        assertTrue(read(schedulerRoot, "JobCompletionService.java").contains("aggregateResultPayload("));
        assertTrue(read(schedulerRoot, "RecoveryService.java").contains("restoreJobs("));

        for (String component : List.of(
                "AssignmentService.java",
                "ResultCommitService.java",
                "LeaseService.java",
                "JobCompletionService.java",
                "RecoveryService.java"
        )) {
            String source = read(schedulerRoot, component);
            for (String concreteInfrastructure : List.of(
                    "com.rabbitmq",
                    "transport.rabbitmq",
                    "server.db.DatabaseManager",
                    "java.sql",
                    "javafx"
            )) {
                assertFalse(source.contains(concreteInfrastructure),
                        component + " depends on concrete infrastructure: " + concreteInfrastructure);
            }
        }

        Path transitionRoot = schedulerRoot.resolve("transition");
        try (var sources = Files.list(transitionRoot)) {
            for (Path sourceFile : sources.filter(path -> path.toString().endsWith(".java")).toList()) {
                String source = Files.readString(sourceFile);
                for (String infrastructure : List.of(
                        "server.db.",
                        "com.rabbitmq",
                        "transport.rabbitmq",
                        "java.sql",
                        "javafx",
                        "java.util.concurrent"
                )) {
                    assertFalse(source.contains(infrastructure),
                            sourceFile.getFileName()
                                    + " crosses the pure-decision boundary: "
                                    + infrastructure);
                }
            }
        }
    }

    private static String read(Path schedulerRoot, String file) throws IOException {
        return Files.readString(schedulerRoot.resolve(file));
    }

    private static Path schedulerSourceRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(
                    "taskflow-core/src/main/java/server/scheduler"
            );
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Could not find scheduler sources from " + System.getProperty("user.dir"));
    }
}
