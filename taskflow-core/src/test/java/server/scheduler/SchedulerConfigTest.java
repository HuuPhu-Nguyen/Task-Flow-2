package server.scheduler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchedulerConfigTest {
    @TempDir
    Path tempDir;

    @Test
    void usesCurrentRuntimeDefaults() {
        SchedulerConfig config = SchedulerConfig.defaults();

        assertEquals(60_000L, config.taskTimeoutMillis());
        assertEquals(120_000L, config.taskLeaseMillis());
        assertEquals(20, config.maxTaskRetries());
        assertEquals(1000, config.inboundQueueCapacity());
        assertEquals(1_000L, config.maxActiveJobs());
        assertEquals(100_000L, config.maxActiveTasks());
        assertEquals(100_000L, config.maxPendingOutboxRows());
        assertEquals(300, config.jobResultMaxDeliveryAttempts());
        assertEquals(100, config.schedulerMessageBatchSize());
        assertEquals(100, config.schedulerDeadlineBatchSize());
        assertEquals(100, config.schedulerDispatchBatchSize());
        assertEquals(1, config.schedulerMaxAssignmentsPerJobPerRound());
        assertEquals(100, config.schedulerOutboxBatchSize());
        assertEquals(10_000L, config.metricsLogIntervalMillis());
        assertEquals(6.0, config.peerScoreLoadWeight());
        assertEquals(2.0, config.peerScoreLatencyWeight());
        assertEquals(1.5, config.peerScoreDurationWeight());
        assertEquals(4.0, config.peerScoreFailureWeight());
        assertEquals(200.0, config.peerScoreLatencyBaselineMillis());
        assertEquals(5_000.0, config.peerScoreDurationBaselineMillis());
        assertEquals(0.2, config.peerScoreEwmaAlpha());
    }

    @Test
    void readsEnvironmentOverrides() {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.ofEntries(
                Map.entry("TASKFLOW_TASK_TIMEOUT_MS", "120000"),
                Map.entry("TASKFLOW_TASK_LEASE_MS", "180000"),
                Map.entry("TASKFLOW_MAX_TASK_RETRIES", "5"),
                Map.entry("TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY", "77"),
                Map.entry("TASKFLOW_MAX_ACTIVE_JOBS", "79"),
                Map.entry("TASKFLOW_MAX_ACTIVE_TASKS", "81"),
                Map.entry("TASKFLOW_MAX_PENDING_OUTBOX_ROWS", "83"),
                Map.entry("TASKFLOW_JOB_RESULT_MAX_DELIVERY_ATTEMPTS", "11"),
                Map.entry("TASKFLOW_SCHEDULER_MESSAGE_BATCH_SIZE", "13"),
                Map.entry("TASKFLOW_SCHEDULER_DEADLINE_BATCH_SIZE", "17"),
                Map.entry("TASKFLOW_SCHEDULER_DISPATCH_BATCH_SIZE", "19"),
                Map.entry("TASKFLOW_SCHEDULER_MAX_ASSIGNMENTS_PER_JOB_PER_ROUND", "7"),
                Map.entry("TASKFLOW_SCHEDULER_OUTBOX_BATCH_SIZE", "23"),
                Map.entry("TASKFLOW_METRICS_LOG_INTERVAL_MS", "3000"),
                Map.entry("TASKFLOW_SCORE_LOAD_WEIGHT", "4.5"),
                Map.entry("TASKFLOW_SCORE_LATENCY_WEIGHT", "1.25"),
                Map.entry("TASKFLOW_SCORE_DURATION_WEIGHT", "2.75"),
                Map.entry("TASKFLOW_SCORE_FAILURE_WEIGHT", "8.5"),
                Map.entry("TASKFLOW_SCORE_LATENCY_BASELINE_MS", "400"),
                Map.entry("TASKFLOW_SCORE_DURATION_BASELINE_MS", "9000"),
                Map.entry("TASKFLOW_SCORE_EWMA_ALPHA", "0.75")
        ));

        assertEquals(120_000L, config.taskTimeoutMillis());
        assertEquals(180_000L, config.taskLeaseMillis());
        assertEquals(5, config.maxTaskRetries());
        assertEquals(77, config.inboundQueueCapacity());
        assertEquals(79L, config.maxActiveJobs());
        assertEquals(81L, config.maxActiveTasks());
        assertEquals(83L, config.maxPendingOutboxRows());
        assertEquals(11, config.jobResultMaxDeliveryAttempts());
        assertEquals(13, config.schedulerMessageBatchSize());
        assertEquals(17, config.schedulerDeadlineBatchSize());
        assertEquals(19, config.schedulerDispatchBatchSize());
        assertEquals(7, config.schedulerMaxAssignmentsPerJobPerRound());
        assertEquals(23, config.schedulerOutboxBatchSize());
        assertEquals(3_000L, config.metricsLogIntervalMillis());
        assertEquals(4.5, config.peerScoreLoadWeight());
        assertEquals(1.25, config.peerScoreLatencyWeight());
        assertEquals(2.75, config.peerScoreDurationWeight());
        assertEquals(8.5, config.peerScoreFailureWeight());
        assertEquals(400.0, config.peerScoreLatencyBaselineMillis());
        assertEquals(9_000.0, config.peerScoreDurationBaselineMillis());
        assertEquals(0.75, config.peerScoreEwmaAlpha());
    }

    @Test
    void rejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_TASK_LEASE_MS", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_SCHEDULER_INBOUND_QUEUE_CAPACITY", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_ACTIVE_JOBS", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_ACTIVE_TASKS", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_MAX_PENDING_OUTBOX_ROWS", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_JOB_RESULT_MAX_DELIVERY_ATTEMPTS", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_SCHEDULER_MESSAGE_BATCH_SIZE", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_SCHEDULER_DEADLINE_BATCH_SIZE", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_SCHEDULER_DISPATCH_BATCH_SIZE", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of(
                        "TASKFLOW_SCHEDULER_MAX_ASSIGNMENTS_PER_JOB_PER_ROUND",
                        "0"
                )));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.ofEntries(
                        Map.entry("TASKFLOW_SCHEDULER_DISPATCH_BATCH_SIZE", "3"),
                        Map.entry("TASKFLOW_SCHEDULER_MAX_ASSIGNMENTS_PER_JOB_PER_ROUND", "4")
                )));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_SCHEDULER_OUTBOX_BATCH_SIZE", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_SCORE_FAILURE_WEIGHT", "-1")));
        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromEnvironment(Map.of("TASKFLOW_SCORE_EWMA_ALPHA", "1.5")));
    }

    @Test
    void readsYamlConfigFile() throws Exception {
        Path configPath = tempDir.resolve("taskflow.yml");
        Files.writeString(configPath, """
                scheduler:
                  taskTimeoutMs: 90000
                  taskLeaseMs: 150000
                  maxTaskRetries: 6
                  inboundQueueCapacity: 123
                  maxActiveJobs: 125
                  maxActiveTasks: 127
                  maxPendingOutboxRows: 129
                  jobResultMaxDeliveryAttempts: 12
                  schedulerMessageBatchSize: 14
                  schedulerDeadlineBatchSize: 16
                  schedulerDispatchBatchSize: 18
                  schedulerMaxAssignmentsPerJobPerRound: 6
                  schedulerOutboxBatchSize: 20
                  metricsLogIntervalMs: 2500
                  scoring:
                    loadWeight: 3.5
                    latencyWeight: 1.5
                    durationWeight: 2.5
                    failureWeight: 7.0
                    latencyBaselineMs: 450
                    durationBaselineMs: 8000
                    ewmaAlpha: 0.6
                """);

        SchedulerConfig config = SchedulerConfig.fromFile(configPath);

        assertEquals(90_000L, config.taskTimeoutMillis());
        assertEquals(150_000L, config.taskLeaseMillis());
        assertEquals(6, config.maxTaskRetries());
        assertEquals(123, config.inboundQueueCapacity());
        assertEquals(125L, config.maxActiveJobs());
        assertEquals(127L, config.maxActiveTasks());
        assertEquals(129L, config.maxPendingOutboxRows());
        assertEquals(12, config.jobResultMaxDeliveryAttempts());
        assertEquals(14, config.schedulerMessageBatchSize());
        assertEquals(16, config.schedulerDeadlineBatchSize());
        assertEquals(18, config.schedulerDispatchBatchSize());
        assertEquals(6, config.schedulerMaxAssignmentsPerJobPerRound());
        assertEquals(20, config.schedulerOutboxBatchSize());
        assertEquals(2_500L, config.metricsLogIntervalMillis());
        assertEquals(3.5, config.peerScoreLoadWeight());
        assertEquals(1.5, config.peerScoreLatencyWeight());
        assertEquals(2.5, config.peerScoreDurationWeight());
        assertEquals(7.0, config.peerScoreFailureWeight());
        assertEquals(450.0, config.peerScoreLatencyBaselineMillis());
        assertEquals(8_000.0, config.peerScoreDurationBaselineMillis());
        assertEquals(0.6, config.peerScoreEwmaAlpha());
    }

    @Test
    void environmentOverridesYamlConfigFile() throws Exception {
        Path configPath = tempDir.resolve("taskflow.yml");
        Files.writeString(configPath, """
                scheduler:
                  scoring:
                    loadWeight: 3
                """);

        SchedulerConfig config = SchedulerConfig.fromRuntime(Map.of(
                SchedulerConfig.CONFIG_PATH_ENV, configPath.toString(),
                "TASKFLOW_SCORE_LOAD_WEIGHT", "12"
        ));

        assertEquals(12.0, config.peerScoreLoadWeight());
    }

    @Test
    void retiredTaskCountCapacitySettingIsIgnored() {
        SchedulerConfig config = SchedulerConfig.fromEnvironment(Map.of(
                "TASKFLOW_MAX_TASKS_PER_PEER",
                "not-used"
        ));

        assertEquals(SchedulerConfig.DEFAULT_MAX_TASK_RETRIES, config.maxTaskRetries());
    }

    @Test
    void explicitMissingConfigFileFailsFast() {
        Path missing = tempDir.resolve("missing.yml");

        assertThrows(IllegalArgumentException.class,
                () -> SchedulerConfig.fromRuntime(Map.of(SchedulerConfig.CONFIG_PATH_ENV, missing.toString())));
    }

    @Test
    void tf0402ConstructorUsesRoundQuotaDefault() {
        SchedulerConfig defaults = SchedulerConfig.defaults();

        SchedulerConfig config = new SchedulerConfig(
                defaults.taskTimeoutMillis(),
                defaults.taskLeaseMillis(),
                defaults.maxTaskRetries(),
                defaults.inboundQueueCapacity(),
                defaults.jobResultMaxDeliveryAttempts(),
                11,
                12,
                13,
                14,
                defaults.metricsLogIntervalMillis(),
                defaults.peerScoreLoadWeight(),
                defaults.peerScoreLatencyWeight(),
                defaults.peerScoreDurationWeight(),
                defaults.peerScoreFailureWeight(),
                defaults.peerScoreLatencyBaselineMillis(),
                defaults.peerScoreDurationBaselineMillis(),
                defaults.peerScoreEwmaAlpha()
        );

        assertEquals(13, config.schedulerDispatchBatchSize());
        assertEquals(SchedulerConfig.DEFAULT_MAX_ACTIVE_JOBS, config.maxActiveJobs());
        assertEquals(SchedulerConfig.DEFAULT_MAX_ACTIVE_TASKS, config.maxActiveTasks());
        assertEquals(
                SchedulerConfig.DEFAULT_MAX_PENDING_OUTBOX_ROWS,
                config.maxPendingOutboxRows()
        );
        assertEquals(SchedulerConfig.DEFAULT_SCHEDULER_MAX_ASSIGNMENTS_PER_JOB_PER_ROUND,
                config.schedulerMaxAssignmentsPerJobPerRound());
    }

    @Test
    void preTf0402ConstructorUsesBatchAndRoundDefaults() {
        SchedulerConfig defaults = SchedulerConfig.defaults();

        SchedulerConfig config = new SchedulerConfig(
                defaults.taskTimeoutMillis(),
                defaults.taskLeaseMillis(),
                defaults.maxTaskRetries(),
                defaults.inboundQueueCapacity(),
                defaults.jobResultMaxDeliveryAttempts(),
                defaults.metricsLogIntervalMillis(),
                defaults.peerScoreLoadWeight(),
                defaults.peerScoreLatencyWeight(),
                defaults.peerScoreDurationWeight(),
                defaults.peerScoreFailureWeight(),
                defaults.peerScoreLatencyBaselineMillis(),
                defaults.peerScoreDurationBaselineMillis(),
                defaults.peerScoreEwmaAlpha()
        );

        assertEquals(SchedulerConfig.DEFAULT_SCHEDULER_MESSAGE_BATCH_SIZE,
                config.schedulerMessageBatchSize());
        assertEquals(SchedulerConfig.DEFAULT_SCHEDULER_DEADLINE_BATCH_SIZE,
                config.schedulerDeadlineBatchSize());
        assertEquals(SchedulerConfig.DEFAULT_SCHEDULER_DISPATCH_BATCH_SIZE,
                config.schedulerDispatchBatchSize());
        assertEquals(SchedulerConfig.DEFAULT_SCHEDULER_MAX_ASSIGNMENTS_PER_JOB_PER_ROUND,
                config.schedulerMaxAssignmentsPerJobPerRound());
        assertEquals(SchedulerConfig.DEFAULT_SCHEDULER_OUTBOX_BATCH_SIZE,
                config.schedulerOutboxBatchSize());
        assertEquals(SchedulerConfig.DEFAULT_MAX_ACTIVE_JOBS, config.maxActiveJobs());
        assertEquals(SchedulerConfig.DEFAULT_MAX_ACTIVE_TASKS, config.maxActiveTasks());
        assertEquals(
                SchedulerConfig.DEFAULT_MAX_PENDING_OUTBOX_ROWS,
                config.maxPendingOutboxRows()
        );
    }
}
