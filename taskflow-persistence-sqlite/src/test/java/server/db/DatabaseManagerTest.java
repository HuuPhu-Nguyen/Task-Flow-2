package server.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.RequesterTokens;
import server.registry.PeerMetricsSnapshot;
import server.registry.PeerRegistryRecord;
import server.registry.PeerStatus;
import server.registry.PeerTransport;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void persistsJobAndTaskLifecycleToConfiguredDatabasePath() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-1", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-1", "job-1");
            db.markTaskAssigned("task-1", "peer-1", 123L);
            db.markTaskCompleted("task-1", 456L, 333L);
            db.markJobCompleted("job-1");

            assertEquals(DatabaseManager.CURRENT_SCHEMA_VERSION, db.getSchemaVersion());
            assertTrue(db.hasJob("job-1"));
            assertFalse(db.hasJob("missing-job"));

            List<DatabaseManager.JobRecord> jobs = db.getJobHistory();
            assertEquals(1, jobs.size());
            DatabaseManager.JobRecord job = jobs.getFirst();
            assertEquals("job-1", job.jobId());
            assertEquals("TEST_TASK", job.taskType());
            assertEquals("requester-1", job.requesterId());
            assertEquals("COMPLETED", job.status());
            assertEquals(1, job.fileCount());

            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-1");
            assertEquals(1, tasks.size());
            DatabaseManager.TaskRecord task = tasks.getFirst();
            assertEquals("task-1", task.taskId());
            assertEquals("job-1", task.jobId());
            assertEquals("peer-1", task.assignedPeerId());
            assertEquals("COMPLETED", task.status());
            assertEquals(123L, task.startedAt());
            assertEquals(456L, task.completedAt());
            assertEquals(333L, task.durationMs());
            assertEquals(0, task.retryCount());
            assertEquals("", task.leaseOwnerId());
            assertEquals(0L, task.leaseExpiresAt());

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-1");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals("job-1", attempt.jobId());
            assertEquals("task-1", attempt.taskId());
            assertEquals(1, attempt.attemptNumber());
            assertEquals("peer-1", attempt.peerId());
            assertEquals(123L, attempt.startedAt());
            assertEquals(456L, attempt.finishedAt());
            assertEquals(333L, attempt.durationMs());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, attempt.outcome());
            assertEquals("", attempt.failureReason());
        } finally {
            db.close();
        }
    }

    @Test
    void persistsPeerRegistryMetadataAcrossRestart() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-restart-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "TCP_PEER",
                    PeerTransport.TCP,
                    Set.of("image_conversion", "text_analysis"),
                    100L,
                    150L,
                    0L,
                    PeerStatus.CONNECTED,
                    new PeerMetricsSnapshot(2L, 1L, 30L, 250L)
            )));
        } finally {
            db.close();
        }

        DatabaseManager reopened = new DatabaseManager(dbPath.toString());
        try {
            List<PeerRegistryRecord> peers = reopened.loadPeerRecords();
            assertEquals(1, peers.size());
            PeerRegistryRecord peer = peers.getFirst();
            assertEquals("peer-1", peer.peerId());
            assertEquals("TCP_PEER", peer.runtimeType());
            assertEquals(PeerTransport.TCP, peer.transport());
            assertEquals(Set.of("IMAGE_CONVERSION", "TEXT_ANALYSIS"), peer.supportedTaskTypes());
            assertEquals(100L, peer.firstSeenAtMillis());
            assertEquals(150L, peer.lastHeartbeatAtMillis());
            assertEquals(0L, peer.lastDisconnectedAtMillis());
            assertEquals(PeerStatus.CONNECTED, peer.status());
            assertEquals(new PeerMetricsSnapshot(2L, 1L, 30L, 250L), peer.metricsSnapshot());
        } finally {
            reopened.close();
        }
    }

    @Test
    void peerRegistryUpsertPreservesFirstSeenAndUpdatesHeartbeatCapabilities() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-heartbeat-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "TCP_PEER",
                    PeerTransport.TCP,
                    Set.of("image_conversion"),
                    100L,
                    150L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "TCP_PEER",
                    PeerTransport.TCP,
                    Set.of("text_analysis"),
                    999L,
                    300L,
                    0L,
                    PeerStatus.CONNECTED,
                    new PeerMetricsSnapshot(4L, 0L, 45L, 500L)
            )));

            PeerRegistryRecord peer = db.loadPeerRecords().getFirst();
            assertEquals(100L, peer.firstSeenAtMillis());
            assertEquals(300L, peer.lastHeartbeatAtMillis());
            assertEquals(Set.of("TEXT_ANALYSIS"), peer.supportedTaskTypes());
            assertEquals(PeerStatus.CONNECTED, peer.status());
            assertEquals(new PeerMetricsSnapshot(4L, 0L, 45L, 500L), peer.metricsSnapshot());
        } finally {
            db.close();
        }
    }

    @Test
    void peerRegistryUpsertMarksPeerDisconnected() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-disconnect-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "RABBITMQ_PEER",
                    PeerTransport.RABBITMQ,
                    Set.of("image_conversion"),
                    100L,
                    200L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "RABBITMQ_PEER",
                    PeerTransport.RABBITMQ,
                    Set.of("image_conversion"),
                    100L,
                    200L,
                    450L,
                    PeerStatus.DISCONNECTED,
                    new PeerMetricsSnapshot(1L, 1L, 25L, 120L)
            )));

            PeerRegistryRecord peer = db.loadPeerRecords().getFirst();
            assertEquals(PeerStatus.DISCONNECTED, peer.status());
            assertEquals(450L, peer.lastDisconnectedAtMillis());
            assertEquals(200L, peer.lastHeartbeatAtMillis());
            assertEquals(new PeerMetricsSnapshot(1L, 1L, 25L, 120L), peer.metricsSnapshot());
        } finally {
            db.close();
        }
    }

    @Test
    void peerRegistryDuplicatePeerIdUpdatesSingleDurableRecord() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-duplicate-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "TCP_PEER",
                    PeerTransport.TCP,
                    Set.of("image_conversion"),
                    100L,
                    150L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-1",
                    "RABBITMQ_PEER",
                    PeerTransport.RABBITMQ,
                    Set.of("text_analysis"),
                    50L,
                    175L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));

            List<PeerRegistryRecord> peers = db.loadPeerRecords();
            assertEquals(1, peers.size());
            PeerRegistryRecord peer = peers.getFirst();
            assertEquals("peer-1", peer.peerId());
            assertEquals(100L, peer.firstSeenAtMillis());
            assertEquals(PeerTransport.RABBITMQ, peer.transport());
            assertEquals(Set.of("TEXT_ANALYSIS"), peer.supportedTaskTypes());
        } finally {
            db.close();
        }
    }

    @Test
    void peerRegistryMetadataCoexistsWithTaskRetryHistoryRows() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-peer-registry-task-state-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.upsertPeerRecord(new PeerRegistryRecord(
                    "peer-lease",
                    "TCP_PEER",
                    PeerTransport.TCP,
                    Set.of("test_task"),
                    100L,
                    150L,
                    0L,
                    PeerStatus.CONNECTED,
                    PeerMetricsSnapshot.empty()
            )));
            db.insertJob("job-retry", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-retry", "job-retry");
            assertTrue(db.markTaskAssigned("task-retry", "peer-lease", 200L));
            assertTrue(db.markTaskRetried("task-retry", 1));
        } finally {
            db.close();
        }

        DatabaseManager reopened = new DatabaseManager(dbPath.toString());
        try {
            List<PeerRegistryRecord> peers = reopened.loadPeerRecords();
            assertEquals(1, peers.size());
            assertEquals("peer-lease", peers.getFirst().peerId());
            assertEquals(PeerStatus.CONNECTED, peers.getFirst().status());

            List<DatabaseManager.TaskRecord> tasks = reopened.getTasksForJob("job-retry");
            assertEquals(1, tasks.size());
            DatabaseManager.TaskRecord task = tasks.getFirst();
            assertEquals("PENDING", task.status());
            assertNull(task.assignedPeerId());
            assertEquals(1, task.retryCount());
        } finally {
            reopened.close();
        }
    }

    @Test
    void rejectsTaskRowsWithoutExistingJob() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-foreign-key-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertFalse(db.insertTask("orphan-task", "missing-job"));
            assertEquals(0, db.getTasksForJob("missing-job").size());
            assertTrue(tasksTableReferencesJobs(dbPath));
        } finally {
            db.close();
        }
    }

    @Test
    void retriedTaskRowsClearPreviousAssignmentState() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-retry-state-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-retry", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-retry", "job-retry");
            db.markTaskAssigned("task-retry", "peer-1", 123L);

            assertTrue(db.markTaskRetried("task-retry", 1));

            List<DatabaseManager.TaskRecord> tasks = db.getTasksForJob("job-retry");
            assertEquals(1, tasks.size());
            DatabaseManager.TaskRecord task = tasks.getFirst();
            assertEquals("PENDING", task.status());
            assertEquals(1, task.retryCount());
            assertNull(task.assignedPeerId());
            assertEquals(0L, task.startedAt());
            assertEquals(0L, task.completedAt());
            assertEquals(0L, task.durationMs());
            assertEquals("", task.leaseOwnerId());
            assertEquals(0L, task.leaseExpiresAt());
        } finally {
            db.close();
        }
    }

    @Test
    void assignedTaskPersistsLeaseOwnerAndExpiryForResume() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-lease-state-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-lease", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-lease", "job-lease");

            assertTrue(db.markTaskAssigned(
                    "task-lease",
                    "peer-1",
                    100L,
                    "COORDINATOR_A",
                    900L));

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-lease").getFirst();
            assertEquals("ASSIGNED", task.status());
            assertEquals("peer-1", task.assignedPeerId());
            assertEquals(100L, task.startedAt());
            assertEquals("COORDINATOR_A", task.leaseOwnerId());
            assertEquals(900L, task.leaseExpiresAt());

            JobStateStore.ResumableTaskState resumedTask =
                    db.loadRunningJobsForResume().getFirst().tasks().getFirst();
            assertEquals("ASSIGNED", resumedTask.status());
            assertEquals("peer-1", resumedTask.assignedPeerId());
            assertEquals(100L, resumedTask.startedAt());
            assertEquals("COORDINATOR_A", resumedTask.leaseOwnerId());
            assertEquals(900L, resumedTask.leaseExpiresAt());
        } finally {
            db.close();
        }
    }

    @Test
    void releaseExpiredTaskLeaseForResumeClearsAssignmentAndClosesAttempt() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-expired-lease-release-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-expired-lease", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-expired-lease", "job-expired-lease");
            assertTrue(db.markTaskAssigned(
                    "task-expired-lease",
                    "peer-1",
                    100L,
                    "COORDINATOR_A",
                    150L));

            assertFalse(db.releaseExpiredTaskLeaseForResume("task-expired-lease", 149L));
            assertTrue(db.releaseExpiredTaskLeaseForResume("task-expired-lease", 175L));

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-expired-lease").getFirst();
            assertEquals("PENDING", task.status());
            assertNull(task.assignedPeerId());
            assertEquals(0, task.retryCount());
            assertEquals(0L, task.startedAt());
            assertEquals("", task.leaseOwnerId());
            assertEquals(0L, task.leaseExpiresAt());

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-expired-lease");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, attempt.outcome());
            assertEquals("lease_expired", attempt.failureReason());
            assertEquals(175L, attempt.finishedAt());
            assertEquals(75L, attempt.durationMs());
        } finally {
            db.close();
        }
    }

    @Test
    void persistsTaskAttemptHistoryAcrossRetryAndRestart() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-attempt-history-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-attempts", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-attempts", "job-attempts");
            assertTrue(db.markTaskAssigned("task-attempts", "peer-1", 100L));
            assertTrue(db.markTaskRetried(
                    "task-attempts",
                    1,
                    JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED,
                    "processor failed",
                    175L));
            assertTrue(db.markTaskAssigned("task-attempts", "peer-2", 220L));
            assertTrue(db.markTaskCompleted("task-attempts", 280L, 60L, "result"));
        } finally {
            db.close();
        }

        DatabaseManager reopened = new DatabaseManager(dbPath.toString());
        try {
            List<JobStateStore.TaskAttemptRecord> attempts = reopened.loadTaskAttempts("job-attempts");
            assertEquals(2, attempts.size());

            JobStateStore.TaskAttemptRecord first = attempts.getFirst();
            assertEquals(1, first.attemptNumber());
            assertEquals("peer-1", first.peerId());
            assertEquals(100L, first.startedAt());
            assertEquals(175L, first.finishedAt());
            assertEquals(75L, first.durationMs());
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, first.outcome());
            assertEquals("processor failed", first.failureReason());

            JobStateStore.TaskAttemptRecord second = attempts.get(1);
            assertEquals(2, second.attemptNumber());
            assertEquals("peer-2", second.peerId());
            assertEquals(220L, second.startedAt());
            assertEquals(280L, second.finishedAt());
            assertEquals(60L, second.durationMs());
            assertEquals(JobStateStore.TaskAttemptOutcome.SUCCEEDED, second.outcome());
            assertEquals("", second.failureReason());
        } finally {
            reopened.close();
        }
    }

    @Test
    void resetTaskForResumeRecordsRestartReleaseAttempt() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-restart-release-attempt-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-restart-release", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-restart-release", "job-restart-release");
            assertTrue(db.markTaskAssigned("task-restart-release", "peer-1", 123L));
            assertTrue(db.resetTaskForResume("task-restart-release"));

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-restart-release").getFirst();
            assertEquals("PENDING", task.status());
            assertNull(task.assignedPeerId());
            assertEquals(0, task.retryCount());

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-restart-release");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals("peer-1", attempt.peerId());
            assertEquals(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED, attempt.outcome());
            assertEquals("coordinator_restart", attempt.failureReason());
            assertTrue(attempt.finishedAt() >= 123L);
        } finally {
            db.close();
        }
    }

    @Test
    void markRunningJobsFailedOnStartupClosesRunningAttempts() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-startup-failed-attempt-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-startup-fail", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-startup-fail", "job-startup-fail");
            assertTrue(db.markTaskAssigned("task-startup-fail", "peer-1", 100L));

            assertEquals(1, db.markRunningJobsFailedOnStartup(250L));

            List<JobStateStore.TaskAttemptRecord> attempts = db.loadTaskAttempts("job-startup-fail");
            assertEquals(1, attempts.size());
            JobStateStore.TaskAttemptRecord attempt = attempts.getFirst();
            assertEquals(JobStateStore.TaskAttemptOutcome.JOB_FAILED, attempt.outcome());
            assertEquals("coordinator_startup_reconciliation", attempt.failureReason());
            assertEquals(250L, attempt.finishedAt());
            assertEquals(150L, attempt.durationMs());
        } finally {
            db.close();
        }
    }

    @Test
    void persistsTaskPayloadsAndCompletedResultsForResume() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-resume-state-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());
        String requesterTokenHash = RequesterTokens.hashToken("resume-token");
        String requesterIdentityKey = "resume-public-key";

        try {
            assertTrue(db.insertJobWithTasks(
                    "job-resume",
                    "TEST_TASK",
                    "requester-1",
                    requesterTokenHash,
                    requesterIdentityKey,
                    "csv",
                    List.of(
                            new JobStateStore.TaskStartupState("task-job-resume-0", "payload-alpha"),
                            new JobStateStore.TaskStartupState("task-job-resume-1", "payload-beta")
                    )
            ));
            db.markTaskAssigned("task-job-resume-0", "peer-1", 123L);
            assertTrue(db.markTaskCompleted("task-job-resume-0", 456L, 333L, "result-alpha"));

            List<JobStateStore.ResumableJobState> jobs = db.loadRunningJobsForResume();
            assertEquals(1, jobs.size());
            JobStateStore.ResumableJobState job = jobs.getFirst();
            assertEquals("job-resume", job.jobId());
            assertEquals("TEST_TASK", job.taskType());
            assertEquals("requester-1", job.requesterId());
            assertEquals(requesterTokenHash, job.requesterTokenHash());
            assertEquals(requesterIdentityKey, job.requesterIdentityKey());
            assertEquals("csv", job.parameter());
            assertEquals(2, job.tasks().size());

            JobStateStore.ResumableTaskState completed = job.tasks().getFirst();
            assertEquals("task-job-resume-0", completed.taskId());
            assertEquals("COMPLETED", completed.status());
            assertEquals("payload-alpha", completed.payload());
            assertEquals("result-alpha", completed.resultPayload());

            JobStateStore.ResumableTaskState pending = job.tasks().get(1);
            assertEquals("task-job-resume-1", pending.taskId());
            assertEquals("PENDING", pending.status());
            assertEquals("payload-beta", pending.payload());
            assertNull(pending.resultPayload());
        } finally {
            db.close();
        }
    }

    @Test
    void loadsCompletedJobResultFromPersistedTaskResults() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-completed-result-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());
        String requesterTokenHash = RequesterTokens.hashToken("completed-token");
        String requesterIdentityKey = "completed-public-key";

        try {
            assertTrue(db.insertJobWithTasks(
                    "job-completed-result",
                    "TEST_TASK",
                    "requester-1",
                    requesterTokenHash,
                    requesterIdentityKey,
                    "csv",
                    List.of(
                            new JobStateStore.TaskStartupState("task-job-completed-result-1", "payload-beta"),
                            new JobStateStore.TaskStartupState("task-job-completed-result-0", "payload-alpha")
                    )
            ));
            assertTrue(db.markTaskAssigned("task-job-completed-result-0", "peer-1", 123L));
            assertTrue(db.markTaskCompleted("task-job-completed-result-0", 456L, 333L, "result-alpha"));
            assertTrue(db.markTaskAssigned("task-job-completed-result-1", "peer-2", 789L));
            assertTrue(db.markTaskCompleted("task-job-completed-result-1", 987L, 198L, "result-beta"));
            assertTrue(db.markJobCompleted("job-completed-result"));

            var result = db.loadCompletedJobResult("job-completed-result");

            assertTrue(result.isPresent());
            assertEquals("job-completed-result", result.get().jobId());
            assertEquals("TEST_TASK", result.get().taskType());
            assertEquals(requesterTokenHash, result.get().requesterTokenHash());
            assertEquals(requesterIdentityKey, result.get().requesterIdentityKey());
            assertEquals(List.of("result-alpha", "result-beta"), result.get().resultsByTaskId());
        } finally {
            db.close();
        }
    }

    @Test
    void loadsCompletedJobSemanticResultPayload() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-completed-semantic-result-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.insertJobWithTasks(
                    "job-semantic-result",
                    "TEXT_ANALYSIS",
                    "requester-1",
                    RequesterTokens.hashToken("completed-token"),
                    "",
                    "csv",
                    List.of(
                            new JobStateStore.TaskStartupState("task-job-semantic-result-0", "payload-alpha"),
                            new JobStateStore.TaskStartupState("task-job-semantic-result-1", "payload-beta")
                    )
            ));
            assertTrue(db.markTaskAssigned("task-job-semantic-result-0", "peer-1", 123L));
            assertTrue(db.markTaskCompleted("task-job-semantic-result-0", 456L, 333L, "result-alpha"));
            assertTrue(db.markTaskAssigned("task-job-semantic-result-1", "peer-2", 789L));
            assertTrue(db.markTaskCompleted("task-job-semantic-result-1", 987L, 198L, "result-beta"));
            assertTrue(db.markJobCompleted("job-semantic-result", Map.of(
                    "documentCount", 2,
                    "totalWords", 42)));

            var result = db.loadCompletedJobResult("job-semantic-result");

            assertTrue(result.isPresent());
            assertEquals(Map.of("documentCount", 2.0, "totalWords", 42.0), result.get().resultPayload());
            assertEquals(List.of("result-alpha", "result-beta"), result.get().resultsByTaskId());
        } finally {
            db.close();
        }
    }

    @Test
    void completedJobResultLookupRejectsMissingTaskResults() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-incomplete-result-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertTrue(db.insertJobWithTasks(
                    "job-incomplete-result",
                    "TEST_TASK",
                    "requester-1",
                    "csv",
                    List.of(
                            new JobStateStore.TaskStartupState("task-job-incomplete-result-0", "payload-alpha"),
                            new JobStateStore.TaskStartupState("task-job-incomplete-result-1", "payload-beta")
                    )
            ));
            assertTrue(db.markTaskAssigned("task-job-incomplete-result-0", "peer-1", 123L));
            assertTrue(db.markTaskCompleted("task-job-incomplete-result-0", 456L, 333L, "result-alpha"));
            assertTrue(db.markJobCompleted("job-incomplete-result"));

            assertTrue(db.loadCompletedJobResult("job-incomplete-result").isEmpty());
        } finally {
            db.close();
        }
    }

    @Test
    void resetTaskForResumeClearsStaleAssignmentWithoutIncrementingRetry() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-resume-reset-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJobWithTasks(
                    "job-reset",
                    "TEST_TASK",
                    "requester-1",
                    "",
                    List.of(new JobStateStore.TaskStartupState("task-job-reset-0", "payload"))
            );
            db.markTaskAssigned("task-job-reset-0", "peer-1", 123L);

            assertTrue(db.resetTaskForResume("task-job-reset-0"));

            DatabaseManager.TaskRecord task = db.getTasksForJob("job-reset").getFirst();
            assertEquals("PENDING", task.status());
            assertNull(task.assignedPeerId());
            assertEquals(0L, task.startedAt());
            assertEquals(0, task.retryCount());
        } finally {
            db.close();
        }
    }

    @Test
    void taskStatusUpdatesRejectInvalidTransitions() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-task-transition-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-completed", "TEST_TASK", "requester-1", 1);
            db.insertTask("task-completed", "job-completed");
            assertFalse(db.markTaskCompleted("task-completed", 100L, 10L));
            assertTrue(db.markTaskAssigned("task-completed", "peer-1", 100L));
            assertTrue(db.markTaskCompleted("task-completed", 200L, 100L));
            assertFalse(db.markTaskRetried("task-completed", 1));
            assertFalse(db.markTaskFailed("task-completed"));
            assertFalse(db.markTaskAssigned("task-completed", "peer-2", 300L));

            DatabaseManager.TaskRecord completed = db.getTasksForJob("job-completed").getFirst();
            assertEquals("COMPLETED", completed.status());
            assertEquals("peer-1", completed.assignedPeerId());
            assertEquals(200L, completed.completedAt());
            assertEquals(100L, completed.durationMs());
            assertEquals(0, completed.retryCount());

            db.insertJob("job-failed", "TEST_TASK", "requester-2", 1);
            db.insertTask("task-failed", "job-failed");
            assertTrue(db.markTaskFailed("task-failed"));
            assertFalse(db.markTaskAssigned("task-failed", "peer-3", 400L));
            assertFalse(db.markTaskCompleted("task-failed", 500L, 100L));
            assertFalse(db.markTaskRetried("task-failed", 1));

            DatabaseManager.TaskRecord failed = db.getTasksForJob("job-failed").getFirst();
            assertEquals("FAILED", failed.status());
            assertNull(failed.assignedPeerId());
            assertEquals(0, failed.retryCount());
        } finally {
            db.close();
        }
    }

    @Test
    void jobStatusUpdatesRejectTerminalOverwrites() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-job-transition-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("job-completed", "TEST_TASK", "requester-1", 1);
            assertTrue(db.markJobCompleted("job-completed"));
            assertFalse(db.markJobFailed("job-completed"));

            DatabaseManager.JobRecord completed = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("job-completed"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("COMPLETED", completed.status());

            db.insertJob("job-failed", "TEST_TASK", "requester-2", 1);
            assertTrue(db.markJobFailed("job-failed"));
            assertFalse(db.markJobCompleted("job-failed"));

            DatabaseManager.JobRecord failed = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("job-failed"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("FAILED", failed.status());
        } finally {
            db.close();
        }
    }

    @Test
    void migratesLegacyTasksTableToForeignKeySchema() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-legacy-migration-test.db");
        createLegacyDatabaseWithoutTaskForeignKey(dbPath);

        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            assertEquals(DatabaseManager.CURRENT_SCHEMA_VERSION, db.getSchemaVersion());
            assertTrue(tasksTableReferencesJobs(dbPath));
            assertTrue(columnExists(dbPath, "jobs", "parameter"));
            assertTrue(columnExists(dbPath, "jobs", "requester_token_hash"));
            assertTrue(columnExists(dbPath, "jobs", "requester_identity_key"));
            assertTrue(columnExists(dbPath, "jobs", "result_payload_json"));
            assertTrue(columnExists(dbPath, "tasks", "payload_json"));
            assertTrue(columnExists(dbPath, "tasks", "result_payload_json"));
            assertTrue(columnExists(dbPath, "tasks", "lease_owner_id"));
            assertTrue(columnExists(dbPath, "tasks", "lease_expires_at"));
            assertTrue(tableExists(dbPath, "task_attempts"));
            assertEquals(1, db.getTasksForJob("legacy-job").size());
            assertFalse(db.insertTask("orphan-task", "missing-job"));
        } finally {
            db.close();
        }
    }

    @Test
    void rejectsDatabaseSchemaNewerThanRuntimeSupports() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-future-schema-test.db");
        createFutureSchemaVersionDatabase(dbPath);

        SQLException failure = assertThrows(SQLException.class, () -> new DatabaseManager(dbPath.toString()));
        assertTrue(failure.getMessage().contains("newer than supported version"));
    }

    @Test
    void marksRunningJobsAndNonTerminalTasksFailedOnStartup() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-recovery-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("running-job", "TEST_TASK", "requester-1", 2);
            db.insertTask("completed-task", "running-job");
            db.insertTask("assigned-task", "running-job");
            db.markTaskAssigned("completed-task", "peer-1", 100L);
            db.markTaskCompleted("completed-task", 200L, 100L);
            db.markTaskAssigned("assigned-task", "peer-2", 150L);

            db.insertJob("completed-job", "TEST_TASK", "requester-2", 1);
            db.insertTask("completed-job-task", "completed-job");
            db.markTaskAssigned("completed-job-task", "peer-3", 220L);
            db.markTaskCompleted("completed-job-task", 250L, 50L);
            db.markJobCompleted("completed-job");

            assertEquals(1, db.markRunningJobsFailedOnStartup(999L));

            DatabaseManager.JobRecord runningJob = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("running-job"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("FAILED", runningJob.status());
            assertEquals(999L, runningJob.completedAt());

            DatabaseManager.JobRecord completedJob = db.getJobHistory().stream()
                    .filter(job -> job.jobId().equals("completed-job"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("COMPLETED", completedJob.status());
            assertTrue(completedJob.completedAt() > 0L);

            List<DatabaseManager.TaskRecord> runningTasks = db.getTasksForJob("running-job");
            DatabaseManager.TaskRecord completedTask = runningTasks.stream()
                    .filter(task -> task.taskId().equals("completed-task"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("COMPLETED", completedTask.status());
            assertEquals(200L, completedTask.completedAt());

            DatabaseManager.TaskRecord assignedTask = runningTasks.stream()
                    .filter(task -> task.taskId().equals("assigned-task"))
                    .findFirst()
                    .orElseThrow();
            assertEquals("FAILED", assignedTask.status());
            assertEquals(999L, assignedTask.completedAt());
        } finally {
            db.close();
        }
    }

    @Test
    void rollsBackAtomicJobStartupWhenTaskInsertFails() throws Exception {
        Path dbPath = tempDir.resolve("taskflow-startup-rollback-test.db");
        DatabaseManager db = new DatabaseManager(dbPath.toString());

        try {
            db.insertJob("existing-job", "TEST_TASK", "requester-1", 1);
            db.insertTask("duplicate-task", "existing-job");

            assertFalse(db.insertJobWithTasks(
                    "new-job",
                    "TEST_TASK",
                    "requester-2",
                    1,
                    List.of("duplicate-task")
            ));

            assertTrue(db.getJobHistory().stream().noneMatch(job -> job.jobId().equals("new-job")));
            assertEquals(0, db.getTasksForJob("new-job").size());
            assertEquals(1, db.getTasksForJob("existing-job").size());
        } finally {
            db.close();
        }
    }

    private static void createLegacyDatabaseWithoutTaskForeignKey(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE jobs (
                    job_id           TEXT    PRIMARY KEY,
                    task_type        TEXT    NOT NULL,
                    requester_node_id TEXT   NOT NULL,
                    status           TEXT    NOT NULL DEFAULT 'RUNNING',
                    submitted_at     INTEGER NOT NULL,
                    completed_at     INTEGER,
                    file_count       INTEGER NOT NULL
                )
            """);
            stmt.execute("""
                CREATE TABLE tasks (
                    task_id          TEXT    PRIMARY KEY,
                    job_id           TEXT    NOT NULL,
                    assigned_peer_id TEXT,
                    status           TEXT    NOT NULL DEFAULT 'PENDING',
                    started_at       INTEGER,
                    completed_at     INTEGER,
                    duration_ms      INTEGER,
                    retry_count      INTEGER NOT NULL DEFAULT 0
                )
            """);
            stmt.execute("""
                INSERT INTO jobs(job_id, task_type, requester_node_id, status, submitted_at, file_count)
                VALUES('legacy-job', 'TEST_TASK', 'requester-1', 'RUNNING', 100, 1)
            """);
            stmt.execute("""
                INSERT INTO tasks(task_id, job_id, status)
                VALUES('legacy-task', 'legacy-job', 'PENDING')
            """);
        }
    }

    private static void createFutureSchemaVersionDatabase(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE schema_version (
                    id         INTEGER PRIMARY KEY CHECK (id = 1),
                    version    INTEGER NOT NULL CHECK (version >= 0),
                    applied_at INTEGER NOT NULL
                )
            """);
            stmt.execute("INSERT INTO schema_version(id, version, applied_at) VALUES(1, 999, 100)");
        }
    }

    private static boolean tasksTableReferencesJobs(Path dbPath) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA foreign_key_list(tasks)")) {
            while (rs.next()) {
                if ("jobs".equals(rs.getString("table"))
                        && "job_id".equals(rs.getString("from"))
                        && "job_id".equals(rs.getString("to"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean columnExists(Path dbPath, String tableName, String columnName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA table_info(" + tableName + ")")) {
            while (rs.next()) {
                if (columnName.equals(rs.getString("name"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean tableExists(Path dbPath, String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             var ps = conn.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name=?")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }
}
