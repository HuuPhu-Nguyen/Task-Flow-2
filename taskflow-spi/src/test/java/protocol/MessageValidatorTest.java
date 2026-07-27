package protocol;

import objectstore.ObjectReference;
import objectstore.TaskFlowObjectKeys;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageValidatorTest {
    private static final String EXECUTOR_INSTANCE_ID =
            "550e8400-e29b-41d4-a716-446655440099";
    @Test
    void acceptsValidJobSubmissions() {
        JobSubmitMessage message = new JobSubmitMessage(
                "requester-1",
                "2026-07-04T00:00:00Z",
                "job-1",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                "token-job-1"
        );

        assertDoesNotThrow(() -> MessageValidator.validate(message));
    }

    @Test
    void rejectsPeerIdsThatWouldBeSanitizedForRouting() {
        PingMessage message = new PingMessage("peer/unsafe", "2026-07-04T00:00:00Z");

        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(message)
        );

        assertTrue(error.getMessage().contains("nodeId contains unsupported characters"));
    }

    @Test
    void rejectsUnsafeJobIds() {
        JobSubmitMessage message = new JobSubmitMessage(
                "requester-1",
                "2026-07-04T00:00:00Z",
                "../job",
                "TEXT_ANALYSIS",
                List.of("payload"),
                "summary",
                "token-bad-job"
        );

        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(message)
        );

        assertTrue(error.getMessage().contains("Job id may contain only"));
    }

    @Test
    void rejectsOversizeJobPayloads() {
        System.setProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY, "48");
        try {
            JobSubmitMessage message = new JobSubmitMessage(
                    "requester-1",
                    "2026-07-04T00:00:00Z",
                    "job-oversize",
                    "TEXT_ANALYSIS",
                    List.of("this-payload-is-longer-than-the-test-limit"),
                    "summary",
                    "token-job-oversize"
            );

            MessageValidationException error = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(message)
            );

            assertTrue(error.getMessage().contains(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void taskCountLimitAllowsExactBoundaryAndRejectsNextTask() {
        System.setProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY, "2");
        try {
            JobSubmitMessage exact = submission("job-task-exact", List.of("a", "b"));
            JobSubmitMessage over = submission("job-task-over", List.of("a", "b", "c"));

            assertDoesNotThrow(() -> MessageValidator.validate(exact));
            MessageValidationException error = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(over)
            );
            assertEquals(MessageValidator.REASON_MAX_TASKS_PER_JOB, error.reasonCode());
        } finally {
            System.clearProperty(PayloadLimits.MAX_TASKS_PER_JOB_PROPERTY);
        }
    }

    @Test
    void inlineByteLimitAllowsExactBoundaryAndRejectsOneByteLowerMaximum() {
        JobSubmitMessage message = submission("job-inline-boundary", List.of("payload"));
        long measured = PayloadLimits.jobPayloadJsonBytes(
                message.getTaskPayloads(),
                message.getParameter()
        );
        System.setProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY, String.valueOf(measured));
        try {
            assertDoesNotThrow(() -> MessageValidator.validate(message));
            System.setProperty(
                    PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY,
                    String.valueOf(measured - 1L)
            );

            MessageValidationException error = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(message)
            );
            assertEquals(
                    MessageValidator.REASON_MAX_INLINE_MESSAGE_BYTES,
                    error.reasonCode()
            );
        } finally {
            System.clearProperty(PayloadLimits.MAX_JOB_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void referencedPayloadLimitAllowsExactBoundaryAndRejectsOneByteOver() {
        ObjectReference reference = new ObjectReference(
                TaskFlowObjectKeys.objectKey("inputs", "input"),
                11L,
                "a".repeat(64),
                "application/octet-stream"
        );
        JobSubmitMessage message = submission(
                "job-reference-boundary",
                List.of(Map.of("nested", List.of(reference)))
        );
        System.setProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY, "11");
        try {
            assertDoesNotThrow(() -> MessageValidator.validate(message));
            System.setProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY, "10");

            MessageValidationException error = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(message)
            );
            assertEquals(
                    MessageValidator.REASON_MAX_REFERENCED_PAYLOAD_BYTES,
                    error.reasonCode()
            );
        } finally {
            System.clearProperty(PayloadLimits.MAX_INPUT_BYTES_PROPERTY);
        }
    }

    @Test
    void inlineFileLimitRejectsExactBoundaryInSubmitAndAssignment() {
        String base64 = Base64.getEncoder().encodeToString(new byte[] {1, 2, 3});
        Map<String, Object> payload = Map.of(
                "fileName", "sample.png",
                "base64Data", base64
        );
        System.setProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY, "3");
        try {
            JobSubmitMessage submit = submission("job-inline-file", List.of(payload));
            TaskAssignMessage assignment = new TaskAssignMessage(
                    "coordinator-1",
                    "2026-07-27T00:00:00Z",
                    "task-1",
                    "job-inline-file",
                    "IMAGE_CONVERSION",
                    1,
                    "550e8400-e29b-41d4-a716-446655440000",
                    1_780_000_000_000L,
                    payload,
                    "png"
            );

            MessageValidationException submitError = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(submit)
            );
            MessageValidationException assignmentError = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(assignment)
            );

            assertEquals(MessageValidator.REASON_MAX_INLINE_PAYLOAD_BYTES,
                    submitError.reasonCode());
            assertEquals(MessageValidator.REASON_MAX_INLINE_PAYLOAD_BYTES,
                    assignmentError.reasonCode());
        } finally {
            System.clearProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY);
        }
    }

    @Test
    void rejectsLegacyFilesystemReferenceMetadata() {
        Map<String, Object> legacyReference = Map.of(
                "storageType", "local-file",
                "location", "payloads/input.bin",
                "sizeBytes", 11L,
                "sha256", "a".repeat(64)
        );

        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(
                        submission("job-legacy-reference", List.of(legacyReference))
                )
        );

        assertEquals(MessageValidator.REASON_INVALID_PAYLOAD_REFERENCE, error.reasonCode());
    }

    @Test
    void rejectsOversizeResultPayloads() {
        System.setProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY, "48");
        try {
            TaskResultMessage message = new TaskResultMessage(
                    "peer-1",
                    "2026-07-04T00:00:00Z",
                    "task-1",
                    "job-1",
                    1,
                    "550e8400-e29b-41d4-a716-446655440000",
                    "this-result-is-longer-than-the-test-limit",
                    true,
                    ""
            );

            MessageValidationException error = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(message)
            );

            assertTrue(error.getMessage().contains(PayloadLimits.MAX_RESULT_BYTES_ENV));
        } finally {
            System.clearProperty(PayloadLimits.MAX_RESULT_BYTES_PROPERTY);
        }
    }

    @Test
    void acceptsTaskResultReferenceOwnedByItsExactAssignment() {
        String assignmentId = "550e8400-e29b-41d4-a716-446655440000";
        ObjectReference reference = new ObjectReference(
                TaskFlowObjectKeys.attemptOutputKey(
                        "job-1",
                        "task-1",
                        2,
                        assignmentId
                ),
                12L,
                "a".repeat(64),
                "application/octet-stream"
        );
        TaskResultMessage message = new TaskResultMessage(
                "peer-1",
                "2026-07-27T00:00:00Z",
                "task-1",
                "job-1",
                2,
                assignmentId,
                Map.of("objectReference", reference),
                true,
                ""
        );

        assertDoesNotThrow(() -> MessageValidator.validate(message));
    }

    @Test
    void rejectsTaskResultReferenceOwnedByAnotherAttempt() {
        String assignmentId = "550e8400-e29b-41d4-a716-446655440000";
        ObjectReference staleReference = new ObjectReference(
                TaskFlowObjectKeys.attemptOutputKey(
                        "job-1",
                        "task-1",
                        1,
                        assignmentId
                ),
                12L,
                "a".repeat(64),
                "application/octet-stream"
        );
        TaskResultMessage message = new TaskResultMessage(
                "peer-1",
                "2026-07-27T00:00:00Z",
                "task-1",
                "job-1",
                2,
                assignmentId,
                Map.of("objectReference", staleReference),
                true,
                ""
        );

        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(message)
        );

        assertEquals(
                MessageValidator.REASON_INVALID_TASK_OUTPUT_REFERENCE,
                error.reasonCode()
        );
    }

    @Test
    void acceptsExecutorAndRequesterOnlyCapacityAdvertisements() {
        PongMessage executor = new PongMessage(
                "peer-1",
                "2026-07-26T00:00:00Z",
                List.of("TEXT_ANALYSIS", "IMAGE_CONVERSION"),
                EXECUTOR_INSTANCE_ID,
                1L,
                8,
                5,
                Map.of("TEXT_ANALYSIS", 4, "IMAGE_CONVERSION", 2)
        );
        PongMessage requesterOnly = new PongMessage(
                "requester-1",
                "2026-07-26T00:00:00Z",
                List.of(),
                EXECUTOR_INSTANCE_ID,
                1L,
                0,
                0,
                Map.of()
        );

        assertDoesNotThrow(() -> MessageValidator.validate(executor));
        assertDoesNotThrow(() -> MessageValidator.validate(requesterOnly));
    }

    @Test
    void rejectsMalformedCapacityAdvertisementsWithStructuredReason() {
        List<InvalidCapacity> invalidAdvertisements = List.of(
                invalidCapacity(
                        "not-a-uuid", 1L, 8, 8,
                        Map.of("TEXT_ANALYSIS", 1), "executorInstanceId"
                ),
                invalidCapacity(
                        EXECUTOR_INSTANCE_ID, 0L, 8, 8,
                        Map.of("TEXT_ANALYSIS", 1), "capacitySnapshotSequence"
                ),
                invalidCapacity(
                        EXECUTOR_INSTANCE_ID, 1L, 0, 0,
                        Map.of("TEXT_ANALYSIS", 1), "totalCapacityUnits"
                ),
                invalidCapacity(
                        EXECUTOR_INSTANCE_ID, 1L, 8, 9,
                        Map.of("TEXT_ANALYSIS", 1), "availableCapacityUnits"
                ),
                invalidCapacity(
                        EXECUTOR_INSTANCE_ID, 1L, 8, 8,
                        Map.of("IMAGE_CONVERSION", 1), "exactly match"
                ),
                invalidCapacity(
                        EXECUTOR_INSTANCE_ID, 1L, 8, 8,
                        Map.of("TEXT_ANALYSIS", 0), "must be positive"
                ),
                new InvalidCapacity(
                        new PongMessage(
                                "requester-1",
                                "2026-07-26T00:00:00Z",
                                List.of(),
                                EXECUTOR_INSTANCE_ID,
                                1L,
                                1,
                                1,
                                Map.of()
                        ),
                        "Requester-only"
                )
        );

        for (InvalidCapacity invalid : invalidAdvertisements) {
            MessageValidationException error = assertThrows(
                    MessageValidationException.class,
                    () -> MessageValidator.validate(invalid.message())
            );
            assertTrue(error.getMessage().contains(invalid.expectedMessage()));
            assertEquals(
                    MessageValidator.REASON_INVALID_CAPACITY_ADVERTISEMENT,
                    error.reasonCode()
            );
        }
    }

    private static InvalidCapacity invalidCapacity(String instanceId,
                                                    long sequence,
                                                    int totalUnits,
                                                    int availableUnits,
                                                    Map<String, Integer> limits,
                                                    String expectedMessage) {
        PongMessage message = new PongMessage(
                "peer-1",
                "2026-07-26T00:00:00Z",
                List.of("TEXT_ANALYSIS"),
                instanceId,
                sequence,
                totalUnits,
                availableUnits,
                limits
        );
        return new InvalidCapacity(message, expectedMessage);
    }

    private static JobSubmitMessage submission(String jobId, List<Object> payloads) {
        return new JobSubmitMessage(
                "requester-1",
                "2026-07-26T00:00:00Z",
                jobId,
                "TEXT_ANALYSIS",
                payloads,
                "parameter",
                "token-" + jobId
        );
    }

    private record InvalidCapacity(PongMessage message, String expectedMessage) {
    }
}
