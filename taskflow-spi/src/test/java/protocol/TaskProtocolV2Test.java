package protocol;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskProtocolV2Test {
    private static final String ASSIGNMENT_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final long LEASE_EXPIRES_AT = 1_780_000_000_000L;

    private final Gson gson = new Gson();

    @Test
    void serializesAndDeserializesVersionTwoTaskAssignment() {
        TaskAssignMessage message = assignment();

        JsonObject json = JsonParser.parseString(gson.toJson(message)).getAsJsonObject();

        assertEquals(2, json.get("protocolVersion").getAsInt());
        assertEquals(4, json.get("attemptNumber").getAsInt());
        assertEquals(ASSIGNMENT_ID, json.get("assignmentId").getAsString());
        assertEquals(LEASE_EXPIRES_AT, json.get("leaseExpiresAtEpochMillis").getAsLong());
        assertEquals("summary", json.get("parameter").getAsString());
        assertFalse(json.has("param"));

        TaskAssignMessage decoded = gson.fromJson(json, TaskAssignMessage.class);
        assertDoesNotThrow(() -> MessageValidator.validate(decoded));
        assertEquals(4, decoded.getAttemptNumber());
        assertEquals(ASSIGNMENT_ID, decoded.getAssignmentId());
        assertEquals(LEASE_EXPIRES_AT, decoded.getLeaseExpiresAtEpochMillis());
        assertEquals("summary", decoded.getParameter());
    }

    @Test
    void serializesAndDeserializesVersionTwoTaskResult() {
        TaskResultMessage message = new TaskResultMessage(
                "worker-1",
                "2026-07-22T06:00:00Z",
                "task-1",
                "job-1",
                4,
                ASSIGNMENT_ID,
                "result",
                true,
                null
        );

        JsonObject json = JsonParser.parseString(gson.toJson(message)).getAsJsonObject();
        assertEquals(2, json.get("protocolVersion").getAsInt());
        assertEquals(4, json.get("attemptNumber").getAsInt());
        assertEquals(ASSIGNMENT_ID, json.get("assignmentId").getAsString());

        TaskResultMessage decoded = gson.fromJson(json, TaskResultMessage.class);
        assertDoesNotThrow(() -> MessageValidator.validate(decoded));
        assertEquals(4, decoded.getAttemptNumber());
        assertEquals(ASSIGNMENT_ID, decoded.getAssignmentId());
        assertEquals("result", decoded.getResultPayload());
    }

    @Test
    void coordinatorEnrichmentTurnsPluginTemplateIntoVersionTwoAssignment() {
        TaskAssignMessage template = new TaskAssignMessage(
                "COORDINATOR",
                "2026-07-22T06:00:00Z",
                "task-1",
                "job-1",
                "TEXT_ANALYSIS",
                "payload",
                "summary"
        );

        TaskAssignMessage assignment = template.withAssignmentIdentity(4, ASSIGNMENT_ID, LEASE_EXPIRES_AT);

        assertEquals(ProtocolVersions.VERSION_1, template.getProtocolVersion());
        assertEquals(ProtocolVersions.ASSIGNMENT_IDENTITY, assignment.getProtocolVersion());
        assertDoesNotThrow(() -> MessageValidator.validate(assignment));
        assertEquals(4, assignment.getAttemptNumber());
        assertEquals(ASSIGNMENT_ID, assignment.getAssignmentId());
    }

    @Test
    void rejectsMissingZeroNegativeBlankAndMalformedAssignmentFields() {
        assertInvalidAssignment(assignmentJsonWithout("attemptNumber"));
        assertInvalidAssignment(assignmentJsonWith("attemptNumber", 0));
        assertInvalidAssignment(assignmentJsonWith("attemptNumber", -1));
        assertInvalidAssignment(assignmentJsonWithout("assignmentId"));
        assertInvalidAssignment(assignmentJsonWith("assignmentId", " "));
        assertInvalidAssignment(assignmentJsonWith("assignmentId", "1-1-1-1-1"));
        assertInvalidAssignment(assignmentJsonWithout("leaseExpiresAtEpochMillis"));
        assertInvalidAssignment(assignmentJsonWith("leaseExpiresAtEpochMillis", 0));
        assertInvalidAssignment(assignmentJsonWith("leaseExpiresAtEpochMillis", -1));

        assertInvalidResult(resultJsonWithout("attemptNumber"));
        assertInvalidResult(resultJsonWith("attemptNumber", 0));
        assertInvalidResult(resultJsonWith("attemptNumber", -1));
        assertInvalidResult(resultJsonWithout("assignmentId"));
        assertInvalidResult(resultJsonWith("assignmentId", ""));
        assertInvalidResult(resultJsonWith("assignmentId", "not-a-uuid"));
    }

    @Test
    void rejectsLegacyTaskMessagesWithStructuredCompatibilityReason() {
        TaskAssignMessage legacyAssignment = gson.fromJson(
                assignmentJsonWith("protocolVersion", ProtocolVersions.VERSION_1),
                TaskAssignMessage.class
        );
        TaskResultMessage legacyResult = gson.fromJson(
                resultJsonWith("protocolVersion", ProtocolVersions.LEGACY),
                TaskResultMessage.class
        );

        MessageValidationException assignmentError = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(legacyAssignment)
        );
        MessageValidationException resultError = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(legacyResult)
        );

        assertEquals(MessageValidator.REASON_ASSIGNMENT_PROTOCOL_V2_REQUIRED,
                assignmentError.reasonCode());
        assertEquals(MessageValidator.REASON_ASSIGNMENT_PROTOCOL_V2_REQUIRED,
                resultError.reasonCode());
    }

    private TaskAssignMessage assignment() {
        return new TaskAssignMessage(
                "worker-1",
                "2026-07-22T06:00:00Z",
                "task-1",
                "job-1",
                "TEXT_ANALYSIS",
                4,
                ASSIGNMENT_ID,
                LEASE_EXPIRES_AT,
                "payload",
                "summary"
        );
    }

    private JsonObject assignmentJsonWithout(String field) {
        JsonObject json = JsonParser.parseString(gson.toJson(assignment())).getAsJsonObject();
        json.remove(field);
        return json;
    }

    private JsonObject assignmentJsonWith(String field, Number value) {
        JsonObject json = JsonParser.parseString(gson.toJson(assignment())).getAsJsonObject();
        json.addProperty(field, value);
        return json;
    }

    private JsonObject assignmentJsonWith(String field, String value) {
        JsonObject json = JsonParser.parseString(gson.toJson(assignment())).getAsJsonObject();
        json.addProperty(field, value);
        return json;
    }

    private JsonObject resultJsonWithout(String field) {
        JsonObject json = validResultJson();
        json.remove(field);
        return json;
    }

    private JsonObject resultJsonWith(String field, Number value) {
        JsonObject json = validResultJson();
        json.addProperty(field, value);
        return json;
    }

    private JsonObject resultJsonWith(String field, String value) {
        JsonObject json = validResultJson();
        json.addProperty(field, value);
        return json;
    }

    private JsonObject validResultJson() {
        return JsonParser.parseString(gson.toJson(new TaskResultMessage(
                "worker-1",
                "2026-07-22T06:00:00Z",
                "task-1",
                "job-1",
                4,
                ASSIGNMENT_ID,
                "result",
                true,
                null
        ))).getAsJsonObject();
    }

    private void assertInvalidAssignment(JsonObject json) {
        TaskAssignMessage decoded = gson.fromJson(json, TaskAssignMessage.class);
        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(decoded)
        );
        assertEquals(MessageValidator.REASON_INVALID_ASSIGNMENT_IDENTITY, error.reasonCode());
    }

    private void assertInvalidResult(JsonObject json) {
        TaskResultMessage decoded = gson.fromJson(json, TaskResultMessage.class);
        MessageValidationException error = assertThrows(
                MessageValidationException.class,
                () -> MessageValidator.validate(decoded)
        );
        assertEquals(MessageValidator.REASON_INVALID_ASSIGNMENT_IDENTITY, error.reasonCode());
    }
}
