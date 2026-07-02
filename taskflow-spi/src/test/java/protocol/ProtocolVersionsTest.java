package protocol;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolVersionsTest {
    private final Gson gson = new Gson();

    @Test
    void messagesSerializeCurrentProtocolVersion() {
        JsonObject json = JsonParser.parseString(
                gson.toJson(new PingMessage("peer-1", "2026-07-02T00:00:00Z"))
        ).getAsJsonObject();

        assertEquals(ProtocolVersions.CURRENT, json.get(ProtocolVersions.FIELD_NAME).getAsInt());
    }

    @Test
    void missingProtocolVersionIsLegacyCompatible() {
        JsonObject json = new JsonObject();

        assertEquals(ProtocolVersions.LEGACY, ProtocolVersions.read(json, "Message"));
        ProtocolVersions.requireSupported(json, "Message");
    }

    @Test
    void acceptsCurrentAndExplicitLegacyVersions() {
        ProtocolVersions.requireSupported(ProtocolVersions.CURRENT, "Message");
        ProtocolVersions.requireSupported(ProtocolVersions.LEGACY, "Message");
    }

    @Test
    void rejectsUnsupportedFutureVersionWithClearError() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolVersions.requireSupported(ProtocolVersions.CURRENT + 1, "Message")
        );

        assertEquals("Message uses unsupported TaskFlow protocolVersion 2; supported versions are 0 through 1.",
                error.getMessage());
    }

    @Test
    void rejectsInvalidProtocolVersionWithClearError() {
        JsonObject json = new JsonObject();
        json.addProperty(ProtocolVersions.FIELD_NAME, "1.5");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolVersions.read(json, "Message")
        );

        assertEquals("Message protocolVersion must be an integer.", error.getMessage());
    }
}
