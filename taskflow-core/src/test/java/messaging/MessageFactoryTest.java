package messaging;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import protocol.Message;
import protocol.MessageType;
import protocol.PingMessage;
import protocol.ProtocolVersions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageFactoryTest {
    private final Gson gson = new Gson();

    @Test
    void parsesRegisteredMessageType() {
        MessageFactory factory = new MessageFactory();
        factory.register(MessageType.PING, json -> gson.fromJson(json, PingMessage.class));

        Message parsed = factory.fromJson(gson.toJson(new PingMessage("peer-1", "2026-06-12T00:00:00Z")));

        PingMessage ping = assertInstanceOf(PingMessage.class, parsed);
        assertEquals(MessageType.PING, ping.getType());
        assertEquals(ProtocolVersions.CURRENT, ping.getProtocolVersion());
        assertEquals("peer-1", ping.getNodeId());
    }

    @Test
    void parsesLegacyMessageWithoutProtocolVersion() {
        MessageFactory factory = new MessageFactory();
        factory.register(MessageType.PING, json -> gson.fromJson(json, PingMessage.class));

        Message parsed = factory.fromJson("""
                {"type":"PING","nodeId":"peer-1","time":"2026-06-12T00:00:00Z"}
                """);

        PingMessage ping = assertInstanceOf(PingMessage.class, parsed);
        assertEquals(MessageType.PING, ping.getType());
        assertEquals(ProtocolVersions.LEGACY, ping.getProtocolVersion());
        assertEquals("peer-1", ping.getNodeId());
    }

    @Test
    void rejectsUnsupportedFutureProtocolVersionWithClearError() {
        MessageFactory factory = new MessageFactory();
        factory.register(MessageType.PING, json -> gson.fromJson(json, PingMessage.class));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> factory.fromJson("""
                        {"protocolVersion":2,"type":"PING","nodeId":"peer-1","time":"2026-06-12T00:00:00Z"}
                        """)
        );

        assertEquals("Message uses unsupported TaskFlow protocolVersion 2; supported versions are 0 through 1.",
                error.getMessage());
    }

    @Test
    void rejectsInvalidProtocolVersionWithClearError() {
        MessageFactory factory = new MessageFactory();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> factory.fromJson("""
                        {"protocolVersion":"future","type":"PING","nodeId":"peer-1","time":"2026-06-12T00:00:00Z"}
                        """)
        );

        assertEquals("Message protocolVersion must be an integer.", error.getMessage());
    }

    @Test
    void rejectsMalformedJsonWithClearError() {
        MessageFactory factory = new MessageFactory();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> factory.fromJson("{")
        );
        assertEquals("Message JSON must be a valid object.", error.getMessage());
    }

    @Test
    void rejectsMissingOrUnknownTypeWithClearError() {
        MessageFactory factory = new MessageFactory();

        assertEquals("Message JSON is missing required type field.",
                assertThrows(IllegalArgumentException.class, () -> factory.fromJson("{}")).getMessage());
        assertEquals("Unknown message type: UNKNOWN",
                assertThrows(IllegalArgumentException.class,
                        () -> factory.fromJson("{\"type\":\"UNKNOWN\"}")).getMessage());
    }

    @Test
    void rejectsInvalidParsedMessageFields() {
        MessageFactory factory = new MessageFactory();
        factory.register(MessageType.PING, json -> gson.fromJson(json, PingMessage.class));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> factory.fromJson("""
                        {"type":"PING","nodeId":"peer/unsafe","time":"2026-06-12T00:00:00Z"}
                        """)
        );

        assertEquals("Message nodeId contains unsupported characters.", error.getMessage());
    }

    @Test
    void rejectsInvalidRegistrations() {
        MessageFactory factory = new MessageFactory();

        assertThrows(IllegalArgumentException.class, () -> factory.register(" ", json -> null));
        assertThrows(NullPointerException.class, () -> factory.register(MessageType.PING, null));
    }
}
