package transport.rabbitmq;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import protocol.JobResultMessage;
import protocol.JobSubmitMessage;
import protocol.Message;
import protocol.MessageType;
import protocol.PingMessage;
import protocol.PongMessage;
import protocol.TaskAssignMessage;
import protocol.TaskResultMessage;
import transport.InboundTransportMessage;
import transport.OutboundTransportMessage;
import transport.TransportAcknowledgement;
import transport.TransportRoute;

import java.nio.charset.StandardCharsets;

public class RabbitMqMessageCodec {
    private static final String FIELD_ROUTE = "route";
    private static final String FIELD_FROM_NODE_ID = "fromNodeId";
    private static final String FIELD_MESSAGE = "message";

    private final Gson gson = new Gson();

    public byte[] encode(OutboundTransportMessage outbound) {
        JsonObject root = new JsonObject();
        root.addProperty(FIELD_ROUTE, outbound.route().name());
        root.addProperty(FIELD_FROM_NODE_ID, outbound.fromNodeId());
        root.add(FIELD_MESSAGE, gson.toJsonTree(outbound.message()));
        return gson.toJson(root).getBytes(StandardCharsets.UTF_8);
    }

    public InboundTransportMessage decode(
            byte[] body,
            TransportRoute fallbackRoute,
            TransportAcknowledgement acknowledgement
    ) {
        JsonObject root = JsonParser.parseString(new String(body, StandardCharsets.UTF_8)).getAsJsonObject();
        TransportRoute route = root.has(FIELD_ROUTE)
                ? TransportRoute.valueOf(root.get(FIELD_ROUTE).getAsString())
                : fallbackRoute;
        String fromNodeId = root.has(FIELD_FROM_NODE_ID)
                ? root.get(FIELD_FROM_NODE_ID).getAsString()
                : "";
        Message message = parseMessage(root.getAsJsonObject(FIELD_MESSAGE));
        return new InboundTransportMessage(route, fromNodeId, message, acknowledgement);
    }

    private Message parseMessage(JsonObject messageJson) {
        String type = messageJson.get("type").getAsString();
        String json = gson.toJson(messageJson);
        return switch (type) {
            case MessageType.JOB_SUBMIT -> gson.fromJson(json, JobSubmitMessage.class);
            case MessageType.TASK_ASSIGN -> gson.fromJson(json, TaskAssignMessage.class);
            case MessageType.TASK_RESULT -> gson.fromJson(json, TaskResultMessage.class);
            case MessageType.JOB_RESULT -> gson.fromJson(json, JobResultMessage.class);
            case MessageType.PING -> gson.fromJson(json, PingMessage.class);
            case MessageType.PONG -> gson.fromJson(json, PongMessage.class);
            default -> throw new IllegalArgumentException("Unknown message type: " + type);
        };
    }
}
