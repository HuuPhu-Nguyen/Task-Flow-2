package transport.rabbitmq;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import protocol.JobResultMessage;
import protocol.JobResultRequestMessage;
import protocol.JobSubmitMessage;
import protocol.Message;
import protocol.MessageType;
import protocol.MessageValidator;
import protocol.PingMessage;
import protocol.PongMessage;
import protocol.ProtocolVersions;
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
        MessageValidator.validatePeerId(outbound.fromNodeId(), "RabbitMQ envelope fromNodeId");
        MessageValidator.validate(outbound.message());
        JsonObject root = new JsonObject();
        root.addProperty(ProtocolVersions.FIELD_NAME, ProtocolVersions.CURRENT);
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
        JsonObject root = parseObject(new String(body, StandardCharsets.UTF_8), "RabbitMQ broker envelope");
        ProtocolVersions.normalizeSupportedVersion(root, "RabbitMQ envelope");
        TransportRoute route = route(root, fallbackRoute);
        String fromNodeId = requiredString(root, FIELD_FROM_NODE_ID, "RabbitMQ envelope fromNodeId");
        MessageValidator.validatePeerId(fromNodeId, "RabbitMQ envelope fromNodeId");
        Message message = parseMessage(requiredMessage(root));
        return new InboundTransportMessage(route, fromNodeId, message, acknowledgement);
    }

    private Message parseMessage(JsonObject messageJson) {
        ProtocolVersions.normalizeSupportedVersion(messageJson, "RabbitMQ message");
        JsonElement typeElement = messageJson.get("type");
        if (typeElement == null || typeElement.isJsonNull() || !typeElement.isJsonPrimitive()) {
            throw new IllegalArgumentException("RabbitMQ message JSON is missing required type field.");
        }
        String type = typeElement.getAsString();
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("RabbitMQ message type is required.");
        }
        String json = gson.toJson(messageJson);
        Message message = switch (type) {
            case MessageType.JOB_SUBMIT -> gson.fromJson(json, JobSubmitMessage.class);
            case MessageType.JOB_RESULT_REQUEST -> gson.fromJson(json, JobResultRequestMessage.class);
            case MessageType.TASK_ASSIGN -> gson.fromJson(json, TaskAssignMessage.class);
            case MessageType.TASK_RESULT -> gson.fromJson(json, TaskResultMessage.class);
            case MessageType.JOB_RESULT -> gson.fromJson(json, JobResultMessage.class);
            case MessageType.PING -> gson.fromJson(json, PingMessage.class);
            case MessageType.PONG -> gson.fromJson(json, PongMessage.class);
            default -> throw new IllegalArgumentException("Unknown message type: " + type);
        };
        MessageValidator.validate(message);
        return message;
    }

    private static JsonObject requiredMessage(JsonObject root) {
        JsonElement messageElement = root.get(FIELD_MESSAGE);
        if (messageElement == null || messageElement.isJsonNull() || !messageElement.isJsonObject()) {
            throw new IllegalArgumentException("RabbitMQ envelope is missing required message object.");
        }
        return messageElement.getAsJsonObject();
    }

    private static JsonObject parseObject(String json, String context) {
        try {
            JsonElement parsed = JsonParser.parseString(json);
            if (parsed == null || !parsed.isJsonObject()) {
                throw new IllegalArgumentException(context + " must be a valid JSON object.");
            }
            return parsed.getAsJsonObject();
        } catch (IllegalStateException | JsonParseException e) {
            throw new IllegalArgumentException(context + " must be a valid JSON object.", e);
        }
    }

    private static TransportRoute route(JsonObject root, TransportRoute fallbackRoute) {
        if (!root.has(FIELD_ROUTE) || root.get(FIELD_ROUTE).isJsonNull()) {
            if (fallbackRoute == null) {
                throw new IllegalArgumentException("RabbitMQ envelope route is required.");
            }
            return fallbackRoute;
        }
        JsonElement routeElement = root.get(FIELD_ROUTE);
        if (!routeElement.isJsonPrimitive()) {
            throw new IllegalArgumentException("RabbitMQ envelope route must be a string.");
        }
        try {
            return TransportRoute.valueOf(routeElement.getAsString());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown RabbitMQ envelope route: " + routeElement.getAsString(), e);
        }
    }

    private static String requiredString(JsonObject root, String fieldName, String description) {
        JsonElement value = root.get(fieldName);
        if (value == null || value.isJsonNull() || !value.isJsonPrimitive()) {
            throw new IllegalArgumentException(description + " is required.");
        }
        return value.getAsString();
    }
}
