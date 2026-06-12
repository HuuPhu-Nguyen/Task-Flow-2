package messaging;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import protocol.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class MessageFactory {
    private final Map<String, Function<String, Message>> registry = new HashMap<>();

    public void register(String type, Function<String, Message> parser) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Message type is required.");
        }
        registry.put(type, Objects.requireNonNull(parser, "parser"));
    }

    public Message fromJson(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("Message JSON is required.");
        }

        JsonObject obj;
        try {
            obj = JsonParser.parseString(json).getAsJsonObject();
        } catch (IllegalStateException | JsonParseException e) {
            throw new IllegalArgumentException("Message JSON must be a valid object.", e);
        }

        JsonElement typeElement = obj.get("type");
        if (typeElement == null || typeElement.isJsonNull() || !typeElement.isJsonPrimitive()) {
            throw new IllegalArgumentException("Message JSON is missing required type field.");
        }

        String type = typeElement.getAsString();
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Message type is required.");
        }

        Function<String, Message> parser = registry.get(type);
        if (parser == null) {
            throw new IllegalArgumentException("Unknown message type: " + type);
        }
        return parser.apply(json);
    }
}
