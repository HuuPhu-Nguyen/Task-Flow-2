package messaging;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import protocol.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class MessageFactory {
    private Map<String, Function<String, Message>> registry = new HashMap<>();

    public void register(String type, Function<String, Message> parser) {
        registry.put(type, parser);
    }

    public Message fromJson(String json) {
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        String type = obj.get("type").getAsString();
        Function<String, Message> parser = registry.get(type);
        if (parser == null) {
            throw new RuntimeException("Unknown message type: " + type);
        }
        return parser.apply(json);
    }
}
