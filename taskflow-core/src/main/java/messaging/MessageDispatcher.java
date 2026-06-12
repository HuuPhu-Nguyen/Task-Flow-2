package messaging;

import protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class MessageDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageDispatcher.class);

    private final Map<String, MessageHandler> handlers = new HashMap<>();

    public void register(String type, MessageHandler handler) {
        handlers.put(type, handler);
    }

    public void dispatch(Message message, PrintWriter out) {
        MessageHandler handler = handlers.get(message.getType());
        if (handler != null) {
            handler.handle(message, out);
        } else {
            LOGGER.warn("event=message_handler_missing type={}", message.getType());
        }
    }
}
