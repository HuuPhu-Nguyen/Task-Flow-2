package messaging;

import protocol.Message;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class MessageDispatcher {

    private Map<String, MessageHandler> handlers = new HashMap<>();

    public void register(String type, MessageHandler handler) {
        handlers.put(type, handler);
    }

    public void dispatch(Message message, PrintWriter out) {
        MessageHandler handler = handlers.get(message.getType());
        if (handler != null) {
            handler.handle(message, out);
        } else {
            System.out.println("No handler for type: " + message.getType());
        }
    }
}