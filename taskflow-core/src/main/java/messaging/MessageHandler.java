package messaging;

import protocol.Message;
import java.io.PrintWriter;

@FunctionalInterface
public interface MessageHandler {
    void handle(Message message, PrintWriter out);
}