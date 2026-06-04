package messaging.handlers;

import com.google.gson.Gson;
import messaging.MessageHandler;
import protocol.Message;
import protocol.PingMessage;
import protocol.PongMessage;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

public class PingHandler implements MessageHandler {

    private final Gson gson = new Gson();
    private final Supplier<Collection<String>> supportedTaskTypesSupplier;

    public PingHandler() {
        this(Collections::emptyList);
    }

    public PingHandler(Supplier<Collection<String>> supportedTaskTypesSupplier) {
        this.supportedTaskTypesSupplier = supportedTaskTypesSupplier == null
                ? Collections::emptyList
                : supportedTaskTypesSupplier;
    }

    @Override
    public void handle(Message message, PrintWriter out) {

        PingMessage ping = (PingMessage) message;
        PongMessage response = new PongMessage(
                ping.getNodeId(),
                Instant.now().toString(),
                supportedTaskTypesSupplier.get()
        );
        synchronized (out) {
            out.println(gson.toJson(response));
        }
        System.out.println("Handled PING -> sent PONG");
    }
}
