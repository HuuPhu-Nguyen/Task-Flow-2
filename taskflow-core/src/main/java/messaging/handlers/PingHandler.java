package messaging.handlers;

import com.google.gson.Gson;
import messaging.MessageHandler;
import messaging.SafeJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.Message;
import protocol.PingMessage;
import protocol.PongMessage;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

public class PingHandler implements MessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PingHandler.class);

    private final Gson gson = new Gson();
    private final Supplier<String> nodeIdSupplier;
    private final Supplier<Collection<String>> supportedTaskTypesSupplier;

    public PingHandler() {
        this(Collections::emptyList);
    }

    public PingHandler(Supplier<Collection<String>> supportedTaskTypesSupplier) {
        this(() -> "", supportedTaskTypesSupplier);
    }

    public PingHandler(String nodeId, Supplier<Collection<String>> supportedTaskTypesSupplier) {
        this(() -> nodeId, supportedTaskTypesSupplier);
    }

    public PingHandler(Supplier<String> nodeIdSupplier,
                       Supplier<Collection<String>> supportedTaskTypesSupplier) {
        this.nodeIdSupplier = nodeIdSupplier == null ? () -> "" : nodeIdSupplier;
        this.supportedTaskTypesSupplier = supportedTaskTypesSupplier == null
                ? Collections::emptyList
                : supportedTaskTypesSupplier;
    }

    @Override
    public void handle(Message message, PrintWriter out) {

        PingMessage ping = (PingMessage) message;
        String responseNodeId = nodeIdSupplier.get();
        if (responseNodeId == null || responseNodeId.isBlank()) {
            responseNodeId = ping.getNodeId();
        }
        PongMessage response = new PongMessage(
                responseNodeId,
                Instant.now().toString(),
                supportedTaskTypesSupplier.get()
        );
        if (!SafeJsonWriter.send(out, gson, response)) {
            LOGGER.warn("event=ping_response_send_failed node_id={}", ping.getNodeId());
            throw new IllegalStateException("Could not send PONG response.");
        }
        LOGGER.debug("event=ping_handled node_id={}", ping.getNodeId());
    }
}
