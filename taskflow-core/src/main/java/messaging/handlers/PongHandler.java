package messaging.handlers;

import messaging.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protocol.Message;
import protocol.PongMessage;

import java.io.PrintWriter;

public class PongHandler implements MessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PongHandler.class);

    @Override
    public void handle(Message message, PrintWriter out) {

        PongMessage pong = (PongMessage) message;

        LOGGER.debug("event=pong_received node_id={}", pong.getNodeId());
    }
}
