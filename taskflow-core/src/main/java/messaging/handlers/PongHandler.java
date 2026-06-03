package messaging.handlers;

import messaging.MessageHandler;
import protocol.Message;
import protocol.PongMessage;

import java.io.PrintWriter;

public class PongHandler implements MessageHandler {

    @Override
    public void handle(Message message, PrintWriter out) {

        PongMessage pong = (PongMessage) message;

        System.out.println("Received PONG from " +
                pong.getNodeId());
    }
}