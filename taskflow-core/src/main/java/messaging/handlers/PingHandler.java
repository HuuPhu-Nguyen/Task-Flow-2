package messaging.handlers;

import com.google.gson.Gson;
import messaging.MessageHandler;
import protocol.Message;
import protocol.PingMessage;
import protocol.PongMessage;

import java.io.PrintWriter;
import java.time.Instant;

public class PingHandler implements MessageHandler {

    private Gson gson = new Gson();

    @Override
    public void handle(Message message, PrintWriter out) {

        PingMessage ping = (PingMessage) message;
        PongMessage response = new PongMessage(
                ping.getNodeId(),
                Instant.now().toString()
        );
        synchronized (out) {
            out.println(gson.toJson(response));
        }
        System.out.println("Handled PING -> sent PONG");
    }
}