package messaging;

import com.google.gson.Gson;

import java.io.PrintWriter;
import java.util.Objects;

public final class SafeJsonWriter {

    private SafeJsonWriter() {
    }

    public static boolean send(PrintWriter out, Gson gson, Object message) {
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(gson, "gson");

        synchronized (out) {
            out.println(gson.toJson(message));
            out.flush();
            return !out.checkError();
        }
    }
}
