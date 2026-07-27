package server.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class PrometheusMetricsEndpoint implements AutoCloseable {
    public static final String PATH = "/metrics";
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PrometheusMetricsEndpoint.class);

    private final MetricsEndpointConfig config;
    private final CoordinatorMetricsCollector collector;
    private final PrometheusMetricsRenderer renderer = new PrometheusMetricsRenderer();
    private HttpServer server;
    private ExecutorService executor;

    public PrometheusMetricsEndpoint(
            MetricsEndpointConfig config,
            CoordinatorMetricsCollector collector
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.collector = Objects.requireNonNull(collector, "collector");
    }

    public synchronized void start() throws IOException {
        if (!config.enabled() || server != null) {
            return;
        }
        HttpServer created = HttpServer.create(
                new InetSocketAddress(config.host(), config.port()),
                0
        );
        ExecutorService createdExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "taskflow-metrics-endpoint");
            thread.setDaemon(true);
            return thread;
        });
        boolean started = false;
        try {
            created.createContext(PATH, this::handle);
            created.setExecutor(createdExecutor);
            created.start();
            server = created;
            executor = createdExecutor;
            started = true;
            LOGGER.info(
                    "event=metrics_endpoint_started host={} port={} path={} format=prometheus",
                    config.host(),
                    created.getAddress().getPort(),
                    PATH
            );
        } finally {
            if (!started) {
                created.stop(0);
                createdExecutor.shutdownNow();
            }
        }
    }

    public synchronized int boundPort() {
        return server == null ? -1 : server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!PATH.equals(exchange.getRequestURI().getPath())) {
                send(exchange, 404, "not found\n", "text/plain; charset=utf-8");
                return;
            }
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                send(exchange, 405, "method not allowed\n", "text/plain; charset=utf-8");
                return;
            }
            String body;
            try {
                body = renderer.render(collector.snapshot());
            } catch (RuntimeException e) {
                LOGGER.warn("event=metrics_scrape_failed error={}", e.getMessage(), e);
                send(exchange, 503, "metrics unavailable\n", "text/plain; charset=utf-8");
                return;
            }
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            send(exchange, 200, body, PrometheusMetricsRenderer.CONTENT_TYPE);
        }
    }

    private static void send(
            HttpExchange exchange,
            int status,
            String body,
            String contentType
    ) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    @Override
    public synchronized void close() {
        boolean stopped = server != null;
        if (server != null) {
            server.stop(0);
            server = null;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        if (stopped) {
            LOGGER.info("event=metrics_endpoint_stopped");
        }
    }
}
