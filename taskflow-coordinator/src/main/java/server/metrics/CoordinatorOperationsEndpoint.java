package server.metrics;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.health.CoordinatorHealth;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Owns the coordinator's single bounded operational HTTP listener.
 */
public final class CoordinatorOperationsEndpoint implements AutoCloseable {
    public static final String METRICS_PATH = "/metrics";
    public static final String LIVENESS_PATH = "/health/live";
    public static final String READINESS_PATH = "/health/ready";
    static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CoordinatorOperationsEndpoint.class);
    private static final Gson GSON = new Gson();

    private final MetricsEndpointConfig config;
    private final CoordinatorHealth health;
    private final PrometheusMetricsRenderer renderer = new PrometheusMetricsRenderer();
    private volatile CoordinatorMetricsCollector metricsCollector;
    private HttpServer server;
    private ExecutorService executor;

    public CoordinatorOperationsEndpoint(
            MetricsEndpointConfig config,
            CoordinatorHealth health
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.health = Objects.requireNonNull(health, "health");
    }

    public void installMetricsCollector(CoordinatorMetricsCollector collector) {
        metricsCollector = Objects.requireNonNull(collector, "collector");
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
            Thread thread = new Thread(runnable, "taskflow-operations-endpoint");
            thread.setDaemon(true);
            return thread;
        });
        boolean started = false;
        try {
            created.createContext(METRICS_PATH, this::handleMetrics);
            created.createContext(LIVENESS_PATH, this::handleLiveness);
            created.createContext(READINESS_PATH, this::handleReadiness);
            created.setExecutor(createdExecutor);
            created.start();
            server = created;
            executor = createdExecutor;
            started = true;
            LOGGER.info(
                    "event=coordinator_operations_endpoint_started host={} port={} "
                            + "metrics_path={} liveness_path={} readiness_path={}",
                    config.host(),
                    created.getAddress().getPort(),
                    METRICS_PATH,
                    LIVENESS_PATH,
                    READINESS_PATH
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

    private void handleMetrics(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!validGet(exchange, METRICS_PATH)) {
                return;
            }
            CoordinatorMetricsCollector collector = metricsCollector;
            if (collector == null) {
                send(
                        exchange,
                        503,
                        "metrics unavailable while coordinator is starting\n",
                        "text/plain; charset=utf-8"
                );
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

    private void handleLiveness(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!validGet(exchange, LIVENESS_PATH)) {
                return;
            }
            CoordinatorHealth.LivenessSnapshot snapshot = health.liveness();
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            send(
                    exchange,
                    snapshot.live() ? 200 : 503,
                    GSON.toJson(snapshot) + "\n",
                    JSON_CONTENT_TYPE
            );
        }
    }

    private void handleReadiness(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!validGet(exchange, READINESS_PATH)) {
                return;
            }
            CoordinatorHealth.ReadinessSnapshot snapshot = health.readiness();
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            send(
                    exchange,
                    snapshot.ready() ? 200 : 503,
                    GSON.toJson(snapshot) + "\n",
                    JSON_CONTENT_TYPE
            );
        }
    }

    private static boolean validGet(HttpExchange exchange, String path) throws IOException {
        if (!path.equals(exchange.getRequestURI().getPath())) {
            send(exchange, 404, "not found\n", "text/plain; charset=utf-8");
            return false;
        }
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Allow", "GET");
            send(exchange, 405, "method not allowed\n", "text/plain; charset=utf-8");
            return false;
        }
        return true;
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
            LOGGER.info("event=coordinator_operations_endpoint_stopped");
        }
    }
}
