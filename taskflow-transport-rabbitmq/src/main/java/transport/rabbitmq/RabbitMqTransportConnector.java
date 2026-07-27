package transport.rabbitmq;

import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ProtocolVersionMismatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Owns one interruptible initial-connection loop. RabbitMQ's Java client only
 * performs automatic recovery after a connection has succeeded, so runtime
 * entry points use this owner to keep bounded startup retry running during
 * broker outages.
 */
public final class RabbitMqTransportConnector implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqTransportConnector.class);

    private final Object lifecycleLock = new Object();
    private final RabbitMqTransportConfig config;
    private final RabbitMqRecoveryPolicy recoveryPolicy;
    private final ConnectionAttempt connectionAttempt;
    private final Delay delay;

    private volatile State state = State.IDLE;
    private volatile int attempts;
    private boolean closed;
    private boolean connectCalled;
    private Thread connectingThread;
    private RabbitMqTransport connectedTransport;

    public RabbitMqTransportConnector(RabbitMqTransportConfig config,
                                      RabbitMqRecoveryPolicy recoveryPolicy) {
        this(
                config,
                recoveryPolicy,
                () -> new RabbitMqTransport(config, recoveryPolicy),
                Thread::sleep
        );
    }

    RabbitMqTransportConnector(RabbitMqTransportConfig config,
                               RabbitMqRecoveryPolicy recoveryPolicy,
                               ConnectionAttempt connectionAttempt,
                               Delay delay) {
        this.config = Objects.requireNonNull(config, "config");
        this.recoveryPolicy = Objects.requireNonNull(recoveryPolicy, "recoveryPolicy");
        this.connectionAttempt = Objects.requireNonNull(connectionAttempt, "connectionAttempt");
        this.delay = Objects.requireNonNull(delay, "delay");
    }

    public RabbitMqTransport connect() throws Exception {
        long startedNanos = System.nanoTime();
        synchronized (lifecycleLock) {
            if (connectCalled) {
                throw new IllegalStateException("connect may only be called once");
            }
            if (closed) {
                throw new IllegalStateException("connector is closed");
            }
            connectCalled = true;
            connectingThread = Thread.currentThread();
            state = State.CONNECTING;
        }

        while (true) {
            int attempt = ++attempts;
            try {
                RabbitMqTransport transport = Objects.requireNonNull(
                        connectionAttempt.connect(),
                        "connection attempt returned null"
                );
                synchronized (lifecycleLock) {
                    if (closed) {
                        closeQuietly(transport);
                        throw new InterruptedException("RabbitMQ connection startup was stopped");
                    }
                    connectedTransport = transport;
                    connectingThread = null;
                    state = State.CONNECTED;
                }
                LOGGER.info(
                        "event=rabbitmq_initial_connection_ready attempts={} elapsed_ms={} host={} port={}",
                        attempt,
                        elapsedMillis(startedNanos),
                        config.host(),
                        config.port()
                );
                return transport;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                markStopped();
                LOGGER.info(
                        "event=rabbitmq_initial_connection_stopped attempts={} elapsed_ms={} host={} port={}",
                        attempt,
                        elapsedMillis(startedNanos),
                        config.host(),
                        config.port()
                );
                throw e;
            } catch (Exception e) {
                synchronized (lifecycleLock) {
                    if (closed) {
                        connectingThread = null;
                        state = State.CLOSED;
                        Thread.currentThread().interrupt();
                        throw stoppedException(e);
                    }
                }
                if (!isRetryable(e)) {
                    markFailed();
                    LOGGER.error(
                            "event=rabbitmq_initial_connection_failed_permanently attempt={} elapsed_ms={} "
                                    + "host={} port={} error_type={} error={}",
                            attempt,
                            elapsedMillis(startedNanos),
                            config.host(),
                            config.port(),
                            e.getClass().getSimpleName(),
                            e.getMessage()
                    );
                    throw e;
                }
                long retryDelayMillis = recoveryPolicy.retryDelayMillis(attempt);
                synchronized (lifecycleLock) {
                    if (closed) {
                        connectingThread = null;
                        state = State.CLOSED;
                        Thread.currentThread().interrupt();
                        throw stoppedException(e);
                    }
                    state = State.WAITING_TO_RETRY;
                }
                LOGGER.warn(
                        "event=rabbitmq_initial_connection_retry_scheduled attempt={} delay_ms={} "
                                + "elapsed_ms={} host={} port={} error_type={} error={}",
                        attempt,
                        retryDelayMillis,
                        elapsedMillis(startedNanos),
                        config.host(),
                        config.port(),
                        e.getClass().getSimpleName(),
                        e.getMessage()
                );
                try {
                    delay.sleep(retryDelayMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    markStopped();
                    LOGGER.info(
                            "event=rabbitmq_initial_connection_stopped attempts={} elapsed_ms={} host={} port={}",
                            attempt,
                            elapsedMillis(startedNanos),
                            config.host(),
                            config.port()
                    );
                    throw interrupted;
                }
                synchronized (lifecycleLock) {
                    if (closed) {
                        connectingThread = null;
                        state = State.CLOSED;
                        throw stoppedException(e);
                    }
                    state = State.CONNECTING;
                }
            }
        }
    }

    /**
     * Transfers close ownership to the runtime shutdown coordinator after all
     * consumers and state owners have been constructed.
     */
    public RabbitMqTransport releaseTransportOwnership() {
        synchronized (lifecycleLock) {
            if (closed || state != State.CONNECTED || connectedTransport == null) {
                throw new IllegalStateException("no connected transport is owned");
            }
            RabbitMqTransport transport = connectedTransport;
            connectedTransport = null;
            return transport;
        }
    }

    public State state() {
        return state;
    }

    public int attempts() {
        return attempts;
    }

    @Override
    public void close() {
        Thread threadToInterrupt;
        RabbitMqTransport transportToClose;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            state = State.CLOSED;
            threadToInterrupt = connectingThread;
            connectingThread = null;
            transportToClose = connectedTransport;
            connectedTransport = null;
        }
        if (threadToInterrupt != null && threadToInterrupt != Thread.currentThread()) {
            threadToInterrupt.interrupt();
        }
        closeQuietly(transportToClose);
    }

    static boolean isRetryable(Exception error) {
        boolean transientTransportFailure = false;
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException
                    || cause instanceof PossibleAuthenticationFailureException
                    || cause instanceof ProtocolVersionMismatchException
                    || cause instanceof IllegalArgumentException) {
                return false;
            }
            if (cause instanceof IOException || cause instanceof TimeoutException) {
                transientTransportFailure = true;
            }
        }
        return transientTransportFailure;
    }

    private void markStopped() {
        synchronized (lifecycleLock) {
            connectingThread = null;
            state = closed ? State.CLOSED : State.FAILED;
        }
    }

    private void markFailed() {
        synchronized (lifecycleLock) {
            connectingThread = null;
            state = closed ? State.CLOSED : State.FAILED;
        }
    }

    private static InterruptedException stoppedException(Exception cause) {
        InterruptedException stopped =
                new InterruptedException("RabbitMQ connection startup was stopped");
        stopped.initCause(cause);
        return stopped;
    }

    private static long elapsedMillis(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000L;
    }

    private static void closeQuietly(RabbitMqTransport transport) {
        if (transport == null) {
            return;
        }
        try {
            transport.close();
        } catch (Exception closeError) {
            LOGGER.warn(
                    "event=rabbitmq_initial_connection_cleanup_failed error={}",
                    closeError.getMessage(),
                    closeError
            );
        }
    }

    public enum State {
        IDLE,
        CONNECTING,
        WAITING_TO_RETRY,
        CONNECTED,
        FAILED,
        CLOSED
    }

    @FunctionalInterface
    interface ConnectionAttempt {
        RabbitMqTransport connect() throws Exception;
    }

    @FunctionalInterface
    interface Delay {
        void sleep(long delayMillis) throws InterruptedException;
    }
}
