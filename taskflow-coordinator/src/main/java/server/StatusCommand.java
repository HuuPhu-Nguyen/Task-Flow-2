package server;

import server.db.BrokerOutboxStore;
import server.db.DatabaseManager;
import server.db.JobStateStore;
import server.registry.PeerMetricsSnapshot;
import server.registry.PeerRegistryRecord;
import server.registry.PeerStatus;
import transport.rabbitmq.RabbitMqDlqClient;
import transport.rabbitmq.RabbitMqDlqMessage;
import transport.rabbitmq.RabbitMqQueueStatus;
import transport.rabbitmq.RabbitMqTransportConfig;

import java.io.PrintStream;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class StatusCommand {
    private static final String COMMAND = "status";
    private static final int DEFAULT_LIMIT = 20;
    private static final int SUMMARY_OUTBOX_LIMIT = 1_000;
    private static final int BODY_PREVIEW_CHARACTERS = 160;
    private static final String TRANSPORT_ENV = "TASKFLOW_TRANSPORT";

    private StatusCommand() {
    }

    static boolean isCommand(String[] args) {
        return args != null && args.length > 0 && COMMAND.equalsIgnoreCase(args[0]);
    }

    static void run(String[] args, PrintStream out) throws Exception {
        run(
                args,
                out,
                DatabaseStatusDataSource::new,
                DefaultRabbitMqStatusClient::new,
                System.getenv()
        );
    }

    static void run(String[] args,
                    PrintStream out,
                    StatusDataSourceFactory dataSourceFactory,
                    RabbitMqStatusClientFactory rabbitMqStatusClientFactory,
                    Map<String, String> environment) throws Exception {
        StatusOptions options = parse(args);
        if (options.help()) {
            out.println(usage());
            return;
        }

        switch (options.view()) {
            case SUMMARY -> printSummary(out, dataSourceFactory, rabbitMqStatusClientFactory, environment, options.limit());
            case JOBS -> printJobs(out, dataSourceFactory, options.limit());
            case PEERS -> printPeers(out, dataSourceFactory, options.limit());
            case OUTBOX -> printOutbox(out, dataSourceFactory, options.limit());
            case QUEUES -> printQueues(out, rabbitMqStatusClientFactory);
            case DLQ -> printDlq(out, rabbitMqStatusClientFactory, options.limit());
        }
    }

    static String usage() {
        return String.join(System.lineSeparator(),
                "Usage: java -jar taskflow-coordinator-<version>-coordinator-runtime.jar status [summary|jobs|peers|outbox|queues|dlq] [count]",
                "Views:",
                "  summary  SQLite jobs/tasks/attempts/peers/outbox plus RabbitMQ summary unless TASKFLOW_TRANSPORT=tcp",
                "  jobs     Recent persisted jobs with task, retry, and lease counts",
                "  peers    Last-known persisted participant rows (peer registry compatibility view)",
                "  outbox   Pending coordinator RabbitMQ outbox rows",
                "  queues   Passive RabbitMQ queue depth and consumer counts",
                "  dlq      RabbitMQ dead-letter queue inspection summary");
    }

    private static void printSummary(PrintStream out,
                                     StatusDataSourceFactory dataSourceFactory,
                                     RabbitMqStatusClientFactory rabbitMqStatusClientFactory,
                                     Map<String, String> environment,
                                     int limit) {
        out.println("TaskFlow coordinator status");
        try (StatusDataSource source = dataSourceFactory.open()) {
            List<DatabaseManager.JobRecord> jobs = source.jobs();
            List<PeerRegistryRecord> peers = source.peers();
            List<BrokerOutboxStore.OutboxRecord> outbox = source.pendingOutbox(SUMMARY_OUTBOX_LIMIT);

            TaskTotals taskTotals = TaskTotals.empty();
            AttemptTotals attemptTotals = AttemptTotals.empty();
            for (DatabaseManager.JobRecord job : jobs) {
                List<DatabaseManager.TaskRecord> tasks = source.tasksForJob(job.jobId());
                taskTotals = taskTotals.plus(TaskTotals.from(tasks));
                attemptTotals = attemptTotals.plus(AttemptTotals.from(source.attemptsForJob(job.jobId())));
            }

            out.printf("database status=available path=%s schema=%d%n", DatabaseManager.DB_PATH, source.schemaVersion());
            out.printf("jobs total=%d running=%d completed=%d failed=%d%n",
                    jobs.size(),
                    countJobs(jobs, "RUNNING"),
                    countJobs(jobs, "COMPLETED"),
                    countJobs(jobs, "FAILED"));
            printTaskTotals(out, "tasks", taskTotals);
            printAttemptTotals(out, "attempts", attemptTotals);
            out.printf("peers total=%d connected=%d disconnected=%d%n",
                    peers.size(),
                    countPeers(peers, PeerStatus.CONNECTED),
                    countPeers(peers, PeerStatus.DISCONNECTED));
            out.printf("outbox pendingVisible=%d limit=%d failedAttempts=%d%n",
                    outbox.size(),
                    SUMMARY_OUTBOX_LIMIT,
                    outbox.stream().filter(record -> record.attemptCount() > 0).count());
        } catch (Exception e) {
            printDatabaseUnavailable(out, e);
        }

        if (isRabbitMqSelected(environment)) {
            printRabbitMqSummary(out, rabbitMqStatusClientFactory, limit);
        } else {
            out.printf("rabbitmq status=not_selected transport=%s%n",
                    value(environment.getOrDefault(TRANSPORT_ENV, "rabbitmq")));
        }
    }

    private static void printJobs(PrintStream out,
                                  StatusDataSourceFactory dataSourceFactory,
                                  int limit) {
        out.printf("TaskFlow jobs limit=%d%n", limit);
        try (StatusDataSource source = dataSourceFactory.open()) {
            List<DatabaseManager.JobRecord> jobs = source.jobs().stream()
                    .limit(limit)
                    .toList();
            if (jobs.isEmpty()) {
                out.println("jobs none");
                return;
            }
            for (int i = 0; i < jobs.size(); i++) {
                DatabaseManager.JobRecord job = jobs.get(i);
                List<DatabaseManager.TaskRecord> tasks = source.tasksForJob(job.jobId());
                TaskTotals taskTotals = TaskTotals.from(tasks);
                AttemptTotals attemptTotals = AttemptTotals.from(source.attemptsForJob(job.jobId()));
                out.printf(
                        "job[%d] id=%s type=%s status=%s requester=%s files=%d submittedAt=%d completedAt=%d tasks=%d pending=%d assigned=%d completed=%d failed=%d retries=%d attempts=%d%n",
                        i + 1,
                        value(job.jobId()),
                        value(job.taskType()),
                        value(job.status()),
                        value(job.requesterId()),
                        job.fileCount(),
                        job.submittedAt(),
                        job.completedAt(),
                        taskTotals.total(),
                        taskTotals.pending(),
                        taskTotals.assigned(),
                        taskTotals.completed(),
                        taskTotals.failed(),
                        taskTotals.retries(),
                        attemptTotals.total());
            }
        } catch (Exception e) {
            printDatabaseUnavailable(out, e);
        }
    }

    private static void printPeers(PrintStream out,
                                   StatusDataSourceFactory dataSourceFactory,
                                   int limit) {
        out.printf("TaskFlow participants (peers compatibility view) limit=%d%n", limit);
        try (StatusDataSource source = dataSourceFactory.open()) {
            List<PeerRegistryRecord> peers = source.peers().stream()
                    .limit(limit)
                    .toList();
            if (peers.isEmpty()) {
                out.println("peers none");
                return;
            }
            for (int i = 0; i < peers.size(); i++) {
                PeerRegistryRecord peer = peers.get(i);
                PeerMetricsSnapshot metrics = peer.metricsSnapshot();
                out.printf(
                        "peer[%d] id=%s status=%s transport=%s runtime=%s taskTypes=%s completed=%d failed=%d latencyMs=%d taskDurationMs=%d firstSeen=%d lastHeartbeat=%d lastDisconnected=%d%n",
                        i + 1,
                        value(peer.peerId()),
                        value(peer.status()),
                        value(peer.transport()),
                        value(peer.runtimeType()),
                        value(String.join(",", peer.supportedTaskTypes())),
                        metrics.completedTasks(),
                        metrics.failedTasks(),
                        metrics.latencyEwmaMs(),
                        metrics.taskDurationEwmaMs(),
                        peer.firstSeenAtMillis(),
                        peer.lastHeartbeatAtMillis(),
                        peer.lastDisconnectedAtMillis());
            }
        } catch (Exception e) {
            printDatabaseUnavailable(out, e);
        }
    }

    private static void printOutbox(PrintStream out,
                                    StatusDataSourceFactory dataSourceFactory,
                                    int limit) {
        out.printf("TaskFlow outbox limit=%d%n", limit);
        try (StatusDataSource source = dataSourceFactory.open()) {
            List<BrokerOutboxStore.OutboxRecord> outbox = source.pendingOutbox(limit);
            if (outbox.isEmpty()) {
                out.println("outbox pending=0");
                return;
            }
            for (int i = 0; i < outbox.size(); i++) {
                BrokerOutboxStore.OutboxRecord record = outbox.get(i);
                BrokerOutboxStore.OutboxMessage message = record.message();
                out.printf(
                        "outbox[%d] id=%d route=%s peer=%s from=%s message=%s createdAt=%d attempts=%d lastAttemptAt=%d lastError=%s%n",
                        i + 1,
                        record.outboxId(),
                        value(message.route()),
                        value(message.peerNodeId()),
                        value(message.fromNodeId()),
                        value(message.message().getClass().getSimpleName()),
                        record.createdAt(),
                        record.attemptCount(),
                        record.lastAttemptAt(),
                        quoted(record.lastError()));
            }
        } catch (Exception e) {
            printDatabaseUnavailable(out, e);
        }
    }

    private static void printQueues(PrintStream out,
                                    RabbitMqStatusClientFactory rabbitMqStatusClientFactory) {
        out.println("TaskFlow RabbitMQ queues");
        try (RabbitMqStatusClient client = rabbitMqStatusClientFactory.open()) {
            printQueueRows(out, client.inspectQueueStatus());
        } catch (Exception e) {
            printRabbitMqUnavailable(out, e);
        }
    }

    private static void printDlq(PrintStream out,
                                 RabbitMqStatusClientFactory rabbitMqStatusClientFactory,
                                 int limit) {
        out.printf("TaskFlow RabbitMQ DLQ limit=%d%n", limit);
        try (RabbitMqStatusClient client = rabbitMqStatusClientFactory.open()) {
            client.declareTopology();
            printDlqMessages(out, client.inspectDlq(limit), limit);
        } catch (Exception e) {
            printRabbitMqUnavailable(out, e);
        }
    }

    private static void printRabbitMqSummary(PrintStream out,
                                             RabbitMqStatusClientFactory rabbitMqStatusClientFactory,
                                             int limit) {
        try (RabbitMqStatusClient client = rabbitMqStatusClientFactory.open()) {
            client.declareTopology();
            List<RabbitMqQueueStatus> queues = client.inspectQueueStatus();
            List<RabbitMqDlqMessage> messages = client.inspectDlq(limit);
            long availableQueues = queues.stream().filter(RabbitMqQueueStatus::available).count();
            long queuedMessages = queues.stream().mapToLong(RabbitMqQueueStatus::messageCount).sum();
            long redrivable = messages.stream().filter(RabbitMqDlqMessage::redrivable).count();
            out.printf("rabbitmq status=available queues=%d availableQueues=%d queuedMessages=%d dlqVisible=%d dlqRedrivable=%d%n",
                    queues.size(),
                    availableQueues,
                    queuedMessages,
                    messages.size(),
                    redrivable);
        } catch (Exception e) {
            printRabbitMqUnavailable(out, e);
        }
    }

    private static void printQueueRows(PrintStream out, List<RabbitMqQueueStatus> queues) {
        if (queues.isEmpty()) {
            out.println("queues none");
            return;
        }
        for (int i = 0; i < queues.size(); i++) {
            RabbitMqQueueStatus queue = queues.get(i);
            out.printf("queue[%d] role=%s name=%s messages=%d consumers=%d available=%s error=%s%n",
                    i + 1,
                    value(queue.role()),
                    value(queue.queueName()),
                    queue.messageCount(),
                    queue.consumerCount(),
                    queue.available(),
                    quoted(queue.error()));
        }
    }

    private static void printDlqMessages(PrintStream out, List<RabbitMqDlqMessage> messages, int limit) {
        long redrivable = messages.stream().filter(RabbitMqDlqMessage::redrivable).count();
        out.printf("dlq visible=%d limit=%d redrivable=%d nonRedrivable=%d%n",
                messages.size(),
                limit,
                redrivable,
                messages.size() - redrivable);
        for (int i = 0; i < messages.size(); i++) {
            RabbitMqDlqMessage message = messages.get(i);
            out.printf(
                    "dlq[%d] id=%s route=%s originalRoutingKey=%s deadLetterQueue=%s reason=%s deadLetterCount=%d redriveCount=%d redrivable=%s bodyPreview=%s%n",
                    i + 1,
                    value(message.messageId()),
                    value(message.inferredRoute()),
                    value(message.originalRoutingKey()),
                    value(message.deadLetterQueue()),
                    value(message.deadLetterReason()),
                    message.deadLetterCount(),
                    message.redriveCount(),
                    message.redrivable(),
                    quoted(message.bodyPreview(BODY_PREVIEW_CHARACTERS)));
            if (!message.redrivable()) {
                out.printf("dlq[%d] nonRedrivableReason=%s%n", i + 1, quoted(message.nonRedrivableReason()));
            }
        }
    }

    private static void printTaskTotals(PrintStream out, String label, TaskTotals totals) {
        out.printf("%s total=%d pending=%d assigned=%d completed=%d failed=%d retries=%d activeLeases=%d expiredLeases=%d%n",
                label,
                totals.total(),
                totals.pending(),
                totals.assigned(),
                totals.completed(),
                totals.failed(),
                totals.retries(),
                totals.activeLeases(),
                totals.expiredLeases());
    }

    private static void printAttemptTotals(PrintStream out, String label, AttemptTotals totals) {
        out.printf("%s total=%d running=%d succeeded=%d retryScheduled=%d terminalFailure=%d dispatchFailed=%d jobFailed=%d%n",
                label,
                totals.total(),
                totals.count(JobStateStore.TaskAttemptOutcome.RUNNING),
                totals.count(JobStateStore.TaskAttemptOutcome.SUCCEEDED),
                totals.count(JobStateStore.TaskAttemptOutcome.RETRY_SCHEDULED),
                totals.count(JobStateStore.TaskAttemptOutcome.TERMINAL_FAILURE),
                totals.count(JobStateStore.TaskAttemptOutcome.DISPATCH_FAILED),
                totals.count(JobStateStore.TaskAttemptOutcome.JOB_FAILED));
    }

    private static long countJobs(List<DatabaseManager.JobRecord> jobs, String status) {
        return jobs.stream()
                .filter(job -> status.equalsIgnoreCase(value(job.status())))
                .count();
    }

    private static long countPeers(List<PeerRegistryRecord> peers, PeerStatus status) {
        return peers.stream()
                .filter(peer -> peer.status() == status)
                .count();
    }

    private static boolean isRabbitMqSelected(Map<String, String> environment) {
        String configured = environment.get(TRANSPORT_ENV);
        if (configured == null || configured.isBlank()) {
            return true;
        }
        return switch (configured.trim().toLowerCase(Locale.ROOT)) {
            case "rabbitmq" -> true;
            case "tcp" -> false;
            default -> throw new IllegalArgumentException(
                    TRANSPORT_ENV + " must be either tcp or rabbitmq, not '" + configured + "'.");
        };
    }

    private static StatusOptions parse(String[] args) {
        if (!isCommand(args)) {
            throw new IllegalArgumentException(usage());
        }
        if (args.length == 1) {
            return new StatusOptions(StatusView.SUMMARY, DEFAULT_LIMIT, false);
        }
        if (args.length > 3) {
            throw new IllegalArgumentException(usage());
        }

        String viewToken = args[1].trim().toLowerCase(Locale.ROOT);
        if ("--help".equals(viewToken) || "-h".equals(viewToken) || "help".equals(viewToken)) {
            return new StatusOptions(StatusView.SUMMARY, DEFAULT_LIMIT, true);
        }
        if (isInteger(viewToken)) {
            return new StatusOptions(StatusView.SUMMARY, parseCount(viewToken), false);
        }

        StatusView view = switch (viewToken) {
            case "summary" -> StatusView.SUMMARY;
            case "jobs" -> StatusView.JOBS;
            case "peers" -> StatusView.PEERS;
            case "outbox" -> StatusView.OUTBOX;
            case "queues" -> StatusView.QUEUES;
            case "dlq" -> StatusView.DLQ;
            default -> throw new IllegalArgumentException(usage());
        };
        int limit = args.length == 3 ? parseCount(args[2]) : DEFAULT_LIMIT;
        return new StatusOptions(view, limit, false);
    }

    private static int parseCount(String raw) {
        try {
            int count = Integer.parseInt(raw);
            if (count <= 0) {
                throw new IllegalArgumentException("count must be positive");
            }
            return count;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("count must be a positive integer", e);
        }
    }

    private static boolean isInteger(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private static void printDatabaseUnavailable(PrintStream out, Exception e) {
        out.printf("database status=unavailable path=%s error=%s%n",
                DatabaseManager.DB_PATH,
                quoted(e.getMessage()));
    }

    private static void printRabbitMqUnavailable(PrintStream out, Exception e) {
        out.printf("rabbitmq status=unavailable error=%s%n", quoted(e.getMessage()));
    }

    private static String value(Object value) {
        return value == null ? "" : singleLine(value.toString());
    }

    private static String quoted(String value) {
        String text = value(value);
        return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String singleLine(String value) {
        return value == null
                ? ""
                : value.replace('\r', ' ').replace('\n', ' ').trim();
    }

    enum StatusView {
        SUMMARY,
        JOBS,
        PEERS,
        OUTBOX,
        QUEUES,
        DLQ
    }

    record StatusOptions(StatusView view, int limit, boolean help) {
    }

    record TaskTotals(long total,
                      long pending,
                      long assigned,
                      long completed,
                      long failed,
                      long retries,
                      long activeLeases,
                      long expiredLeases) {
        static TaskTotals empty() {
            return new TaskTotals(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        static TaskTotals from(List<DatabaseManager.TaskRecord> tasks) {
            long now = System.currentTimeMillis();
            long pending = 0L;
            long assigned = 0L;
            long completed = 0L;
            long failed = 0L;
            long retries = 0L;
            long activeLeases = 0L;
            long expiredLeases = 0L;
            for (DatabaseManager.TaskRecord task : tasks) {
                String status = value(task.status()).toUpperCase(Locale.ROOT);
                switch (status) {
                    case "PENDING" -> pending++;
                    case "ASSIGNED" -> assigned++;
                    case "COMPLETED" -> completed++;
                    case "FAILED" -> failed++;
                    default -> {
                    }
                }
                retries += Math.max(0, task.retryCount());
                if ("ASSIGNED".equals(status) && hasLease(task)) {
                    if (task.leaseExpiresAt() > now) {
                        activeLeases++;
                    } else {
                        expiredLeases++;
                    }
                }
            }
            return new TaskTotals(tasks.size(), pending, assigned, completed, failed, retries, activeLeases, expiredLeases);
        }

        TaskTotals plus(TaskTotals other) {
            return new TaskTotals(
                    total + other.total,
                    pending + other.pending,
                    assigned + other.assigned,
                    completed + other.completed,
                    failed + other.failed,
                    retries + other.retries,
                    activeLeases + other.activeLeases,
                    expiredLeases + other.expiredLeases
            );
        }

        private static boolean hasLease(DatabaseManager.TaskRecord task) {
            return task.leaseOwnerId() != null
                    && !task.leaseOwnerId().isBlank()
                    && task.leaseExpiresAt() > 0L;
        }
    }

    record AttemptTotals(long total, Map<JobStateStore.TaskAttemptOutcome, Long> counts) {
        static AttemptTotals empty() {
            return new AttemptTotals(0L, new EnumMap<>(JobStateStore.TaskAttemptOutcome.class));
        }

        static AttemptTotals from(List<JobStateStore.TaskAttemptRecord> attempts) {
            Map<JobStateStore.TaskAttemptOutcome, Long> counts =
                    new EnumMap<>(JobStateStore.TaskAttemptOutcome.class);
            for (JobStateStore.TaskAttemptRecord attempt : attempts) {
                if (attempt.outcome() != null) {
                    counts.merge(attempt.outcome(), 1L, Long::sum);
                }
            }
            return new AttemptTotals(attempts.size(), counts);
        }

        long count(JobStateStore.TaskAttemptOutcome outcome) {
            return counts.getOrDefault(outcome, 0L);
        }

        AttemptTotals plus(AttemptTotals other) {
            Map<JobStateStore.TaskAttemptOutcome, Long> merged =
                    new EnumMap<>(JobStateStore.TaskAttemptOutcome.class);
            merged.putAll(counts);
            other.counts.forEach((outcome, count) -> merged.merge(outcome, count, Long::sum));
            return new AttemptTotals(total + other.total, merged);
        }
    }

    interface StatusDataSource extends AutoCloseable {
        int schemaVersion() throws Exception;

        List<DatabaseManager.JobRecord> jobs();

        List<DatabaseManager.TaskRecord> tasksForJob(String jobId);

        List<JobStateStore.TaskAttemptRecord> attemptsForJob(String jobId);

        List<PeerRegistryRecord> peers();

        List<BrokerOutboxStore.OutboxRecord> pendingOutbox(int limit);

        @Override
        default void close() throws Exception {
        }
    }

    @FunctionalInterface
    interface StatusDataSourceFactory {
        StatusDataSource open() throws Exception;
    }

    private static final class DatabaseStatusDataSource implements StatusDataSource {
        private final DatabaseManager database;

        private DatabaseStatusDataSource() throws Exception {
            this.database = new DatabaseManager();
        }

        @Override
        public int schemaVersion() throws Exception {
            return database.getSchemaVersion();
        }

        @Override
        public List<DatabaseManager.JobRecord> jobs() {
            return database.getJobHistory();
        }

        @Override
        public List<DatabaseManager.TaskRecord> tasksForJob(String jobId) {
            return database.getTasksForJob(jobId);
        }

        @Override
        public List<JobStateStore.TaskAttemptRecord> attemptsForJob(String jobId) {
            return database.loadTaskAttempts(jobId);
        }

        @Override
        public List<PeerRegistryRecord> peers() {
            return database.loadPeerRecords();
        }

        @Override
        public List<BrokerOutboxStore.OutboxRecord> pendingOutbox(int limit) {
            return database.loadPendingBrokerOutbox(limit);
        }

        @Override
        public void close() {
            database.close();
        }
    }

    interface RabbitMqStatusClient extends AutoCloseable {
        void declareTopology() throws Exception;

        List<RabbitMqQueueStatus> inspectQueueStatus() throws Exception;

        List<RabbitMqDlqMessage> inspectDlq(int limit) throws Exception;

        @Override
        default void close() throws Exception {
        }
    }

    @FunctionalInterface
    interface RabbitMqStatusClientFactory {
        RabbitMqStatusClient open() throws Exception;
    }

    private static final class DefaultRabbitMqStatusClient implements RabbitMqStatusClient {
        private final RabbitMqDlqClient client;

        private DefaultRabbitMqStatusClient() throws Exception {
            this.client = new RabbitMqDlqClient(RabbitMqTransportConfig.fromEnvironment());
        }

        @Override
        public void declareTopology() throws Exception {
            client.declareTopology();
        }

        @Override
        public List<RabbitMqQueueStatus> inspectQueueStatus() throws Exception {
            return client.inspectQueueStatus();
        }

        @Override
        public List<RabbitMqDlqMessage> inspectDlq(int limit) throws Exception {
            return client.inspect(limit);
        }

        @Override
        public void close() throws Exception {
            client.close();
        }
    }
}
