package transport;

public enum TransportRoute {
    JOB_SUBMIT("jobs.submit"),
    TASK_ASSIGN("tasks.assign"),
    TASK_RESULT("tasks.result"),
    JOB_RESULT("jobs.result"),
    HEARTBEAT("heartbeats");

    private final String routingKey;

    TransportRoute(String routingKey) {
        this.routingKey = routingKey;
    }

    public String routingKey() {
        return routingKey;
    }
}
