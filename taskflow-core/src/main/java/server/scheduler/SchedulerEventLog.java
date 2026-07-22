package server.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/** Keeps scheduler events on the historical TaskScheduler logger contract. */
final class SchedulerEventLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskScheduler.class);

    Map<String, Object> fields(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("keyValues must be in pairs");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            out.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return out;
    }

    Map<String, Object> assignmentTraceFields(String jobId,
                                              String taskId,
                                              int attemptNumber,
                                              String assignmentId,
                                              String workerId,
                                              Object... additionalFields) {
        Map<String, Object> out = fields(
                "job_id", jobId,
                "task_id", taskId,
                "attempt_number", attemptNumber,
                "assignment_id", assignmentId,
                "worker_id", workerId
        );
        Map<String, Object> extras = fields(additionalFields);
        for (Map.Entry<String, Object> entry : extras.entrySet()) {
            if (out.putIfAbsent(entry.getKey(), entry.getValue()) != null) {
                throw new IllegalArgumentException(
                        "Assignment trace field cannot be replaced: " + entry.getKey()
                );
            }
        }
        return out;
    }

    void info(String event, Map<String, Object> fields) {
        LOGGER.info("event={}{}", event, formatFields(fields));
    }

    void error(String event, Map<String, Object> fields) {
        LOGGER.error("event={}{}", event, formatFields(fields));
    }

    private static String formatFields(Map<String, Object> fields) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            builder.append(' ')
                    .append(entry.getKey())
                    .append('=')
                    .append(String.valueOf(entry.getValue()));
        }
        return builder.toString();
    }
}
