package gui;

import protocol.JobResultMessage;

import java.util.Set;

final class GuiJobResultRouter {
    static final String DEFAULT_FAILURE_MESSAGE = "The job failed before producing downloadable results.";

    private GuiJobResultRouter() {
    }

    static RoutedJobResult route(JobResultMessage result, Set<String> activeJobIds) {
        if (result == null || result.getJobId() == null || !activeJobIds.remove(result.getJobId())) {
            return new RoutedJobResult(Action.IGNORE, result);
        }
        Action action = result.isSuccessful() ? Action.SHOW_DOWNLOAD : Action.SHOW_FAILURE;
        return new RoutedJobResult(action, result);
    }

    static String failureMessage(JobResultMessage result) {
        if (result == null || result.getErrorMessage() == null || result.getErrorMessage().isBlank()) {
            return DEFAULT_FAILURE_MESSAGE;
        }
        return result.getErrorMessage();
    }

    enum Action {
        IGNORE,
        SHOW_DOWNLOAD,
        SHOW_FAILURE
    }

    record RoutedJobResult(Action action, JobResultMessage result) {
    }
}
