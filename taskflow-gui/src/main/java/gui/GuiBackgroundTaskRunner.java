package gui;

import javafx.concurrent.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;

final class GuiBackgroundTaskRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(GuiBackgroundTaskRunner.class);

    interface View {
        void begin(Task<?> task, Runnable cancelAction);

        void finish(Task<?> task);

        void setStatus(String message);

        void showError(String message);
    }

    interface TaskLauncher {
        void launch(String threadName, Task<?> task);
    }

    private final TaskLauncher taskLauncher;
    private volatile Task<?> activeTask;

    GuiBackgroundTaskRunner() {
        this(GuiBackgroundTaskRunner::launchDaemonThread);
    }

    GuiBackgroundTaskRunner(TaskLauncher taskLauncher) {
        this.taskLauncher = Objects.requireNonNull(taskLauncher, "taskLauncher");
    }

    <T> void run(String taskName, Task<T> task, View view, Consumer<T> onSucceeded) {
        Objects.requireNonNull(taskName, "taskName");
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(onSucceeded, "onSucceeded");

        activeTask = task;
        view.begin(task, () -> task.cancel(true));

        task.setOnSucceeded(event -> {
            finish(task, view);
            view.setStatus(taskName + " complete.");
            onSucceeded.accept(task.getValue());
        });
        task.setOnFailed(event -> {
            finish(task, view);
            Throwable error = task.getException();
            String message = errorMessage(error);
            view.setStatus(taskName + " failed.");
            LOGGER.error("event=gui_background_task_failed task={} error={}", taskName, message, error);
            view.showError(taskName + " failed: " + message);
        });
        task.setOnCancelled(event -> {
            finish(task, view);
            view.setStatus(taskName + " cancelled.");
        });

        taskLauncher.launch(threadName(taskName), task);
    }

    void cancelActiveTask() {
        Task<?> task = activeTask;
        if (task != null) {
            task.cancel(true);
        }
    }

    Task<?> activeTask() {
        return activeTask;
    }

    private void finish(Task<?> task, View view) {
        if (activeTask == task) {
            activeTask = null;
        }
        view.finish(task);
    }

    static String threadName(String taskName) {
        return "gui-" + taskName.toLowerCase(Locale.ROOT).replace(' ', '-');
    }

    static String errorMessage(Throwable error) {
        if (error == null || error.getMessage() == null) {
            return "Unknown error";
        }
        return error.getMessage();
    }

    private static void launchDaemonThread(String threadName, Task<?> task) {
        Thread worker = new Thread(task, threadName);
        worker.setDaemon(true);
        worker.start();
    }
}
