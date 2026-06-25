package gui;

import javafx.concurrent.Task;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class GuiBackgroundTaskRunnerTest {
    @Test
    void runMarksTaskActiveConfiguresViewAndLaunchesDaemonTask() {
        CapturingLauncher launcher = new CapturingLauncher();
        GuiBackgroundTaskRunner runner = new GuiBackgroundTaskRunner(launcher);
        RecordingView view = new RecordingView();
        Task<String> task = newTask();

        runner.run("Submit job", task, view, ignored -> {
        });

        assertSame(task, runner.activeTask());
        assertSame(task, view.startedTask.get());
        assertSame(task, launcher.task.get());
        assertEquals("gui-submit-job", launcher.threadName.get());
    }

    @Test
    void succeededTaskFinishesViewClearsActiveTaskAndRunsCallback() {
        CapturingLauncher launcher = new CapturingLauncher();
        GuiBackgroundTaskRunner runner = new GuiBackgroundTaskRunner(launcher);
        RecordingView view = new RecordingView();
        AtomicInteger successCalls = new AtomicInteger();
        Task<String> task = newTask();

        runner.run("Submit job", task, view, ignored -> successCalls.incrementAndGet());
        task.getOnSucceeded().handle(null);

        assertNull(runner.activeTask());
        assertSame(task, view.finishedTask.get());
        assertEquals("Submit job complete.", view.status.get());
        assertEquals(1, successCalls.get());
    }

    @Test
    void failedTaskFinishesViewClearsActiveTaskAndReportsFailure() {
        CapturingLauncher launcher = new CapturingLauncher();
        GuiBackgroundTaskRunner runner = new GuiBackgroundTaskRunner(launcher);
        RecordingView view = new RecordingView();
        Task<String> task = newTask();

        runner.run("Stage inputs", task, view, ignored -> {
        });
        task.getOnFailed().handle(null);

        assertNull(runner.activeTask());
        assertSame(task, view.finishedTask.get());
        assertEquals("Stage inputs failed.", view.status.get());
        assertEquals("Stage inputs failed: Unknown error", view.error.get());
    }

    @Test
    void cancelledTaskFinishesViewClearsActiveTaskAndReportsCancellation() {
        CapturingLauncher launcher = new CapturingLauncher();
        GuiBackgroundTaskRunner runner = new GuiBackgroundTaskRunner(launcher);
        RecordingView view = new RecordingView();
        Task<String> task = newTask();

        runner.run("Stage inputs", task, view, ignored -> {
        });
        task.getOnCancelled().handle(null);

        assertNull(runner.activeTask());
        assertSame(task, view.finishedTask.get());
        assertEquals("Stage inputs cancelled.", view.status.get());
    }

    @Test
    void formatsThreadNamesAndErrorMessages() {
        assertEquals("gui-stage-inputs", GuiBackgroundTaskRunner.threadName("Stage inputs"));
        assertEquals("Unknown error", GuiBackgroundTaskRunner.errorMessage(null));
        assertEquals("disk full", GuiBackgroundTaskRunner.errorMessage(new IllegalStateException("disk full")));
    }

    private static Task<String> newTask() {
        return new Task<>() {
            @Override
            protected String call() {
                return "done";
            }
        };
    }

    private static final class CapturingLauncher implements GuiBackgroundTaskRunner.TaskLauncher {
        private final AtomicReference<String> threadName = new AtomicReference<>();
        private final AtomicReference<Task<?>> task = new AtomicReference<>();

        @Override
        public void launch(String threadName, Task<?> task) {
            this.threadName.set(threadName);
            this.task.set(task);
        }
    }

    private static final class RecordingView implements GuiBackgroundTaskRunner.View {
        private final AtomicReference<Task<?>> startedTask = new AtomicReference<>();
        private final AtomicReference<Task<?>> finishedTask = new AtomicReference<>();
        private final AtomicReference<String> status = new AtomicReference<>();
        private final AtomicReference<String> error = new AtomicReference<>();

        @Override
        public void begin(Task<?> task, Runnable cancelAction) {
            startedTask.set(task);
        }

        @Override
        public void finish(Task<?> task) {
            finishedTask.set(task);
        }

        @Override
        public void setStatus(String message) {
            status.set(message);
        }

        @Override
        public void showError(String message) {
            error.set(message);
        }
    }
}
