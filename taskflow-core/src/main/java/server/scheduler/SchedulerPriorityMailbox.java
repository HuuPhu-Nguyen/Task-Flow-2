package server.scheduler;

import protocol.TaskResultMessage;
import server.model.MessageEnvelope;

import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded scheduler mailbox with one task-result reserve. Task results are
 * dequeued before ordinary submissions, but still consume the scheduler's
 * existing message-stage budget.
 */
final class SchedulerPriorityMailbox extends AbstractQueue<MessageEnvelope>
        implements BlockingQueue<MessageEnvelope> {
    static final int TASK_RESULT_RESERVE_CAPACITY = 1;

    private final int ordinaryCapacity;
    private final ArrayDeque<MessageEnvelope> ordinary = new ArrayDeque<>();
    private final ArrayDeque<MessageEnvelope> taskResults = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition ordinaryNotFull = lock.newCondition();
    private final Condition taskResultsNotFull = lock.newCondition();
    private boolean taskResultReserveSaturated;

    SchedulerPriorityMailbox(int ordinaryCapacity) {
        if (ordinaryCapacity <= 0) {
            throw new IllegalArgumentException("ordinaryCapacity must be positive");
        }
        this.ordinaryCapacity = ordinaryCapacity;
    }

    @Override
    public boolean offer(MessageEnvelope envelope) {
        Objects.requireNonNull(envelope, "envelope");
        lock.lock();
        try {
            ArrayDeque<MessageEnvelope> lane = laneFor(envelope);
            if (lane.size() >= capacityFor(envelope)) {
                recordSaturation(envelope);
                return false;
            }
            lane.addLast(envelope);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(MessageEnvelope envelope) throws InterruptedException {
        Objects.requireNonNull(envelope, "envelope");
        lock.lockInterruptibly();
        try {
            ArrayDeque<MessageEnvelope> lane = laneFor(envelope);
            Condition notFull = notFullFor(envelope);
            int capacity = capacityFor(envelope);
            while (lane.size() >= capacity) {
                recordSaturation(envelope);
                notFull.await();
            }
            lane.addLast(envelope);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(MessageEnvelope envelope, long timeout, TimeUnit unit)
            throws InterruptedException {
        Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(unit, "unit");
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            ArrayDeque<MessageEnvelope> lane = laneFor(envelope);
            Condition notFull = notFullFor(envelope);
            int capacity = capacityFor(envelope);
            while (lane.size() >= capacity) {
                recordSaturation(envelope);
                if (nanos <= 0L) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            lane.addLast(envelope);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public MessageEnvelope take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (isEmptyLocked()) {
                notEmpty.await();
            }
            return removeNextLocked();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public MessageEnvelope poll(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit");
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (isEmptyLocked()) {
                if (nanos <= 0L) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            return removeNextLocked();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public MessageEnvelope poll() {
        lock.lock();
        try {
            return isEmptyLocked() ? null : removeNextLocked();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public MessageEnvelope peek() {
        lock.lock();
        try {
            MessageEnvelope priority = taskResults.peekFirst();
            return priority == null ? ordinary.peekFirst() : priority;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        lock.lock();
        try {
            return ordinaryCapacity - ordinary.size()
                    + TASK_RESULT_RESERVE_CAPACITY - taskResults.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super MessageEnvelope> target) {
        return drainTo(target, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super MessageEnvelope> target, int maxElements) {
        Objects.requireNonNull(target, "target");
        if (target == this) {
            throw new IllegalArgumentException("Cannot drain a mailbox into itself.");
        }
        if (maxElements <= 0) {
            return 0;
        }
        lock.lock();
        try {
            int drained = 0;
            while (drained < maxElements && !isEmptyLocked()) {
                target.add(removeNextLocked());
                drained++;
            }
            return drained;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Iterator<MessageEnvelope> iterator() {
        lock.lock();
        try {
            List<MessageEnvelope> snapshot =
                    new ArrayList<>(taskResults.size() + ordinary.size());
            snapshot.addAll(taskResults);
            snapshot.addAll(ordinary);
            return List.copyOf(snapshot).iterator();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return taskResults.size() + ordinary.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object candidate) {
        lock.lock();
        try {
            return taskResults.contains(candidate) || ordinary.contains(candidate);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Object candidate) {
        lock.lock();
        try {
            if (taskResults.remove(candidate)) {
                taskResultReserveSaturated = false;
                taskResultsNotFull.signal();
                return true;
            }
            if (ordinary.remove(candidate)) {
                ordinaryNotFull.signal();
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            ordinary.clear();
            taskResults.clear();
            taskResultReserveSaturated = false;
            ordinaryNotFull.signalAll();
            taskResultsNotFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    SchedulerMailbox.DepthSnapshot depthSnapshot() {
        lock.lock();
        try {
            return new SchedulerMailbox.DepthSnapshot(
                    ordinary.size(),
                    ordinaryCapacity,
                    taskResults.size(),
                    TASK_RESULT_RESERVE_CAPACITY,
                    taskResultReserveSaturated
            );
        } finally {
            lock.unlock();
        }
    }

    private ArrayDeque<MessageEnvelope> laneFor(MessageEnvelope envelope) {
        return isTaskResult(envelope) ? taskResults : ordinary;
    }

    private Condition notFullFor(MessageEnvelope envelope) {
        return isTaskResult(envelope) ? taskResultsNotFull : ordinaryNotFull;
    }

    private int capacityFor(MessageEnvelope envelope) {
        return isTaskResult(envelope) ? TASK_RESULT_RESERVE_CAPACITY : ordinaryCapacity;
    }

    private MessageEnvelope removeNextLocked() {
        MessageEnvelope envelope = taskResults.pollFirst();
        if (envelope != null) {
            taskResultReserveSaturated = false;
            taskResultsNotFull.signal();
            return envelope;
        }
        envelope = ordinary.removeFirst();
        ordinaryNotFull.signal();
        return envelope;
    }

    private boolean isEmptyLocked() {
        return taskResults.isEmpty() && ordinary.isEmpty();
    }

    private static boolean isTaskResult(MessageEnvelope envelope) {
        return envelope.message() instanceof TaskResultMessage;
    }

    private void recordSaturation(MessageEnvelope envelope) {
        if (isTaskResult(envelope)) {
            taskResultReserveSaturated = true;
        }
    }
}
