package server.scheduler;

import server.job.AssignmentIdentity;
import server.job.EmbarrassinglyParallelJob;
import server.job.JobFactory;
import server.job.TaskUnit;
import server.registry.AssignmentCapacityReservation;

/** Maps an exact assignment generation to its immutable plugin resource cost. */
final class CapacityReservations {
    private CapacityReservations() {
    }

    static AssignmentCapacityReservation forAssignment(
            EmbarrassinglyParallelJob<?, ?> job,
            TaskUnit<?> task,
            AssignmentIdentity identity
    ) {
        if (!job.getJobId().equals(task.getJobId())) {
            throw new IllegalArgumentException("Task does not belong to the supplied job.");
        }
        if (!task.getTaskId().equals(identity.taskId())) {
            throw new IllegalArgumentException(
                    "Assignment identity does not belong to the supplied task."
            );
        }
        return new AssignmentCapacityReservation(
                job.getJobId(),
                task.getTaskId(),
                identity.attemptNumber(),
                identity.assignmentId(),
                identity.workerId(),
                job.getTaskType(),
                JobFactory.resourceProfile(job.getTaskType()).capacityUnitCost()
        );
    }
}
