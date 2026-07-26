# 0012: Use Simple Weighted-Capacity Scheduling

Status: Accepted

Date: 2026-07-22

Scope: Frozen end-state for Phases 0–8; deterministic bounded scheduling work
is planned in Phase 4.

## Context

Task types have materially different execution cost: a text task, image
conversion, and video transcode should not consume identical executor capacity.
At the same time, TaskFlow runs embarrassingly parallel jobs without task
dependencies and is not intended to reproduce a cluster orchestrator.

The scheduler needs a deterministic eligibility rule, bounded accounting, and
cross-job fairness before it needs multidimensional placement, preemption, or
topology awareness.

## Decision

TaskFlow uses simple weighted-capacity scheduling. Executor participants
advertise total and available capacity units plus maximum concurrency by task
type. Each task type declares a capacity-unit cost. A participant is eligible
only when it is connected and healthy, supports the task type, has enough free
units, and has not reached that type's concurrency limit.

Cross-job selection is deterministic round-robin with a configured maximum per
job per round. An existing quality score may break ties only among eligible
participants. Capacity reservation and release happen exactly once around the
authoritative attempt transition, and all pending/deadline/dispatch work is
bounded.

## Alternatives Considered

- **Treat every task as equal cost:** rejected because expensive media work can
  oversubscribe an executor while low-cost capacity accounting looks healthy.
- **Use only a heuristic quality score:** rejected because a score cannot
  replace hard capacity and task-type concurrency eligibility.
- **Build a Kubernetes-style general resource scheduler:** rejected because
  arbitrary resource dimensions, topology, affinity, preemption, and bin
  packing are outside the workload and evidence needs.
- **First-in-first-out across all tasks:** rejected because one large job can
  starve later small jobs even when compatible capacity exists.

## Consequences

- Task/plugin metadata must expose a documented capacity-unit cost, while
  participants expose bounded available units and type concurrency.
- Tests must prove no overcommit, exact-once capacity release, and deterministic
  fairness under controlled events.
- Scheduler indexes, batches, and mailbox remain explicitly bounded (I7).
- Weighted units are a coarse admission model, not a promise of CPU, memory,
  disk, GPU, or deadline isolation.
- Optional memory/disk estimates remain diagnostics unless a later accepted ADR
  makes them hard constraints.

## Conditions That Would Invalidate This Decision

A replacement ADR is required when measured supported workloads need hard
multidimensional resources, accelerators, data locality, affinity/anti-affinity,
preemption, deadlines, or inter-task dependency scheduling that weighted units
cannot express.

The replacement should prefer integration with an established resource
orchestrator over growing an untested general scheduler inside TaskFlow. A new
task type or an unmeasured desire for sophistication does not invalidate this
decision.

## Evidence And Implementation Status

- [Backpressure scope](../BACKPRESSURE_SCOPE.md)
- [Guarantees and non-goals](../GUARANTEES.md)
- [Failure model](../FAILURE_MODEL.md)
- TF-0401/TF-0402 implement event-driven indexes and fair bounded cycle stages.
- TF-0403 implements persistent round-robin job passes, the configurable
  per-job assignment quota, and capacity-wait eviction/reactivation.
- TF-0404 through TF-0406 still own weighted capacity, admission, and
  persistent-overload behavior.

## Related Documents

- [ADR 0007: Single authoritative coordinator](0007-single-authoritative-coordinator.md)
- [ADR 0011: Object storage for large payloads](0011-object-storage-large-payloads.md)
