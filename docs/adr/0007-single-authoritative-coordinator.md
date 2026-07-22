# 0007: Use One Authoritative Coordinator

Status: Accepted

Date: 2026-07-22

Scope: Frozen architecture for Phases 0–8; implemented deployment constraint.

## Context

TaskFlow participant nodes may submit jobs in the requester role, execute work
in the executor role, or perform both roles. Symmetric participant capability
does not make authoritative scheduling symmetric. Job acceptance, task state,
assignment, leases, retries, authoritative result commitment, job completion,
and outbound publication intent require one unambiguous owner to protect I1–I6.

Multiple active coordinators would require shared concurrent durable state,
leader election, and database-enforced leader fencing. Those mechanisms are not
part of the core single-coordinator target.

## Decision

Phases 0–8 support exactly one authoritative coordinator process. It is the
only process allowed to create assignments, change authoritative job/task
state, evaluate completion, or persist coordinator outbox intent.

Participant nodes never coordinate authoritative scheduling or result
commitment directly with one another. Their requester and executor roles may be
combined in one process without sharing coordinator authority. In-memory
coordinator state is a projection of the authoritative store, not a second
authority.

## Alternatives Considered

- **Peer-to-peer scheduling and shared result authority:** rejected because it
  requires distributed ownership, conflict resolution, and consensus that the
  project does not implement.
- **Active-active coordinators in the core scope:** deferred because process
  membership alone cannot fence stale leaders; shared PostgreSQL state, leader
  epochs, takeover, and split-brain tests belong to the optional HA phase.
- **Restricting every participant to only a requester or executor role:**
  rejected because deployment-role separation is useful but does not improve
  authoritative-state safety.

## Consequences

- Coordinator downtime pauses new authoritative transitions; zero-downtime
  coordinator availability is an explicit non-goal for the core scope.
- SQLite can be used under a single-writer contract without pretending it is a
  consensus system.
- Every correctness-sensitive transition and outbound intent must flow through
  coordinator-owned durable boundaries.
- Participants can scale execution horizontally, but coordinator throughput and
  availability remain system-wide limits that must be measured honestly.
- The architecture is described as coordinator-mediated, not peer-to-peer.

## Conditions That Would Invalidate This Decision

This decision must be replaced by a new ADR before enabling multiple
coordinator writers. A replacement is justified only by a concrete requirement
such as a zero-downtime coordinator availability objective, measured
single-coordinator throughput exhaustion that cannot be removed within the
current design, or an operator requirement for automated standby takeover.

The replacement must include shared authoritative storage, leader election
with a monotonically increasing fencing epoch, database rejection of stale
leaders, automated takeover, and split-brain evidence. Starting a second
coordinator without those mechanisms does not invalidate this decision; it is
an unsupported deployment.

## Evidence And Implementation Status

- [Guarantees and non-goals](../GUARANTEES.md)
- [Failure model](../FAILURE_MODEL.md)
- [Current execution behavior](../EXECUTION_GUARANTEES.md)
- `taskflow-coordinator`

## Related Documents

- [ADR 0008: SQLite single-writer state store](0008-sqlite-single-writer-state-store.md)
- [ADR 0010: At-least-once execution and fenced results](0010-at-least-once-generation-fenced-results.md)
