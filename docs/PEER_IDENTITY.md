# Participant Identity (`peer` Compatibility Names)

This document records TaskFlow's participant identity contract. Existing
`peerId`, `nodeId`, registry, route, and configuration names are retained wire,
schema, and artifact compatibility names. On assignment/result paths they
identify the participant acting in the executor role; they do not confer
coordinator authority.

This identity is routing metadata, not user/account authentication. Requester
tokens and signatures are covered in `docs/EXECUTION_GUARANTEES.md`.

## Current Contract

All supported participants use explicit peer IDs over RabbitMQ.

- `TASKFLOW_PEER_ID` configures a stable ID.
- `protocol.PeerIdentity` normalizes and validates IDs.
- Safe IDs contain letters, digits, hyphens, and underscores. Unsafe character
  runs become underscores, and leading/trailing separators are removed.
- If `TASKFLOW_PEER_ID` is unset, the command-line runtime generates a unique
  process-scoped ID with `RABBITMQ_PEER`; JavaFX uses `GUI_PEER`.
- Generated IDs are intentionally not stable across restart. Configure a stable
  ID for durable log correlation, broker routes, and peer-scoped job IDs.

## RabbitMQ Routing

Participants publish heartbeats and peer-routed messages with the same explicit
ID. The coordinator rejects a heartbeat when the broker envelope sender and the
inner protocol `nodeId` disagree. Scheduler job submission also requires the
outer sender and inner `nodeId` to match before replay responses can be routed.

Task-assignment and final-result queues are keyed by peer ID. Two active
participants with the same ID are an invalid deployment: RabbitMQ cannot
disambiguate ownership of that route. Give every concurrently active process a
unique `TASKFLOW_PEER_ID`.

The coordinator's in-memory registry treats heartbeat presence as live
membership. A missing heartbeat beyond the configured timeout removes the
participant, persists its disconnected status when available, and emits the
scheduler unavailability event.

## Persisted Metadata

When SQLite persistence is available, the coordinator stores last-known
participant metadata keyed by peer ID:

- runtime type and transport;
- advertised task capabilities;
- first-seen, last-heartbeat, and last-disconnect timestamps;
- connected/disconnected status;
- completed/failed attempt counts, heartbeat latency, and task-duration EWMA.

Broker consumers, channels, and connections are process-local and are not
stored in the participant registry.

Rows written before the sole-transport migration can contain an obsolete
transport value. The loader preserves their runtime label for operator history
but exposes an unrecognized transport as `UNKNOWN`; it cannot recreate a
runtime path.

## Job IDs

GUI and command-line submitters generate job IDs through `protocol.JobIds`:

```text
JOB_<sanitized-peer-id>_<epoch-millis>_<full-uuid>
```

The shape is safe for logs, SQLite keys, task IDs, and output folders. The peer
segment aids routing and traceability; the UUID supplies collision resistance.

The scheduler treats `jobId` as an idempotency key scoped to the requester token
hash and optional verified public key. The peer segment is not ownership. The
same owner can replay an exact request through another valid route, while a
changed owner or canonical request is rejected. SQLite schema v12 preserves
that decision across coordinator restart.

## Limits

- RabbitMQ has no separate control route that can disconnect a duplicate process
  after both processes begin consuming the same peer queue.
- Generated fallback IDs are not persisted across participant restart.
- Peer IDs are not user/account identities or authorization principals.

## Evidence

Focused tests include:

- `PeerIdentityTest` for normalization and generated fallback IDs;
- `JobIdsTest` for peer-scoped, collision-resistant job IDs;
- `RabbitMqCoordinatorConnectionTest` and `RabbitMqJobSubmissionClientTest` for
  JavaFX heartbeat, route, submission, and job-ID use;
- `PeerNodeTest` for command-line identity, submission, and result behavior;
- `InMemoryPeerRegistryTest` for duplicate active ID rejection and persistence
  hooks;
- `DatabaseManagerTest` for current RabbitMQ metadata, historical-value
  degradation, heartbeat updates, disconnect state, restart reload, duplicate
  upsert behavior, and coexistence with task retry state.
