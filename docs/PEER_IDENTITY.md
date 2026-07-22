# Participant Identity (`peer` Compatibility Names)

This document records the current participant identity contract for TaskFlow.
The existing peer-ID, `nodeId`, registry, route, and configuration names are
retained compatibility names. On assignment and result paths they identify the
participant acting in the executor role; they do not confer coordinator
authority. The tracked protocol and schema currently use `peerId`, `nodeId`,
and `assigned_peer_id` rather than a literal `workerId`; this terminology task
does not rename persisted or wire identity fields. This document does not add
authentication guarantees; requester tokens and requester signatures are
covered separately in `docs/EXECUTION_GUARANTEES.md`.

## Current Contract

TaskFlow participants use explicit peer IDs across TCP, RabbitMQ, command-line
participants, and JavaFX GUI participants.

- `TASKFLOW_PEER_ID` is the shared configuration variable for stable peer IDs.
- Peer IDs are normalized through the shared `protocol.PeerIdentity` helper.
- Safe peer IDs contain letters, digits, hyphens, and underscores. Other
  characters are converted to underscores, repeated unsafe separators collapse,
  and leading or trailing separators are removed.
- If `TASKFLOW_PEER_ID` is unset, each runtime generates a safe unique
  process-scoped fallback ID with a role prefix such as `TCP_PEER`,
  `RABBITMQ_PEER`, or `GUI_PEER`.
- Generated fallback IDs are intentionally runtime-scoped, not restart-stable.
  Set `TASKFLOW_PEER_ID` for a stable identity across restarts, logs, broker
  routes, and peer-scoped job IDs.

Generated runtime IDs keep local demos and concurrent ad-hoc participants from
colliding by default. A persistent locally generated ID store is deferred until
local identity, duplicate detection, restarts, and concurrent participants have a
clearer user-facing policy.

## TCP Behavior

The TCP coordinator no longer identifies participants by server-side socket address.
It sends an initial `PING`, and updated participants answer with a `PONG` whose
`nodeId` is their explicit peer ID. The first `PONG`, `JOB_SUBMIT`, or
`TASK_RESULT` from a connection establishes the connection peer ID.

After registration, later messages on that connection must keep the same
nonblank `nodeId`. A message that tries to switch identity closes the
connection.

Active duplicate TCP peer IDs are rejected by the coordinator registry. The
duplicate connection is closed without replacing the existing participant
record and without emitting a scheduler peer-disconnect event for the existing
participant.

## RabbitMQ Behavior

RabbitMQ participants publish heartbeats and peer-routed messages with the same
explicit peer ID. The coordinator rejects heartbeats whose broker envelope
sender and protocol `nodeId` disagree; scheduler job submissions also require
the outer sender and inner `nodeId` to match before any replay response can be
routed.

Current RabbitMQ routing uses peer-specific queues keyed by peer ID. Two active
RabbitMQ participants configured with the same peer ID are an invalid deployment
configuration because the broker cannot disambiguate which process should own
that peer route. Configure unique `TASKFLOW_PEER_ID` values for concurrent
RabbitMQ participants.

## Persisted Metadata

When SQLite persistence is available, the coordinator records durable
last-known participant metadata keyed by peer ID:

- runtime type and transport;
- supported task types from heartbeat metadata;
- first-seen, last-heartbeat, and last-disconnect timestamps;
- connected or disconnected status;
- scheduler metric snapshots such as completed attempts, failed attempts,
  heartbeat latency, and task-duration EWMA.

Runtime connection handles, sockets, RabbitMQ consumers, and broker channels are
not persisted. They remain in the in-memory registry for the active coordinator
process.

## Job IDs

GUI and command-line submitters generate job IDs through `protocol.JobIds`:

```text
JOB_<sanitized-peer-id>_<epoch-millis>_<full-uuid>
```

The generated shape is safe for logs, SQLite keys, task IDs, and output folder
names. The peer ID segment makes submitted jobs traceable to the submitting
participant, while the full UUID suffix avoids relying on a short random fragment.

The scheduler treats the submitted job ID as an idempotency key scoped to the
requester token hash and optional verified public key. The peer-ID segment is a
routing/traceability aid, not ownership: the same owner can replay an exact
request after reconnect under a different valid route, while a changed owner or
request is rejected. SQLite schema-v12 request hashes preserve that decision
across coordinator restart.

## Deferred Work

- RabbitMQ does not yet have a control route to reject or disconnect a duplicate
  participant process after it has started consuming a shared peer queue.
- Peer IDs are not user/account authentication.

## Evidence

Focused tests cover:

- `PeerIdentityTest` for safe ID normalization and generated fallback IDs.
- `JobIdsTest` for peer-scoped, collision-resistant generated job IDs.
- `PingHandlerTest` for explicit TCP heartbeat responses.
- `InMemoryPeerRegistryTest` for duplicate active peer rejection and peer
  metadata persistence hooks.
- `DatabaseManagerTest` for SQLite peer metadata persistence, heartbeat
  capability updates, disconnect state, restart reload, duplicate peer-id
  upsert behavior, and coexistence with task retry state.
- `PeerHandlerTest` for TCP explicit-ID registration and duplicate rejection.
- `TcpCoordinatorConnectionTest` for JavaFX TCP heartbeat identity.
- `PeerNodeTest`, `TcpJobSubmissionClientTest`,
  `RabbitMqCoordinatorConnectionTest`, and `RabbitMqJobSubmissionClientTest`
  for command-line and GUI peer ID and job ID usage.
