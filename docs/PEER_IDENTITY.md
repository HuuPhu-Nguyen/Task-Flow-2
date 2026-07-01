# Peer Identity Scope

This document records the current peer identity contract for TaskFlow. It does
not add authentication guarantees; requester tokens and requester signatures are
covered separately in `docs/EXECUTION_GUARANTEES.md`.

## Current Contract

TaskFlow peers use explicit peer IDs across TCP, RabbitMQ, command-line peers,
and JavaFX GUI peers.

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

Generated runtime IDs keep local demos and concurrent ad-hoc peers from
colliding by default. A persistent locally generated ID store is deferred until
local identity, duplicate detection, restarts, and concurrent peers have a
clearer user-facing policy.

## TCP Behavior

The TCP coordinator no longer identifies peers by server-side socket address.
It sends an initial `PING`, and updated peers answer with a `PONG` whose
`nodeId` is their explicit peer ID. The first `PONG`, `JOB_SUBMIT`, or
`TASK_RESULT` from a connection establishes the connection peer ID.

After registration, later messages on that connection must keep the same
nonblank `nodeId`. A message that tries to switch identity closes the
connection.

Active duplicate TCP peer IDs are rejected by the coordinator registry. The
duplicate connection is closed without replacing the existing peer record and
without emitting a scheduler peer-disconnect event for the existing peer.

## RabbitMQ Behavior

RabbitMQ peers publish heartbeats and peer-routed messages with the same
explicit peer ID. The coordinator rejects heartbeats whose broker envelope
sender and protocol `nodeId` disagree.

Current RabbitMQ routing uses peer-specific queues keyed by peer ID. Two active
RabbitMQ peers configured with the same peer ID are an invalid deployment
configuration because the broker cannot disambiguate which process should own
that peer route. Configure unique `TASKFLOW_PEER_ID` values for concurrent
RabbitMQ peers.

## Persisted Metadata

When SQLite persistence is available, the coordinator records durable
last-known peer metadata keyed by peer ID:

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
peer, while the full UUID suffix avoids relying on a short random fragment.

The scheduler rejects a `JOB_SUBMIT` when its job ID is already active. When
persistence is enabled, it also rejects a submitted job ID that already exists
in persisted job history.

## Deferred Work

- RabbitMQ does not yet have a control route to reject or disconnect a duplicate
  broker peer process after it has started consuming a shared peer queue.
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
