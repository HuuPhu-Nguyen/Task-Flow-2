# Runtime Strategy

This document records TaskFlow's supported transport direction. It does not add
guarantees beyond `docs/EXECUTION_GUARANTEES.md`.

## Decision

RabbitMQ is the sole supported runtime transport for the coordinator,
command-line participants (`taskflow-peer`), and JavaFX participants.

TF-0301 completed the migration by removing the socket coordinator, participant
and GUI implementations; their tests and smoke scripts; the transport selector;
and their release surface. Runtime entry points now delegate directly to their
RabbitMQ implementations. `TASKFLOW_TRANSPORT` is no longer read.

The `taskflow-peer`, `PeerNode`, `peerId`, and `nodeId` names remain compatibility
vocabulary for participant roles and persisted/wire fields. They do not indicate
a second transport or distributed coordinator authority.

## One Delivery Contract

All supported runtime delivery behavior is defined against RabbitMQ:

- shared routes carry heartbeats, job submissions, and task results;
- participant-specific routes carry task assignments and final job results;
- consumers use manual acknowledgement and configured prefetch;
- invalid or incompatible messages are rejected without requeue and can enter
  the dead-letter workflow;
- duplicate or stale scheduler outcomes are acknowledged without changing
  authoritative state;
- coordinator submissions and task results stay unacknowledged until bounded
  mailbox admission and scheduler/store classification; connection loss before
  settlement returns broker ownership, and a post-commit redelivery is a typed
  harmless duplicate;
- admitted scheduler work runs in configurable message, combined-deadline,
  dispatch, and terminal/outbox batches. Message/deadline ordering prevents
  starvation in either direction, while the independent durable outbox
  replayer applies the configured outbound load bound;
- scheduler-mailbox saturation and explicitly transient handler/storage
  failures use `RETRY_TRANSIENT` under the documented bounded TTL schedule;
- deterministic processing failures use the same bounded schedule and enter
  final quarantine after exhaustion, while validation failures reject
  immediately into the ordinary dead-letter workflow;
- coordinator `TASK_ASSIGN` and final `JOB_RESULT` publication intent is stored
  in SQLite outbox rows before publication when persistence is available;
- application messages use persistent RabbitMQ delivery mode, while configured
  topology durability applies to shared/retry/dead-letter resources and not the
  exclusive auto-delete peer endpoints;
- publisher confirms and mandatory-return detection decide whether a
  peer-routed coordinator publish can proceed to its conditional SQLite sent
  mark. Any failure, including the sent mark itself, leaves the row replayable,
  so delivery can be duplicated.
- coordinator shutdown closes intake before consumer cancellation, drains
  already admitted envelopes within a fixed bound, closes the broker channel
  to return unsettled deliveries, and closes SQLite only after database-using
  background components stop.
- coordinator and command-line participant startup uses one interruptible,
  timeout-bounded connection owner with capped exponential backoff; established
  transports automatically recover connections, recorded topology, channels,
  and consumers through the same bounded delay policy;
- broker-offline coordinator publications stay in the SQLite outbox and replay
  their exact identity after recovery rather than creating an extra assignment
  generation.

At-least-once delivery and execution remain explicit. Publisher confirmation is
not consumer completion, and exactly-once delivery is not claimed.

The five typed delivery dispositions, bounded delayed retry queues, observable
delivery-attempt/failure metadata, and automatic final quarantine are
implemented. The remaining limits in `docs/RABBITMQ_SCOPE.md` remain
authoritative.

## Current Runtime

- The coordinator is the sole scheduling and durable-state authority.
- Command-line and JavaFX participants can enable requester, executor, or both
  roles through their Maven runtime profiles.
- Participants use explicit sanitized IDs from `TASKFLOW_PEER_ID`, or a unique
  process-scoped generated fallback.
- SQLite stores last-known participant metadata and coordinator outbox rows;
  live broker consumers and channels remain process-local.
- GUI and command-line submitters use peer-scoped, collision-resistant job IDs.
- Exact duplicate submissions are resolved through requester ownership and the
  persisted canonical request hash.
- RabbitMQ command-line and JavaFX participants submit jobs, execute assignments,
  publish results, and receive live final results through the same broker
  contract.
- RabbitMQ GUI `JOB_RESULT_REQUEST` replay after restart is not implemented;
  exact duplicate submission replay and live `JOB_RESULT` delivery are the
  available paths.
- Live broker tests, broker-backed CI, Docker Compose, and the JavaFX desktop
  smoke helper exercise the supported runtime. The coordinator live gates
  include pre-ack redelivery, post-commit/pre-ack duplicate classification,
  pre-stop/post-stop shutdown delivery ownership, unavailable startup, and a
  managed single-broker stop/restart with active work, recovered participant
  routes, outbox replay, stale fencing, and completion.

## Legacy Data

No runtime creates legacy socket participants or transport metadata. Existing
SQLite peer-registry rows written with the removed transport value remain
readable: their persisted runtime label is preserved for operator history and
their unrecognized transport is exposed as `UNKNOWN`. Historical rows do not
reactivate a runtime path.

## Migration

Deployments upgrading from the removed socket runtime must:

1. provide a reachable RabbitMQ broker;
2. configure `TASKFLOW_RABBITMQ_*` settings as needed;
3. remove `TASKFLOW_TRANSPORT` from environment, container, and service files;
4. expose broker port `5672` (or the configured AMQP port), not the former
   coordinator-listener port;
5. assign unique stable `TASKFLOW_PEER_ID` values to concurrent participants.

## Production-Readiness Gates

RabbitMQ is the only supported transport, but the implementation remains
transitional rather than production-ready. The remaining promotion gates are:

- a final participant-side `TASK_RESULT` durability decision;
- RabbitMQ-cluster failover or multi-coordinator design only if a deployment
  requires those capabilities beyond the tested single-broker recovery scope;
- direct TLS/certificate configuration if brokers are used across untrusted
  networks;
- continued broker-backed CI, Docker Compose, and JavaFX smoke evidence;
- measured overload evidence before adding adaptive throttling.

## Public Claim Rule

Use wording such as **sole supported RabbitMQ transport**, **at-least-once
broker delivery**, and **transitional runtime**.

TaskFlow can claim bounded poison-message quarantine under the configured
RabbitMQ retry schedule and bounded single-broker restart recovery under the
documented assumptions. Do not describe TaskFlow as production-ready,
exactly-once, zero-downtime, clustered, or multi-coordinator recoverable until
the corresponding gates are implemented and tested.
