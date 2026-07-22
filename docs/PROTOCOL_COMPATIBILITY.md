# Protocol Compatibility

TaskFlow checks compatibility per message. There is no connection handshake or
downgrade negotiation today.

## Version Field

New coordinator and participant messages serialize `protocolVersion: 2`.

- Version `2` is the current protocol.
- Missing `protocolVersion` is normalized to legacy version `0`.
- The framework recognizes versions `0` through `2`, but acceptance is decided
  by message type because task-assignment semantics changed in version `2`.
- Versions below `0`, above `2`, and non-integer values are rejected before
  runtime dispatch.

An accepted legacy version is not rewritten semantically. Normalization only
makes the parsed version explicit so the shared validator can apply the matrix
below.

## Message-Type Compatibility Matrix

| Message type | New sender emits | Receiver accepts v2 | Receiver accepts v1 | Receiver accepts missing/v0 | Incompatible disposition |
|---|---:|---|---|---|---|
| `PING`, `PONG` | v2 | Yes | Yes | Yes | Reject unsupported future/negative versions. |
| `JOB_SUBMIT` | v2 | Yes | Yes | Yes | Reject invalid versions or fields before scheduler/plugin dispatch. |
| `JOB_RESULT_REQUEST` | v2 | Yes | Yes | Yes | Reject invalid versions or fields before authorization. |
| `JOB_RESULT` | v2 | Yes | Yes | Yes | Reject invalid versions or fields before requester/result-handler dispatch. |
| `PEER_DISCONNECTED` | v2 | Yes | Yes | Yes | Reject invalid versions or fields before scheduler dispatch. |
| `TASK_ASSIGN` | v2 with assignment identity | Yes, with all required v2 fields | No | No | Reject with reason code `assignment_protocol_v2_required`; RabbitMQ may dead-letter it, and TCP drops it before execution. |
| `TASK_RESULT` | v2 with assignment identity | Yes, with all required v2 fields | No | No | Reject with reason code `assignment_protocol_v2_required`; it cannot reach task commitment and is never broker-requeued for this permanent incompatibility. |

The unchanged message types keep legacy compatibility because their semantics
did not change. `TASK_ASSIGN` and `TASK_RESULT` require a coordinated version-2
cutover: drain in-flight version-1 work and pending version-1 assignment outbox
rows before upgrading. A version-1 coordinator rejects version-2 messages as
future, while a version-2 coordinator rejects identity-less version-0/1 task
results.

## Version 2 Task Fields

`TASK_ASSIGN` requires:

- positive `attemptNumber`;
- canonical UUID-shaped `assignmentId`;
- positive `leaseExpiresAtEpochMillis`;
- the existing job ID, task ID, task type, payload, and parameter fields.

The canonical serialized parameter name is `parameter`. The decoder also reads
the former `param` spelling so stored/plugin-owned payload templates can be
upgraded without changing plugin payload semantics.

`TASK_RESULT` must echo the assignment's positive `attemptNumber` and exact
`assignmentId`, in addition to the existing result fields. The shared peer
execution engine copies both values from the received assignment for success
and failure results.

Missing fields deserialize to invalid zero/null values and are rejected.
Blank, zero, negative, shortened, or malformed assignment identity values are
rejected with reason code `invalid_assignment_identity`.

## Structured Rejection and Settlement

`MessageValidationException` exposes a stable `reasonCode` for transport and
scheduler logs. The task-protocol codes introduced with version `2` are:

- `assignment_protocol_v2_required` for version-0/1 task assignments/results;
- `invalid_assignment_identity` for missing or malformed v2 identity fields;
- `unsupported_protocol_version` when a message object bypasses parser-level
  version checks.

TCP `MessageFactory` validation drops incompatible task messages before they
enter the scheduler or worker engine. RabbitMQ codec failures are logged with
`action=reject` and settled using reject-without-requeue; when TaskFlow DLQ
routing is enabled, RabbitMQ can quarantine the rejected body there. The
scheduler repeats validation as a defense in depth and rejects a broker
acknowledgement without requeue if an incompatible result is injected directly.

These permanent compatibility failures are distinct from temporary scheduler
mailbox saturation, which still requeues a broker delivery for backpressure.

## TCP Messages

TCP messages carry `protocolVersion` on the root JSON object alongside `type`,
`nodeId`, and `time`. `messaging.MessageFactory` checks the supported numeric
range before looking up the registered parser, then calls the shared validator
to enforce the message-type matrix and field rules.

## RabbitMQ Envelopes

RabbitMQ carries `protocolVersion` in two places:

- the broker envelope root, next to `route`, `fromNodeId`, and `message`;
- the inner protocol message object.

Envelope versions `0`, `1`, and `2` remain readable because envelope semantics
did not change. Inner-message compatibility follows the matrix. A legacy
envelope can therefore contain a valid v2 task result, but a v2 envelope cannot
make a legacy inner task result commit-eligible.

`RabbitMqMessageCodec` checks both numeric ranges and validates the inner
message before returning an inbound transport message. Invalid deliveries are
rejected; with dead-lettering enabled, RabbitMQ can route them to the TaskFlow
DLQ for inspection, discard, or redrive after compatible code is deployed.

## Field and Size Validation

`protocol.MessageValidator` remains the shared framework boundary. In addition
to version-2 assignment identity, it enforces:

- known message types;
- required `nodeId` and `time` fields;
- peer IDs that match TaskFlow's safe peer-ID contract;
- job IDs, task IDs, and task types limited to letters, numbers, `.`, `:`, `_`,
  and `-`;
- `TASKFLOW_MAX_TASKS_PER_JOB` on submitted payloads and advertised task types;
- `TASKFLOW_MAX_JOB_PAYLOAD_BYTES` on job payloads and task assignments;
- `TASKFLOW_MAX_RESULT_BYTES` on task and final job results.

Invalid `JOB_SUBMIT` and `JOB_RESULT_REQUEST` messages that reach the scheduler
are converted to failed `JOB_RESULT` responses when the requester can be
routed. Invalid non-submit messages are rejected or dropped. This is a
message-safety boundary, not user/account authentication.

## Current Fencing Boundary

Version `2` prevents identity-less version-0/1 task results from entering the
commit path and makes assignment identity available end to end. SQLite schema
version `10` now persists that tuple in task state and attempt audit, restores
complete unexpired identities, and releases incomplete legacy assignments. It
also makes the supplied v2 identity authoritative during successful-result
commitment: task ID, `ASSIGNED` state, attempt number, assignment ID, and the
reporting participant must all match in SQLite. Matching, duplicate, and stale
store/scheduler dispositions are automated. Full same-participant ABA coverage
is still described as partial until TF-0106 runs the complete scenario through
deterministic scheduler and RabbitMQ integration tests.

## Plugin and Transport Expectations

Plugins should use SPI message classes rather than raw JSON. Server job plugins
continue to produce task-assignment payload templates; the coordinator adds its
framework-owned assignment identity before publication. Participant processors
should return through the shared execution engine so results echo that identity.

Plugins must not construct RabbitMQ envelopes or bypass transport codecs. New
transports must emit the current version, apply the per-message compatibility
matrix, call the shared validator after parsing, and give permanent validation
failures a terminal non-requeue disposition. A transport-specific envelope
should be versioned separately from its inner TaskFlow message.
