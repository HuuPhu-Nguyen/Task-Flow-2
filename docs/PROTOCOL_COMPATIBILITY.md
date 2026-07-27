# Protocol Compatibility

TaskFlow checks compatibility per message. There is no connection handshake or
downgrade negotiation today.

## Version Field

RabbitMQ envelopes and new inner messages other than capacity advertisements
serialize `protocolVersion: 2`. New `PONG` capacity advertisements serialize
inner `protocolVersion: 3`.

- Version `2` remains the current general-message and broker-envelope protocol.
- Version `3` is reserved for the inner `PONG` capacity advertisement.
- Missing `protocolVersion` is normalized to legacy version `0`.
- The framework recognizes inner-message versions `0` through `3`, but
  acceptance is decided by message type because task-assignment semantics
  changed in version `2` and capacity advertisement semantics changed in
  version `3`.
- Versions below `0`, above `3`, and non-integer values are rejected before
  runtime dispatch. Version `3` on any non-`PONG` message is also rejected.

An accepted legacy version is not rewritten semantically. Normalization only
makes the parsed version explicit so the shared validator can apply the matrix
below.

## Message-Type Compatibility Matrix

| Message type | New sender emits | Receiver accepts v3 | Receiver accepts v2 | Receiver accepts v1 | Receiver accepts missing/v0 | Incompatible disposition |
|---|---:|---|---|---|---|---|
| `PING` | v2 | No | Yes | Yes | Yes | Reject unsupported or invalid versions. |
| `PONG` | v3 | Yes, with all required capacity fields | Yes, liveness only | Yes, liveness only | Yes, liveness only | Legacy heartbeats clear scheduling capacity; reject unsupported or invalid versions. |
| `JOB_SUBMIT` | v2 | No | Yes | Yes | Yes | Reject invalid versions or fields before scheduler/plugin dispatch. |
| `JOB_RESULT_REQUEST` | v2 | No | Yes | Yes | Yes | Reject invalid versions or fields before authorization. |
| `JOB_RESULT` | v2 | No | Yes | Yes | Yes | Additive optional `admissionRejection` is ignored by older readers; reject invalid versions or fields before requester/result-handler dispatch. |
| `PEER_DISCONNECTED` | v2 | No | Yes | Yes | Yes | Reject invalid versions or fields before scheduler dispatch. |
| `TASK_ASSIGN` | v2 with assignment identity | No | Yes, with all required v2 fields | No | No | Reject with reason code `assignment_protocol_v2_required`; RabbitMQ may dead-letter it. |
| `TASK_RESULT` | v2 with assignment identity and optional failure classification | No | Yes, with all required v2 fields | No | No | Reject with reason code `assignment_protocol_v2_required`; it cannot reach task commitment and is never broker-requeued for this permanent incompatibility. |

The unchanged message types keep legacy compatibility because their semantics
did not change. `TASK_ASSIGN` and `TASK_RESULT` require a coordinated version-2
cutover: drain in-flight version-1 work and pending version-1 assignment outbox
rows before upgrading. A version-1 coordinator rejects version-2 messages as
future, while a version-2 coordinator rejects identity-less version-0/1 task
results.

## Version 3 Capacity Advertisement

A v3 `PONG` includes:

- a canonical UUID `executorInstanceId`;
- a positive, monotonically increasing `capacitySnapshotSequence`;
- positive executor `totalCapacityUnits` and non-negative
  `availableCapacityUnits`, with available not greater than total;
- `maxConcurrencyByTaskType`, with one positive limit for every advertised
  task type and no extra task types.

A requester-only participant advertises no task types and uses zero total/free
units plus an empty concurrency map, while retaining a valid instance UUID and
positive sequence.

For one live executor instance, only a higher sequence replaces the scheduling
snapshot. A stale sequence refreshes liveness without changing capacity. A
snapshot from another executor instance using the same live participant ID is
acknowledged as an instance conflict and changes neither liveness nor capacity.
Versions 0 through 2 remain useful for liveness and capability inspection, but
clear scheduling capacity with reason `CAPACITY_PROTOCOL_UNSUPPORTED`.

## Job-Submission Idempotency Compatibility

Requester-scoped submission idempotency adds no wire field and does not require
a protocol-version bump. The coordinator derives a versioned `v1:` SHA-256
request hash from existing validated `JOB_SUBMIT` fields: the requester token
hash and optional verified public key, normalized task type, null-to-empty
parameter, and the ordered canonical JSON payload digests. `jobId` remains the
separate idempotency key; message time, signature, and routing node are excluded
from the hash.

An exact duplicate therefore may carry a new time and a re-signed node ID after
reconnect while still resolving to the original running status or terminal
result. The RabbitMQ envelope `fromNodeId` must match the inner submission's
`nodeId`, preventing a captured submission from routing the replay response
elsewhere. The durable owner remains the token hash plus optional public key,
not the routing ID.

SQLite schema v12 persists the derived hash atomically with new job/task rows.
Rows migrated from older schemas have no derivable original-submission hash and
are rejected as legacy collisions instead of being guessed from possibly
plugin-transformed task snapshots. Version-0/1/2 `JOB_SUBMIT` messages remain
wire-readable; only jobs first accepted by schema-v12 code gain durable exact
submission replay.

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

An unsuccessful result may add `failureClassification`. Current senders use
`RETRYABLE` for ordinary processor failures and
`PERMANENT_PAYLOAD_INTEGRITY` for exact object length/SHA-256 mismatch. Missing
classification defaults to `RETRYABLE`, preserving earlier protocol-v2
behavior. A successful result carrying an explicit classification is rejected
with `invalid_task_failure_classification`.

This additive field does not require protocol v3: an older v2 coordinator may
ignore it and apply its former retry policy, while a current coordinator makes
the first accepted permanent-integrity failure terminal. Executor and
coordinator artifacts must therefore be upgraded together before relying on
the no-logical-retry corruption guarantee. An older executor remains compatible
and sends an unclassified failure, which a current coordinator treats as
retryable.

Missing fields deserialize to invalid zero/null values and are rejected.
Blank, zero, negative, shortened, or malformed assignment identity values are
rejected with reason code `invalid_assignment_identity`.

## Structured Rejection and Settlement

`MessageValidationException` exposes a stable `reasonCode` for transport and
scheduler logs. The task-protocol codes introduced with version `2` are:

- `assignment_protocol_v2_required` for version-0/1 task assignments/results;
- `invalid_assignment_identity` for missing or malformed v2 identity fields;
- `invalid_task_failure_classification` when a successful task result carries
  failure-only classification metadata;
- `unsupported_protocol_version` when a message object bypasses parser-level
  version checks.

Every consumed delivery is settled through one of five domain-aware outcomes:

| Disposition | Meaning | Current RabbitMQ settlement |
|---|---|---|
| `ACK_SUCCESS` | The delivery was handled successfully. | Acknowledge. |
| `ACK_DUPLICATE_OR_STALE` | The event is already applied, obsolete, or addressed to another current owner. | Acknowledge without changing authoritative state. |
| `RETRY_TRANSIENT` | Temporary infrastructure unavailability or bounded-ingress pressure prevented handling. | Publisher-confirmed handoff to the configured bounded TTL retry schedule; exhaustion enters final quarantine. |
| `REJECT_INVALID` | The envelope/message is malformed, unsupported, or permanently invalid. | Reject without requeue; RabbitMQ dead-letters it when configured. |
| `QUARANTINE_POISON` | Processing failed deterministically, including assignment-identity cache conflicts. | The same bounded TTL schedule, followed by one publisher-confirmed final-quarantine handoff after exhaustion. |

RabbitMQ codec failures are logged with `reason_code` and
`disposition=REJECT_INVALID`. The scheduler repeats validation as defense in
depth and gives an incompatible result the same terminal disposition when it is
injected directly. Duplicate and stale scheduler or executor outcomes are
acknowledged explicitly rather than falling through a generic success path.

The shared failure classifier maps `MessageValidationException` and invalid
arguments to rejection, explicit transient failures plus I/O, timeout,
interruption, cancellation, and executor-saturation failures to transient
retry, and otherwise-unclassified deterministic failures to poison
quarantine. There is no configurable generic
`catch (Exception) -> requeue` policy.

Retry publications preserve the original routing key and stable first/current
failure reason in TaskFlow headers. `x-taskflow-delivery-attempt` starts at 1;
the default `1000,5000,30000` millisecond schedule therefore permits four
processing deliveries before quarantine.

These permanent compatibility failures are distinct from temporary scheduler
mailbox saturation, which receives `RETRY_TRANSIENT` for backpressure.

## RabbitMQ Envelopes

RabbitMQ carries `protocolVersion` in two places:

- the broker envelope root, next to `route`, `fromNodeId`, and `message`;
- the inner protocol message object.

Envelope versions `0`, `1`, and `2` remain readable because envelope semantics
did not change; envelope version `3` is invalid. Inner-message compatibility
follows the matrix. A v2 envelope can therefore contain a valid v3 capacity
`PONG`, while its inner version remains authoritative. A legacy envelope can
contain a valid v2 task result, but a v2 envelope cannot make a legacy inner
task result commit-eligible.

`RabbitMqMessageCodec` checks both numeric ranges and validates the inner
message before returning an inbound transport message. Invalid deliveries are
rejected; with dead-lettering enabled, RabbitMQ can route them to the TaskFlow
DLQ for inspection, discard, or redrive after compatible code is deployed.

## Conversion Object-reference Compatibility

TF-0503 changes the conversion plugin-owned `FilePayload` JSON, not the
framework message envelope. Its referenced form is now:

```json
{
  "fileName": "sample.png",
  "objectReference": {
    "key": "taskflow/inputs/550e8400-e29b-41d4-a716-446655440000",
    "contentLength": 1234,
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "contentType": "image/png"
  }
}
```

The former `payloadReference` / `storageType: local-file` / `location` /
`sizeBytes` shape is intentionally rejected as `invalid_payload_reference`.
It cannot identify portable data on another machine. All requester,
coordinator-plugin, and executor conversion artifacts must therefore be
upgraded together before sending referenced conversion media. Small legacy
inline conversion payloads remain readable only when their raw bytes are below
the configured inline limit.

No protocol-version bump is used because `JOB_SUBMIT` and `TASK_ASSIGN` already
treat task payload JSON as plugin-owned semantic data and no framework envelope
field or assignment/result identity changed. TF-0504 separately adds the
optional framework-owned `TASK_RESULT.failureClassification` described above;
that additive field preserves wire parsing but requires a coordinated upgrade
for its no-retry semantics. TF-0505 uses the existing plugin-owned
`resultPayload` object for output `ObjectReference` metadata and the existing
protocol-v2 attempt/assignment fields for ownership; it adds no envelope field
or version. Executors and coordinators must still be upgraded together because
older validators do not enforce the attempt-output key. This does not claim
mixed-version compatibility for the changed plugin payload shape.

## Field and Size Validation

`protocol.MessageValidator` remains the shared framework boundary. In addition
to version-2 assignment identity and version-3 capacity fields, it enforces:

- known message types;
- required `nodeId` and `time` fields;
- peer IDs that match TaskFlow's safe peer-ID contract;
- job IDs, task IDs, and task types limited to letters, numbers, `.`, `:`, `_`,
  and `-`;
- `TASKFLOW_MAX_TASKS_PER_JOB` on submitted payloads and advertised task types;
- `TASKFLOW_MAX_JOB_PAYLOAD_BYTES` on job payloads and task assignments;
- `TASKFLOW_MAX_INPUT_BYTES` on each recursively discovered submitted or
  assigned `ObjectReference.contentLength`;
- `TASKFLOW_MAX_RESULT_BYTES` on task and final job results, including
  recursively discovered result references;
- `TASKFLOW_MAX_INLINE_PAYLOAD_BYTES` as an exclusive raw-byte ceiling for
  recursively discovered `base64Data` in submissions, assignments, task
  results, and final job results;
- valid portable object-reference metadata, valid Base64, and explicit
  rejection of the removed local-filesystem reference shape; and
- exact output-reference ownership: each reference in a successful
  `TASK_RESULT` must use the key derived from that message's `jobId`, `taskId`,
  `attemptNumber`, and `assignmentId`.

The inline-media boundary returns `max_inline_payload_bytes`; malformed Base64
returns `invalid_inline_payload`; malformed, unsafe, or legacy references
return `invalid_payload_reference`; and a declared object length above the
applicable input/result bound returns `max_referenced_payload_bytes`. A valid
reference owned by another assignment returns
`invalid_task_output_reference`.

Invalid `JOB_SUBMIT` and `JOB_RESULT_REQUEST` messages that reach the scheduler
are converted to failed `JOB_RESULT` responses when the requester can be
routed. Invalid non-submit messages are rejected or dropped. This is a
message-safety boundary, not user/account authentication.

Task-count, inline-byte, referenced-payload, active-job, active-task, and
pending-outbox admission failures use unsuccessful protocol-v2 `JOB_RESULT`
with the existing human-readable `errorMessage` plus optional:

```json
{
  "admissionRejection": {
    "limit": "MAX_ACTIVE_TASKS",
    "configuredMaximum": 100000,
    "observedValue": 100001
  }
}
```

No message type or version changed. Legacy JSON without the field deserializes
with no rejection detail, and older readers may ignore the additive field.
Ordinary successful and terminal-result constructors never attach it.
Mailbox-full deliveries have not entered scheduler admission, so they retain
the broker `RETRY_TRANSIENT` disposition and do not invent a requester result.

## Current Fencing Boundary

Version `2` prevents identity-less version-0/1 task results from entering the
commit path and makes assignment identity available end to end. SQLite schema
version `10` now persists that tuple in task state and attempt audit, restores
complete unexpired identities, and releases incomplete legacy assignments. It
also makes the supplied v2 identity authoritative during successful-result
commitment: task ID, `ASSIGNED` state, attempt number, assignment ID, and the
reporting participant must all match in SQLite. Matching, duplicate, and stale
store/scheduler dispositions are automated. The complete same-participant
attempt-1/X to attempt-2/Y sequence is covered by deterministic real-SQLite and
live RabbitMQ integration tests, so identity reuse cannot bypass the v2
generation fence.

## Plugin and Transport Expectations

Plugins should use SPI message classes rather than raw JSON. Server job plugins
continue to produce task-assignment payload templates; the coordinator adds its
framework-owned assignment identity before publication. Participant processors
should return through the shared execution engine so results echo that identity.
Paired server and executor plugins must also declare the same immutable
`TaskResourceProfile`; the scalar capacity-unit cost affects placement, while
the optional memory and temporary-disk estimates are diagnostic only.

Retry safety is an SPI declaration, not a new wire field. The existing version-2
`TaskAssignMessage` is the processor execution context: `taskId` stays stable
across logical retry generations, while `assignmentId` stays stable only across
redelivery of one generation. A `REQUIRES_IDEMPOTENCY_KEY` plugin must document
which identity it sends to which external key. Paired server/executor artifacts
must declare the same value; the coordinator uses the server-side mirror for
pre-acceptance policy validation.

Plugins must not construct RabbitMQ envelopes or bypass transport codecs.
RabbitMQ transport code emits the current version, applies the per-message
compatibility matrix, calls the shared validator after parsing, and gives
permanent validation failures a terminal non-requeue disposition. The broker
envelope is versioned separately from its inner TaskFlow message.
