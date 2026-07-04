# Protocol Compatibility

TaskFlow protocol compatibility is checked per message. There is no connection
handshake or downgrade negotiation today.

## Version Field

Every current protocol message serializes `protocolVersion: 1`.

- Version `1` is the current protocol.
- Missing `protocolVersion` is treated as legacy version `0` so older local
  peers and saved broker messages can still be read.
- Receivers accept versions `0` through `1`.
- Receivers reject versions above the current range before dispatching the
  message to scheduler, peer, GUI, or plugin code.

Unsupported versions fail with a clear `IllegalArgumentException` that names the
unsupported value and the supported range. Invalid non-integer version fields
fail before message-type dispatch.

## TCP Messages

TCP messages carry `protocolVersion` on the root JSON object alongside `type`,
`nodeId`, and `time`. `messaging.MessageFactory` enforces compatibility before
looking up the registered message parser. After parsing, the same message
validator used by other transports rejects missing framework fields, unsafe
peer/job/task identifiers, unsafe task-type names, excessive task counts, and
configured payload-size violations before dispatch.

## RabbitMQ Envelopes

RabbitMQ broker messages carry `protocolVersion` in two places:

- the broker envelope root, next to `route`, `fromNodeId`, and `message`;
- the inner protocol message object.

`RabbitMqMessageCodec` checks both fields before returning an inbound transport
message. Legacy broker envelopes and inner messages that omit the field are
accepted as version `0`. Future envelope or inner-message versions are rejected,
which also makes those DLQ entries non-redrivable until code that understands
the new protocol is deployed.

RabbitMQ envelopes also validate `fromNodeId` and the inner message before
dispatch. Invalid broker deliveries are rejected by the transport consumer; when
dead-lettering is enabled, RabbitMQ can route them to the TaskFlow DLQ for
inspection, quarantine, discard, or redrive if the body is otherwise a valid
TaskFlow envelope.

## Field and Size Validation

`protocol.MessageValidator` is the shared framework-level validation boundary.
It enforces:

- known message types;
- required `nodeId` and `time` fields;
- peer IDs that already match TaskFlow's safe peer-id contract;
- job IDs, task IDs, and task-type names limited to letters, numbers, `.`,
  `:`, `_`, and `-`;
- `TASKFLOW_MAX_TASKS_PER_JOB` on submitted task payload lists and advertised
  task-type lists;
- `TASKFLOW_MAX_JOB_PAYLOAD_BYTES` on submitted job payloads and task
  assignments;
- `TASKFLOW_MAX_RESULT_BYTES` on task results and final job results.

Invalid `JOB_SUBMIT` and `JOB_RESULT_REQUEST` messages that reach the scheduler
are converted to failed `JOB_RESULT` responses when the requester can be routed.
Invalid non-submit scheduler messages are rejected or dropped instead of being
requeued indefinitely. This validation is a message-safety boundary; it is not
user/account authentication.

## Plugin Expectations

Plugins should construct and consume the SPI message classes instead of
building raw protocol JSON. Plugin-owned payload and result objects can evolve
inside `taskPayloads`, `resultPayload`, and task result payloads, but plugin
authors should keep their own payload compatibility explicit when changing
those shapes.

Plugins should not create RabbitMQ envelopes or bypass transport codecs. If a
new task type needs framework fields outside plugin-owned payloads, add the
field to the shared protocol contract and update these compatibility rules.

## Transport Expectations

New transports must emit the current protocol version and must reject
unsupported versions before dispatching a message to runtime code. They should
also call the shared message validator after parsing and before dispatch.
Transports with their own envelope metadata should version that envelope
separately from the inner protocol message, as the RabbitMQ transport does.

For rolling upgrades, deploy receivers that accept a new protocol version before
senders begin emitting it. Until then, future-version messages are rejected
rather than silently interpreted as an older contract.
