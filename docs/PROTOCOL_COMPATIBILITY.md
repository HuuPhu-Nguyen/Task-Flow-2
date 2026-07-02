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
looking up the registered message parser.

## RabbitMQ Envelopes

RabbitMQ broker messages carry `protocolVersion` in two places:

- the broker envelope root, next to `route`, `fromNodeId`, and `message`;
- the inner protocol message object.

`RabbitMqMessageCodec` checks both fields before returning an inbound transport
message. Legacy broker envelopes and inner messages that omit the field are
accepted as version `0`. Future envelope or inner-message versions are rejected,
which also makes those DLQ entries non-redrivable until code that understands
the new protocol is deployed.

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
unsupported versions before dispatching a message to runtime code. Transports
with their own envelope metadata should version that envelope separately from
the inner protocol message, as the RabbitMQ transport does.

For rolling upgrades, deploy receivers that accept a new protocol version before
senders begin emitting it. Until then, future-version messages are rejected
rather than silently interpreted as an older contract.
