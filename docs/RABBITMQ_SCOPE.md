# RabbitMQ Runtime Scope Decision

This document records the RabbitMQ support decision for TaskFlow. It does not
add new runtime guarantees beyond `docs/EXECUTION_GUARANTEES.md`. The broader
transport direction is recorded in `docs/RUNTIME_STRATEGY.md`.

## Decision

RabbitMQ is the planned primary runtime, but the current RabbitMQ
implementation remains a transitional broker adapter, not a fully supported
production runtime.

The current implementation is useful for broker-backed demos and focused
integration coverage, but it does not yet provide the durability workflows
needed to call RabbitMQ a complete supported runtime. TCP remains the current
default compatibility/demo runtime until the replacement gates in
`docs/RUNTIME_STRATEGY.md` pass.

## Current RabbitMQ Guarantees

The current RabbitMQ path includes:

- Coordinator and command-line peer entry points selected with
  `TASKFLOW_TRANSPORT=rabbitmq`.
- Shared broker routes for heartbeat, job submission, and task result messages.
- Peer-specific broker routes for task assignment and job result messages.
- JSON protocol serialization through the same protocol message types used by
  TCP.
- Command-line peer submit mode builds payloads and saves successful final
  results through `ClientJobPlugin`.
- Publisher confirms for broker publishes.
- Mandatory-return detection for unroutable peer-targeted publishes.
- Manual acknowledgement, deferred acknowledgement, requeue, and reject
  behavior.
- RabbitMQ prefetch configuration.
- Dead-letter exchange and queue topology declaration.
- Live broker tests for shared-route delivery, peer-route delivery,
  acknowledgement drain, handler-failure requeue, reject-to-dead-letter,
  prefetch behavior, broker-side connection-close recovery, and coordinator
  job completion.
- Docker Compose demo coverage for a RabbitMQ-backed image conversion job when
  Docker is available.

## Current RabbitMQ Limits

TaskFlow does not yet provide:

- JavaFX RabbitMQ submission or JavaFX RabbitMQ worker runtime.
- Stable shared peer identity across GUI and command-line peers.
- Peer-scoped job IDs.
- Durable coordinator outbox persistence.
- Replay of confirmed or unconfirmed outbound messages after coordinator crash.
- Defined idempotency and duplicate handling for outbox replay.
- Crash-timing coverage for coordinator failure before publish, after publish
  before acknowledgement/update, or during replay.
- TaskFlow DLQ inspection.
- TaskFlow DLQ quarantine/discard decisions.
- TaskFlow DLQ redrive back to the correct normal route.
- Adaptive broker/peer backpressure beyond bounded scheduler ingress and broker
  prefetch.

Because these pieces are missing, docs should keep RabbitMQ language scoped to
the tested behavior above.

## Gates For Supported Runtime Status

RabbitMQ can be reconsidered as the primary supported runtime only after the
replacement gates in `docs/RUNTIME_STRATEGY.md` and the behavior gates below
are implemented and tested.

First close runtime replacement gaps:

- Add JavaFX RabbitMQ submission, worker execution, result routing, result
  saving, and broker-failure handling behind the existing GUI service
  boundaries.
- Define stable peer identity for GUI and command-line peers, including
  configured IDs, generated IDs, sanitization, and duplicate-ID behavior.
- Generate peer-scoped, collision-resistant job IDs on every submit path.

Then implement durable outbox/replay:

- Define an outbox persistence contract.
- Define message identity and replay ordering.
- Define idempotency rules and duplicate handling.
- Cover coordinator crash before publish.
- Cover coordinator crash after publish but before acknowledgement or state
  update.
- Cover coordinator crash during replay.
- Prove replay does not create successful duplicate task results or duplicate
  terminal job completion.

Then implement DLQ review/redrive:

- Persist or expose original route and routing-key metadata.
- Preserve failure reason and dead-letter context.
- Track redrive count.
- Define review decisions: redrive, quarantine, discard.
- Avoid automatic requeue of poisoned messages.
- Test redrive to the correct normal route and quarantine/discard behavior.

Then promote evidence:

- Add a RabbitMQ-backed CI profile that starts a broker and runs focused live
  integration gates.
- Keep Docker Compose and manual or automated GUI evidence aligned with the
  README path.
- Update public docs so quick-start, demos, limitations, execution guarantees,
  and transport scope describe the same default and support status.

Only after those gates should documentation describe RabbitMQ as the primary
supported runtime rather than a transitional adapter.

## Public Claim Rule

Until the gates above are complete, public docs should say that RabbitMQ mode is
functional and tested for the listed broker behaviors, but transitional. Avoid
phrases such as production-ready broker runtime, durable RabbitMQ recovery, full
broker outage recovery, or built-in DLQ redrive.
