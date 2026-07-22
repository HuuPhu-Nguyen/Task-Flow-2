# 0004: Use Semantic Final Result Payloads With Compatibility Task-Result Lists

Status: Accepted

Date: 2026-07-04

## Context

Early TaskFlow result handling treated final job output mainly as an ordered
list of task results. That works for simple file/list plugins, but framework
plugins also need final outputs such as summaries, reports, rankings,
reductions, or preview models that are not just the task-result list.

Existing clients and plugins still need compatibility with list-based result
handling.

## Decision

`JobResultMessage.resultPayload` is the semantic final job payload.
`JobResultMessage.resultsByTaskId` remains the compatibility list for existing
task-result consumers.

Server plugins may override `EmbarrassinglyParallelJob.aggregateResultPayload()`
to produce a plugin-defined final result. Client plugins receive the complete
`JobResultMessage` through `ClientJobPlugin.handleResult(...)` and decide how
to save, render, print, or otherwise handle successful final results.

Aggregation is a deterministic replay function of committed per-task results.
The coordinator may invoke it again after recovering a durable `FINALIZING`
job, using the canonical task order rather than the original result-arrival
order.

## Consequences

- Plugin authors can evolve final result formats without changing scheduler,
  transport, GUI, or peer runtime internals.
- The GUI and RabbitMQ command-line submitter route successful final results
  through client plugins.
- Failed terminal `JOB_RESULT` messages remain user-visible outcomes and are
  not passed to client plugins as successful payloads.
- Persistence stores schema-v6 completed semantic final payloads so successful
  completed results can be reconstructed with both semantic and compatibility
  data when ownership checks pass.
- Schema-v11 finalization recovery may recompute the semantic payload before it
  is terminally stored, so server plugins must not make aggregation depend on
  clocks, randomness, process-local history, or non-idempotent external side
  effects.
- Existing list/file-result plugins remain compatible through the default
  `handleResult(...)` implementation.

## Evidence

- `docs/PEER_LIFECYCLE.md`
- `docs/PLUGIN_AUTHORING.md`
- `docs/EXECUTION_GUARANTEES.md`
- `taskflow-spi/src/main/java/protocol/JobResultMessage.java`
- `taskflow-spi/src/main/java/client/ClientJobPlugin.java`
- `taskflow-spi/src/main/java/server/job/EmbarrassinglyParallelJob.java`
- `plugins/example`

## Related Documents

- [Participant Requester And Result Lifecycle](../PEER_LIFECYCLE.md)
- [Plugin Authoring](../PLUGIN_AUTHORING.md)
- [Execution Guarantees](../EXECUTION_GUARANTEES.md)
