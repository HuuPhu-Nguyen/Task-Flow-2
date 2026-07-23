# Observability Scope

This document records what TaskFlow exposes through logs today and what remains
deferred before adding a dedicated metrics backend.

## Current Position

TaskFlow currently relies on structured SLF4J/Logback event logs, scheduler
metrics snapshots, and coordinator operator status commands. These outputs are
suitable for local runs, CI output, Docker demo inspection, and focused
troubleshooting.

TaskFlow does not yet provide a metrics exporter, dashboard, alerting rules,
distributed tracing, log retention policy, DLQ dashboard, or aggregated DLQ
metrics.

## Current Event Map

Queue pressure and scheduler health:

- `scheduler_metrics` includes `queue_depth`, `active_jobs`,
  `dispatch_latency_ms`, `retry_count`, `task_success_rate`, `failure_count`,
  `taskflow_task_results_committed_total`,
  `taskflow_task_results_stale_total`,
  `taskflow_task_results_duplicate_total`,
  `taskflow_assignment_generations_total`, `unknown_result_count`, and
  `result_storage_failure_count`. The four `taskflow_*_total` fields are
  monotonic process-lifetime counters with stable names; they are log fields,
  not an exported metrics-backend contract.
- Coordinator `status summary`, `status jobs`, `status peers`, `status outbox`,
  `status queues`, and `status dlq` commands report persisted job/task state,
  retry and lease counts, last-known peers, pending coordinator outbox rows,
  RabbitMQ queue depths, and DLQ inspection summaries.
- `scheduler_ingress_requeued reason=mailbox_full` records RabbitMQ
  scheduler-delivery requeue when the bounded scheduler mailbox is full.

Job lifecycle:

- `job_started` records accepted scheduler jobs with `job_id`, `task_type`,
  requester, task count, and the plugin's `retry_safety` declaration.
- `job_submission_replayed` records an exact owner/request-hash replay with job
  ID, type, requester route, and durable job status. It does not imply another
  job/task creation transition.
- `job_submission_conflict` distinguishes `REQUEST_CONFLICT`,
  `OWNER_CONFLICT`, and `LEGACY_CONFLICT` without logging the requester token or
  canonical request hash.
- `job_completed` records terminal scheduler completion with success and result
  count.
- `job_failed` records terminal scheduler failure reasons.
- `job_start_failed`, `job_start_failure_requester_missing`, and
  `job_start_failure_send_failed` record rejected or unroutable startup
  failures.
- RabbitMQ command-line submitters log `job_submitted`, `job_result_received`,
  `job_completed`, and `job_failed`.
- JavaFX submission and save paths log `gui_job_submitted`,
  `gui_results_saved`, and `gui_results_save_failed`.

Task assignment, retry, and failure:

- `peer_processor_registered` records each discovered executor processor with
  `task_type`, `retry_safety`, and `plugin_class`.
- `task_assignment_created` records each newly created assignment generation.
  RabbitMQ outbox assignments also include `outbox_id` and the immediate
  `outbox_published` outcome; non-outbox assignments include the same committed
  identity before the send attempt.
- `task_result_committed` records the one authoritative successful result after
  the SQLite commit decision has been applied to scheduler memory.
- `task_result_stale_rejected` records an obsolete assignment generation that
  the authoritative store rejected, including the same-participant ABA case.
- `task_result_duplicate_ignored` records a repeated result for an assignment
  whose authoritative result was already committed. It is intentionally
  distinct from stale-assignment rejection.
- Each of those four events carries the complete correlation tuple as
  `job_id`, `task_id`, `attempt_number`, `assignment_id`, and `worker_id`.
  These are the structured-log spellings of the protocol/domain fields
  `jobId`, `taskId`, `attemptNumber`, `assignmentId`, and `workerId`; `workerId`
  identifies the participant while it acts in the executor role.
- `task_result_not_committed` is retained only for unknown-task and
  storage-failure dispositions, also with the complete correlation tuple.
  Duplicate, stale, and unknown broker results are acknowledged; storage
  failure is requeued by the scheduler's processing-failure path.
- `task_failed`, `task_timeout`, and `task_peer_unavailable` record failed
  attempts with `retry_count` and `terminal_failure`.
- `task_lease_expired` records assigned work whose persisted lease expired
  before a result was accepted.
- `peer_unavailable_tasks_released` records how many tasks were returned for
  retry or made terminal after peer disconnect or heartbeat timeout.
- `task_dispatch_failed` records failed assignment sends before work reaches a
  peer.
- `task_assignment_cache_hit`, command-line `task_assignment_duplicate`, and
  JavaFX `gui_rabbitmq_task_assignment_duplicate` record running/completed
  assignment-cache hits with peer, task, assignment, disposition, cache size,
  and cumulative eviction count.
- `assignment_cache_evicted` records capacity or TTL eviction with assignment
  ID, entry state, post-eviction size, and cumulative eviction count. The
  executor engine's cache snapshot also exposes current size, running/completed
  entries, running/completed duplicate counts, and total/capacity/TTL eviction
  counts for tests and runtime adapters.
- `task_assignment_cache_conflict` and
  `gui_rabbitmq_task_assignment_cache_conflict` record permanent rejection when
  a live assignment ID is reused for a different task identity.

Final result delivery and abandoned states:

- `job_result_delivery_deferred` records a failed final `JOB_RESULT` delivery
  attempt.
- `job_result_delivery_abandoned` records final-result delivery exhaustion with
  attempt count, requester, success flag, and reason.
- `job_terminal_persistence_deferred` records a non-outbox terminal write
  that did not commit; final-result delivery is suppressed and the active
  pending-completion projection remains available for retry.
- `job_result_requester_missing` and `job_result_request_send_failed` record
  failed `JOB_RESULT_REQUEST` responses.
- With RabbitMQ coordinator outbox enabled, `job_completed` includes
  `outbox_id` and `outbox_published` for terminal final-result publication.

Persistence and recovery:

- `database_initialized`, `database_unavailable`, and `database_disabled`
  record whether SQLite history is available for the coordinator run.
- `running_job_resumed` and scheduler `job_resumed` record rebuildable
  `RUNNING` or `FINALIZING` jobs restored at startup. The first event retains
  its compatibility name for both nonterminal durable states.
- `running_job_not_resumable`,
  `non_resumable_running_jobs_marked_failed`,
  `abandoned_jobs_marked_failed`, and
  `abandoned_job_reconciliation_failed` record recovery decisions that prevent
  silent history drift.
- `scheduler_persistence_failed` records post-start persistence writes that
  fail during scheduler state transitions.
- `scheduler_durable_transition_replayed` records an exact
  `ALREADY_APPLIED` durable transition whose matching projection may be
  installed idempotently.
- `scheduler_durable_transition_rejected` records a `STALE_STATE` conditional
  transition that preserves the current projection.

RabbitMQ publish, acknowledgement, and DLQ routing:

- `rabbitmq_connected` records the broker connection configuration, including
  prefetch, publisher-confirm timeout, and dead-letter setting.
- `rabbitmq_topology_declared` records dead-letter exchange, dead-letter queue,
  and quarantine queue declaration when enabled.
- `rabbitmq_dlq_topology_declared` records the DLQ and quarantine queues used
  by the operator DLQ command path.
- `rabbitmq_publish_unroutable`, `rabbitmq_publish_not_confirmed`,
  `rabbitmq_publish_confirm_timeout`, and
  `rabbitmq_publish_returned_unmatched` record broker publication failures or
  mandatory-return anomalies.
- `broker_outbox_publish_deferred`, `broker_outbox_publish_failed`, and
  `broker_outbox_publish_mark_failed` record scheduler-side outbox publish
  attempts that were not fully marked sent.
- `rabbitmq_outbox_published`, `rabbitmq_outbox_publish_deferred`,
  `rabbitmq_outbox_publish_failed`, `rabbitmq_outbox_publish_mark_failed`, and
  `rabbitmq_outbox_replay` record coordinator outbox replay activity.
- `scheduler_message_validation_failed` and
  `scheduler_message_processing_failed` include stable `reason_code` and
  `disposition` fields. `scheduler_delivery_disposed` records every non-success
  scheduler settlement, and `scheduler_message_settlement_failed` records a
  failed broker settlement attempt.
- `rabbitmq_delivery_settlement_failed` and
  `gui_rabbitmq_delivery_settlement_failed` record participant settlement
  failures with the requested disposition.
- `task_result_publish_failed` and `rabbitmq_heartbeat_failed` record peer-side
  RabbitMQ publish failures.
- `rabbitmq_delivery_decode_failed` records malformed broker input with
  `reason_code` and `disposition=REJECT_INVALID`. With dead-lettering enabled,
  RabbitMQ routes the rejected delivery to the configured dead-letter queue.
- `rabbitmq_delivery_handler_failed` records the classified `reason_code` and
  one of the typed failure dispositions. Participant assignment, execution, and
  result-publication failure events expose the same fields.
- `rabbitmq_dlq_redriven`, `rabbitmq_dlq_quarantined`, and
  `rabbitmq_dlq_discarded` record explicit operator decisions on DLQ entries.
- `rabbitmq_dlq_redrive_rejected status=not_redrivable`,
  `rabbitmq_dlq_redrive_deferred`, `rabbitmq_dlq_quarantine_deferred`,
  `rabbitmq_dlq_publish_unroutable`, `rabbitmq_dlq_publish_not_confirmed`,
  `rabbitmq_dlq_publish_confirm_timeout`, and
  `rabbitmq_dlq_publish_returned_unmatched` record DLQ workflow failures or
  deferrals.

## Current Limits

Current observability is log-based plus command-line status inspection.
Operators can grep logs for structured events or run the coordinator status
command against SQLite/RabbitMQ state, but TaskFlow does not aggregate those
events into a metrics backend or provide exporter or dashboard contracts. The
stable fencing-counter names above do not have high-cardinality labels or
metric exemplars; assignment correlation remains on structured log events.

SQLite persistence now records durable task-attempt history rows for assignment,
success, retry, terminal failure, dispatch failure, startup reconciliation, and
restart release, including each new attempt's assignment UUID, worker, assigned
time, and lease deadline. SQLite task rows also record the latest attempt number
and current assignment UUID alongside lease owner and expiry for assigned work.
SQLite broker outbox rows record coordinator-originated
RabbitMQ task assignments and final job results until they are marked sent.
The status summary reports `FINALIZING` separately from ordinary `RUNNING`
jobs so an interrupted aggregation boundary is visible to operators.
TaskFlow DLQ commands emit decision logs and preserve redrive counts in broker
headers, but TaskFlow still does not provide promoted lease dashboards, outbox
dashboards, DLQ dashboards, or aggregated DLQ metrics. The status command is a
terminal inspection surface, not a dashboard contract. Those remaining behaviors
are deferred in the recovery and RabbitMQ scope documents.

## Metrics Backend Deferral

A dedicated metrics backend is deferred until at least one of these behavior
tracks is implemented and needs promoted operational visibility:

- promoted lease or attempt-history dashboards;
- promoted RabbitMQ outbox replay dashboards or metrics;
- promoted DLQ dashboards or metrics beyond the current command output and
  structured logs.

Before adding a metrics exporter, define:

- exporter mappings, units, and bounded label cardinality for the existing
  fencing counters, plus names for any newly promoted metrics;
- which log events remain canonical and which become metrics;
- promoted lease and attempt-history metric contracts;
- outbox and DLQ state transitions for RabbitMQ metrics;
- focused tests that prove emitted metrics match scheduler and broker state.

## Public Claim Rule

Public docs should describe current observability as structured logs,
log-based scheduler metrics, coordinator status commands, SQLite task-attempt
audit rows, SQLite task lease fields, and SQLite coordinator outbox rows. Avoid
claiming built-in dashboards, alerting, tracing, promoted lease timelines,
outbox dashboards, or DLQ dashboards until those systems exist and are tested.
