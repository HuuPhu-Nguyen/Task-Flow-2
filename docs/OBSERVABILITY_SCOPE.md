# Observability Scope

This document records what TaskFlow exposes through logs today and what remains
deferred before adding a dedicated metrics backend.

## Current Position

TaskFlow currently relies on structured SLF4J/Logback event logs plus scheduler
metrics snapshots. These logs are suitable for local runs, CI output, Docker
demo inspection, and focused troubleshooting.

TaskFlow does not yet provide a metrics exporter, dashboard, alerting rules,
distributed tracing, log retention policy, DLQ dashboard, or aggregated DLQ
metrics.

## Current Event Map

Queue pressure and scheduler health:

- `scheduler_metrics` includes `queue_depth`, `active_jobs`,
  `dispatch_latency_ms`, `retry_count`, `task_success_rate`, `success_count`,
  and `failure_count`.
- `scheduler_ingress_requeued reason=mailbox_full` records RabbitMQ
  scheduler-delivery requeue when the bounded scheduler mailbox is full.
- TCP coordinator `peer_status` logs include peer connection state and active
  scheduler work snapshots.

Job lifecycle:

- `job_started` records accepted scheduler jobs with `job_id`, `task_type`,
  requester, and task count.
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

- `task_assigned` records assignment ownership with `job_id`, `task_id`,
  `peer_id`, and `dispatch_latency_ms`.
- `task_completed` records accepted successful task results.
- `task_failed`, `task_timeout`, and `task_peer_unavailable` record failed
  attempts with `retry_count` and `terminal_failure`.
- `task_lease_expired` records assigned work whose persisted lease expired
  before a result was accepted.
- `peer_unavailable_tasks_released` records how many tasks were returned for
  retry or made terminal after peer disconnect or heartbeat timeout.
- `task_dispatch_failed` records failed assignment sends before work reaches a
  peer.

Final result delivery and abandoned states:

- `job_result_delivery_deferred` records a failed final `JOB_RESULT` delivery
  attempt.
- `job_result_delivery_abandoned` records final-result delivery exhaustion with
  attempt count, requester, success flag, and reason.
- `job_terminal_persistence_degraded` records terminal history writes that
  failed after result delivery or abandonment.
- `job_result_requester_missing` and `job_result_request_send_failed` record
  failed `JOB_RESULT_REQUEST` responses.
- With RabbitMQ coordinator outbox enabled, `job_completed` includes
  `outbox_id` and `outbox_published` for terminal final-result publication.

Persistence and recovery:

- `database_initialized`, `database_unavailable`, and `database_disabled`
  record whether SQLite history is available for the coordinator run.
- `running_job_resumed` and scheduler `job_resumed` record rebuildable jobs
  restored at startup.
- `running_job_not_resumable`,
  `non_resumable_running_jobs_marked_failed`,
  `abandoned_jobs_marked_failed`, and
  `abandoned_job_reconciliation_failed` record recovery decisions that prevent
  silent history drift.
- `scheduler_persistence_failed` records post-start persistence writes that
  fail during scheduler state transitions.

RabbitMQ publish, acknowledgement, and DLQ routing:

- `rabbitmq_connected` records the broker connection configuration, including
  prefetch, publisher-confirm timeout, dead-letter setting, and handler-failure
  requeue policy.
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
- `scheduler_message_ack_failed`, `scheduler_message_requeue_failed`, and
  `rabbitmq_delivery_requeue_failed` record failed acknowledgement settlement.
- `task_result_publish_failed` and `rabbitmq_heartbeat_failed` record peer-side
  RabbitMQ publish failures.
- `rabbitmq_delivery_decode_failed action=reject` records malformed broker
  delivery rejection. With dead-lettering enabled, RabbitMQ routes rejected
  deliveries to the configured dead-letter queue.
- `rabbitmq_delivery_handler_failed action=reject` can also route failed
  handler deliveries to the dead-letter queue when
  `TASKFLOW_RABBITMQ_REQUEUE_ON_HANDLER_FAILURE=false`.
- `rabbitmq_dlq_redriven`, `rabbitmq_dlq_quarantined`, and
  `rabbitmq_dlq_discarded` record explicit operator decisions on DLQ entries.
- `rabbitmq_dlq_redrive_rejected status=not_redrivable`,
  `rabbitmq_dlq_redrive_deferred`, `rabbitmq_dlq_quarantine_deferred`,
  `rabbitmq_dlq_publish_unroutable`, `rabbitmq_dlq_publish_not_confirmed`,
  `rabbitmq_dlq_publish_confirm_timeout`, and
  `rabbitmq_dlq_publish_returned_unmatched` record DLQ workflow failures or
  deferrals.

## Current Limits

Current observability is log-based. Operators can grep logs for structured
events, but TaskFlow does not aggregate those events into a metrics backend or
provide stable dashboard contracts.

SQLite persistence now records durable task-attempt history rows for assignment,
success, retry, terminal failure, dispatch failure, startup reconciliation, and
restart release. SQLite task rows also record lease owner and expiry for
assigned work. SQLite broker outbox rows record coordinator-originated
RabbitMQ task assignments and final job results until they are marked sent.
TaskFlow DLQ commands emit decision logs and preserve redrive counts in broker
headers, but TaskFlow still does not provide promoted lease dashboards, outbox
dashboards, DLQ dashboards, or aggregated DLQ metrics. Those remaining
behaviors are deferred in the recovery and RabbitMQ scope documents.

## Metrics Backend Deferral

A dedicated metrics backend is deferred until at least one of these behavior
tracks is implemented and needs promoted operational visibility:

- promoted lease or attempt-history dashboards;
- promoted RabbitMQ outbox replay dashboards or metrics;
- promoted DLQ dashboards or metrics beyond the current command output and
  structured logs.

Before adding a metrics exporter, define:

- metric names, units, and label cardinality;
- which log events remain canonical and which become metrics;
- promoted lease and attempt-history metric contracts;
- outbox and DLQ state transitions for RabbitMQ metrics;
- focused tests that prove emitted metrics match scheduler and broker state.

## Public Claim Rule

Public docs should describe current observability as structured logs,
log-based scheduler metrics, SQLite task-attempt audit rows, SQLite task lease
fields, and SQLite coordinator outbox rows. Avoid claiming built-in dashboards,
alerting, tracing, promoted lease timelines, outbox dashboards, or DLQ
dashboards until those systems exist and are tested.
