# Observability Scope

This document records TaskFlow's structured-log, status-command, and
Prometheus metrics surfaces. The normative common envelope, outcome/reason
vocabulary, correlation rules, and data-protection constraints for coordinator
scheduler state transitions are defined in
[Coordinator event schema](OBSERVABILITY.md).

## Current Position

TaskFlow exposes structured SLF4J/Logback event logs, scheduler metrics
snapshots, coordinator operator status commands, and a coordinator-local
operational HTTP endpoint for Prometheus metrics, liveness, and readiness.
These outputs are suitable for local runs, CI output, Docker demo inspection,
scraping, and focused troubleshooting.

TaskFlow does not provide built-in dashboards, alerting rules, distributed
tracing, a log retention policy, or a DLQ dashboard.

## Coordinator Operational Endpoint

The RabbitMQ coordinator serves Prometheus text at `GET /metrics`, liveness at
`GET /health/live`, and readiness for new jobs at `GET /health/ready`.
The listener is enabled by default on `127.0.0.1:9464`; loopback is the safe
default for direct local runs. Configure it with the environment variables
below or the corresponding Java system properties. A nonblank system property
takes precedence over its environment variable.

| Setting | System property | Default | Meaning |
|---|---|---:|---|
| `TASKFLOW_METRICS_ENABLED` | `taskflow.metricsEnabled` | `true` | Compatibility name for the shared listener switch. `false` disables metrics and both health routes. |
| `TASKFLOW_METRICS_HOST` | `taskflow.metricsHost` | `127.0.0.1` | Bind address or host. Use `0.0.0.0` only when the surrounding network boundary is intentional. |
| `TASKFLOW_METRICS_PORT` | `taskflow.metricsPort` | `9464` | TCP port. Port `0` is supported for test-managed ephemeral binding. |

For a local coordinator:

```text
curl http://127.0.0.1:9464/metrics
curl http://127.0.0.1:9464/health/live
curl http://127.0.0.1:9464/health/ready
```

The Docker Compose coordinator binds the listener to `0.0.0.0` inside the
container and publishes host port `9464`, so the same host URL works for the
demo. Exact status codes, readiness inputs, bounded reasons, automatic
recovery, degraded admission behavior, and probe limits are defined in the
[coordinator health contract](HEALTH.md).

### Exported metric contract

All counters are process-lifetime values and reset on coordinator restart.
Gauges describe the current in-process or durable state observed at scrape
time. Histograms use seconds and fixed buckets. The exporter has no
`job_id`, `task_id`, `assignment_id`, or `worker_id` labels; in fact, its only
label is Prometheus' fixed `le` label on histogram buckets. Entity correlation
remains in structured logs.

| Metric | Type | Unit | Meaning |
|---|---|---|---|
| `taskflow_jobs_accepted_total` | counter | jobs | New jobs installed after durable acceptance. Authorized submission replay does not increment it. |
| `taskflow_jobs_completed_total` | counter | jobs | Jobs whose successful terminal projection was applied. |
| `taskflow_jobs_failed_total` | counter | jobs | Jobs whose unsuccessful terminal projection was applied. |
| `taskflow_tasks_assigned_total` | counter | assignments | Task assignments installed after the authoritative assignment boundary. |
| `taskflow_task_assignment_generations_total` | counter | generations | Authoritative task assignment generations created. It advances with `taskflow_tasks_assigned_total`; the separate name makes assignment fencing explicit. |
| `taskflow_task_results_committed_total` | counter | results | Results accepted by the exact assignment fence and committed. |
| `taskflow_task_results_stale_total` | counter | results | Results rejected as an obsolete assignment generation. |
| `taskflow_task_results_duplicate_total` | counter | results | Repeated results classified as already committed. |
| `taskflow_tasks_retried_total` | counter | retries | Failed attempts durably closed with another logical task retry permitted. |
| `taskflow_task_lease_expirations_total` | counter | expirations | Current assignments durably closed after their leases expired. |
| `taskflow_scheduler_mailbox_depth` | gauge | messages | Envelopes currently admitted to the scheduler mailbox. |
| `taskflow_scheduler_pending_tasks` | gauge | tasks | Pending task entries currently indexed for dispatch. |
| `taskflow_scheduler_due_deadlines` | gauge | deadline entries | Timeout and lease deadline entries scheduled and pending evaluation. This is index pressure, not only entries whose wall-clock deadline has already passed. |
| `taskflow_outbox_pending` | gauge | rows | Pending durable coordinator broker-outbox rows. |
| `taskflow_outbox_oldest_age_seconds` | gauge | seconds | Age of the oldest pending broker-outbox row; zero when no row is pending. |
| `taskflow_broker_redeliveries_total` | counter | deliveries | Inbound broker deliveries observed with the broker redelivered flag or a delivery-attempt header greater than one. Each observed delivery increments at most once. |
| `taskflow_broker_quarantined_total` | counter | deliveries | Failed deliveries whose automatic terminal-quarantine publication was broker-confirmed. |
| `taskflow_worker_capacity_used` | gauge | capacity units | Capacity units reserved by current authoritative assignments, aggregated across workers. |
| `taskflow_orphan_outputs_total` | counter | deletion operations | Output deletions completed after authoritative orphan classification. |
| `taskflow_assignment_latency_seconds` | histogram | seconds | Time from the task's current pending eligibility timestamp to authoritative assignment creation. |
| `taskflow_result_commit_latency_seconds` | histogram | seconds | Time from assignment start to authoritative task-result commit. |
| `taskflow_recovery_duration_seconds` | histogram | seconds | Successful SQLite startup recovery duration. A run with unavailable or failed recovery contributes no observation. |

If SQLite is disabled, unavailable, or cannot answer the scrape-time outbox
aggregate, both outbox gauges are emitted as `NaN`, rather than presenting an
unknown observation as zero. All other metric reads are process-local
snapshots. Metrics do not alter scheduler decisions, broker settlement, or
durable state.

## Current Event Map

Queue pressure and scheduler health:

- `coordinator_operations_endpoint_started` records the shared listener bind
  and all three fixed routes; `coordinator_operations_endpoint_stopped`
  records lifecycle closure.
- `coordinator_health_changed` emits only when the readiness state or bounded
  reason set changes. It includes `status`, `live`, `ready`, `degraded`, and
  the identity-free reasons defined in [`HEALTH.md`](HEALTH.md). Repeated
  identical scrapes do not emit repeated transitions.
- `scheduler_metrics` includes `queue_depth`, `active_jobs`, `active_tasks`,
  `overloaded`, `overload_primary_reason`, `overload_configured_maximum`,
  `overload_observed_value`, `overload_reasons`, `job_submit_prefetch`, and
  `pending_outbox_observation_healthy`,
  `pending_tasks_indexed`, `runnable_jobs_indexed`,
  `capacity_waiting_jobs_indexed`,
  `live_assignments_indexed`, `deadline_entries_indexed`,
  `deadline_head_checks_total`, `deadline_entries_popped_total`,
  `deadline_entries_validated_total`, `deadline_stale_rejected_total`,
  `deadline_reschedules_total`,
  `dispatch_latency_ms`, `retry_count`, `task_success_rate`, `failure_count`,
  `taskflow_task_results_committed_total`,
  `taskflow_task_results_stale_total`,
  `taskflow_task_results_duplicate_total`,
  `taskflow_assignment_generations_total`,
  `taskflow_payload_integrity_failures_total`, `unknown_result_count`, and
  `result_storage_failure_count`. The five `taskflow_*_total` fields are
  monotonic process-lifetime counters with stable names. The three
  task-result counters map directly to the exporter; the legacy assignment and
  payload-integrity spellings remain log/snapshot fields. The exporter uses
  `taskflow_task_assignment_generations_total` for its assignment-generation
  contract. Workload-index field semantics and complexity are defined in
  [`SCHEDULER.md`](SCHEDULER.md).
- Coordinator `status summary`, `status jobs`, `status peers`, `status outbox`,
  `status queues`, and `status dlq` commands report persisted job/task state,
  retry and lease counts, last-known peers, pending coordinator outbox rows,
  RabbitMQ queue depths, and DLQ inspection summaries.
- `scheduler_ingress_retry_requested reason=mailbox_full` records the bounded
  RabbitMQ retry disposition when the scheduler mailbox is full.
- `scheduler_ingress_stopped action=broker_requeue_on_transport_close` records
  a delivery that reached the closed coordinator ingress gate and was
  deliberately left unsettled for RabbitMQ ownership recovery.
- `scheduler_overload_started`, `scheduler_overload_changed`, and
  `scheduler_overload_recovered` expose the immutable process-local overload
  projection. Active reasons use stable bounded enum values:
  `TASK_RESULT_RESERVE_CAPACITY`, `SUBMISSION_MAILBOX_CAPACITY`,
  `MAX_PENDING_OUTBOX_ROWS`, `MAX_ACTIVE_JOBS`, and `MAX_ACTIVE_TASKS`.
  Events include primary reason, configured maximum, observed value, the full
  bounded reason summary, fixed job-submit prefetch, and outbox-observation
  health; they contain no job IDs.
- `rabbitmq_route_prefetch_applied` records the dedicated `JOB_SUBMIT`
  consumer's route, prefetch `1`, and explicit stable consumer tag.
- `scheduler_overload_outbox_count_failed` and
  `rabbitmq_outbox_pressure_refresh_failed` distinguish failed overload
  observations from an observed zero. A failure cannot clear the last known
  outbox pressure.

Job lifecycle:

- `job_started` records accepted scheduler jobs with the common event envelope,
  `job_id`, `task_type`, requester, task count, and the plugin's `retry_safety`
  declaration.
- `job_submission_replayed` records an exact owner/request-hash replay with job
  ID, type, requester route, and durable job status. It does not imply another
  job/task creation transition.
- `job_submission_conflict` distinguishes `REQUEST_CONFLICT`,
  `OWNER_CONFLICT`, and `LEGACY_CONFLICT` without logging the requester token or
  canonical request hash.
- `job_admission_rejected` identifies a pre-J0/T0 limit rejection with
  `job_id`, `task_type`, requester, typed `limit`, `configured_maximum`, and
  `observed_value`. Pending-outbox count read failure remains the distinct
  `job_start_failed` storage-failure path.
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
- `attempt_output_staged` is emitted by a conversion executor after the
  assignment-owned object is available. It carries `job_id`, `task_id`,
  `attempt_number`, `assignment_id`, `object_key`, `content_length`, and
  bounded `outcome=CREATED|REUSED`. This is a staging event, not evidence that
  SQLite committed the pointer.
- `task_result_stale_rejected` records an obsolete assignment generation that
  the authoritative store rejected, including the same-participant ABA case.
- `task_result_duplicate_ignored` records a repeated result for an assignment
  whose authoritative result was already committed. It is intentionally
  distinct from stale-assignment rejection.
- Each of those four events, plus dispatch, executor-failure, timeout,
  lease-expiry, and per-task participant-unavailability transitions, carries
  the complete correlation tuple as
  `job_id`, `task_id`, `attempt_number`, `assignment_id`, and `worker_id`.
  These are the structured-log spellings of the protocol/domain fields
  `jobId`, `taskId`, `attemptNumber`, `assignmentId`, and `workerId`; `workerId`
  identifies the participant while it acts in the executor role.
- `task_result_not_committed` is retained only for unknown-task and
  storage-failure dispositions, also with the complete correlation tuple.
  Duplicate, stale, and unknown broker results are acknowledged; storage
  failure receives bounded delayed retry through the scheduler's
  processing-failure path.
- `task_failed`, `task_timeout`, `task_lease_expired`, and
  `task_peer_unavailable` record failed attempts with the exact assignment
  tuple, `retry_count`, and `terminal_failure`. `task_failed` also includes the
  bounded `failure_classification`; participant-unavailability events use a
  stable `reason_code`.
- `payload_integrity_failure_detected` is emitted by the executor before it
  returns the permanent failure result. It includes the assignment correlation
  tuple, object key, mismatch type, expected/actual length, and
  expected/actual SHA-256.
- `payload_integrity_failure_committed` is emitted only after the coordinator
  durably closes the exact current attempt as terminal. It carries the complete
  assignment correlation tuple, `PERMANENT_PAYLOAD_INTEGRITY`, the error, and
  the current `taskflow_payload_integrity_failures_total` value.
- `orphan_output_gc_configured` records the safety window, fixed interval, and
  batch bound when the coordinator owns cleanup. `orphan_output_gc_disabled`
  gives the stable reason `configuration`, `invalid_configuration`, or
  `object_store_configuration` without logging credentials.
- `orphan_output_deleted` records the exact old attempt key only after SQLite
  classifies it `orphan_candidate` and idempotent deletion succeeds.
  `orphan_output_preserved` is debug-level evidence for `active` and
  `authoritative` classifications.
- `orphan_output_delete_deferred` records a typed object-store failure after
  schema-v13 retry state commits.
  `orphan_output_delete_failure_unrecorded` distinguishes the separate SQLite
  write failure, and `orphan_output_gc_retry_clear_failed` records a harmless
  repeated-delete requirement after the object side effect succeeded.
- `orphan_output_gc_deferred` records listing, classification-state, retry-state,
  or unexpected failure for a pass. `orphan_output_gc_batch` reports examined,
  deleted, active, authoritative, ignored/preserved, failed, store-unavailable,
  and configured-limit values. These bounded log fields retain exact per-pass
  evidence while `taskflow_orphan_outputs_total` exports the process-lifetime
  completed-deletion count.
- `task_lease_expired` records the exact assigned generation whose persisted
  lease expired before a result was accepted.
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
- `executor_capacity_snapshot_accepted`,
  `executor_capacity_snapshot_stale_ignored`, and
  `executor_capacity_protocol_incompatible` classify executor advertisements.
- `assignment_capacity_reserved` and `assignment_capacity_released` carry the
  exact job, task, attempt, assignment, worker, type, and unit-cost tuple; the
  release event also includes its authoritative reason/outcome.
- `coordinator_capacity_projection_invalid` disables further dispatch after an
  unexpected exact reservation-ledger mismatch; recovery requires coordinator
  restart.
- `scheduler_metrics` includes process-lifetime capacity snapshot,
  reservation, and projection-failure counters plus current active-reservation
  and reserved-unit gauges. Per-worker/type values stay in structured events
  rather than creating high-cardinality metric keys.

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
  prefetch, publisher-confirm timeout, dead-letter setting, retry-delay
  schedule, maximum delivery attempts, connection timeout, and recovery
  backoff bounds.
- `coordinator_broker_startup_waiting` and `peer_broker_startup_waiting`
  identify a process whose initial broker connection has not completed. The
  coordinator listener reports `503/STARTING` for both health routes until its
  scheduler loop is activated.
- `rabbitmq_initial_connection_retry_scheduled` records each failed initial
  attempt, elapsed time, next delay, endpoint, and exception type.
  `rabbitmq_initial_connection_ready` records the attempt count and elapsed
  time at readiness. `rabbitmq_initial_connection_failed_permanently`
  distinguishes authentication, protocol, or configuration failures, while
  `rabbitmq_initial_connection_stopped` identifies interrupted shutdown.
- `rabbitmq_connection_interrupted`,
  `rabbitmq_connection_recovery_retry_scheduled`,
  `rabbitmq_connection_recovery_started`,
  `rabbitmq_topology_recovery_started`, and
  `rabbitmq_connection_recovery_completed` expose an established connection's
  automatic recovery cycle and elapsed recovery time.
- `rabbitmq_connection_closed` distinguishes an application-initiated or
  otherwise non-recovering connection shutdown from an interrupted connection
  entering automatic recovery.
- `rabbitmq_topology_declared` records dead-letter exchange, dead-letter queue,
  final quarantine queue, retry-delay schedule, and maximum delivery attempts.
- `rabbitmq_dlq_topology_declared` records the DLQ, quarantine, and retry
  topology used by the operator command path.
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
- `rabbitmq_coordinator_shutdown_started` records consumer count and the
  scheduler drain bound. `rabbitmq_coordinator_shutdown_drain_timeout` records
  the forced interrupt/channel-close fallback.
- `rabbitmq_coordinator_shutdown_action_failed`,
  `rabbitmq_coordinator_consumer_cancel_failed`, and
  `rabbitmq_coordinator_shutdown_close_failed` identify a lifecycle component
  that did not stop cleanly.
- `rabbitmq_coordinator_shutdown_database_close_deferred` records the
  scheduler, peer-monitor, outbox-replayer, and transport stop outcomes when
  SQLite cannot be closed safely. `rabbitmq_coordinator_shutdown_completed`
  reports the same final booleans.
- `scheduler_message_validation_failed` and
  `scheduler_message_processing_failed` include stable `reason_code` and
  `disposition` fields. `scheduler_delivery_disposed` records every non-success
  scheduler settlement, and `scheduler_message_settlement_failed` records a
  failed broker settlement attempt.
- `rabbitmq_delivery_settlement_failed` and
  `gui_rabbitmq_delivery_settlement_failed` record participant settlement
  failures with the requested disposition and reason code.
- `task_result_publish_failed` and `rabbitmq_heartbeat_failed` record peer-side
  RabbitMQ publish failures.
- `rabbitmq_delivery_decode_failed` records malformed broker input with
  `reason_code` and `disposition=REJECT_INVALID`. With dead-lettering enabled,
  RabbitMQ routes the rejected delivery to the configured dead-letter queue.
- `rabbitmq_delivery_handler_failed` records the classified `reason_code` and
  one of the typed failure dispositions plus current and maximum delivery
  attempts. Participant assignment, execution, and result-publication failure
  events expose the same classification fields.
- `rabbitmq_delivery_retry_scheduled` records original routing key, stable
  reason code, disposition, current/next/max delivery attempt, delay, and retry
  queue. `rabbitmq_delivery_quarantined` records the same failure identity and
  terminal quarantine queue.
- `rabbitmq_delivery_retry_publish_failed`,
  `rabbitmq_delivery_quarantine_publish_failed`,
  `rabbitmq_settlement_publish_unroutable`,
  `rabbitmq_settlement_publish_not_confirmed`, and
  `rabbitmq_settlement_publish_confirm_timeout` record a retry/quarantine
  handoff that could not be confirmed. These paths reject the source without
  immediate requeue.
- `rabbitmq_dlq_redriven`, `rabbitmq_dlq_quarantined`, and
  `rabbitmq_dlq_discarded` record explicit operator decisions on DLQ entries.
- `rabbitmq_dlq_redrive_rejected status=not_redrivable`,
  `rabbitmq_dlq_redrive_deferred`, `rabbitmq_dlq_quarantine_deferred`,
  `rabbitmq_dlq_publish_unroutable`, `rabbitmq_dlq_publish_not_confirmed`,
  `rabbitmq_dlq_publish_confirm_timeout`, and
  `rabbitmq_dlq_publish_returned_unmatched` record DLQ workflow failures or
  deferrals.
- RabbitMQ queue status includes each configured retry queue and the final
  quarantine queue. DLQ/quarantine message inspection exposes
  `deliveryAttempt`, `failureReason`, and `failureDisposition`.

## Current Limits

Current observability combines structured logs, command-line status inspection,
and the bounded-label Prometheus endpoint above. Operators can scrape current
coordinator metrics, grep logs for exact transition evidence, or run the
coordinator status command against SQLite/RabbitMQ state. The exporter does not
provide metric exemplars; assignment correlation remains on structured log
events.

Coordinator scheduler events always include `timestamp`,
`coordinator_instance_id`, `outcome`, and `failure_reason_code`; applicable
job/task/assignment fields follow the normative
[event schema](OBSERVABILITY.md).

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
headers. The exporter aggregates observed broker redeliveries and confirmed
automatic quarantine handoffs, but TaskFlow still does not provide promoted
lease dashboards, outbox dashboards, a DLQ dashboard, or a durable per-message
DLQ analytics store. The status command is a terminal inspection surface, not a
dashboard contract. Those remaining behaviors are deferred in the recovery and
RabbitMQ scope documents.

## Operational Endpoint Limits

The coordinator endpoint is an operational surface, not an embedded
time-series database or authenticated management API. It does not persist
counter or histogram state across restarts, retain samples, evaluate alerts,
authenticate clients, terminate TLS, or ship a dashboard. Deployments that
expose it beyond loopback must supply their own network boundary, TLS, and
monitoring backend.

The endpoint deliberately exposes aggregates only. Exact job, task,
assignment, and worker investigations use the structured event schema,
SQLite attempt history, and status commands. Adding any label requires an
explicit bounded-cardinality review and a test update; entity identifiers are
prohibited as labels.

## Public Claim Rule

Public docs may describe the coordinator's bounded-label Prometheus endpoint,
structured logs, scheduler snapshots, status commands, SQLite task-attempt
audit rows, SQLite task lease fields, and SQLite coordinator outbox rows. Avoid
claiming built-in dashboards, alerting, tracing, promoted lease timelines,
outbox dashboards, or DLQ dashboards until those systems exist and are tested.
