# Coordinator Health Contract

The RabbitMQ coordinator exposes liveness and readiness for new jobs on the
same bounded operational HTTP listener as Prometheus metrics:

```text
GET /health/live
GET /health/ready
GET /metrics
```

The default listener is `127.0.0.1:9464`. It starts before the coordinator's
interruptible RabbitMQ connection loop, so a broker-offline startup can be
observed without pretending that the process is ready.

## Liveness

Liveness answers only whether the coordinator's process loop is running. It
does not call SQLite, inspect the outbox, or wait for RabbitMQ.

| HTTP | `state` | Meaning |
|---:|---|---|
| `503` | `STARTING` | The process exists, but the scheduler loop has not been activated yet. |
| `200` | `UP` | The scheduler thread is alive. Dependency loss does not kill liveness. |
| `503` | `DOWN` | The activated scheduler thread is no longer running. |

After activation, a broker outage changes readiness while
`GET /health/live` continues to return `200`. During initial broker connection,
the listener is observable but liveness remains `503/STARTING` because the
scheduler loop does not yet exist.

## Readiness for new jobs

`GET /health/ready` returns `200` only in state `READY`. It returns `503` in
`STARTING`, `DEGRADED`, or `DOWN`.

Readiness is recomputed on every request and before production admission of a
genuinely new job. It requires all of the following:

- the scheduler process loop is alive;
- SQLite accepts a write transaction;
- the RabbitMQ connection, publisher channel, and currently owned consumer
  channels are usable and not inside automatic recovery;
- the pending coordinator outbox count is observable and strictly below
  `TASKFLOW_MAX_PENDING_OUTBOX_ROWS`;
- active-job and active-task hard admission limits are not already blocking
  new work;
- the scheduler's exact capacity projection is valid.

The JSON body contains `state`, `live`, `ready`, `degraded`, component
booleans, the observed/current outbox values, and a bounded `reasons` list. No
job, task, assignment, requester, or worker identity appears.

The reason vocabulary is fixed:

| Reason | Meaning |
|---|---|
| `STARTING` | Runtime dependencies and scheduler loop are not activated yet. |
| `PROCESS_LOOP_NOT_RUNNING` | The activated scheduler thread stopped. |
| `SQLITE_NOT_WRITABLE` | The authoritative store did not accept the rollback-only write probe. |
| `BROKER_NOT_USABLE` | RabbitMQ connection/channel recovery is incomplete or the transport is closed. |
| `OUTBOX_OBSERVATION_UNAVAILABLE` | SQLite could not provide the pending-outbox aggregate. |
| `OUTBOX_THRESHOLD_REACHED` | Pending rows are at or above the configured admission threshold. |
| `SCHEDULER_ADMISSION_BLOCKED` | The active-job or active-task hard limit is currently reached. |
| `SCHEDULER_TERMINAL_OVERLOAD` | Exact worker-capacity projection was invalidated by an identity mismatch; restart/recovery reconstruction is required. |
| `READINESS_PROBE_FAILED` | An unexpected component probe failure prevented a complete readiness decision. |

Transient mailbox and task-result-reserve saturation remain the bounded broker
backpressure contract in [Backpressure scope](BACKPRESSURE_SCOPE.md). They do
not become a terminal scheduler-health condition.

## Degraded admission and recovery

`DEGRADED` means the process loop is alive and can continue handling or
recovering already accepted work, but it must not durably accept a new job.
The scheduler classifies an exact requester/job/request-hash replay before this
gate, so an accepted job can still return its current status or terminal
result. A genuinely new submission that reaches the scheduler while degraded
receives the existing unsuccessful `JOB_RESULT` response and creates no J0/T0
job/task projection. Broker loss normally prevents that delivery until
recovery; unsettled deliveries remain governed by the existing RabbitMQ
ownership rules.

Readiness failures are not latched. Restoring RabbitMQ, restoring SQLite write
capability, draining the outbox below its threshold, or clearing an active
admission limit makes the next probe ready automatically. A terminal
capacity-projection invalidation deliberately remains unready until restart
reconstructs that process-local projection from authoritative state.

SQLite writability is tested by updating the singleton `schema_version` row to
its existing value inside a transaction that is always rolled back. The probe
therefore exercises the real writer/locking path without committing a schema,
job, task, attempt, outbox, peer, or object-lifecycle mutation.

## Listener configuration and limits

The shared listener retains the existing configuration:

| Environment | System property | Default |
|---|---|---:|
| `TASKFLOW_METRICS_ENABLED` | `taskflow.metricsEnabled` | `true` |
| `TASKFLOW_METRICS_HOST` | `taskflow.metricsHost` | `127.0.0.1` |
| `TASKFLOW_METRICS_PORT` | `taskflow.metricsPort` | `9464` |

Despite the compatibility setting names, these values configure the whole
operational listener. Setting `TASKFLOW_METRICS_ENABLED=false` creates no
listener, so metrics and both health routes are unavailable. During startup,
health routes are available immediately while `/metrics` returns `503` until
the runtime collector is installed.

The listener has one daemon executor thread, supports `GET` only, returns
`Cache-Control: no-store`, and provides no authentication or TLS. Loopback is
the safe default. Any deployment that binds beyond loopback must supply its
own network access control and TLS termination.

## Evidence

- [`CoordinatorHealthTest`](../taskflow-coordinator/src/test/java/server/health/CoordinatorHealthTest.java)
  fixes startup, ready, degraded, recovered, down, and probe-failure state
  mapping.
- [`CoordinatorReadinessProbeTest#actualSqliteOutboxAndSchedulerSignalsRecoverWithoutRestart`](../taskflow-coordinator/src/test/java/server/health/CoordinatorReadinessProbeTest.java)
  exercises real SQLite writability/outbox observation and every bounded
  readiness signal.
- [`DatabaseManagerTest#writabilityProbeRollsBackAndRecoversAfterInjectedWriteFailure`](../taskflow-persistence-sqlite/src/test/java/server/db/DatabaseManagerTest.java)
  proves injected write failure, no durable probe mutation, and recovery.
- [`RabbitMqTransportHealthTest#recoveryCallbacksRemoveAndRestoreConnectionUsability`](../taskflow-transport-rabbitmq/src/test/java/transport/rabbitmq/RabbitMqTransportHealthTest.java)
  fixes transport recovery callback semantics.
- [`CoordinatorOperationsEndpointTest`](../taskflow-coordinator/src/test/java/server/metrics/CoordinatorOperationsEndpointTest.java)
  fixes routes, status codes, content types, startup behavior, methods, and
  lifecycle.
- [`SchedulerAdmissionTest#degradedCoordinatorRejectsNewJobBeforeInMemoryAcceptance`](../taskflow-core/src/test/java/server/scheduler/SchedulerAdmissionTest.java)
  and
  [`SchedulerAdmissionTest#degradedCoordinatorStillServesExactAcceptedSubmissionReplay`](../taskflow-core/src/test/java/server/scheduler/SchedulerAdmissionTest.java)
  prove the pre-acceptance boundary and accepted-work replay.
- [`RabbitMqBrokerRecoveryIntegrationTest#unavailableStartupAndActiveBrokerRestartRecoverOutboxConsumersAndFencing`](../taskflow-coordinator/src/test/java/server/rabbitmq/RabbitMqBrokerRecoveryIntegrationTest.java)
  proves live-but-unready broker outage and automatic readiness recovery
  against a managed real broker while accepted work remains authoritative.
