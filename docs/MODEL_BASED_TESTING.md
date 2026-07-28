# Bounded Model-Based Scheduler Proof

`TaskFlowModelPropertyTest` compares a small reference model with the real
`TaskScheduler` and a temporary SQLite database after every generated event.
The reference model is test-only: it does not reuse scheduler transition code,
persistence code, or transport state.

## System under test

Each sequence uses one three-task job and one unit of advertised executor
capacity. The harness composes:

- the production scheduler and capacity ledger;
- the production SQLite state store;
- the production startup recovery path;
- the production outbox replayer;
- an injected clock and deterministic UUID generator; and
- a bounded publisher fixture that makes the first publication attempt fail
  and the replay succeed.

The generator covers assignment, duplicate assignment, successful result,
duplicate result, stale result, retryable failure, lease expiry, executor
disconnect, coordinator restart/reload, and outbox replay. A mandatory prefix
gives every seed meaningful coverage of all ten event families before the
remaining generated steps. Once generated failures stop, the driver supplies
valid current results until the job is terminal, then repeats duplicate,
stale, replay, and restart events against the terminal state.

## Properties checked after every event

- each task has at most one `SUCCEEDED` attempt;
- terminal job and task states never regress;
- durable attempt numbers never decrease and attempt identities are unique;
- stale assignment results receive the duplicate/stale disposition and do not
  change the reference or durable state;
- the capacity-one ledger never becomes negative and equals the durable
  assigned-task count;
- every observed committed outbox intent is either successfully published or
  still present in SQLite as pending; and
- restart hydration matches both the reference model and the durable task,
  attempt, assignment, retry, and result state.

Every assertion failure reports the seed in decimal and hexadecimal followed
by the complete event trace.

## Bounded CI set

The push-fast CI job runs the focused class explicitly in addition to the full
reactor. The generated method uses exactly 32 random steps for each of these
eight checked-in seeds:

```text
3520704001
3520704017
3520704033
3520704049
3520704065
3520704081
3520704097
3520704113
```

The duplicate-focused method uses seed `3520704129`. Other explicit bounds are
a 32-entry scheduler mailbox, a 256-entry publication queue, and at most 128
publication observations per event. Lease progress uses the injected clock;
the proof contains no wall-clock sleep.

Run the same CI evidence locally:

```powershell
.\mvnw.cmd --batch-mode --no-transfer-progress -pl taskflow-coordinator -am "-Dtest=TaskFlowModelPropertyTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

## Limits

This is bounded deterministic model evidence, not exhaustive state-space
exploration. It covers one job, three independent tasks, a capacity-one
executor projection, in-process coordinator restarts, SQLite reopen, and
outbox publication/replay decisions. It does not kill an operating-system
process, run RabbitMQ or MinIO, inject network partitions, execute native
plugins, prove retry exhaustion, or shrink a failing sequence automatically.
The separate [process crash-window matrix](CRASH_WINDOW_MATRIX.md) supplies
the OS-process/RabbitMQ/MinIO evidence. The separate
[100,000-task correctness-chaos experiment](reports/correctness-chaos.md)
supplies seeded mixed-failure evidence with a real restarted RabbitMQ container
and in-JVM coordinator/executor component restarts.
