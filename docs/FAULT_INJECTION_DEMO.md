# Fault-Injection Demo

This demo exercises one supported failure path: TCP peer disconnect retry.

It uses the scheduler test harness instead of launching desktop GUI or RabbitMQ
processes, so it is repeatable on a clean development machine and does not rely
on restart-lease recovery, outbox replay, or DLQ redrive.

## Scenario

The demo injects this sequence:

1. A `TEST_TASK` job is accepted by the scheduler.
2. The first task assignment goes to `peer-1`.
3. A `PeerDisconnectedMessage` with reason `tcp_disconnect` is injected for
   `peer-1`.
4. The scheduler releases the assigned task and immediately assigns the same
   task id to `peer-2`.
5. A stale success result from `peer-1` is ignored.
6. A success result from `peer-2` completes the job.

## Run

On Windows PowerShell:

```powershell
.\scripts\demo-tcp-peer-disconnect-retry.ps1
```

Equivalent Maven command:

```powershell
.\mvnw.cmd -pl taskflow-core -am "-Dtest=TaskSchedulerFailureTest#peerDisconnectReleasesAssignedTaskForImmediateRetryAndIgnoresStaleResult" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

## Expected Output

The Maven output should end with `BUILD SUCCESS`, and the script should print a
Surefire summary like:

```text
Surefire summary: Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
Expected behavior verified:
- first assignment goes to peer-1
- tcp_disconnect releases the assigned task
- retry assignment goes to peer-2 with the same task id
- stale result from peer-1 is ignored
- accepted result from peer-2 completes the job
Demo complete.
```

## Scope

This demo proves the implemented scheduler behavior for peer disconnect while a
task is assigned. It does not claim:

- coordinator restart recovery for assigned tasks;
- restart-lease ownership of in-flight work;
- RabbitMQ durable outbox replay;
- TaskFlow DLQ inspection or redrive.

Those separate areas remain documented in `docs/RECOVERY_SCOPE.md` and
`docs/RABBITMQ_SCOPE.md`.
