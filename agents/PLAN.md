# TaskFlow Active Plan

## Active task

- Queue ID: TF-0709
- Status: Complete
- Goal: add one clean-tree, opt-in overload experiment that submits new work
  faster than the coordinator can process until configured limits activate,
  then publish exact heap, admission, progress, recovery, mailbox, broker
  queue, and durable outbox evidence in `docs/reports/overload.md`.
- Invariants protected: durable preservation of accepted work (I1),
  generation-fenced single authoritative completion (I2/I3), monotonic
  terminal state (I4), durable replayable outbound intent (I5), duplicate and
  redelivery tolerance (I6), bounded coordinator memory and work queues (I7),
  and result/expiry/admission progress after pressure falls under I10's stated
  assumptions.
- Failure mode: sustained `JOB_SUBMIT` pressure can otherwise hide unbounded
  heap retention, silently lose accepted work, omit typed overload responses,
  exclude accepted `TASK_RESULT` or lease-expiry processing, obscure mailbox
  and durable-outbox pressure, or require a coordinator restart before new
  admission resumes.
- Expected files/modules: bounded experiment configuration and metric
  calculations plus an opt-in coordinator experiment in
  `taskflow-coordinator` test sources; a clean-tree PowerShell verifier;
  ignored raw evidence under `target/overload/`;
  `docs/reports/overload.md`; and the active handoff/evidence files.
- Durable-state changes: none. The harness uses a disposable schema-v14
  SQLite database through production `DatabaseManager`, and independently
  audits accepted jobs/tasks, attempt generations, terminal states, and
  broker-outbox rows before treating the run as evidence.
- Protocol changes: none. The experiment uses current protocol-v2
  `JOB_SUBMIT`, `TASK_ASSIGN`, `TASK_RESULT`, and typed
  `JOB_RESULT.admissionRejection` messages through the supported RabbitMQ
  transport.
- Tests required: configuration bounds and fixed report-grade constants;
  submission/mailbox saturation; exact typed limit responses; no unexplained
  accepted, rejected, or broker-owned submission; accepted result commitment
  and lease expiry while submission pressure remains; retained-heap plateau;
  observable configured mailbox and outbox thresholds; confirmed outbox
  replay; durable completion of every accepted job; fresh admission after
  pressure clears without restart; raw-property completeness/checksums;
  focused/live tests; full Maven and diff gates.
- Documentation required: `docs/reports/overload.md` with tested commit,
  command, hardware/JVM/container versions, fixed workload and thresholds,
  metric boundaries, exact results, raw links/checksums, durable audits,
  bottlenecks, and honest limitations. Update guarantees/failure-model
  evidence only where this experiment adds direct proof.
- Known non-goals: new limits or adaptive throttling, changing public defaults,
  a target RPS or production sizing claim, multi-host or multi-coordinator
  overload, clustered RabbitMQ, native-plugin or object-store load, arbitrary
  payload distributions, GC-latency guarantees, and exactly-once plugin side
  effects.

## Smallest design and abstraction questions

1. Keep the experiment opt-in and outside Surefire's default naming patterns,
   matching the Phase 7 correctness, scaling, and recovery harnesses. Normal
   CI gets deterministic configuration/metric tests; the report command owns
   Docker and the sustained workload.
2. Run the production scheduler, priority mailbox, admission policy,
   schema-v14 SQLite store, RabbitMQ transport, protocol-v2 envelopes, and
   outbox replayer. A test-only gated `BrokerOutboxPublisher` controls one
   existing publication boundary; it does not introduce a production
   abstraction.
3. Hold the first flood assignment publication long enough to fill the
   configured ordinary lane and leave submissions broker-owned. Publish one
   already accepted result into the fixed result reserve and allow one
   already accepted lease to become due before releasing the scheduler.
4. While the scheduler is held, seed exactly the configured pending-outbox
   threshold through `DatabaseManager`; the already accepted gated assignment
   is the documented one-row overshoot because the threshold stops new
   admission rather than already accepted durable intent. Release publication,
   defer replay, and require every flood submission to receive the existing
   typed outbox-limit response.
5. Define retained-heap plateau before measurement as the span of the final
   three post-GC wave samples under unchanged fixed pressure. Report both the
   configured fixed heap and observed sample range; do not interpret the
   result as production GC guidance.
6. Observe scheduler mailbox depth/capacity directly, broker queue depth from
   the real container, and pending outbox from SQLite. The outbox admission
   threshold is not misrepresented as a universal hard row cap: already
   accepted retries/finalization may add durable intent after new admission
   stops.
7. Keep current assignment/result publication live while the seeded backlog
   holds new admission closed. After the fixed waves, replay every pending row
   through the production replayer, audit all accepted jobs terminal and all
   outbox rows published, then submit and complete one fresh job without
   restarting the scheduler.

## Ordered implementation and verification

1. Add bounded overload experiment configuration and metric calculations with
   deterministic normal tests.
2. Add the opt-in real-RabbitMQ/SQLite experiment and its narrow test-only
   publication gate, raw metrics, database, broker-depth, response, and
   assignment evidence.
3. Add the clean-tree PowerShell wrapper to invoke the fixed-heap report run,
   validate exact counts/bounds/integrity, and generate a checksum manifest.
4. Run focused small calibrations and correct harness-only defects without
   publishing calibration values.
5. Run focused tests, affected live RabbitMQ suites, full Maven, and diff
   audits; commit and push the coherent harness and verify the remote hash.
6. Run the clean report-grade command against that exact pushed harness
   revision and independently audit the raw properties, SQLite state, line
   counts, broker drain, and checksums.
7. Write `docs/reports/overload.md` from measured evidence without inventing a
   target RPS or broader guarantee.
8. Run report-link/reference checks, required live selectors,
   `git diff --check`, and the full Maven reactor; commit/push the report,
   verify remote hash and CI, then update queue/status/log completion evidence.

## Report-grade evidence captured

- Harness revision `85d431e96cbfd2f7b05ad1f49bc7dd1df6e1a6cd`
  was clean, pushed, and matched `task-flow-2/main` before measurement.
- `.\scripts\verify-overload.ps1` passed in 282.143 seconds with a real
  `rabbitmq:3.13-management` container and schema-v14 SQLite.
- Exact accounting was 1,004 submissions: 4 accepted/completed and 1,000
  unique typed `MAX_PENDING_OUTBOX_ROWS` rejections.
- Ordinary/result mailbox high water was 1/1 at capacities 1/1, broker-ready
  submission high water was 32, pending-outbox high water was the documented
  17 rows at the 16-row new-admission threshold, and final broker/outbox
  depths were zero.
- Four current results committed while four expired generations were
  retry-scheduled; all four jobs/tasks became durable `COMPLETED`, all 28
  outbox rows were published, and a fresh post-pressure job completed with
  restart count zero.
- The final-three retained-heap samples spanned 46,048 bytes with a maximum
  of 17,803,432 bytes under the fixed 256 MiB SerialGC experiment JVM.
- Independent response/assignment cardinality, read-only SQLite integrity and
  attempt audit, and all nine manifest checksums matched. The checksum
  manifest SHA-256 is
  `334848021235561242c2163bdb354489a29f3623122bd07a896f8ca5b929740e`.
- Report links and diff checks passed; the prescribed transport and
  coordinator RabbitMQ live selectors passed 8/8 and 10/10, and the complete
  25-module Maven reactor passed after report authoring.
- Report commit `6c6d18bc4c07478cb3c0eb5b6d3f29f5b20a3154`
  was pushed to `task-flow-2/main`, and `git ls-remote` matched the local hash.
- GitHub Actions run `30433722421` completed successfully for that exact
  report hash: both `Maven Tests` and `RabbitMQ Integration Tests` passed.
- TF-0709 completion is ready for its final pushed completion record and
  local queue/status/log handoff update.
