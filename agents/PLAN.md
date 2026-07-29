# TaskFlow Active Plan

## Active task

- Queue ID: TF-0708
- Status: Complete
- Goal: add one clean-tree, opt-in recovery experiment that measures worker
  failure detection, lease-expiry-to-reassignment, coordinator restart
  recovery, RabbitMQ restart recovery, reconstruction of 10,000 and 100,000
  persisted tasks, coordinator outbox replay throughput, and object-orphan
  cleanup rate, then publish the exact evidence in
  `docs/reports/recovery.md`.
- Invariants protected: durable accepted-work recovery (I1), single and
  generation-fenced authoritative results during recovery (I2/I3), monotonic
  terminal state (I4), durable replayable outbound intent (I5), duplicate
  tolerance across reconnect/replay (I6), bounded recovery/outbox/cleanup
  batches (I7), object-output ownership and deletion safety (I9), and
  eventual progress after failures stop under I10's stated assumptions. The
  task measures existing mechanisms; it does not replace their authorities.
- Failure mode: recovery claims can be misleading if they time only database
  opening, omit reconstruction and authority audits, treat broker process
  startup as application recovery, publish outbox rows without settlement, or
  delete synthetic files without exercising MinIO ownership classification.
  Unbounded fixtures or mixed warm/cold data can also make the measurements
  irreproducible.
- Expected files/modules: bounded experiment configuration and calculation
  tests plus an opt-in recovery experiment in `taskflow-coordinator` test
  sources; narrow package-owned test probes only where package-private
  production batch methods must be timed directly; a clean-tree PowerShell
  verifier; ignored raw output under `target/recovery/`;
  `docs/reports/recovery.md`; and the active handoff/evidence files.
- Durable-state changes: none. The harness creates disposable schema-v14
  SQLite databases through `DatabaseManager`, seeds accepted jobs/tasks and
  pending broker outbox rows through production APIs, audits their state
  before and after recovery, and discards only ignored experiment output
  during its own fresh run setup.
- Protocol changes: none. RabbitMQ scenarios use current protocol-v2 messages,
  durable coordinator outbox envelopes, persistent confirmed publication, and
  the existing reconnect policy.
- Tests required: configuration bounds and report-grade constants; exact
  10,000/100,000 task fixture and reconstruction counts; unchanged accepted
  state and schema/integrity audits; attempt-generation and lease-expiry
  reassignment assertions; real RabbitMQ stop/start plus exact pending-outbox
  replay/delivery counts; real MinIO orphan creation, bounded deletion, and
  surviving-object audit; raw-property completeness/checksums; focused
  calibration; full Maven and diff gates.
- Documentation required: `docs/reports/recovery.md` with tested commit,
  command, hardware/JVM/container versions, metric boundaries, fixture sizes,
  warm-up/repetition policy, exact results, raw links/checksums, integrity
  audits, and honest limitations. Update guarantees/failure-model evidence
  only where the experiment adds a directly supported quantitative claim.
- Known non-goals: changing recovery mechanisms or defaults, recovery SLOs,
  OS-process coordinator restart unless the harness explicitly performs one,
  worker compute correctness after a kill, RabbitMQ clustering or durable
  volume-loss recovery, MinIO clustering, production sizing, multi-host or
  multi-coordinator recovery, power-loss filesystem semantics, and
  exactly-once plugin side effects.

## Smallest design and abstraction questions

1. Keep the experiment opt-in and outside Surefire's default naming patterns,
   matching TF-0706/TF-0707. Normal CI gets deterministic configuration and
   metric-calculation tests; the report command owns Docker and the large
   fixtures.
2. Exercise the production `PeerLivenessMonitor` at the coordinator's
   90-second timeout for worker-failure detection. Record the monitor's fixed
   five-second scan quantization instead of presenting the result as precise
   process supervision.
3. Measure lease-expiry-to-reassignment with the production scheduler,
   schema-v14 SQLite state, a real clock, deterministic assignment IDs, and a
   configured short lease. The duration begins at the persisted lease
   deadline and ends when generation N+1 is emitted and durably visible.
4. Seed active jobs through `DatabaseManager.commitJobSubmission`, close the
   database to model a stopped coordinator, reopen it, and time
   `CoordinatorStartupRecovery.recoverPersistedJobs` plus exact reconstruction
   audit. Use the same task shape at 10,000 and 100,000 tasks and report
   seeding separately from recovery.
5. Use a real restarted RabbitMQ Testcontainers broker behind a stable
   Toxiproxy endpoint and the production
   `RabbitMqTransport`/`RabbitMqOutboxReplayer`. Rabbit recovery ends only when
   every precommitted pending row has been delivered and marked published;
   steady-state outbox throughput is measured separately so broker boot time
   is not conflated with replay rate.
6. Use a real MinIO container, the ServiceLoader-selected production
   `ObjectStore`, schema-v14 `DatabaseManager` authority classification, and
   production `OrphanOutputGc`. Keys represent old attempt outputs for unknown
   tasks, so they are valid orphan candidates; deletion is batch-bounded and
   the post-run listing must be empty.
7. Add only test-source package probes if direct batch timing cannot be
   observed through existing public metrics. They isolate no production
   dependency and expose no runtime API; a broader recovery framework or new
   production interface would be disproportionate.

## Ordered implementation and verification

1. Add the bounded recovery configuration/metrics types and normal default
   tests, including exact report-grade sizes and limits.
2. Add the opt-in experiment and narrow package probes for peer detection,
   lease reassignment, SQLite restart/reconstruction, real broker restart and
   outbox replay, and real MinIO orphan cleanup.
3. Add the clean-tree PowerShell wrapper to capture environment, invoke the
   report-grade run, validate raw artifacts/counts/integrity, generate
   checksums, and reject partial evidence.
4. Run focused configuration tests and small dirty-tree calibrations; correct
   harness-only defects without publishing calibration values.
5. Run the required focused/live checks, full Maven reactor, and diff audit;
   commit and push the coherent harness, then verify the remote hash.
6. Run the clean report-grade command against that exact pushed harness
   revision and independently audit every raw property, database/object/broker
   count, bound, and checksum.
7. Write `docs/reports/recovery.md` from the measured evidence, update only
   directly supported evidence links, and record limitations without adding an
   SLO.
8. Run link/method/reference checks, required RabbitMQ live selectors,
   `git diff --check`, and the full Maven reactor; commit/push the report,
   verify the remote hash and CI, then update queue/status/log completion
   evidence with TF-0709 still unstarted.

## Completion evidence

- Reproducible harness commit:
  `9b313fd2794039a23cf2623d21d1c67a4a058908`.
- Measured report commit:
  `c30b85fb068dfe6f2b9240bc935517c6b1d03669`.
- Both commits were pushed to `task-flow-2/main` and independently matched by
  `git ls-remote`.
- The clean `.\scripts\verify-recovery.ps1` report-grade run passed all fixed
  workloads, property/count/bound checks, seven SQLite integrity audits, and
  21 raw checksums. Independent `sqlite3` and checksum recalculation agreed.
- Focused configuration tests, both prescribed RabbitMQ live suites,
  the exact combined GitHub RabbitMQ selector, PowerShell parsing, local-link
  validation, `git diff --check`, and two final full Maven reactors passed.
- TF-0709 remains unstarted until this completion record is committed and
  pushed.
