# TaskFlow Active Plan

## Active task

- Queue ID: TF-0710
- Status: Local verification complete; awaiting commit, push, and remote CI
- Goal: split repository automation into explicit push-fast,
  push-integration, scheduled-chaos, and manual-benchmark evidence tiers
  without moving long nondeterministic report runs into the push gate.
- Invariants protected: all I1-I10 evidence remains reachable in an
  appropriate tier; in particular, push-fast retains the deterministic state
  machine/model and architecture guards, push-integration retains adapter
  durability/delivery contracts, and scheduled failures retain exact seeds
  and raw logs for replay.
- Failure mode: the current two-job workflow runs a broad default reactor and
  a mixed RabbitMQ selector only on `main` pushes. It does not make the
  compilation/unit/architecture boundary explicit, does not separately gate
  SQLite and MinIO contracts, mixes broker fault recovery into ordinary
  integration, provides no scheduled larger-seed/fault tier, and provides no
  deliberate manual entry point for report-grade correctness/scaling runs.
- Expected files/modules: `.github/workflows/ci.yml`; new scheduled-chaos and
  manual-benchmark workflow files; bounded model-test run configuration under
  `taskflow-coordinator` test sources; `docs/CI_EVIDENCE_TIERS.md`;
  model/RabbitMQ/README evidence links; and active handoff files.
- Durable-state changes: none. SQLite remains schema v14 and the task changes
  only test-harness configuration and automation.
- Protocol/runtime changes: none. No wire message, routing topology, runtime
  dependency, admission default, scheduler behavior, or supported guarantee
  changes.
- Tests required: default and expanded model-run configuration bounds;
  unchanged push-fast seed behavior; a locally executable push-fast selector;
  SQLite migration/persistence contract; real MinIO contract; managed and
  live RabbitMQ integration selectors; reduced scheduled correctness-chaos
  calibration with explicit seed plus broker/coordinator/executor faults;
  workflow YAML parsing/structural audit; full Maven and diff gates.
- Documentation required: one tier matrix naming trigger, runner,
  selectors/workload, expected duration class, artifacts/retention,
  reproduction command, and limitations. Update existing model/RabbitMQ/README
  claims so broker recovery is not still described as an ordinary push job.
- Known non-goals: changing production behavior, running 100,000-task or
  scaling reports on every push, claiming GitHub-hosted performance numbers
  are comparable to the report host, adding JavaFX window-driving CI, adding
  an external CI service, or silently requiring a self-hosted benchmark
  runner that is not documented.

## Smallest design and tier decisions

1. Keep `.github/workflows/ci.yml` as the required push workflow, but remove
   its `main`-only filter so every branch push and pull request receives both
   named push tiers.
2. Push-fast compiles all production/test sources, runs every default test
   except explicitly infrastructure/fault/report classes, then runs the exact
   architecture classes and bounded `TaskFlowModelPropertyTest` seed set as
   separate visible steps. Experiments remain outside Surefire defaults.
3. Push-integration runs the SQLite schema/migration and reusable persistence
   contracts, the real Testcontainers MinIO contract, all focused non-chaos
   integration/live tests, the reusable RabbitMQ/Testcontainers contract, and
   retained command-line/GUI broker-adapter tests. The existing service
   RabbitMQ stays pinned and health checked.
4. Move `RabbitMqBrokerRecoveryIntegrationTest` and
   `CrashWindowMatrixTest` to scheduled chaos. Add one reduced, non-report
   `CorrectnessChaosExperiment` run so the scheduled tier actually includes
   broker, coordinator-component, and executor-component failure injection
   without rerunning the manual 100,000-task report.
5. Make the model harness accept bounded explicit seed-start/count/step
   properties. Defaults remain the checked-in eight seeds × 32 generated
   steps; scheduled chaos uses more seeds and steps. Every failure continues
   to print decimal/hex seed and trace.
6. Scheduled chaos derives a reproducible decimal seed, records commit,
   seed, bounds, and event source before execution, and uploads that file,
   experiment raw evidence, and all Surefire reports with `if: always()`.
7. Manual benchmarks use `workflow_dispatch` choices on an explicitly
   documented `[self-hosted, windows, x64, taskflow-benchmark]` runner because
   the clean-tree report verifiers and machine-profile evidence are
   Windows/PowerShell oriented. They invoke the unchanged report-grade
   correctness and scaling verifiers and retain raw bundles. An absent runner
   means the requested manual job queues; it does not weaken push CI.
8. Use only `contents: read`, official checkout/setup-java/upload-artifact
   actions, bounded timeouts, unique artifact names, and explicit retention.
   Do not use path filters that could leave a required check pending.

## Ordered implementation and verification

1. Add bounded model-run configuration with tests, preserving the exact
   existing push-fast defaults and seed diagnostics.
2. Rewrite `ci.yml` into visible push-fast and push-integration jobs and
   retain the RabbitMQ service health boundary and participant adapter tests.
3. Add weekly/manual scheduled-chaos automation with derived/explicit seed,
   reduced failure workload, expanded model sequences, and always-uploaded
   logs/raw evidence.
4. Add the manual report-grade correctness/scaling dispatcher for the labeled
   benchmark runner, without triggering it during ordinary validation.
5. Document the four tiers and update stale README, model, RabbitMQ, failure
   model, and execution-guarantee references.
6. Parse and structurally audit every workflow; run configuration tests,
   push-fast locally, the complete push-integration selectors against the
   required broker/Docker prerequisites, and a reduced scheduled-chaos
   calibration.
7. Run the full Maven reactor, documentation/link checks, dependency/diff
   audits, and workflow selector/cardinality checks.
8. Commit and push the coherent tier split, verify the exact remote hash and
   new GitHub push-fast/push-integration jobs, then update queue/status/log
   completion evidence. Scheduled and manual triggers are verified
   structurally and by local equivalent commands; do not claim a scheduled or
   self-hosted run that was not actually dispatched.

## Local verification evidence

- Official Docker `actionlint` passed all three workflows with the declared
  custom self-hosted label; both PowerShell verifier files parse, and their
  workflow parameter contracts match.
- The exact push-fast compilation, unit/component, architecture, and default
  model commands passed.
- The exact push-integration selector passed SQLite 81/81, MinIO 16/16,
  RabbitMQ contract/live 17/17, coordinator live/integration 14/14,
  command-line participant 14/14, and GUI adapter 16/16 tests. The repository
  broker was returned to stopped.
- The expanded 32-seed x 128-step model run passed. The scheduled managed
  broker/process selector passed 10/10. The exact 10,000-task seeded
  mixed-failure workload passed with 10,000 completions, one broker restart,
  one coordinator-component restart, two executor terminations, 100 delayed
  results, and no pending outbox row at completion.
- A separate 100-task calibration proved the scheduled workflow's absolute
  output path writes its retained evidence under the repository-root artifact
  tree.
- The focused model/configuration suite passed 5/5; the final complete
  25-module `.\mvnw.cmd test` reactor passed; 169 touched-document local links,
  workflow lint, PowerShell parsing, `git diff --check`, and container cleanup
  passed.
- The manual benchmark workflow was not dispatched because no matching
  self-hosted runner is assumed. Its triggers, labels, scripts, parameters,
  timeouts, and artifact paths were verified structurally.
