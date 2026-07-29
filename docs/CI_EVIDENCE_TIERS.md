# CI Evidence Tiers

TaskFlow separates evidence by cost, infrastructure, and diagnostic value.
This keeps deterministic feedback on every pushed branch while retaining
adapter contracts, seeded fault evidence, and report-grade experiments without
turning a benchmark into a merge-time gate.

The workflows follow GitHub's official
[workflow syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax),
[artifact retention](https://docs.github.com/en/actions/tutorials/store-and-share-data),
and
[self-hosted runner label](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/use-in-a-workflow)
contracts.

## Tier matrix

| Tier | Trigger | Runner and bound | Evidence | Retained output |
|---|---|---|---|---|
| Push-fast | every branch push, pull request, or manual dispatch of `ci.yml` | GitHub-hosted Ubuntu; 30 minutes | full production/test compilation; non-infrastructure unit/component tests; exact architecture classes; default eight-seed x 32-step scheduler model; whitespace | Surefire reports on failure, 14 days |
| Push-integration | every branch push, pull request, or manual dispatch of `ci.yml` | GitHub-hosted Ubuntu with a pinned RabbitMQ service and Docker; 45 minutes | SQLite migrations and reusable persistence contract; real MinIO contract; reusable RabbitMQ/Testcontainers contract; focused live coordinator/transport, participant, and GUI service-adapter integration | Surefire reports on every run, 14 days |
| Scheduled chaos | Mondays at 04:17 UTC or manual dispatch of `scheduled-chaos.yml` | GitHub-hosted Ubuntu with Docker; 120 minutes | 32 seeds x 128 generated model steps; managed coordinator broker-recovery and process-kill matrices; reduced 10,000-task mixed duplicate/delay/executor-termination/broker-restart/coordinator-restart run | commit/seed/bounds, mixed-chaos raw files, and every Surefire report even on failure, 30 days |
| Manual benchmark | explicit dispatch of `manual-benchmarks.yml` | labeled `[self-hosted, windows, x64, taskflow-benchmark]` runner; 360 minutes | unchanged report-grade 100,000-task correctness-chaos verifier, scaling verifier, or both | complete raw report bundles and Surefire reports, 90 days |

The GitHub repository must configure branch protection against the visible
`Push-fast` and `Push-integration` job names if they are required checks.
Workflow YAML cannot change repository branch-protection settings.

## Push-fast

Push-fast compiles all reactor production and test sources before executing
tests. Its unit selector excludes only named infrastructure/fault/report
families owned by later tiers. Plugin contracts and in-memory object-store
contracts stay in this tier; the SQLite, real MinIO, and real RabbitMQ
bindings do not.

Architecture and model tests are separate workflow steps so a failure is
visible as an architecture or state-model failure rather than hidden inside
one broad reactor result. The model defaults remain the checked-in seeds in
[`MODEL_BASED_TESTING.md`](MODEL_BASED_TESTING.md).

Local PowerShell equivalent:

```powershell
.\mvnw.cmd --batch-mode --no-transfer-progress -DskipTests test-compile
.\mvnw.cmd --batch-mode --no-transfer-progress `
  "-Dtest=!*IntegrationTest,!*LiveTest,!DatabaseManagerTest,!SqlitePersistenceContractTest,!MinioObjectStoreContractTest,!RabbitMqBrokerContractTest,!CrashWindowMatrixTest,!TaskFlowModelPropertyTest,!*ArchitectureTest,!*Experiment" `
  "-Dsurefire.failIfNoSpecifiedTests=false" test
.\mvnw.cmd --batch-mode --no-transfer-progress `
  "-Dtest=ObjectStoreArchitectureTest,SchedulerArchitectureTest,RabbitMqOnlyRuntimeArchitectureTest" `
  "-Dsurefire.failIfNoSpecifiedTests=false" test
.\mvnw.cmd --batch-mode --no-transfer-progress `
  -pl taskflow-coordinator -am `
  "-Dtest=TaskFlowModelPropertyTest" `
  "-Dsurefire.failIfNoSpecifiedTests=false" test
git diff-tree --check --no-commit-id --root -r HEAD
```

## Push-integration

Push-integration starts a `rabbitmq:3.13-management` service for the opt-in
live adapter suites. Testcontainers separately owns the reusable
RabbitMQ/Toxiproxy and MinIO instances. The selector includes:

- `DatabaseManagerTest` and `SqlitePersistenceContractTest`;
- `MinioObjectStoreContractTest`;
- `RabbitMqBrokerContractTest`, every non-chaos `*LiveTest` and
  `*IntegrationTest`, and the shutdown/acknowledgement paths;
- command-line `PeerNodeTest`; and
- `RabbitMqJobSubmissionClientTest` and
  `RabbitMqCoordinatorConnectionTest`.

`RabbitMqBrokerRecoveryIntegrationTest` is deliberately excluded here and
owned by scheduled chaos. `CrashWindowMatrixTest` is also scheduled. The
reusable broker contract still exercises its own isolated Testcontainers
restart/reconnect behavior on every push.

Local PowerShell equivalent, from the repository broker's original stopped
state:

```powershell
docker compose up -d rabbitmq
.\mvnw.cmd --batch-mode --no-transfer-progress `
  -pl taskflow-persistence-sqlite,taskflow-objectstore-minio,taskflow-transport-rabbitmq,taskflow-coordinator,taskflow-peer,taskflow-gui `
  -am `
  "-Dtaskflow.rabbitmq.live=true" `
  "-Dtest=*IntegrationTest,*LiveTest,DatabaseManagerTest,SqlitePersistenceContractTest,MinioObjectStoreContractTest,RabbitMqBrokerContractTest,PeerNodeTest,RabbitMqJobSubmissionClientTest,RabbitMqCoordinatorConnectionTest,!RabbitMqBrokerRecoveryIntegrationTest" `
  "-Dsurefire.failIfNoSpecifiedTests=false" test
docker compose stop rabbitmq
```

The stop command restores the documented local starting state; it is not part
of the GitHub job, whose service container is job-scoped.

## Scheduled chaos

For a scheduled event, the workflow derives a decimal seed from the fixed
TF-0706 seed plus `github.run_number`. Manual dispatch accepts a decimal seed.
Before tests start, `target/scheduled-chaos/run.properties` records the exact
commit, event, seed, model bounds, and mixed-chaos fault counts. The artifact
step uses `always()` so that this identity and any Surefire failure trace
survive a failed run.

The bounded model accepts:

- `taskflow.model.seedStart`;
- `taskflow.model.seedCount` in `[1, 256]`; and
- `taskflow.model.stepsPerSeed` in `[1, 1024]`.

Seeds advance by 16; the duplicate-focused scenario receives the next seed
after the generated range. An assertion failure still prints the exact seed
in decimal/hex and the complete event trace.

The scheduled mixed-chaos workload is deliberately not report grade. It uses
10,000 lightweight tasks, four executor fixtures, 5% deterministic duplicate
assignment/result publication, 100 delayed results, two executor
terminations, one broker restart, and one coordinator-component restart. Its
purpose is recurring fault coverage; it must not replace or update the
100,000-task report.

## Manual benchmark

Report-grade verification is manual because measurements must record a stable
host profile and because the PowerShell verifiers use clean-tree and
Windows-process evidence. Configure a trusted Windows x64 self-hosted runner
with the custom `taskflow-benchmark` label, JDK 21 or newer, Git, PowerShell,
and Docker Engine. GitHub requires every `runs-on` label to match; without
that runner, a dispatched job remains queued and is not evidence.

The workflow calls the existing verifiers without changing their workload:

```powershell
.\scripts\verify-correctness-chaos.ps1 -Overwrite
.\scripts\verify-scaling.ps1
```

The checkout step cleans ignored output before either verifier runs.
Correctness also receives its supported `-Overwrite` guard; the scaling
verifier deliberately refuses pre-existing evidence instead of accepting an
overwrite switch. A workflow dispatch is not complete evidence until its raw
properties, checksums, database audits, and artifact upload all pass.

Recovery and overload reports remain reproducible through their documented
manual commands, but TF-0710 does not add them to the benchmark dispatcher
because its required manual tier is the correctness/scaling pair.

## Limits

- Push-fast and push-integration are correctness gates, not performance
  measurements. GitHub-hosted duration is not a capacity result.
- Scheduled chaos explores a larger but still bounded generated set and one
  reduced mixed-failure workload per run. It is not exhaustive.
- Artifact retention is finite and cannot exceed a stricter repository,
  organization, or enterprise limit.
- The self-hosted benchmark workflow declares a runner contract; it does not
  provision or secure the machine.
- JavaFX window driving remains outside CI under
  [`GUI_AUTOMATION_SCOPE.md`](GUI_AUTOMATION_SCOPE.md).
