# TaskFlow Active Plan

## Active task

- Queue ID: TF-0801
- Status: Verified; commit and remote gates pending
- Goal: rewrite the root README into the required evidence-first order so a
  technical reviewer sees the system definition, current guarantees,
  non-goals, architecture, protocol, failure behavior, runnable proof, and
  measured evidence before module or configuration detail.
- Invariants protected: README claims must remain within the implemented and
  tested I1-I10 boundaries; every reliability or quantitative statement must
  point directly to a current test or report; limitations must remain explicit
  enough that no at-least-once, durability, recovery, overload, or benchmark
  statement becomes a production-readiness or exactly-once claim.
- Failure mode: the current 1,184-line README opens with overview/module
  vocabulary, repeats mechanism detail across many sections, delays the
  runnable proof and reports, and contains reliability statements far from
  their evidence. A reviewer can miss the authoritative architecture or read a
  narrow test result as a broader production claim.
- Expected files/modules: `README.md`, this active plan, and local-only
  queue/status/log handoff files after completion.
- Durable-state changes: none.
- Protocol changes: none.
- Tests required: audit the exact top-level section order; audit every
  quantitative/reliability claim for a direct report or test link; validate all
  local Markdown links and linked test/report files; reject unqualified
  `production-ready` wording; run `git diff --check` and the complete Maven
  reactor required by the working rules.
- Documentation required: replace the root README while preserving the current
  supported commands, explicit limits, and links to deeper authoritative docs.
- Known non-goals: adding or changing runtime behavior, guarantees, defaults,
  protocols, schemas, dependencies, benchmark results, report artifacts, or
  configuration files; creating the additional invariant diagrams reserved
  for TF-0802; creating the claim/evidence matrix reserved for TF-0803; or
  changing the stale-result/demo workflow reserved for later tasks.

## Smallest design and claim decisions

1. Use the required opening wording and exactly twelve ordered top-level
   sections after it: Guarantees; Non-goals; Architecture; Assignment/result
   protocol; Failure/recovery; Quick start; stale-result demo; benchmark
   evidence; module map; extension/plugin guide; operational commands; and
   limitations/future work.
2. Keep the existing component diagram because TF-0801 requires an
   architecture diagram. Describe the assignment/result sequence as concise
   ordered text so TF-0802 remains the owner of additional invariant-tied
   diagrams.
3. Link reliability claims directly to focused source tests or report
   artifacts in the same bullet/table row. Link detailed contract wording to
   `docs/GUARANTEES.md`, `docs/FAILURE_MODEL.md`, and related scope docs, but do
   not treat a future-plan document as evidence.
4. Keep exact report values only in one benchmark table, with the report link
   in the same row and the report's one-host/synthetic-workload limitation
   adjacent.
5. Preserve the one-command Docker quick start and deterministic stale-result
   proof near the top. Move detailed Maven/runtime/status/DLQ/MinIO/config/CI
   commands into one compact operational section and link authoritative
   documents instead of reproducing every environment variable.
6. Keep the module map after measured evidence, as required. Summarize each
   role/module in one line and retain compatibility terminology without
   leading the README with it.
7. State that RabbitMQ is the sole supported transport but remains
   transitional and is not broadly production-ready. Keep exactly-once,
   multi-coordinator, security, participant durability, GUI replay, object
   retention, and performance-generalization non-goals explicit.

## Ordered implementation and verification

1. Inventory current README commands, roles, supported workloads, reports,
   guarantees, and limitations against their authoritative local sources.
2. Rewrite `README.md` in the mandated order, deduplicating detailed
   implementation prose into links while preserving verified commands.
3. Run a structural audit for the twelve exact H2 headings and required
   opening paragraph.
4. Audit quantitative values and reliability phrases against direct
   test/report links; audit `production-ready`, exactly-once, transport, and
   authority wording.
5. Validate every local README link and linked evidence target, then run
   whitespace/diff checks.
6. Run the complete `.\mvnw.cmd test` reactor, inspect the final diff/status,
   commit one cohesive TF-0801 documentation change, push it, verify the exact
   remote hash and both push-tier CI jobs, and update local handoff evidence.

## Verification evidence so far

- The README has the required opening definition followed by exactly twelve
  ordered H2 sections matching the queue sequence.
- All 82 Markdown links resolve locally or use an external URL; every linked
  test and report target exists.
- Exact correctness, scaling, recovery, and overload values match their
  report artifacts.
- Reliability and quantitative statements point to focused Java tests or
  measured reports; the only `production-ready` occurrence is an explicit
  narrow limitation.
- `docker compose config --services` resolves the documented broker-backed
  topology: `rabbitmq`, `coordinator`, `peer-a`, `peer-b`, and `submitter`.
- The complete 25-module `.\mvnw.cmd test` reactor passed in 41.236 seconds,
  including the real MinIO and RabbitMQ contract suites.
- `git diff --check` passes. Staged-diff, commit, push, and remote CI gates
  remain pending.
