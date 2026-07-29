# TaskFlow Active Plan

## Active task

- Queue ID: TF-0803
- Status: Verified; commit and remote gates pending
- Goal: create `docs/CLAIM_EVIDENCE.md` with the required five-column matrix
  for all ten mandatory portfolio claims, using direct mechanism, automated
  evidence, reproducible report/experiment, and limitation links.
- Invariants protected: the summary must stay subordinate to the I1-I10
  contract and preserve the distinction between at-least-once execution and
  single authoritative commitment; every durability, fencing, replay,
  boundedness, integrity, and recovery row must state its tested scope.
- Failure mode: evidence currently exists across the guarantees ledger,
  failure model, reports, tests, and diagrams, but reviewers must assemble the
  claim/mechanism/proof/limitation relationship manually. Copying only success
  claims into a matrix could hide single-host, single-coordinator,
  duplicate-delivery, object-retention, or external-side-effect boundaries.
- Expected files/modules: `docs/CLAIM_EVIDENCE.md`, `README.md`, this active
  plan, and local-only queue/status/log handoff files after completion.
- Durable-state changes: none.
- Protocol changes: none.
- Tests required: assert the exact five-column header and exactly ten data
  rows; assert each mandatory claim appears once; require non-empty mechanism,
  automated-evidence, experiment/report, and known-limitation cells; validate
  every local link and named test/report target; retain TF-0801 README order
  and claim discipline; run `git diff --check` and the complete Maven reactor.
- Documentation required: add the matrix, declare it a reviewer index
  subordinate to `docs/GUARANTEES.md`, and link it from the README guarantees
  section.
- Known non-goals: changing guarantees, runtime behavior, protocol, schemas,
  configuration, experiments, test code, or report numbers; claiming
  exactly-once plugin effects, production sizing, HA coordinators, clustered
  brokers, automatic referenced-input deletion, or report-grade evidence
  where only automated contract/process evidence exists.

## Required row decisions

1. **At-least-once execution:** RabbitMQ redelivery, lease reassignment, and
   bounded executor-cache loss can repeat execution; link broker/executor
   tests and correctness chaos; exclude exactly-once external side effects.
2. **Stale-result fencing:** full SQLite assignment tuple conditionally rejects
   obsolete/same-executor ABA results; link deterministic/live fencing and the
   one-command stale-result experiment; distinguish fencing from cancellation.
3. **Single authoritative result:** SQLite conditional commit and monotonic
   task state select at most one result; link store/model/chaos evidence;
   retain the one-coordinator/SQLite scope.
4. **Durable accepted-job recovery:** atomic job/task creation and startup
   hydration preserve accepted work; link persistence/recovery evidence;
   distinguish restart recovery from HA failover.
5. **Transactional assignment outbox:** assignment identity, attempt, lease,
   and serialized outbound intent commit together; link store/process/live
   evidence; publication remains at least once.
6. **Final-result replay:** terminal payload and final `JOB_RESULT` intent
   commit together after durable `FINALIZING`; link finalization/replay tests;
   final delivery can duplicate.
7. **Bounded scheduler ingress:** route-local prefetch, ordinary lane, result
   reserve, admission limits, and bounded cycle stages; link mailbox/live
   overload evidence; pressure rejects or defers new work.
8. **Bounded poison retry:** typed finite TTL stages end in quarantine; link
   reusable/live contracts and chaos evidence; redrive is manual and bounded.
9. **Payload integrity:** length/SHA-256 verification and exact attempt-output
   ownership gate processing/acceptance; link reusable/MinIO/process evidence;
   note referenced-input retention and benchmark gaps.
10. **Overload recovery:** accepted work progresses while typed rejection
    protects bounds, then fresh admission resumes after drain; link focused,
    live, and report evidence; retain one-host synthetic limits.

## Ordered implementation and verification

1. Reconcile the ten rows with `docs/GUARANTEES.md`,
   `docs/FAILURE_MODEL.md`, `docs/ARCHITECTURE_DIAGRAMS.md`, focused tests, and
   current report limitations.
2. Add the exact required Markdown table and a short authority/scope preface;
   use direct links rather than unsupported prose.
3. Add one concise README link without changing its evidence-first order.
4. Run row/header/claim/cell/link/report/current-scope audits and inspect the
   rendered Markdown shape.
5. Run the full `.\mvnw.cmd test` reactor, inspect final and staged diffs,
   commit one cohesive TF-0803 documentation change, push it, verify the exact
   remote hash and both push-tier CI jobs, then update local handoff evidence.

## Verification evidence so far

- `docs/CLAIM_EVIDENCE.md` has the exact required five-column header and
  exactly ten data rows, one for each mandatory claim. Every cell is populated.
- Every automated-evidence and experiment/report cell contains a resolving
  link; all 26 explicitly named Java test methods exist in their linked files.
- All 137 links across the README and matrix resolve. Report values and
  limitations were reconciled against correctness-chaos, recovery, and
  overload artifacts; the payload row explicitly avoids inventing a
  report-grade throughput claim.
- GitHub-flavored Markdown rendering produced one table with one header plus
  ten data rows and 50 data cells.
- TF-0801's twelve-section README order and narrowly qualified production
  boundary remain unchanged. Documentation whitespace and `git diff --check`
  pass.
- The complete 25-module `.\mvnw.cmd test` reactor passed in 47.921 seconds,
  including the real MinIO and RabbitMQ contract suites.
- Staged-diff, commit, push, exact-remote-hash, and remote CI gates remain
  pending.
