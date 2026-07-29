# TaskFlow Active Plan

## Active task

- Queue ID: TF-0802
- Status: Verified; commit and remote gates pending
- Goal: add six reviewer-facing architecture diagrams that show the current
  component boundary, assignment/outbox transaction, result fence, restart
  recovery, staged object lifecycle, and bounded scheduler/deadline flow while
  naming the exact invariants each flow protects.
- Invariants protected: the diagrams must accurately trace I1 durable
  acceptance, I2 single result authority, I3 assignment fencing, I4 monotonic
  terminal state, I5 transactional outbound intent, I6 duplicate tolerance,
  I7 bounded coordinator memory, I8 poison termination, I9 payload integrity,
  and I10 conditional eventual terminality to their implemented owners.
- Failure mode: architecture intent currently depends on prose spread across
  guarantees, the state machine, failure model, payload storage, and scheduler
  documentation. The root README has only a component view, so a reviewer
  cannot quickly see the transaction/commit points, conditional fence,
  restart replay, object authority boundary, or bounded deadline path.
- Expected files/modules: `docs/ARCHITECTURE_DIAGRAMS.md`, `README.md`, this
  active plan, and local-only queue/status/log handoff files after completion.
- Durable-state changes: none.
- Protocol changes: none.
- Tests required: assert exactly six Mermaid diagrams and all required
  diagram titles; assert every diagram has an adjacent invariant declaration;
  validate every local Markdown link and every named implementation/test
  target; inspect Mermaid syntax and reject planned components; run
  `git diff --check` and the complete Maven reactor required by the working
  rules.
- Documentation required: add the diagram artifact and link it from the
  README architecture section without disturbing TF-0801's required order.
- Known non-goals: changing runtime behavior, topology, schemas,
  transactions, protocol fields, scheduler bounds, recovery policy, object
  ownership, or evidence; introducing PostgreSQL, multi-coordinator
  consensus, clustered broker failover, Kubernetes, or other planned
  architecture; building TF-0803's claim/evidence matrix.

## Diagram-to-invariant plan

1. **Component/context:** show requester/executor roles, RabbitMQ, the sole
   authoritative coordinator, bounded mailbox/services, SQLite/outbox,
   plugins, and optional MinIO. Name I1-I10 only where that component boundary
   participates in enforcement or progress.
2. **Assignment transaction and outbox:** show the scheduler proposal,
   conditional SQLite generation/assignment/attempt/outbox transaction,
   post-commit in-memory projection, confirmed RabbitMQ publication, durable
   sent marking, and identical replay. Protect I3, I5, and I6.
3. **Result fencing and conditional commit:** show tuple validation for task,
   state, attempt, assignment UUID, and executor; typed stale/duplicate/current
   outcomes; `FINALIZING`; and the separate terminal-result/outbox
   transaction. Protect I2, I3, I4, I5, and I6.
4. **Coordinator restart/recovery:** show SQLite reopen, resumable job/task
   hydration, unexpired lease preservation, expired/incomplete assignment
   release, `FINALIZING` re-aggregation, pending outbox replay, and restored
   service prerequisites. Protect I1, I3, I4, I5, and I10.
5. **Object-storage staged-output lifecycle:** show deterministic
   attempt-scoped upload, length/SHA-256 verification, result reference,
   conditional authority, requester verification, and bounded orphan
   classification/deletion that preserves active and authoritative objects.
   Protect I2, I3, and I9.
6. **Scheduler queues/deadline flow:** show bounded RabbitMQ prefetch,
   ordinary/result-reserve lanes, result-first dequeue, bounded cycle stages,
   active/pending indexes, fake-clock deadline handling, capacity release,
   retry/failure/assignment decisions, and admission recovery. Protect I4, I7,
   and I10.

## Ordered implementation and verification

1. Reconcile each diagram with `docs/GUARANTEES.md`,
   `docs/STATE_MACHINE.md`, `docs/FAILURE_MODEL.md`,
   `docs/PAYLOAD_STORAGE.md`, `docs/SCHEDULER.md`, relevant ADRs, current
   production classes, and focused tests.
2. Add one concise Mermaid diagram per required flow with an adjacent
   `Protected invariants` line and focused implementation/evidence links.
3. Link the artifact from the existing README Architecture section without
   duplicating the six detailed diagrams there.
4. Run diagram-count/title/invariant/current-component audits, validate every
   link, and render-check Mermaid syntax if a repository-compatible renderer
   is available.
5. Run the full `.\mvnw.cmd test` reactor, inspect final and staged diffs,
   commit one cohesive TF-0802 documentation change, push it, verify the exact
   remote hash and both push-tier CI jobs, then update local handoff evidence.

## Verification evidence so far

- `docs/ARCHITECTURE_DIAGRAMS.md` contains exactly the six required ordered
  Mermaid diagrams, and every diagram has an adjacent `Protected invariants`
  declaration. I1-I10 are all named.
- Current production classes, state-machine transitions, payload ownership,
  scheduler stages, recovery behavior, and ADR boundaries were reconciled
  before finalizing the flows. Planned HA, database, broker, orchestration, and
  deletion components are absent from the Mermaid blocks.
- All seven Mermaid blocks across the README and new artifact rendered
  successfully to SVG through an in-memory syntax check.
- All 109 links across the README and diagram artifact resolve. TF-0801's
  twelve-section README order and narrowly qualified production boundary are
  unchanged.
- Documentation whitespace and `git diff --check` pass. The complete
  25-module `.\mvnw.cmd test` reactor passed in 46.636 seconds, including the
  real MinIO and RabbitMQ contract suites.
- Staged-diff, commit, push, exact-remote-hash, and remote CI gates remain
  pending.
