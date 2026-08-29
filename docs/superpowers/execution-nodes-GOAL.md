# Execution Nodes — Autonomous Build Goal & Progress

**Owner directive (2026-08-29, overnight):** finish all Phase 2–5 plan docs, then implement the
whole execution-nodes subsystem **as specified**, autonomously via a self-paced loop, while the
owner sleeps. Deliver maximal *verified* progress by morning with a precise status report.

**Spec:** `docs/superpowers/specs/2026-08-29-execution-nodes-design.md`
**Plans:** `docs/superpowers/plans/2026-08-29-execution-nodes-phase{1..5}-*.md`

## Guardrails (non-negotiable, autonomous mode)
- Work ONLY on branch `spec/execution-nodes` of the fork `origin` = `shivchander/os1`.
- **Never** push to `main`, never push to `upstream` (`block/buzz`), never open a PR, never do any
  outward-facing/irreversible action. Commits go to the feature branch only.
- **TDD + verify every task:** write the failing test, implement, run `cargo test`/`clippy`
  (and desktop `pnpm` gates for Phase 4). **Commit only work that passes its gates.**
- **No faked progress.** If a task can't be made to pass, stop that task, log the blocker below,
  and continue with independent work; if a whole phase hard-blocks, halt and report.
- Infra-dependent tests (`#[ignore]` e2e needing Postgres+Redis/Docker relay, live LLM providers)
  are written but may be left for the owner to run; note that explicitly rather than claiming green.
- Each commit is DCO-signed (`git commit -s`).

## Honest scope expectation
Phases 1–2 (buzz-core codecs + the pure reconciler/engine against fakes) are fully achievable
overnight with complete unit tests. Phase 3 (real substrate + relay wiring + daemon) is
substantial; Phase 4 (Tauri+React desktop UI) is large; Phase 5 (two-node e2e) needs a live relay.
The realistic morning outcome is **depth-first, fully-tested progress through as many phases as
hold up under their gates**, not necessarily a 100%-complete, fully-e2e-verified system. Every
committed piece will actually pass its tests.

## Phase checklist
- [x] Plans written & committed (all 5 phase plans; contract-consistent)
- [ ] Phase 1 — buzz-core protocol codecs (`cargo test -p buzz-core` green)
- [ ] Phase 2 — buzz-node reconciler core + engine vs fakes (`cargo test -p buzz-node` green)
- [ ] Phase 3 — real substrate + relay wiring + daemon (unit green; e2e written, infra-gated)
- [ ] Phase 4 — desktop Nodes surface + Run-on picker + status (desktop gates green)
- [ ] Phase 5 — move/resilience + two-node e2e (unit green; e2e written, infra-gated)

## Running log (loop updates this — newest first)
- 2026-08-29 (docs complete): All 5 phase plans written and consistency-checked — core contract
  (`DesiredAgent`/`Observed`/`Action`/`Substrate`/`NodeRelay`/`reconcile`/`Engine`/`AgentRuntime`)
  consistent across phases. **Phase 2's implemented interfaces are authoritative**; minor Phase-5
  test-helper names (`FakeRuntime`, `testkit`) reconcile to real code during implementation.
  Committing docs → starting Phase 1 implementation via subagent-driven TDD.
- 2026-08-29: Goal established. Phase-1 plan written. Phase 2–5 plans in progress (4 parallel
  writers). Fork `shivchander/os1` set up; docs on branch `spec/execution-nodes`.
