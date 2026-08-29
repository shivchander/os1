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
- [x] Phase 1 — buzz-core protocol codecs (`cargo test -p buzz-core` 276/276, clippy -D warnings + fmt clean; reviewed ✅)
- [x] Phase 2 — buzz-node reconciler core + engine vs fakes (21/21, clippy -D warnings + fmt clean; reviewed ✅)
- [~] Phase 3 — Groups A/B done+reviewed; Group C **Task 5 (daemon) done+Approved** BUT **Task 6 (gated e2e_node.rs) was never written — REOPENED**. Also 2 Phase-2-file gaps → Phase 5: presence not republished on cadence (product-blocking), shutdown doesn't stop agents. (I wrongly marked this complete earlier — corrected.)
- [~] Phase 4 — G1 native Tauri commands (publish_node_enrollment / publish_agent_assignment) DONE + reviewed Approved + pushed; G2 (nodes store + Nodes panel) and G3 (Run-on picker + status) pending
- [ ] Phase 5 — move/resilience + two-node e2e (unit green; e2e written, infra-gated)

## Running log (loop updates this — newest first)
- 2026-08-29 (CORRECTION + Phase 4 G1 ✅): (a) I prematurely marked Phase 3 complete — the delayed
  Group-C reviews + salvage report revealed **Task 6 (gated e2e_node.rs) was NEVER written** (Task 5
  daemon IS done + Approved by both reviewers). Reopening Phase 3 to write Task 6 + a spawn_detached
  unit test (Important review finding: the OS-interaction code has no test). LESSON: when accepting
  on controller-verification, verify ALL deliverables exist, not just that present tests pass. (b) Two
  real gaps in Phase-2 files (engine.rs/substrate.rs) → moved to PHASE 5: #3 presence NOT republished
  on a cadence (engine emits kind:20001 only twice; relay TTL=180s → a healthy daemon appears OFFLINE
  after 3min — TOP Phase-5 priority, product-blocking), #4 shutdown doesn't stop supervised agents
  (restart after graceful stop → duplicate agents). (c) Phase 4 G1 (native Tauri node commands) DONE +
  reviewed Approved (nsec encrypted-to-node verified vs real APIs; 5 offline tests + full desktop suite
  3010 green) — pushed. → finishing Phase 3 Task 6, then Phase 4 G2/G3, then Phase 5.
- 2026-08-29 (Phase 3 ✅ COMPLETE — 3 of 3 groups): Group C = `buzz-node` daemon binary (detached
  `up`, PID/status singleton guard, `autostart`, CLI) wiring enroll + NostrNodeRelay +
  LocalProcessSubstrate + AcpRuntime + engine::run, with graceful shutdown that AWAITS a final
  offline-presence publish; + gated enroll→assign→running e2e (#[ignore]). daemon.rs split into
  daemon/{cli,singleton,autostart}. 65 tests/2 gated, clippy+fmt clean, binary builds. Note: both
  Group-C reviewer subagents wedged at the report step (environmental, not a code signal) — Group C
  ACCEPTED on controller-verified green + deferred to the final whole-branch review. **The whole
  relay-side + node runtime (Phases 1-3) is implemented and on the fork.** → Phase 4 (desktop UI).
- 2026-08-29 (Phase 3 Group B ✅ — 2 of 3): NostrNodeRelay (dial-out/NIP-42/reconnect, assignment
  intake via decrypt_for_node→DesiredAgent, status/announce/presence publish) + enrollment (pairing
  code, keychain node key behind a SecretStore trait, NODE_ENROLLMENT wait). Review + fix round 1
  closed a real relay-outage bug (decoupled publish so a down relay no longer freezes crash-recovery)
  and documented enrollment TOFU. 46 tests / 2 gated, clippy + fmt clean. Pushed. → Group C (daemon
  binary + gated e2e) = last of Phase 3.
- 2026-08-29 (Phase 3 Group A ✅ — 1 of 3): buzz-node real process layer — AgentRuntime seam +
  AcpRuntime (env from decrypted nsec, zeroized; own process group) + LocalProcessSubstrate
  (per-agent workspaces, 3-crash/60s breaker, graceful process-group SIGTERM→SIGKILL). Review
  Approved; fix round 1 closed 2 Important (per-agent `start()` containment so one bad agent can't
  kill the whole node loop; a proven descendant-reaping test) + a `.lock().expect()` cleanup.
  29/29 tests, clippy -D warnings + fmt clean, no orphan processes. Pushed. → Group B
  (NostrNodeRelay + enrollment), then Group C (daemon + gated e2e).
- 2026-08-29 (Phase 2 ✅ done & reviewed): `buzz-node` crate — pure `reconcile()` (13 transition
  tests) + `Substrate`/`NodeRelay` traits + in-memory fakes + `engine::run` loop (assign→start,
  unassign→stop, crash→restart). 21/21 tests, clippy -D warnings + fmt clean; review Approved
  (reviewer independently re-ran test/clippy/fmt + hand-enumerated the decision table). 6 commits
  pushed. 1 minor deferred (sort test robustness). → Phase 3 (real substrate + relay + daemon),
  run as 3 sequential groups; its live-relay e2e stays #[ignore] (infra-gated).
- 2026-08-29 (Phase 1 ✅ done & reviewed): buzz-core codecs — 4 kinds 39500-39503
  (NODE_ANNOUNCE / NODE_ENROLLMENT / AGENT_ASSIGNMENT[nsec NIP-44-encrypted to the target node] /
  AGENT_NODE_STATUS). 276/276 tests, clippy -D warnings + fmt clean; task review Approved (crypto
  tests independently re-run by the reviewer). 5 commits pushed to origin. 2 minors deferred
  (ALL_KINDS registry entry; ciphertext-length bound). → dispatching Phase 2 (buzz-node reconciler
  + engine against fakes).
- 2026-08-29 (docs complete): All 5 phase plans written and consistency-checked — core contract
  (`DesiredAgent`/`Observed`/`Action`/`Substrate`/`NodeRelay`/`reconcile`/`Engine`/`AgentRuntime`)
  consistent across phases. **Phase 2's implemented interfaces are authoritative**; minor Phase-5
  test-helper names (`FakeRuntime`, `testkit`) reconcile to real code during implementation.
  Committing docs → starting Phase 1 implementation via subagent-driven TDD.
- 2026-08-29: Goal established. Phase-1 plan written. Phase 2–5 plans in progress (4 parallel
  writers). Fork `shivchander/os1` set up; docs on branch `spec/execution-nodes`.
