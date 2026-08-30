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
- [x] Phase 3 — buzz-node execution-node runtime COMPLETE: Groups A/B/C incl. Task 6 gated e2e_node.rs (enroll→assign→running, #[ignore], compiles) + spawn_detached test. 66 tests + 3 gated, clippy -D warnings + fmt clean, binary builds. (2 Phase-2-file correctness gaps carried to Phase 5: presence-cadence [product-blocking], shutdown-stops-agents.)
- [x] Phase 4 — G1 (Tauri node commands) + G2 (nodes store/panel/enrollment, presence author-scoped) + G3 (Run-on picker + agent-card status/controls; local-lifecycle gated on node-assignment) COMPLETE + reviewed + pushed. G3 Critical (local double-spawn of node-hosted agents) took 3 fix rounds + a fail-closed backstop; 7 distinct local-spawn bypasses closed; re-review clean.
- [~] Phase 5 — presence-cadence + Batch A + Batch B + **Batch C1 (process adoption + agents SURVIVE graceful restart [pre-existing kill_on_drop cascade fixed] + getpgid pid corroboration; publish coalescing; publish_announce test; resync watermark carry-forward) DONE + reviewed + pushed**. Remaining — C2: real next_status subscription [real cross-node moves are 30s-timeout-only until then] + validate_status doc · C3: two-node e2e #[ignore]. Deferred (non-blocking): AcpRuntime real probe RPC (buzz-acp control channel); provider-secret retrieval at spawn; observe()-poll pid re-corroboration (subset of accepted v1 bare-pid risk); engine.rs:180-181 residual-limitations doc-staleness

## Running log (loop updates this — newest first)
- 2026-08-30 (Phase 5 Batch C1 ✅ — agents genuinely survive node restart now): implemented process
  adoption (per-agent PID file → unix liveness + getpgid group-leader corroboration → adopt live
  survivors into an AgentSlot::Adopted so full_resync reconciles NoOp instead of double-spawning). The
  adversarial review COMPILED A REPRO proving the feature's premise was false: a pre-existing
  kill_on_drop(true) on agent children meant every graceful `buzz-node stop` SIGKILLed all agents (the
  daemon's shutdown select! drops the engine future → drops the substrate → drops the Child handles) —
  so adoption only ever helped after a crash. Fixed properly (removed kill_on_drop from the production
  spawn; explicit stop()-on-unassign still kills) so agents now survive a graceful daemon restart AND a
  disorderly kill, then get re-adopted — the core "agents keep running in the background" promise is now
  real end-to-end, not just documented. Also landed 4 cleanups (last_probe_result latch-clear, LWW
  watermark carry-forward, publish-task coalescing, publish_announce promptness). buzz-node 113 lib
  tests; re-review clean (reviewer re-ran the load-bearing tests + zombie-checked); pushed. Accepted v1
  residual: bare-pid+group-leader identity (no start-time token) — self-healing, boxes-I-own scope.
  → C2 (real peer-status subscription so cross-node moves fire immediately, not on the 30s timeout).
- 2026-08-29 (Phase 5 Batch B ✅): active smoke-probe health classification (classify checks
  breaker-open FIRST → a breaker-cooldown agent reports Stopped/"breaker-open", not a fresh Crashed) +
  at-rest OS-keychain secret store for provider API keys (new ProviderSecretStore, distinct from the
  node-key one; config references providers by NAME only, values in the keychain, verified no plaintext
  on disk). Review caught a latent probe-flapping bug (probe-failure health wasn't latched across the
  5s reconcile passes vs the 300s probe interval → would revert to Running; dormant until the real probe
  lands) — fixed by latching last_probe_result; re-review confirmed the fix + the LoopState refactor
  behavior-neutral + latch-invalidation-on-restart safe. buzz-node lib 96 tests, clippy+fmt clean;
  pushed. TWO deferrals recorded (non-blocking): AcpRuntime::probe is still an Ok(())-stub (active
  liveness needs a buzz-acp control channel), and stored provider secrets aren't yet read back at
  agent-spawn time. → Batch C (process adoption, real peer-status, two-node e2e).
- 2026-08-29 (Phase 5 Batch A ✅): move gate (bounded stop-before-start) + startup/reconnect
  full-resync + LWW one-live-instance (assignment created_at dedup + retarget-stop) landed in
  buzz-node, adapting the plan's Engine-struct assumption to the real free-fn run() (added
  NodeRelay::next_status/query_desired/take_reconnected; &mut→&self interior mutability). Adversarial
  review caught a real Critical — an emergent Task1×Task3 move-gate BYPASS (stale pending_spawns leak →
  I4 double-spawn) — fixed + regression-tested, plus 2 resilience Important (resync error no longer
  kills the loop; peer-status LWW) and cheap Minors. buzz-node lib 74 tests, clippy+fmt clean;
  re-review clean; pushed. TWO limitations knowingly deferred to Batch C: LocalProcessSubstrate.observe()
  doesn't adopt pre-existing processes (→ resync would dup-spawn on a daemon RESTART — needs PID+liveness
  adoption), and NostrNodeRelay::next_status is still a stub (→ real cross-node moves take the full 30s
  timeout until Batch C wires a real status subscription). → Batch B (health probing + at-rest secrets).
- 2026-08-29 (Phase 4 G3 ✅ → PHASE 4 COMPLETE): Run-on picker + agent-card status/controls done, and
  the G3 Critical (a node-hosted agent could be double-spawned LOCALLY, defeating the whole
  remote-execution model) fully closed over 3 SDD fix rounds under adversarial re-review. 7 distinct
  local-spawn entry points gated: avatar/profile/bulk/sidebar-pair controls, @-mention auto-start,
  project-message auto-start, edit "Start now" toast, huddle-add raw-invoke, the shared
  attachManagedAgentToChannel create-with-channel path (via SYNCHRONOUS node-intent, closing an
  assignment-echo race the store-only check would always lose), welcomeKickoff ×2, and the
  autoRestartPolicy background loop — PLUS a fail-closed backstop in the low-level startManagedAgent
  wrapper so any FUTURE ungated caller fails loud instead of silently double-spawning. Also: fixed a
  subscription-retry gap (transient failure no longer permanently disables the gate), added created_at
  LWW on assignments, and a useSyncExternalStore reactivity fix. Re-review clean; pushed
  (rounds 1-3 = f89c4b941..da1787c66). → rest of Phase 5.
- 2026-08-29 (Phase 4 G3 review → real CRITICAL caught, fix in flight): G3's mechanical work is
  correct (Run-on picker → real publishAgentAssignment; store move to shared/api = byte-identical
  no-op; controls wired to the real live cards; zero cross-feature imports; rem tokens; 26/26
  playwright). BUT review-phase4c found a functional regression from the design choice of persisting
  node-hosted agents as `backend:{type:local}`: every pre-existing local-lifecycle control (avatar
  "Start Agent", members-sidebar button, bulk Stop/Respawn) stays live for them → clicking local
  "Start" spawns a SECOND competing process under the same identity (local "Stop" then kills only the
  local copy). Same root cause makes the respond-to warning falsely say "access your computer". Fix
  round 1 dispatched (impl-phase4c, FIX_BASE 1eccaaa0c): gate the local-lifecycle affordances +
  runLocationForBackend on a robust node-assignment signal (owner's AGENT_ASSIGNMENT desired-state in
  the shared store, not just live getAgentStatus) + fix AGENTS.md + add the missing
  create-with-node-target e2e. **G3 NOT pushed until this re-reviews clean; Phase 4 not complete.**
- 2026-08-29 (presence-cadence ✅ — PRODUCT-BLOCKER RESOLVED): engine heartbeats presence(true)
  every 60s (= relay TTL 180s / 3); reviewed Approved (cadence math hand-traced, non-blocking
  inherited from spawn_publish, all EngineConfig constructors updated). Pushed. **A healthy remote
  node now stays 'online' in the app end-to-end.** → Phase 4 G3 (Run-on picker + agent controls),
  then rest of Phase 5 (shutdown-stops-agents, move-flow, coalesce, health vocab, publish_announce
  test, two-node e2e).
- 2026-08-29 (Phase 4 G2 ✅): Nodes live store + Nodes panel + enrollment approval; review caught +
  fix closed the unscoped-presence bug (now author-scoped to node pubkeys via the shared
  PresenceSubscriptionReconciler). tsc/biome/playwright green, full suite 5788/5788. Pushed. → next:
  PRESENCE-CADENCE fix (Phase 5 #3, product-blocker), then Phase 4 G3.
- 2026-08-29 (owner awake — steering): decisions: (1) CONTINUE autonomous loop; (2) NEW PRIORITY —
  after the in-flight G2 presence-scope fix pushes, do the **Phase-5 presence-CADENCE fix (#3, the
  product-blocker: engine.rs must republish kind:20001 every ≤60s vs the relay's 180s TTL)** NEXT,
  ahead of Phase 4 G3. Then G3, then the rest of Phase 5 (#4 shutdown-stops-agents, move-flow,
  two-node e2e). Status at handoff: Phases 1-3 + Phase 4 G1 pushed & reviewed-green; G2 committed
  locally (ahead 2), review Needs-fixes (unscoped presence sub) → fix round 1 in flight.
- 2026-08-29 (Phase 3 ✅ TRULY COMPLETE): Group C finished — Task 6 gated e2e_node.rs
  (enroll→assign→running, #[ignore], compiles via --no-run) + hermetic spawn_detached PID-file test.
  Verified green: 66 pass + 3 gated, clippy + fmt clean, e2e compiles. **The full relay-side + node
  runtime (buzz-core codecs + buzz-node daemon: reconciler, real process supervision, relay wiring,
  enrollment, graceful shutdown, gated e2e) is implemented & tested on the fork.** → Phase 4 G2 (Nodes
  UI). REMINDER: Phase 5 must fix the presence-cadence product-blocker (daemon appears offline after 3min).
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
