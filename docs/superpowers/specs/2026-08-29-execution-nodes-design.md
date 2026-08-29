# Execution Nodes — relay-native, persistent agent runtimes

**Status:** Draft for review · **Date:** 2026-08-29 · **Path:** `docs/superpowers/specs/2026-08-29-execution-nodes-design.md`

> Working name `buzz-node` is a placeholder; the fork's final naming/rebrand is TBD and does not affect this design.

---

## 1. Context

We are forking Buzz ("evolve the fork" — keep the relay, desktop app, and agent
runtime; rebrand; add new subsystems). The goal is a workspace where the user
creates agents in a local app and **binds each agent to an execution runtime**:

- **Work agents** run on a remote **work node** (clone repos, do dev work there).
- **Personal agents** run on a **local/personal node**.
- **Agents keep running in the background even after the app is quit.**
- **Work and personal are separate communities.**

This is the capability upstream Buzz is still debating in RFC #4174 (a shared
relay-native execution model) — current Buzz only has app-child local agents
(die with the app) and one-shot provider-deploy to cloud substrates. Our fork's
signature feature is a **persistent, relay-native execution node**.

Background review of the base codebase: `FORK-REVIEW.md` (repo root). A comparative
analysis of **OpenAgents** (openagents-org) informed several node mechanics in §9
(detached-daemon persistence, active health probing, at-rest secret encryption)
and the ACP-vs-adapter decision (D7); its OSS bus is weaker than Buzz's relay
(in-memory/ephemeral, bearer-token auth, poll transports), so it is a reference for
the node layer, not a fork base.

## 2. Goals

1. Create an agent in the app and **assign it to a node** the user owns.
2. Agents **survive app quit** (and node reboot), running on always-on machines.
3. **Move an agent** between nodes with a single action; it resurrects with the
   same identity and relay history.
4. The app never needs a direct route into a node (firewalled work node works).
5. Separate **work** and **personal** communities.
6. Reuse Buzz's proven pieces; keep the change surface small.

## 3. Decisions (locked in brainstorming)

| # | Decision | Choice |
|---|----------|--------|
| D1 | Node model | **Persistent daemon** on always-on machines the user owns (not per-agent cloud spin-ups). |
| D2 | Control channel | **Relay-only.** Nodes dial out; the app controls via relay events; no app↔node link. |
| D3 | Key custody | **Key on the node**, delivered NIP-44-encrypted; held in memory. A `Signer` seam is built in for later hardening. |
| D4 | Fork scope | **Evolve the fork:** keep relay + desktop + `buzz-acp`/`buzz-agent`; add the node subsystem + app UI. |
| D5 | Reconcile model | **Declarative desired-state reconciler** (app publishes desired state; nodes converge). Reuse `buzz-backend-kubernetes` logic. |
| D6 | Local unification | The **laptop runs a `buzz-node` background service** too; "run locally" = assign to the laptop-node, so local agents also survive app quit. One model everywhere. |
| D7 | Agent integration | **ACP-only** (reuse `buzz-acp`), behind an `AgentRuntime` trait seam so a non-ACP per-agent adapter (OpenAgents-style registry) can be added later without rework. |

## 4. Non-goals (YAGNI)

- No on-demand cloud/Kubernetes provisioning in v1 (the existing provider path
  remains available but is not the focus).
- No keyless/remote-signer implementation in v1 — only the seam (RFC #6467 later).
- No tool-sandboxing/isolation of the agent from its own key in v1 (seam only).
- No cross-community agent sharing, no multi-owner nodes.
- No node capacity autoscaling; a node may advertise limits, nothing schedules.
- No per-agent (non-ACP) adapters in v1 — ACP-only behind an `AgentRuntime` seam;
  the registry/adapter pattern (for Cursor/Aider/Amp-class agents) is deferred.

## 5. Architecture overview

Four components; one is new (`buzz-node`).

```
   LOCAL (you)                    RELAY (per community)              YOUR MACHINES
 ┌──────────────┐   assign/       ┌───────────────────┐   desired    ┌─────────────────────┐
 │  Desktop app │──create/move───►│  single source of │──state────►  │  buzz-node (daemon)  │
 │  (existing + │   (relay events)│  truth + pub/sub  │  (events)    │  • own identity      │
 │   node UI)   │◄──node+agent────│  + new kinds      │◄──status──── │  • reconciles        │
 └──────────────┘   status        └───────────────────┘   presence   │  • supervises agents │
                                                                     └─────────┬───────────┘
                                                                     ┌─────────▼───────────┐
                                                                     │ buzz-acp + buzz-agent│
                                                                     │ (goose/claude/codex) │
                                                                     │ + persistent workspace│
                                                                     └─────────────────────┘
```

**Governing principle:** desired state (published by the app, stored on the
relay) vs. observed state (the node's local process table) → the node converges
them. We lift Buzz's reconcile/converge/create-intent-fingerprint logic out of
`buzz-backend-kubernetes` and re-point its `Substrate` at local processes.

## 6. Components & interfaces

Each unit has one purpose, a defined interface, and explicit dependencies.

### 6.1 Relay (existing, +new kinds)
- **Does:** single source of truth + pub/sub; carries the new kinds below;
  enforces community isolation + membership (unchanged).
- **Interface:** NIP-01 events/filters (unchanged).
- **Depends on:** nothing new.

### 6.2 `buzz-node` daemon (NEW)
- **Does:** persistent supervisor; owns agent lifetime on one machine.
- **Interface (inbound):** subscribes to the owner's `AGENT_ASSIGNMENT` +
  `NODE_ENROLLMENT`. **Interface (outbound):** publishes `NODE_ANNOUNCE`,
  presence (kind:20001), `AGENT_NODE_STATUS`.
- **Depends on:** `buzz-ws-client`, the lifted reconcile crate, `buzz-acp` (spawned as child).

### 6.3 Desktop app (existing, +Nodes surface & Run-on picker)
- **Does:** create agents; enroll/list nodes; assign/move/stop agents; render status.
- **Interface:** publishes agent records + `AGENT_ASSIGNMENT` + `NODE_ENROLLMENT`; subscribes to node/agent status. Relay-only.
- **Depends on:** existing desktop relay client; existing launch-data resolver.

### 6.4 Agent runtime (existing, unchanged)
- **Does:** `buzz-acp` harness + `buzz-agent`/goose/claude/codex — the LLM+tools loop.
- **Interface:** env in (`BUZZ_PRIVATE_KEY`, `BUZZ_RELAY_URL`, `BUZZ_AUTH_TAG`, launch env); Nostr events out.
- **Depends on:** the node to spawn/supervise it (was: the desktop).

## 7. Event model

New kinds (exact integers assigned in `buzz-core/src/kind.rs` during
implementation; all owner-authorized):

| Kind (working name) | Author | Type | Purpose |
|---|---|---|---|
| `NODE_ANNOUNCE` | node | param-replaceable, `d`=node pubkey | Node capabilities (OS, installed runtimes, workspace root, optional limits). Liveness via kind:20001 presence. |
| `NODE_ENROLLMENT` | owner | param-replaceable, `d`=node pubkey | Trust link: "node N is authorized by owner O." Node acts only on O's commands. |
| `AGENT_ASSIGNMENT` | owner | param-replaceable, `d`=agent pubkey | Desired state: `{ node: <node pubkey>, launch: <resolved launch block>, enc_nsec: NIP44(to=node, agent_nsec), lifetime: {reap?} }`. |
| `AGENT_NODE_STATUS` | node | regular | Observed per-agent state: `starting|running|stopped|crashed` + last error/exit. |

**One record per agent** (keyed by agent pubkey). Its `node` field is the single
source of assignment truth (last-writer-wins). Each node subscribes to all of
its owner's assignment records and runs exactly those where `node == self`.

## 8. Reconcile flows

**Assign:** app creates agent A (existing flow) → publishes
`AGENT_ASSIGNMENT{d=A, node=N, launch, enc_nsec→N}` → node N sees A targets it →
spawns `buzz-acp` for A → A connects as itself, posts presence, works → node
publishes `AGENT_NODE_STATUS{A: running}`.

**Move (N→M):** app edits the assignment's `node` to M and re-encrypts `enc_nsec`
to M → node N sees A no longer targets it → graceful stop → publishes
`status{A: stopped}` → node M waits (bounded) for that stopped status, then spawns
A. Same key + relay history = resurrection on M.

**App quit / reopen:** no effect on agents (node owns lifetime); reopen
re-derives all status from the relay.

**Node reboot/crash:** presence lapses (relay TTL) → app shows node offline. On
restart the node (a background service) reconciles from the relay's desired state
and restarts its assigned agents.

## 9. Node daemon internals

- **Identity & enrollment.** First run generates a keypair, prints an enrollment
  code; the user approves once in the app (publishes `NODE_ENROLLMENT`).
  Thereafter the node NIP-OA-verifies the owner on every command (same check
  `buzz-acp` uses for `!shutdown`).
- **Relay connection.** Reuses `buzz-ws-client` + `buzz-acp` relay patterns
  (dial-out, NIP-42, reconnect/backoff). Publishes `NODE_ANNOUNCE` + presence.
- **Reconcile loop.** Event-driven (live subscription) **plus** periodic resync
  and a full reconcile on startup (query current state, not just tail). Desired
  set = assignments where `node == self`; diff vs. observed local process table;
  converge. Implemented behind a `Substrate` trait (impl = local processes) so
  the loop is unit-testable against a fake — lifted from `buzz-backend-kubernetes`
  (reconcile/converge/create-intent-fingerprint).
- **Agent supervision (ACP-only, behind a seam — D7).** One `buzz-acp` process per
  assigned agent (one harness = one identity), spawned via an `AgentRuntime` trait
  whose only v1 impl is ACP — reuse `buzz-acp` (Claude Code, Codex, Goose,
  OpenClaw, buzz-agent). A non-ACP per-agent adapter (OpenAgents-style registry)
  can slot behind the same trait later without rework. Crash-restart with a
  circuit breaker (reuse `SlotCircuit`: N crashes/window → back off). Keeps agents
  alive independent of the app.
- **Process persistence.** The node itself runs as a detached background process
  (spawn detached + `unref`; PID + `status.json` cross-checked as the singleton
  guard) so it survives both the app and the terminal closing; reboot survival is a
  deliberate opt-in OS login-item (autostart) — the mechanism proven by
  OpenAgents' launcher, no systemd unit required for v1.
- **Health probing.** Beyond presence, the node runs an active per-agent
  smoke-probe (a real round-trip on create/reconfigure + periodically) and reports
  a shared health-reason vocabulary in `AGENT_NODE_STATUS` — richer than a
  liveness ping alone (an OpenAgents pattern worth copying).
- **Config without drift.** The app resolves the effective launch env
  (persona → model → env layering) into the assignment's `launch` block; the node
  applies it mechanically (reusing the desktop resolver — avoids the spec's
  Known Defect #3 double-derivation).
- **Workspaces.** Each agent gets a persistent dir
  (`~/.<fork>/agents/<agent>/workspace`) for repo clones and work, surviving
  turns and restarts; `buzz-dev-mcp` tools resolve against it.
- **Lifetime policy.** Default **run-while-assigned** (no auto-reap), since nodes
  are always-on and owned; inactivity self-stop is opt-in per agent.
- **Key handling.** Decrypt `enc_nsec` (NIP-44 with node key), hold in memory,
  inject into the harness env; never write plaintext to disk; zeroize on
  stop/unassign. **Provider API keys** the node holds are encrypted at rest in the
  OS keychain — never plaintext env files (OpenAgents' plaintext-vs-keychain split
  is the cautionary tale). All signing routes through a `Signer` trait (local impl
  now; isolation/remote-signer later).

## 10. Desktop app changes

1. **Nodes surface (new):** lists the community's nodes (from `NODE_ANNOUNCE` +
   presence): name, machine, online/offline, runtimes, agent count; includes the
   one-time enrollment approval.
2. **Run-on picker:** extend the existing `WhereToRunSection.tsx` from
   "This computer / provider" to **Run on: [node ▾]** (enrolled, online nodes).
   Selecting/changing publishes/edits the `AGENT_ASSIGNMENT` (encrypting the key
   to the chosen node). This gesture *is* "connect a runtime to an agent." The
   app already holds each agent's key from creation (Buzz stores managed-agent
   keys in the OS keyring), so it can NIP-44-encrypt it to the node at assign time.
3. **Status + controls:** agent cards show "running on <node> · healthy" (from
   `AGENT_NODE_STATUS` + presence; reuse the upstream "liveness from presence"
   direction). Start / Stop / Move are assignment edits.
4. **Laptop as a node (D6):** the app installs/runs a `buzz-node` background
   service (login item / user service) so local agents also survive app quit. The
   old app-child "local" mode is retired in favor of the unified node model.

## 11. Communities & topology

- **Two communities:** work and personal. Recommended **two separate relays**
  (physical separation of work/personal); one multi-tenant relay with two
  communities is a supported alternative.
- **Identity:** one portable keypair; a per-community profile (Buzz already does
  this). The app's existing community switcher (remount + `resetCommunityState`)
  toggles work↔personal.
- **Node placement:** nodes enroll **per community** — the work node in the work
  community, the personal node/laptop in the personal one. That enrollment
  boundary *is* the work/personal separation: a work agent can only target a node
  enrolled in the work community.

## 12. Security model

- **Owner authorization:** assignments and enrollments are owner-signed and
  NIP-OA-verified by the node before it acts. A stray assignment can't target a
  node; a rogue node can't be adopted without explicit enrollment approval.
- **Key in transit:** agent keys travel NIP-44-encrypted to the target node;
  decrypted only there; relay never sees plaintext.
- **Honest caveat:** with "key on node, simple," an agent's shell tools can read
  *its own* key (as in Buzz today). Acceptable on owned nodes; the `Signer` seam
  tightens this later (most relevant on the work node). Scope contains blast
  radius: a work node holds only work-agent keys.
- **Revocation:** owner revokes `NODE_ENROLLMENT`; because keys live on the node,
  affected agent keys should be rotated — a limitation the signer seam removes.

## 13. Error handling & edge cases

- **Node offline at assign/move:** desired state persists; node reconciles on
  reconnect; app shows "assigned · waiting for node."
- **One-live-instance (I4):** single per-agent assignment record (LWW) prevents
  two nodes running the same agent (sidesteps upstream bug #3832). On move, the
  new node waits (bounded) for the old node's `stopped` status before spawning →
  at most a brief bounded overlap.
- **Crash recovery:** agent crash → circuit-breaker restart; node/machine reboot
  → background service auto-starts → reconciles from relay.
- **Missed events:** live subscription + periodic resync + full startup reconcile.
- **Stop/unassign:** graceful harness shutdown (drain + offline presence); node
  zeroizes the agent key.
- **Presence staleness (I3):** bounded ≤180s wrong "online" dot on hard node
  death; app shows node offline, agents unknown.

## 14. Testing strategy

- **Reconciler unit tests against a fake `Substrate`** (highest value; mirrors
  `buzz-backend-kubernetes`'s ~30 tests): assign→spawn, unassign→stop, move,
  crash→restart, offline→catch-up, duplicate-guard.
- **Node integration tests** vs. a real relay (Docker e2e): enroll → assign →
  assert connect + presence → move → assert resurrection.
- **Two-node e2e** (reuse `buzz-test-client`): assign, move N→M, assert
  single-live-instance + status events.
- **App:** Playwright/mock-bridge tests for the Nodes surface, Run-on picker, status.
- **Key delivery:** assert nsec is NIP-44-encrypted to the node, decrypts only
  there, never plaintext on the relay.

## 15. Reuse map (what we lift from Buzz)

| Need | Reuse |
|---|---|
| Reconcile/converge/fingerprint | `buzz-backend-kubernetes` (re-point `Substrate` to local processes) |
| Agent process + LLM/tools loop | `buzz-acp` + `buzz-agent` + `buzz-dev-mcp` (unchanged) |
| Relay client (dial-out, NIP-42, reconnect) | `buzz-ws-client` + `buzz-acp` relay patterns |
| Owner verification (`!shutdown`) | `buzz-acp` NIP-OA owner check |
| Launch-env resolution | desktop launch-data resolver (the `launch` block) |
| Run-on UI | extend `WhereToRunSection.tsx` |
| Presence lease / liveness | kind:20001 + upstream "liveness from presence" work |

## 16. Open questions / future

- Final fork naming / rebrand (crate + product names).
- v2 hardening: `Signer` isolation and/or remote signer (RFC #6467).
- Optional node capacity limits + app warnings.
- Optional: adopt the existing cloud provider path as an additional substrate
  behind the same reconcile abstraction.
