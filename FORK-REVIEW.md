# Buzz — Deep Architecture Review

**Oriented toward:** building a client–server system where AI agents run remotely and you interact with them locally through an app.

> **Provenance.** Produced on 2026-08-28 by a deep, code-verified read of the current tree
> (all ~30 crates, `desktop/`, the vision docs, and `docs/remote-agents.md`), cross-checked
> against the running source rather than the documentation. Where the shipped docs and the
> code disagree, this review follows the **code**.
>
> **Trust rule:** `ARCHITECTURE.md` is stale (~2024-era) and `docs/remote-agents.md` is a
> `draft` with a live "Known Defects" list. This document flags every divergence it found.
> Always confirm against code before relying on a detail.

---

## Table of contents

0. [The one idea](#0-the-one-idea-to-take-away)
1. [System topology](#1-system-topology)
2. [Repo map & the stale-docs caveat](#2-repo-map--a-critical-caveat)
3. [The wire contract (client ↔ server)](#3-the-wire-contract-client--server)
4. [The server — `buzz-relay`](#4-the-server--buzz-relay)
5. [The data / eventing backbone](#5-the-data--eventing-backbone)
6. [The agent runtime](#6-the-agent-runtime--how-a-body-actually-lives)
7. [The local client — desktop (Tauri + React)](#7-the-local-client--desktop-tauri--react)
8. [THE CRUX — remote agents](#8-the-crux--remote-agents-deploy-remotely-control-only-via-the-bus)
9. [Honest assessment — risks & gaps](#9-honest-assessment--risks--gaps-to-know-before-forking)
10. [Build-your-own blueprint](#10-build-your-own-blueprint)
11. [Where to read first](#11-where-to-read-first-if-you-dive-into-the-code)

---

## 0. The one idea to take away

> **The relay is the only tether.** Everything is a cryptographically signed event in one
> append-only log. The relay is the single source of truth *and the only communication
> channel*. The local app, remote agents, CLI, and workflows are all just
> **keypair-authenticated WebSocket clients** of that one server. There is deliberately
> **no separate control plane.**

Your goal — *agents run remotely, I interact locally* — is not a feature bolted onto Buzz;
it's a **direct consequence** of that idea. The local app and the remote agent never connect
to each other. Both dial *out* to the relay, authenticate with a keypair, subscribe to
events, and publish events. To steer a remote agent you post an `@mention`; it replies with
an event; you stop it with a `!shutdown` message; its "online" dot is a **lease it renews**,
not a socket you hold.

This is the single most important architectural decision to copy.

---

## 1. System topology

```
        LOCAL                          THE SERVER (bus)                 ANYWHERE
 ┌───────────────────┐          ┌──────────────────────────┐     ┌──────────────────┐
 │  Desktop (Tauri)  │◄────WS───►│                          │◄─WS─►│ Agent body       │
 │  React + native   │  NIP-42  │        buzz-relay         │     │  buzz-acp harness│
 │  relay client     │          │  (Axum, single process)  │     │   └─buzz-agent   │
 └───────────────────┘          │                          │     │      └─dev-mcp   │
 ┌───────────────────┐          │  auth·ingest·fan-out·    │     └──────────────────┘
 │  buzz-cli / CLI   │◄──WS/HTTP►│  REQ·HTTP bridge·git·    │     (deployed via a
 └───────────────────┘          │  media·multi-tenant      │      provider binary;
                                 └───────┬─────────┬────────┘      then relay-only)
                                    Postgres     Redis   S3/MinIO
                                  (event log)  (fan-out) (blobs)
```

Everything that isn't the relay is a **replaceable client**. The desktop *itself* can also
host a local agent body using the identical wire protocol a remote body uses — "run on: this
computer" vs. "run on: provider" differ only in launcher and environment.

---

## 2. Repo map & a critical caveat

**~30 Rust crates**, grouped:

- **Core protocol:** `buzz-core` (zero-I/O types, kinds, filters, verify), `buzz-relay` (the server).
- **Services:** `buzz-db` (Postgres), `buzz-auth` (NIP-42/98), `buzz-pubsub` (Redis), `buzz-search` (FTS), `buzz-audit` (hash chain), `buzz-media` (Blossom/S3), `buzz-deletion` (whole-tenant erasure).
- **Agent surface:** `buzz-acp` (harness), `buzz-agent` (LLM loop), `buzz-dev-mcp` (tools), `sprig` (multicall bundle), `buzz-persona`, `buzz-workflow`, `buzz-cli`.
- **Remote deployment:** `buzz-backend-kubernetes` (the reference provider), `buzz-conformance` (actually the multi-tenant TLA+ checker — *not* provider conformance; see §8).
- **Transport/shared:** `buzz-ws-client`, `buzz-sdk`.
- **Clients:** `desktop/` (Tauri 2 + React 19), `mobile/` (Flutter), `web/` (repo browser).

⚠️ **`ARCHITECTURE.md` is stale (~2024-era).** The code has moved well past it. Confirmed divergences:

| Doc claims | Reality in code |
|---|---|
| Rate limiting "not implemented / test stub only" | Real `RedisRateLimiter` (atomic Lua `INCR`+`EXPIRE`), `buzz-pubsub/rate_limiter.rs`; per-tier limits via `BUZZ_RATE_LIMIT_*` |
| Redis fan-out via `PSUBSCRIBE buzz:channel:*` | Demand-driven **refcounted exact `SUBSCRIBE`/`UNSUBSCRIBE`** + 500 ms debounce, `buzz-pubsub/subscriber.rs:37-172` |
| One global `pg_advisory_lock` for audit | **Per-community** lock via `hashtextextended('buzz_audit:{cid}')`, `buzz-audit/service.rs:58-85` |
| Multi-tenancy framed as future/optional | **Unconditional at schema level**; + read-replica routing & freshness-proof (undocumented), `buzz-db/runtime/replica_fence.rs` |
| — | `buzz-db` split into `runtime/`+`store/` behind a façade (`lib.rs:12-42`) since commit `a3730784f` |
| Crate list | Missing: `buzz-deletion`, cache-invalidation, conn-control, `MediaUploaded` audit action |

The **five-repo ecosystem** (from `AGENTS.md`): `block/buzz` (this OSS source) → `buzz-releases` (signed desktop/mobile builds), `sprout-oss` (relay Docker image), `block-coder-tf-stacks` (Terraform/ArgoCD deploy), `sprout-backend-blox` (the Blox compute provider for remote agents).

---

## 3. The wire contract (client ↔ server)

Every participant — local app *or* remote agent — speaks this.

**Strict one-directional layering (copy this shape wholesale):**
```
buzz-core     zero-I/O: event type, kind registry, filter matching, verification
   ├─ buzz-auth   verify NIP-42/NIP-98 signed events → AuthContext
   ├─ buzz-sdk    38 typed event builders (validate → EventBuilder; never holds keys)
   └─ buzz-ws-client   the actual socket (connect, AUTH, EVENT, REQ)
```
`buzz-core/Cargo.toml:30` explicitly bans tokio/sqlx/redis/axum — this zero-I/O core is the
seam that lets relay, CLI, desktop-native code, and tests all agree on "what is a valid
event" without a runtime.

**Event model.** Buzz does *not* invent a schema — it depends on the external `nostr` crate
(rust-nostr 0.44) and re-exports `Event/Filter/Keys/Kind/PublicKey` verbatim
(`buzz-core/lib.rs:48`). Seven signed fields (`id,pubkey,created_at,kind,tags,content,sig`).
`verify_event` (`verification.rs:11`) recomputes the id and Schnorr-verifies — CPU-bound, so
callers `spawn_blocking`. `StoredEvent` is a relay-side wrapper
(`{event, received_at, channel_id, verified}`), never on the wire.

**Kinds are the only router.** A flat integer namespace, not endpoints. Ranges: ephemeral
`20000–29999` (never stored), replaceable `10000–19999`, parameterized-replaceable
`30000–39999` (keyed `(pubkey,kind,d_tag)`), Buzz-custom `40000+`. Add a feature = add a
`const` in `buzz-core/kind.rs`. `filters_match` (`filter.rs:10`): OR across filters, AND
within; supports kinds/authors/#tags/since/until and **prefix** id-matching; `limit`/`search`
are *not* matched (it's the live-fanout matcher only). A `#h` fallback derives the channel
from `StoredEvent.channel_id` for reactions/deletes with no explicit h-tag (`filter.rs:78-100`).

**The shared client is thin — and largely bypassed.** `NostrWsConnection`
(`buzz-ws-client/connection.rs`) does connect → `wait_for_auth_challenge` (20 s) →
`build_auth_event` (NIP-42) → `authenticate` (20 s) → `send_event` (30 s). But: **REQ has no
builder** (callers hand-build the frame), there is **zero reconnect/backoff** (100 % caller's
job), and timeouts are hardcoded consts. Tellingly, the **heaviest consumers hand-roll raw
`tokio-tungstenite`** — `buzz-acp/relay.rs` is a 6.3 K-line bespoke client. Lesson: one client
shape did not fit all; don't over-invest in a single shared transport.

**Auth reality check.**
- NIP-42 (`nip42.rs`) and NIP-98 (`nip98.rs`) both verify a signed event (±60 s), with a
  `Nip98ReplayGuard` (`buzz-pubsub/nip98_replay.rs`).
- **NIP-42 grants *all* scopes** (`all_known()`, `buzz-auth/lib.rs:149`) — scopes are
  effectively cosmetic; the *real* authorization is **NIP-29 channel membership**, enforced
  at the relay.
- `ChannelAccessChecker` (`buzz-auth/access.rs:31`) is **vestigial** — defined and tested,
  **zero production impls or callers**. Don't mistake the trait for the mechanism.

**The credential broker (`buzz-sdk/broker/`) — the pattern most relevant to you, but
unshipped.** A **custodial RPC proxy**: a remote agent would hold *only its pubkey + an opaque
bearer credential — no secret key, no relay connection*. A host does all signing.
`BrokerRequest → POST /v1/action, Authorization: Bearer …` over a **closed set of 9 named
actions** (ChannelRead/MessagePost/MessageReply/ReactionAdd/ProfileSet/StorageAddress/
AgentsCreate/AgentsUpdate/AgentsDelete — deliberately no generic `sign(bytes)`, so the host
applies per-op policy). Reads return real signed events (verifiable); writes return an
unsigned host attestation `{event_id,kind,created_at}`; `request_id` is the idempotency key.
**Status: scaffolding** — only test doubles, zero callers, the referenced `docs/agent-broker.md`
doesn't exist. **Copy the contract discipline, not the (nonexistent) implementation.**

---

## 4. The server — `buzz-relay`

One Axum+Tokio binary that terminates WebSocket (NIP-01) and a narrow HTTP surface.

**Boot & shape.** `main.rs:96 main()` builds `AppState` (`state.rs:630`, Arc-cloned into
every handler) and spawns ~10 background tasks (Redis subscriber, workflow cron, search
worker, GC/reaper…); `serve()` (`main.rs:1289`) binds listeners and orchestrates graceful
drain. `ConnectionManager` (`state.rs:237`) tracks live sockets and owns backpressure.

**Connection lifecycle** (`connection.rs:125`): accept → send NIP-42 challenge → register →
spawn three loops: `recv_loop` (`:466`, parse+dispatch), `send_loop` (`:327`, control-frame
priority + batched flush), `heartbeat_loop` (`:436`, 30 s ping / 3-miss disconnect),
coordinated by a `CancellationToken`. Slow clients use `try_send` with a grace counter.
Per-message Redis-backed admission at `:652`.

**Event pipeline** (`handlers/ingest.rs:2100 ingest_event` — a transport-neutral ~1 K-line
function shared by WS and HTTP): auth+scope → pubkey match → reject `KIND_AUTH` → ephemeral
route → `verify` (spawn_blocking) → membership → DB insert (`ON CONFLICT DO NOTHING`) →
mark-local → Redis publish → local fan-out → then fire-and-forget search index / audit /
workflow. Client gets `OK` at the *end*, not at DB insert.

**Subscription registry & the security invariant (the best idea in the relay).**
`SubscriptionRegistry` (`subscription.rs:85`) uses six DashMap indexes, all keyed by
`CommunityId` first, split into **structurally disjoint channel-scoped vs. global tiers**.
`fan_out_scoped` (`:379`) branches on `event.channel_id.is_some()` and consults *only* the
matching tier. **A channel subscription is never inserted into any global index and
vice-versa — so a private channel's events are invisible to global subscribers as an
*indexing invariant*, not a runtime check** (pinned by
`test_global_sub_does_not_receive_channel_events`, `:1352`). On top of that, delivery
re-validates *twice*: `push_match` (`:535`) re-fetches current filters/scope, and
`filter_fanout_by_access` (`event.rs:115`) independently re-checks per-recipient community +
membership (10 s-TTL cache, DB fallback, **fail-closed**) at send time. **Delivery never
trusts subscription-time state.** Copy this invariant above all others.

**Multi-node fan-out + dedup.** Every accepted write is delivered locally *and* published to
Redis after `mark_local_event()` (`state.rs:969`) records `(community_id, event_id)` in a 60 s
TTL cache. A background task subscribes to Redis on every pod and re-runs
`filter_fanout_by_access` before delivering to *its* sockets, skipping events already
delivered locally (`event.rs:282,301`). The dedup key includes `community_id` because the same
event id can legitimately exist in two communities.

**REQ** (`handlers/req.rs:51`): resolve accessible channels (10 s cache with **request-local
repair** of stale negatives at `:526` — fixes the "member just added on another pod" race),
gate p-gated/author-only kinds, divert NIP-50 `search` to Postgres FTS (`:584`, one-shot, not
registered for live), else register (`:277`) and stream historical results (bounded,
`buffered(4)`, in-order) then always `EOSE` (`:472`).

**HTTP bridge** reuses the *identical* ingest path: `api/bridge.rs` `submit_event` (`:703`) /
`query_events` (`:973`) / `count_events` (`:1503`) do NIP-98 auth → same `bind_community` →
same `ingest_event()`/REQ gate fns. **Byte-identical validation to WS.** Rest of the HTTP
surface: media, git smart-HTTP, NIP-05, webhooks, invites, admin (`router.rs:62-143`).

**Multi-tenancy** (`tenant.rs:71 bind_community`): normalize Host → empty fails closed (`:84`)
→ `HostResolver::resolve_host` queries `communities` → `TenantContext`, resolved **once** (WS
pre-upgrade at `router.rs:343`; each HTTP handler at its top) and threaded as an arg,
**never re-derived from client input**.

**`tunnel/`** is inter-*relay-pod* session affinity (Redis-fenced lease + generation +
forward-to-owner, `tunnel/directory.rs`, `tunnel/reliable.rs`) for stateful sessions (huddle)
that must stick to one pod under horizontal scale. It is **not** the client↔agent channel —
only relevant if you scale your own bus across nodes with node-pinned sessions.

**Extending it:** new kind → `const` in `kind.rs` → `required_scope_for_kind`
(`ingest.rs:437`) → scope flags → optional validator → `is_side_effect_kind`. New wire verb →
add to `ClientMessage`/`RelayMessage` (`protocol.rs:16/178`) + a dispatch arm
(`connection.rs:560`).

---

## 5. The data / eventing backbone

- **Postgres = the durable log.** `insert_event` (`store/event.rs:295`) is idempotent
  (`ON CONFLICT DO NOTHING` → `was_inserted` bool) — so agent retries are safe with no dedup
  service. Thread `reply_count`/`descendant_count` bumped in the *same transaction* as the
  insert (`GREATEST(x-1,0)` on delete, `store/thread.rs:121`). `query_events` (`:365`) is a
  pure `QueryBuilder` with `push_bind` (no interpolation); `kinds:[]` short-circuits to empty.
  Monthly range partitioning with a **table allowlist + regex-validated** suffixes (no raw
  DDL, `store/partition.rs`). Split into `runtime/`+`store/` behind a façade (`lib.rs:12`).
- **A portable read-replica freshness proof** (`runtime/replica_fence.rs`) — a heartbeat token
  + commit-time floor trigger that proves replica coverage **without the DB's native LSN**.
  Directly reusable on Aurora (which hides LSN on readers). Note: currently **ships disabled**;
  the read-your-writes gap is unsolved.
- **Redis = ephemeral only** (never a system of record): refcounted exact `SUBSCRIBE` fan-out
  (`subscriber.rs:37`), presence (`SET … EX 180` = 3× the 60 s heartbeat, `presence.rs:16`),
  typing (sorted sets), the Lua rate limiter (`rate_limiter.rs:24`), cache invalidation,
  cross-pod connection control. **Redis outage = fan-out blackout with the log intact** — an
  accepted degradation. Fan-out is a *wake-up nudge*, never the delivery guarantee.
- **Search** = a Postgres `GENERATED STORED` tsvector + GIN column (no separate index
  service). `ChannelScope` is an explicit 4-variant enum (`query.rs:44`); buzz-search returns
  **candidates that the relay re-authorizes** — the index is never itself the access decision.
- **Audit** = per-community hash chain (community_id hashed first blocks cross-chain replay,
  `hash.rs:42`), single-writer via `pg_advisory_lock` with `catch_unwind` releasing on panic
  (`service.rs:58`). 11 audit actions (doc says 10 — `MediaUploaded` was added).
- **Known bottlenecks:** no id-tiebreak index on the hot query path; the read-your-writes
  replica gap; Redis-outage fan-out blackout.

---

## 6. The agent runtime — how a body actually lives

**Three OS processes, two generic stdio protocols** (this is the reusable skeleton):
```
buzz-acp   (harness, 1 per agent identity) — the ONLY process that talks to the relay + holds Keys
   │  ACP = JSON-RPC 2.0 over NDJSON (stdio)
buzz-agent (LLM loop; pooled up to 32)     — transport-agnostic; works behind Zed/JetBrains too
   │  MCP = JSON-RPC 2.0 (stdio); fresh McpRegistry per session (full isolation)
buzz-dev-mcp (tools: shell, str_replace, todo, rg, tree)
```
`sprig` is a **multicall binary** bundling all three + `buzz-cli` + shims, dispatching on
`argv[0]` — a tiny deployment footprint (this is what ships in the remote pod image).

**Harness flow** (`buzz-acp`): `HarnessRelay::connect` + NIP-42 (`relay.rs:685`), a background
task owns the socket (`:1623`) → channel discovery via kind:39002/39000 (`:745`) → per-channel
REQ `#h=<ch>` (`:3249`) → a big **biased `select!` loop** (`lib.rs:2406`) multiplexing
results/steer/wake/relay events/inactivity/heartbeat/presence/shutdown. Per-channel
**single-flight**: an `in_flight_channels: HashSet<Uuid>` lock ensures ≤1 prompt per channel
(`queue.rs`); events batch (≤50, oldest-channel-first). Claim a pool agent (`pool.rs:771`) →
`run_prompt_task` (`:1869`) → ACP `session/prompt` (`acp.rs:777`).

**How replies get published — and the key-custody caveat.** The harness does **not** sign the
agent's replies. The LLM posts by invoking `buzz messages send --reply-to` as a **shell tool
call**. The harness injects `BUZZ_PRIVATE_KEY`/`RELAY_URL`/`AUTH_TAG` into the **MCP server's
spawn env** (`build_mcp_servers`, `lib.rs:5069`), and `buzz-dev-mcp`'s shell passes them
through despite `env_clear()` (`shell.rs:166`). **Consequence: any shell command the LLM runs
can read the raw nsec** (`env | grep BUZZ` prints it). Only the *separate* git-signing
`NOSTR_PRIVATE_KEY` is scrubbed (`shim.rs:51`). The harness self-signs *only* its own events
(presence/typing/reactions), with its `Keys` zeroed after config parse. So "the LLM never
touches the key" is too generous: the *LLM-loop process* holds none, but its *tool layer*
holds the root secret by design (the CLI needs it to post as the agent).
**Flag this loudly for your threat model.**

**LLM loop** (`buzz-agent/agent.rs:312 RunCtx::run`): drain steers → maybe handoff →
`llm.complete` (racing cancel + keepalive) → emit chunks → tool calls (`execute_calls`
`:799`, semaphore-bounded, per-call permission) → repeat. **Streaming text is reasoning —
only tool calls publish.** Context management: proactive **self-summarizing handoff at ~90 %
tokens** (`handoff.rs:244`) + a reactive **shrink-ladder** on provider-400 overflow (cap
3/turn, `:129`). **Provider swap = one env var** (`llm.rs:78` dispatches on a `Provider` enum +
a `TokenSource` trait).

**Tool sandbox** (`buzz-dev-mcp`): process-group/Job-Object kill via an RAII `KillGroup`
(`shell.rs:682`), bounded output (8 KB tail / 10 MB artifact / 50 MB hard cap, `:867`), workdir
resolution but **no jail** — the shell runs at the operator's trust level (like bash itself).

**Resilience:** claim/return pool (`AcpClient` is not `Clone`, index-stable slots) +
lazy-wake state machine (`pool_lifecycle.rs`, Listening/Waking/Ready/Failed) + **per-slot
circuit breaker** (`SlotCircuit`: 3 crashes/60 s → 5-min open, half-open probe,
`lib.rs:1461`) + off-loop respawn with an "always-sends-on-Drop" `RespawnGuard`. ACP idle vs.
hard deadlines (`acp.rs:1315`, with a pre-select `Instant::now()` check so the biased loop
can't starve the timer). Native steer via per-turn mpsc with a **cancel+merge fallback that
must always exist**. Inactivity self-reap + graceful shutdown (`lib.rs:1644,3003`: drain 30 s
+ 30 s grace + kill all clients + publish offline).

**Extend:** new LLM = env var; new tool = any MCP stdio server; new persona = drop a
`.persona.md` + `plugin.json`, no code (`buzz-persona/resolve.rs`).

---

## 7. The local client — desktop (Tauri + React)

**The split:** Rust owns OS integration, secrets, all agent-subprocess lifecycle, and the
**raw network sockets**; React owns the **entire Nostr protocol** (subscriptions, NIP-42
orchestration, event parsing/reducers) + all UI. IPC surface: **~300 Tauri commands**
(`src-tauri/lib.rs:519`) for request/response, and **Tauri `Channel` objects** (not
`app.emit`) for high-frequency streams (the relay socket, terminal frames).

**Two relay clients** (worth noting for your own design):
- **(A) TS-orchestrated over a dumb native pipe** — full NIP-42/subscribe/reconnect logic
  lives in TypeScript (`relayClientSession.ts:82`), using native `native_websocket.rs:181` only
  as a raw socket. *Why native at all?* The webview's own net stack bypasses the corporate VPN
  tunnel — so sockets must be native. Auth crosses IPC as **already-signed JSON only**
  (`handleAuthChallenge` → Rust `create_auth_event`, `commands/identity.rs:642`).
- **(B) Fully-native headless client** (`native_relay_client.rs:291`) for background Rust jobs
  (archive sync, catalog, unread catch-up) — its own reconnect/backoff state machine.

**Live sync:** inbound frames are coalesced into one IPC send over an 8 ms/7680-byte window
(`native_websocket_batch.rs:9`, to dodge Tauri's 8 KB fast-path fork; AUTH bypasses it).
`channel_head_cache.rs` (SQLite LRU) paints last-seen state instantly pre-subscribe;
`unread_catch_up.rs` fans out per-channel REQs natively. **Key custody:** precedence env → OS
keyring (one encrypted blob, cross-process `flock`, `secret_store.rs`) → `0600` file →
generate (`app_state.rs:330`); signing only via a narrow native API that refuses during
identity recovery.

**State:** command data → React Query; **live relay data → plain callbacks into feature
stores** (`observerRelayStore.ts`, not React Query); `useRelayAutoHeal` invalidates
relay-dependent queries on reconnect. **Community switching** = key-based remount
(`App.tsx:407,628`) + `resetCommunityState()` (`useCommunityInit.ts:54`) to clear module
singletons the remount can't touch — *convention-enforced, not type-checked*, so a forgotten
cache leaks across communities.

**Agents in the UI are Nostr identities, not process handles** — liveness is inferred from
`turn_liveness`/`turn_completed` **heartbeat events on the bus** (`activeAgentTurnsStore.ts`),
never a direct connection. Deploy is uniform: "This computer" vs. a provider dropdown both
funnel to `deploy_to_provider` (`WhereToRunSection.tsx:16`). The **terminal** feature is a
genuine *local* `$SHELL` via `portable-pty` (`terminal_runtime.rs:410`), not a console into a
remote agent.

---

## 8. THE CRUX — remote agents (deploy remotely, control only via the bus)

Your exact use case. Buzz has a rigorous formal spec (`docs/remote-agents.md`, 1779 lines,
`draft`) plus a real Kubernetes implementation. Here's the model and — critically — **what's
actually built**.

**Five principals:** Desktop `D` (trusted, holds nsec + UI) · Provider `P` (an *untrusted*
`buzz-backend-<id>` executable) · Substrate `S` (opaque; `D` never talks to it) · Agent `A`
(`buzz-acp` on `S`) · Relay `R` (**the only channel** `D`↔`A`).

**M1 — the axiom, enforced by *protocol shape*.** The desktop↔provider wire is exec+stdio with
a **closed 2-variant enum: `Info | Deploy`** (`buzz-backend-kubernetes/wire.rs:21`). No status,
exec, logs, or kill op is *expressible* without a type change. Grep-verified: the desktop's
entire provider surface is `discover` + `probe` (info) + `deploy` — no kill path anywhere.
After deploy, all control flows through the relay: status = presence, stop = `!shutdown`,
reconfigure = re-deploy. (M1 reduces *protocol surface*, not credential presence — the
kubeconfig still exists on `D`.)

**"Desktop is one launcher among many."** A live agent is just `buzz-acp` + keypair + NIP-OA
auth tag + relay URL as env. A bash script/systemd unit/CI job that sets those and execs the
harness is an equally valid launcher **today**. Three nested contracts: harness (binds every
launcher) → provider (binds provider-managed launches) → per-substrate binding policy.

**The provider protocol:**
- **Discovery** scans exe-dir + `PATH` + `~/.local/bin` for `buzz-backend-<id>`; first hit
  wins; discovery executes nothing (`backend.rs:593`).
- **Pre-secret staging gate (implemented, `backend.rs:507`):** resolve id once
  (`resolve_provider_binary:640`, "the ONLY way to resolve") → copy to a private `chmod 0500`
  staging file computing its digest → run `info` on the *staged bytes* → validate
  `protocol_version == 1` exactly (`validate_provider_info:13`; missing = hard error, no
  grandfathering) → run `deploy` on the *same staged bytes* → delete. Closes the
  check-then-exec race so the nsec only reaches the exact binary that answered `info`.
- **`info`** → `{name, version, protocol_version, config_schema}` (10 s). **`deploy`** ←
  `{agent:<payload>, provider_config}` → `{agent_id}` (600 s). Provider output is treated as
  **hostile** — every secret is redacted before storage/display.
- **Payload** carries the raw `private_key_nsec`, `auth_tag`, `relay_url`, merged `env_vars`,
  and a desktop-resolved **`launch` block** (command *name* not path, layered env,
  `policy_env`, owner pubkey; `agents_deploy.rs:197`) so the provider never re-derives desktop
  runtime logic. **Never persisted** — verified: the staged copy is a local dropped on return;
  only an opaque `backend_agent_id` display string persists. **Reserved-key rule:** `D` strips
  identity vars from `env_vars`; the provider must build identity from top-level fields.
- **No `undeploy` op in v1.** Deleting an agent orphans substrate objects (requires
  `force_remote_delete` confirmation); GC + self-reap bound the cost.

**Deploy = converge, not create.** A reconciliation loop keyed on the pubkey *derived from the
nsec* (`reconcile.rs:353`). Step 0 derive+verify identity; step 1 select by truncated label but
**verify the full-pubkey annotation + a `managed-by` marker** before any destructive action;
step 2 evaluate ordered rows against a pure `classify.rs:108` function. **Readiness = container
`state.running`, not pod phase** (`observe.rs:74`). Preconditioned deletes
(`uid`+`resourceVersion` = compare-and-delete). 409 disambiguated by `Status.reason`, never the
code. A non-secret **create-intent fingerprint** (unkeyed SHA-256 over the pod template,
`intent.rs:32`) detects config divergence cheaply. **One create attempt per call** (a measured
pathology: a naive retry loop minted 107 nsec-bearing Secrets in one 600 s call). Conflicts
converge (`adopt_winner:285`), never fail.

**Deploy-state-machine rows — ALL implemented except one:**

| Observed | Action | Status |
|---|---|---|
| deletion-marked | wait, re-enter | ✅ |
| no instance | create + verify startup | ✅ |
| terminated (Succeeded/Failed) | delete residue + recreate | ✅ |
| live & started | strict no-op | ✅ |
| never-started, provably broken | fenced delete + recreate | ✅ |
| never-started, pull-failing | report, never delete | ✅ |
| never-started, recoverable + matching fingerprint | observe (anti-livelock) | ✅ |
| never-started, recoverable + divergent fingerprint | fenced delete + recreate | ✅ |
| indefinite-lifetime (`OnFailure` restart) | — | ❌ **refused** (`config.rs:148`) — prerequisites (crash-loop classification, pinned exit-code contract) don't exist |

**The Kubernetes binding (`buzz-backend-kubernetes`) — faithful to the spec:**
- Hardened **bare Pod**, image's own entrypoint execs the harness as PID 1: `runAsNonRoot`
  UID/GID 10001, drop-ALL caps, `seccompProfile:RuntimeDefault`, no host namespaces,
  `restartPolicy:Never`, `terminationGracePeriodSeconds:60`, workspace `emptyDir` (mortal by
  design) (`pod.rs:66`, `config.rs:47`).
- nsec via a **per-attempt immutable Secret** `buzz-agent-<12hex>-<gen>`,
  `envFrom.secretRef{optional:false}` (`naming.rs:82`, `pod.rs:34`).
- **Image digest-pinned** (`name@sha256:…`; rejects all tag-only refs, `image.rs:37`).
- Deterministic pod name `buzz-agent-<first-12-hex-of-pubkey>` (`naming.rs:75`).
- GC preflight on every deploy deletes terminated pods + their Secret together; orphan-Secret
  age gate = 1200 s, judged on the **apiserver clock only**, never local time (`gc.rs:44`).

**Lifecycle:** self-reap via a pool-independent inactivity timer (`buzz-acp/lib.rs:2271`) firing
the same `shutdown_tx` channel as owner `!shutdown` (NIP-OA-verified, `lib.rs:2756`) and
SIGTERM; presence kind:20001 republished every 60 s (`lib.rs:2247`), relay `SET…EX 180`
(`buzz-pubsub/presence.rs:16`), clean exit does an immediate offline publish + relay close
(`lib.rs:3513`).

**Real-vs-aspirational scorecard (blunt):**

| Invariant | Status at HEAD |
|---|---|
| **I1** identity fail-closed | ✅ Real (`naming.rs:52`, `env.rs:223`) |
| **I2** no secrets in config | ✅ Real (schema has no cred field; `backend.rs:536`) |
| **I3** presence-is-status (≤180 s stale) | ✅ Real (numbers match spec) |
| **I4** one live instance per key per scope | ✅ Real — full state machine + ~30 tests |
| **I5** intentional termination final | ⚠️ **Partial** — bounded/self-reap real; indefinite (`OnFailure`) refused |
| Known Defect #6: pinned clean-exit=0 contract | ❌ **Open** (only `exit(1)` exists) → *why* I5 indefinite stays disabled |
| Known Defect #3: `launch` block | ✅ **Fixed** at HEAD |
| Known Defect #5: pre-secret staging gate | ✅ **Fixed** at HEAD |
| Windows provider `.exe` stripping | ❓ not re-verified |

The current code is *ahead* of the pinned-commit defect list in the spec.

**Conformance:** there is **no dedicated provider-conformance crate** (`buzz-conformance` is
actually the multi-tenant relay TLA+ trace checker — a *different* thing, gating
`Inv_NonInterference`). Provider conformance = ~30 unit tests driving the shipped `deploy()`
against a fake `Substrate` trait + golden stdio wire fixtures against the built binary. No
real-cluster (kind/envtest) suite exists; the spec's formal L1/L2/L3 checklist is written-only.

**Bundling:** the agent binaries ship as a **Tauri sidecar** now (`bundle-sidecars.sh`,
`tauri.conf.json`) — the doc's "bundling deferred" note is stale.

---

## 9. Honest assessment — risks & gaps to know before forking

- **Key custody is generous.** The provider binary receives the raw nsec (by design — the spec
  is explicit it can't contain a hostile provider). And the agent's own **shell tool sees
  `BUZZ_PRIVATE_KEY`** in its env. For tighter isolation, the (unshipped) broker pattern is the
  intended answer — build it.
- **Authorization ≈ channel membership, full stop.** NIP-42 grants all scopes;
  `ChannelAccessChecker` is vestigial. Membership checks at the relay are the real gate — and
  they're solid (delivery-time re-auth, fail-closed).
- **Docs drift.** `ARCHITECTURE.md` stale; `docs/remote-agents.md` a draft with a live
  "Known Defects" list. Confirm against code.
- **Scaling caveats.** No id-tiebreak index on the hot path; read-your-writes replica gap
  unsolved (replica routing ships disabled); Redis outage blacks out fan-out (log intact).
- **Feature gaps:** workflow approval gates don't persist/resume (WF-08); `send_dm`/
  `set_channel_topic` workflow actions are stubs (WF-07); huddle recording/tracks unbuilt;
  broker unshipped; remote indefinite-lifetime agents refused; Windows provider path unverified.

---

## 10. Build-your-own blueprint

**Copy wholesale (the crown jewels):**
1. **Bus-as-single-source-of-truth.** Durable append-only log (Postgres) + non-durable pub/sub
   (Redis) where fan-out is a *nudge*, not the delivery guarantee. Idempotent insert
   (`ON CONFLICT` → `was_inserted`) so client/agent retries are free.
2. **Delivery-time re-authorization.** Never trust subscription-time state; re-check access per
   recipient at send time, fail-closed. Plus the *indexing* invariant (private and global subs
   in structurally disjoint indexes).
3. **Identity(on-bus) vs. body(disposable), split at the type level.** The agent is a keypair
   + history; the machine is replaceable.
4. **"No backchannel" = a closed wire enum, not a policy.** Make status/kill *inexpressible* in
   the deploy protocol; route all post-deploy control through the bus (presence lease + a
   shutdown message).
5. **Deploy = idempotent converge** via a pure `(observed, desired) → action` function tested
   against a fake I/O trait; a **non-secret pre-mutation fingerprint** for cheap idempotency
   (no lock service); **never delete on timeout, only on evidence**; preconditioned deletes.
6. **Presence = heartbeat + TTL lease, clear-on-exit** → bounded staleness with no watchdog.
7. **The 3-process agent skeleton:** bus-client/pool-owner (holds the key) · bus-agnostic LLM
   loop (holds no key) · tool server — joined by two generic stdio JSON-RPC protocols.
   Provider-agnostic context handoff (proactive summarize + reactive shrink-ladder).
   Background-task-owns-the-socket. RAII guards on every async exit point.
8. **The zero-I/O core → key-free builders → thin transport → auth-verify-to-context** layering.

**Reconsider / simplify for a greenfield build:**
- **Do you need Nostr?** Keep the *shape* — signed events, `kind`-as-router, event-id-as-
  idempotency-key — but you can swap the wire format. Nostr buys an existing identity ecosystem
  and interop; it costs key-management UX friction and a flat kind namespace.
- **Do you need multi-tenancy on day one?** Buzz's is unconditional and adds real complexity
  (community-keyed everything, per-tenant audit chains, non-interference proofs). At N=1 it's
  invisible overhead — but retrofitting is painful, so decide early.
- **Tighter agent key custody.** Ship the **broker** (custodial signing, closed action set,
  bearer token) instead of handing agents the raw nsec, if agents run untrusted code.
- **The shared WS client** is under-powered (no reconnect/backoff, hardcoded timeouts) and
  bypassed by heavy consumers — build reconnect/backoff in from the start.
- **Providers:** the exec+stdio 2-op contract is excellent and substrate-agnostic — but write a
  *real* conformance harness (kind/envtest) and pin the **clean-exit=0 contract** first (it's
  the missing keystone for safe supervised restarts).

**The clean fork seams:** `buzz-core` (event/kind/filter/verify contract) · the `Substrate`
trait in `buzz-backend-kubernetes` (swap k8s for a VM/PaaS/SSH deployer — a systemd/SSH one
already exists) · the `Provider` enum + `TokenSource` in `buzz-agent` (swap LLMs) · MCP stdio
(swap tools) · the `HostResolver` trait (tenancy).

**A minimal recommended architecture for *your* system:**
- **Server:** one process = a signed-event log (Postgres) + WS pub/sub with delivery-time
  re-auth + a narrow HTTP mirror. Redis only when you need multi-node fan-out.
- **Client:** thin — connect, auth with a key, subscribe by filter, render; keep protocol logic
  in the app layer, sockets native only if a tunnel/VPN forces it.
- **Agent body:** the 3-process harness/LLM/tools skeleton, launched by *env + exec* so any
  launcher works; self-reaping; presence-as-lease.
- **Remoting:** a 2-op (`info`/`deploy`) provider contract; deploy = converge; control
  exclusively via the bus.

---

## 11. Where to read first (if you dive into the code)

1. `docs/remote-agents.md` — the formal model for your core interest (read it; it's excellent).
2. `crates/buzz-relay/src/handlers/{ingest,event,req}.rs` + `subscription.rs` — the bus core
   and the re-auth invariant.
3. `crates/buzz-acp/src/lib.rs` (the `select!` loop) + `crates/buzz-agent/src/agent.rs` — the
   agent runtime.
4. `crates/buzz-backend-kubernetes/src/{wire,reconcile,classify,pod}.rs` — the remoting
   mechanism.
5. `crates/buzz-core/src/{kind,filter,event,verification}.rs` — the wire contract.
6. `desktop/src/features/*` + `desktop/src-tauri/src/lib.rs` — the local client, if you're
   building an app layer.
