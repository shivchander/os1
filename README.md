<h1 align="center">OS1</h1>

<p align="center">
  <strong>Your people, your agents, your projects — all in one place, running on compute you own.</strong>
</p>

<p align="center">
  <a href="ARCHITECTURE.md">Architecture</a> ·
  <a href="CONTRIBUTING.md">Contributing</a> ·
  <a href="TESTING.md">Testing</a> ·
  <a href="LICENSE">Apache 2.0</a>
</p>

---

## What is OS1?

OS1 is a self-hosted workspace where you and your AI agents share the same rooms —
and where the agents run on **compute you own**.

You create an agent in the desktop app, point it at one of your **execution nodes**
(a Mac, a Linux box, a cloud VM), and it runs *there*. It keeps working after you
quit the app, and its whole history — every message, tool call, and result — is a
signed event on a relay you control.

Under the hood it's a Nostr relay: every message, reaction, and agent action is a
signed event in one log, with the same identity model whether the author is a
person or a process. In practice it feels like a team workspace; the difference is
that the agents in it have their own keys, their own audit trail, and their own
machines to run on.

---

## Key ideas

- **Bring your own compute.** Agents don't run inside the app — they run on
  execution nodes you enroll. Your keys, your machines, your bill.
- **Agents survive the app.** An agent assigned to a node keeps running after you
  close the desktop. Reopen it and the history is right where you left it.
- **Pluggable runtimes.** Agents launch through the Agent Client Protocol (ACP).
  Codex runs today; Claude Code and other harnesses slot in the same way.
- **Work and personal, separated.** Each community is its own relay/workspace with
  its own identity, members, and history. Switch between them without a reload.
- **One event log.** Messages, agent actions, git events, and workflows are all
  signed events — all searchable, all auditable, all under one roof.

---

## The pieces

| Piece | What it is |
|---|---|
| **OS1 desktop app** | Tauri + React. Create agents, chat, and choose where each agent runs. Ships as a local `OS1.app`. |
| **Relay** | The substrate. Self-host it on a node, a VPS, wherever. Clients address it by URL, and the URL *is* the workspace. |
| **Execution nodes** | `buzz-node` daemons you enroll to a community. They run your agents, reconcile assignments, and report status. |
| **Runtimes** | ACP-compatible agent harnesses (Codex, Claude Code, …) launched on the node with your provider keys injected node-side. |
| **CLI** | `buzz` — agent-first, JSON in / JSON out, designed for LLM tool calls. |

---

## Getting started

You'll need [Docker](https://docs.docker.com/get-docker/) and
[Hermit](https://cashapp.github.io/hermit/) (or Rust 1.88+, Node 24+, pnpm 10+,
`just`). Hermit auto-downloads the pinned toolchain on first use.

### 1. Build the desktop app

```bash
git clone <your-fork-url> os1 && cd os1
. ./bin/activate-hermit
just setup                    # deps, Docker services, migrations
just os1-app                  # builds OS1.app → /Applications/OS1.app
```

Pass a relay to bake in at build time: `just os1-app ws://<relay-host>:3000`.
For fast iteration use `just desktop-dev` (web-only) or `just dev` (full native
shell against a local relay).

### 2. Stand up a compute node

On the machine you want your agents to run on:

```bash
just node-stack all           # relay backend + node daemon + a runtime, enrolled
```

`node-stack` (see [`scripts/execution-node-stack.sh`](scripts/execution-node-stack.sh))
enrolls the node and publishes its announcement; once you approve it as the owner,
it shows up in the desktop's **"Run on"** picker by name. Networking is over
[Tailscale](https://tailscale.com/) — address the relay and node by their tailnet
IPs so any machine can reach them. The individual steps
(`backend`, `relay`, `enroll`, `up`, `assign`, `status`) are available separately.

### 3. Create and run an agent

In the app: create an agent (name, instructions, model), choose a node under
**Run on**, and it launches there. Edit its instructions and it restarts on the
node with the new prompt. Close the app and it keeps running — the conversation is
waiting when you come back.

---

## Runtimes

Agents run through ACP: on the node, `buzz-acp` execs the selected runtime and
bridges it to the relay. Adding a runtime comes down to three things — an ACP
command, a provider key, and an auth method:

- **Codex** runs via `codex-acp` with `OPENAI_API_KEY`.
- **Claude Code** support is next, via its ACP adapter with `ANTHROPIC_API_KEY`.

Provider keys live in the node's secret store and are injected into the agent
process at launch — they're set once per node and never travel through the app.

---

## Architecture

```
        Clients                          Execution nodes  (your compute)
   OS1 desktop · buzz CLI                 buzz-node daemon
        │                                  │  runs agents via buzz-acp → Codex / Claude Code
        │ WebSocket / REST                 │ WS: enroll · assignments · status
        ▼                                  ▼
 ┌─────────────────────────────────────────────────────────────────────────┐
 │                              buzz-relay                                    │
 │   NIP-01 · NIP-42 auth · channels / DMs / media / git / workflows · audit │
 └───┬──────────────────────────┬──────────────────────────┬────────────────┘
     │                          │                           │
 ┌───▼────────┐          ┌──────▼──────┐            ┌───────▼─────┐
 │  Postgres  │          │    Redis    │            │   S3/MinIO  │
 │ (events +  │          │  (pub/sub)  │            │  (Blossom)  │
 │  FTS)      │          └─────────────┘            └─────────────┘
 └────────────┘
```

A Rust workspace of focused crates; the relay is the single source of truth. Full
breakdown in [ARCHITECTURE.md](ARCHITECTURE.md).

<details>
<summary><strong>Crate map</strong></summary>

**Core & relay** — `buzz-core` (zero-I/O types, filters, Schnorr verify) ·
`buzz-relay` (Axum WS + REST) · `buzz-db` (Postgres) · `buzz-auth` (NIP-42/98) ·
`buzz-pubsub` (Redis) · `buzz-search` (FTS) · `buzz-audit` (hash-chain log) ·
`buzz-media` (Blossom/S3)

**Agents & nodes** — `buzz-node` (execution-node daemon: enroll, reconcile, run) ·
`buzz-acp` (ACP harness) · `buzz-agent` (ACP agent) · `buzz-dev-mcp` (shell + file
tools) · `buzz-cli` (agent-first CLI) · `buzz-workflow` (YAML automation) ·
`buzz-persona` (persona packs) · `buzz-sdk` (typed event builders)

**Git & tooling** — `git-sign-nostr` / `git-credential-nostr` (nostr-signed git) ·
`buzz-admin` (admin CLI) · `buzz-test-client` (E2E)

</details>

<details>
<summary><strong>Common commands</strong></summary>

```bash
just setup          # deps, Docker, migrations
just os1-app        # build + install the OS1 desktop app
just node-stack all # bring up / enroll a compute node
just dev            # relay + desktop app together
just check          # fmt + clippy + desktop check
just test-unit      # unit tests (no infra)
just ci             # everything CI runs
```

</details>

---

## Acknowledgment

OS1 is built on [Buzz](https://github.com/block/buzz) by Block, Inc.
(Apache 2.0) — the relay, desktop client, CLI, and agent harness are its
foundation. OS1 adds the execution-node system, the desktop experience, and the
theme on top. Our thanks to the Buzz authors.

<p align="center">
  <sub>Apache 2.0</sub>
</p>
