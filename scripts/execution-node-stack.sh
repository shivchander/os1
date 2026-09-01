#!/usr/bin/env bash
# execution-node-stack.sh — bring up a full, self-contained execution-node
# stack on a single Linux host (relay backend + node daemon), with NO Docker
# (uses podman) and NO desktop app for setup. Provisions BOTH agent runtimes —
# Codex and Claude Code — so the desktop can pick either, per agent.
#
# Validated bring-up gotchas baked in:
#   * headless nodes have no OS keychain -> BUZZ_NODE_FILE_KEYSTORE=1
#   * the relay must run this codebase (node kinds 39500-39503 registered)
#   * runtimes are ACP adapters (codex-acp / claude-agent-acp) wrapping the
#     codex / claude CLIs; buzz-acp execs the adapter the assignment names
#   * the daemon needs buzz-acp + the adapters + the CLIs on its PATH
#   * rootless podman here uses the ROOT socket -> drive it with `sudo podman`
#
# Provider keys are loaded into the node's provider secret store, so every
# node-hosted agent inherits them; the desktop's per-agent runtime choice
# decides which is used (Codex -> OPENAI_API_KEY, Claude Code -> ANTHROPIC_API_KEY).
#
# Claude Code via Vertex: instead of an ANTHROPIC_API_KEY, Claude can auth to
# Google Vertex AI. Set ANTHROPIC_VERTEX_PROJECT_ID and copy your Google ADC to
# the node; the `vertex` step installs the ADC (0600) and records the Vertex env
# (CLAUDE_CODE_USE_VERTEX / project / region / model) in NodeConfig.agent_env, so
# every node-hosted claude-agent-acp agent talks to Vertex. See VERTEX_* below.
#
# Usage:
#   OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-... \
#     ./scripts/execution-node-stack.sh all
#   # Claude via Vertex (copy your ADC to the node first):
#   ANTHROPIC_VERTEX_PROJECT_ID=my-proj VERTEX_ADC=~/adc.json \
#     ./scripts/execution-node-stack.sh all
#   ./scripts/execution-node-stack.sh backend|relay|runtimes|enroll|secrets|vertex|up|assign|status|down
#
# Idempotent: safe to re-run (e.g. after a reboot — `all` restarts everything;
# enrollment persists on disk so it is not redone).
set -euo pipefail

# ── Config (override via env) ──────────────────────────────────────────────
REPO_DIR="${REPO_DIR:-$HOME/os1}"
RELAY_URL="${RELAY_URL:-ws://localhost:3000}"
NODE_HOME="${NODE_HOME:-$HOME/.buzz-node}"
OPENAI_API_KEY="${OPENAI_API_KEY:-}"        # Codex provider key (optional)
ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}"  # Claude Code provider key (optional)
AGENT_COMMAND="${BUZZ_ACP_AGENT_COMMAND:-codex-acp}"  # only the dev `assign` smoke-test uses this

# Claude Code via Google Vertex AI (alternative to ANTHROPIC_API_KEY). The
# `vertex` step is a no-op unless ANTHROPIC_VERTEX_PROJECT_ID is set. Model IDs
# mirror the desktop's defaults and are override-able for other Vertex projects.
ANTHROPIC_VERTEX_PROJECT_ID="${ANTHROPIC_VERTEX_PROJECT_ID:-}"  # GCP project serving Claude on Vertex (enables `vertex`)
CLOUD_ML_REGION="${CLOUD_ML_REGION:-global}"                    # Vertex region
VERTEX_ADC="${VERTEX_ADC:-$HOME/.config/gcloud/application_default_credentials.json}"  # Google ADC JSON on the node to install
VERTEX_ANTHROPIC_MODEL="${ANTHROPIC_MODEL:-claude-opus-4-8[1m]}"                 # default model for node claude agents
VERTEX_SONNET_MODEL="${ANTHROPIC_DEFAULT_SONNET_MODEL:-claude-sonnet-5[1m]}"     # `sonnet` alias -> Vertex model
VERTEX_OPUS_MODEL="${ANTHROPIC_DEFAULT_OPUS_MODEL:-claude-opus-4-8[1m]}"         # `opus` alias -> Vertex model
VERTEX_HAIKU_MODEL="${ANTHROPIC_DEFAULT_HAIKU_MODEL:-claude-haiku-4-5@20251001}" # `haiku` alias -> Vertex model
PODMAN="${PODMAN:-sudo podman}"                          # rootless here needs sudo
PG_IMAGE="${PG_IMAGE:-docker.io/library/postgres:16}"
REDIS_IMAGE="${REDIS_IMAGE:-docker.io/library/redis:7}"
MINIO_IMAGE="${MINIO_IMAGE:-docker.io/minio/minio}"
MC_IMAGE="${MC_IMAGE:-docker.io/minio/mc}"

# The daemon's PATH must resolve buzz-node/buzz-acp (repo target) plus the ACP
# adapters + underlying CLIs. The npm global bin holds the adapters; the
# curl-installed CLIs land in ~/.local/bin (or their own ~/.codex|~/.claude bin).
NPM_GLOBAL_BIN="${NPM_GLOBAL_BIN:-$(npm prefix -g 2>/dev/null || true)/bin}"
DAEMON_PATH="$REPO_DIR/target/debug:$NPM_GLOBAL_BIN:$HOME/.local/bin:$HOME/.codex/bin:$HOME/.claude/bin:$HOME/bin:$PATH"

log() { printf '\033[36m==>\033[0m %s\n' "$*"; }
die() { printf '\033[31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

require_build() {
  [ -x "$REPO_DIR/target/debug/buzz-node" ] || die "build first: (cd $REPO_DIR && cargo build -p buzz-node -p buzz-acp -p buzz-relay --example owner_driver)"
  [ -x "$REPO_DIR/target/debug/buzz-relay" ] || die "buzz-relay not built (see above)"
}

# Install both ACP runtimes (adapters + underlying CLIs) so the node can host
# either Codex or Claude Code. buzz-node advertises whichever adapters it finds
# on PATH (NodeCapabilities.runtimes), so this is what makes both selectable.
runtimes() {
  log "installing ACP runtimes: Codex + Claude Code (adapters + CLIs)"
  command -v npm >/dev/null 2>&1 || die "npm is required to install the ACP adapters"
  npm install -g @agentclientprotocol/codex-acp @agentclientprotocol/claude-agent-acp 2>&1 | tail -2 \
    || log "WARNING: adapter npm install failed — install them manually"
  if ! PATH="$DAEMON_PATH" command -v codex >/dev/null 2>&1; then
    log "installing codex CLI"
    curl -fsSL https://chatgpt.com/codex/install.sh | sh \
      || log "WARNING: codex CLI install failed (see https://developers.openai.com/codex/cli/)"
  fi
  if ! PATH="$DAEMON_PATH" command -v claude >/dev/null 2>&1; then
    log "installing claude CLI"
    curl -fsSL https://claude.ai/install.sh | bash \
      || log "WARNING: claude CLI install failed (see https://code.claude.com/docs)"
  fi
  # The ACP adapters are Node scripts (`#!/usr/bin/env node`), so the daemon must
  # resolve BOTH the adapter AND `node` when it spawns them. A headless daemon
  # PATH (login/cron, or a hand-rolled launcher) may not carry the fnm/nvm node,
  # so pin node + the adapters into ~/.local/bin, which is always on DAEMON_PATH.
  mkdir -p "$HOME/.local/bin"
  if nb="$(PATH="$DAEMON_PATH" command -v node 2>/dev/null)"; then
    ln -sf "$(readlink -f "$nb")" "$HOME/.local/bin/node"
  fi
  for a in codex-acp claude-agent-acp; do
    if p="$(PATH="$DAEMON_PATH" command -v "$a" 2>/dev/null)"; then
      ln -sf "$(readlink -f "$p")" "$HOME/.local/bin/$a"
    fi
  done
  log "daemon-PATH runtime check:"
  for bin in buzz-acp codex-acp codex claude-agent-acp claude; do
    if PATH="$DAEMON_PATH" command -v "$bin" >/dev/null 2>&1; then
      log "  ✓ $bin"
    else
      log "  ✗ $bin NOT on daemon PATH"
    fi
  done
}

# Load provider API keys into the node's secret store and register them in the
# node config so the daemon injects them into every agent's environment. Run
# after `enroll` (needs config.json) and before `up`.
secrets() {
  [ -f "$NODE_HOME/config.json" ] || die "enroll first (no $NODE_HOME/config.json)"
  command -v python3 >/dev/null 2>&1 || die "python3 is required to update node providers"
  local dir="$NODE_HOME/provider-secrets"
  mkdir -p "$dir"
  chmod 700 "$dir"
  local loaded=()
  if [ -n "$OPENAI_API_KEY" ]; then
    printf '%s' "$OPENAI_API_KEY" > "$dir/provider_openai"
    chmod 600 "$dir/provider_openai"
    loaded+=(openai)
    log "loaded provider_openai"
  fi
  if [ -n "$ANTHROPIC_API_KEY" ]; then
    printf '%s' "$ANTHROPIC_API_KEY" > "$dir/provider_anthropic"
    chmod 600 "$dir/provider_anthropic"
    loaded+=(anthropic)
    log "loaded provider_anthropic"
  fi
  if [ ${#loaded[@]} -eq 0 ]; then
    log "WARNING: neither OPENAI_API_KEY nor ANTHROPIC_API_KEY set — no provider keys loaded"
    return
  fi
  # Merge the loaded providers into NodeConfig.providers so the daemon resolves
  # their keys from the store into node_env (build_child_env base layer).
  python3 - "$NODE_HOME/config.json" "${loaded[@]}" <<'PY'
import json, sys
path, want = sys.argv[1], set(sys.argv[2:])
cfg = json.load(open(path))
cfg["providers"] = sorted(set(cfg.get("providers", [])) | want)
with open(path, "w") as f:
    json.dump(cfg, f, indent=2)
print("providers:", cfg["providers"])
PY
  log "node providers updated (restart 'up' to apply)"
}

# Claude Code via Vertex: install the Google ADC on the node (0600) and record
# the Vertex env in NodeConfig.agent_env, so every node-hosted claude-agent-acp
# agent authenticates to Vertex instead of using ANTHROPIC_API_KEY. The node's
# claude CLI reads these vars and google-auth refreshes tokens itself (no gcloud
# needed at runtime). Run after `enroll` (needs config.json), before `up`.
# No-op unless ANTHROPIC_VERTEX_PROJECT_ID is set.
vertex() {
  [ -n "$ANTHROPIC_VERTEX_PROJECT_ID" ] || { log "ANTHROPIC_VERTEX_PROJECT_ID unset — skipping Vertex provisioning"; return; }
  [ -f "$NODE_HOME/config.json" ] || die "enroll first (no $NODE_HOME/config.json)"
  command -v python3 >/dev/null 2>&1 || die "python3 is required to update agent_env"
  [ -f "$VERTEX_ADC" ] || die "Google ADC not found at VERTEX_ADC=$VERTEX_ADC — copy it to the node first (e.g. scp ~/.config/gcloud/application_default_credentials.json node:.config/gcloud/)"
  local dest="$HOME/.config/gcloud/application_default_credentials.json"
  mkdir -p "$(dirname "$dest")"
  if [ "$(readlink -f "$VERTEX_ADC")" != "$(readlink -f "$dest" 2>/dev/null || true)" ]; then
    install -m 600 "$VERTEX_ADC" "$dest"
  else
    chmod 600 "$dest"
  fi
  log "installed Vertex ADC at $dest (0600)"
  # Merge the Vertex env into NodeConfig.agent_env (the daemon injects it into
  # every agent subprocess). Model IDs match the desktop's alias resolution.
  ANTHROPIC_VERTEX_PROJECT_ID="$ANTHROPIC_VERTEX_PROJECT_ID" CLOUD_ML_REGION="$CLOUD_ML_REGION" \
  GAC="$dest" VMODEL="$VERTEX_ANTHROPIC_MODEL" VSONNET="$VERTEX_SONNET_MODEL" \
  VOPUS="$VERTEX_OPUS_MODEL" VHAIKU="$VERTEX_HAIKU_MODEL" \
  python3 - "$NODE_HOME/config.json" <<'PY'
import json, os, sys
path = sys.argv[1]
cfg = json.load(open(path))
env = cfg.get("agent_env") or {}
env.update({
    "CLAUDE_CODE_USE_VERTEX": "1",
    "ANTHROPIC_VERTEX_PROJECT_ID": os.environ["ANTHROPIC_VERTEX_PROJECT_ID"],
    "CLOUD_ML_REGION": os.environ["CLOUD_ML_REGION"],
    "GOOGLE_APPLICATION_CREDENTIALS": os.environ["GAC"],
    "ANTHROPIC_MODEL": os.environ["VMODEL"],
    "ANTHROPIC_DEFAULT_SONNET_MODEL": os.environ["VSONNET"],
    "ANTHROPIC_DEFAULT_OPUS_MODEL": os.environ["VOPUS"],
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": os.environ["VHAIKU"],
})
cfg["agent_env"] = env
with open(path, "w") as f:
    json.dump(cfg, f, indent=2)
print("vertex agent_env set (project=%s region=%s model=%s)"
      % (os.environ["ANTHROPIC_VERTEX_PROJECT_ID"], os.environ["CLOUD_ML_REGION"], os.environ["VMODEL"]))
PY
  log "Vertex agent_env recorded (restart 'up' to apply)"
}

backend() {
  log "starting podman containers (pg/redis/minio)"
  $PODMAN start buzz-pg 2>/dev/null || $PODMAN run -d --name buzz-pg -p 5432:5432 \
    -e POSTGRES_USER=buzz -e POSTGRES_PASSWORD=buzz_dev -e POSTGRES_DB=buzz "$PG_IMAGE"
  $PODMAN start buzz-redis 2>/dev/null || $PODMAN run -d --name buzz-redis -p 6379:6379 "$REDIS_IMAGE"
  $PODMAN start buzz-minio 2>/dev/null || $PODMAN run -d --name buzz-minio -p 9000:9000 \
    -e MINIO_ROOT_USER=buzz_dev -e MINIO_ROOT_PASSWORD=buzz_dev_secret "$MINIO_IMAGE" server /data
  sleep 5
  log "ensuring media bucket"
  $PODMAN run --rm --network host -e MC_HOST_local=http://buzz_dev:buzz_dev_secret@localhost:9000 \
    "$MC_IMAGE" mb -p local/buzz-media 2>&1 | tail -1 || true
  $PODMAN ps --format '{{.Names}} {{.Status}}'
}

ensure_env() {
  cd "$REPO_DIR"
  [ -f .env ] || cp .env.example .env
  grep -q '^BUZZ_RELAY_PRIVATE_KEY=' .env || echo "BUZZ_RELAY_PRIVATE_KEY=$(openssl rand -hex 32)" >> .env
  grep -q '^BUZZ_AUTO_MIGRATE=' .env && sed -i 's/^BUZZ_AUTO_MIGRATE=.*/BUZZ_AUTO_MIGRATE=true/' .env || echo "BUZZ_AUTO_MIGRATE=true" >> .env
  # RELAY_URL is the relay's canonical host: it bootstraps the community
  # (tenant binding) AND is used in NIP-42 auth challenges. Every participant —
  # relay, node agent, owner_driver, desktop — must address the relay by this
  # exact host, so set it here from the script's RELAY_URL (e.g. a Tailscale IP).
  grep -q '^RELAY_URL=' .env && sed -i "s|^RELAY_URL=.*|RELAY_URL=$RELAY_URL|" .env || echo "RELAY_URL=$RELAY_URL" >> .env
}

relay() {
  require_build; ensure_env
  log "starting relay (detached)"
  pkill -9 -x buzz-relay 2>/dev/null || true; sleep 1
  cd "$REPO_DIR"
  ( set -o allexport; . ./.env; set +o allexport; setsid ./target/debug/buzz-relay >/tmp/relay.log 2>&1 </dev/null & disown )
  for _ in $(seq 1 20); do ss -ltn 2>/dev/null | grep -q :3000 && { log "relay LISTENING on :3000"; return; }; sleep 1; done
  tail -15 /tmp/relay.log; die "relay did not bind :3000"
}

enroll() {
  require_build
  if [ -f "$NODE_HOME/config.json" ]; then
    if grep -qF "$RELAY_URL" "$NODE_HOME/config.json"; then
      log "already enrolled at $RELAY_URL — skipping"; return
    fi
    # Host changed (e.g. localhost -> Tailscale IP): the community is per-host,
    # so the old enrollment is in a different community. Re-provision the node
    # identity under the new host. owner_driver's owner/agent keys are kept.
    log "config relay_url != $RELAY_URL — re-provisioning node under new host"
    pkill -9 -x buzz-node 2>/dev/null || true
    pkill -9 -x buzz-acp 2>/dev/null || true
    pkill -9 -x codex 2>/dev/null || true
    pkill -9 -x claude-agent-acp 2>/dev/null || true
    pkill -9 -x claude 2>/dev/null || true
    rm -f "$NODE_HOME/config.json" "$NODE_HOME/secrets/node-key"
  fi
  cd "$REPO_DIR"
  log "enrolling node"
  : > /tmp/enroll.log
  BUZZ_NODE_FILE_KEYSTORE=1 setsid ./target/debug/buzz-node enroll --relay-url "$RELAY_URL" >/tmp/enroll.log 2>&1 </dev/null & disown
  for _ in $(seq 1 15); do grep -q 'Node pubkey:' /tmp/enroll.log && break; sleep 1; done
  local node_pk; node_pk="$(awk '/Node pubkey:/{print $3; exit}' /tmp/enroll.log)"
  [ -n "$node_pk" ] || { tail -5 /tmp/enroll.log; die "node did not announce (is the relay this codebase? kinds 39500-39503)"; }
  log "approving node $node_pk (owner_driver)"
  ./target/debug/examples/owner_driver enroll "$RELAY_URL" "$node_pk"
  for _ in $(seq 1 10); do grep -q 'enrolled' /tmp/enroll.log && { log "enrolled"; return; }; sleep 1; done
  tail -5 /tmp/enroll.log; die "enrollment not confirmed"
}

up() {
  require_build
  log "starting node daemon (detached)"
  pkill -9 -x buzz-node 2>/dev/null || true; sleep 1
  cd "$REPO_DIR"; : > /tmp/up.log
  BUZZ_NODE_FILE_KEYSTORE=1 RUST_LOG="${RUST_LOG:-info}" PATH="$DAEMON_PATH" \
    setsid ./target/debug/buzz-node up --foreground >/tmp/up.log 2>&1 </dev/null & disown
  sleep 3
  pgrep -x buzz-node >/dev/null && log "daemon up (logs: /tmp/up.log)" || { tail -10 /tmp/up.log; die "daemon exited"; }
}

# Optional dev smoke-test: assign a Codex agent via owner_driver (bypasses the
# desktop). The real path is creating agents in the app, which drives the
# per-agent runtime choice. Kept for headless verification.
assign() {
  require_build
  [ -n "$OPENAI_API_KEY" ] || die "set OPENAI_API_KEY to assign the codex smoke-test agent"
  [ -f "$NODE_HOME/config.json" ] || die "enroll first"
  local node_pk; node_pk="$(grep -o '"node_pubkey": *"[0-9a-f]*"' "$NODE_HOME/config.json" | grep -o '[0-9a-f]\{64\}')"
  log "assigning codex smoke-test agent ($AGENT_COMMAND) to node $node_pk"
  cd "$REPO_DIR"
  BUZZ_ACP_AGENT_COMMAND="$AGENT_COMMAND" OPENAI_API_KEY="$OPENAI_API_KEY" \
    ./target/debug/examples/owner_driver assign "$RELAY_URL" "$node_pk"
}

status() {
  echo "--- containers ---"; $PODMAN ps --format '{{.Names}} {{.Status}}' 2>/dev/null || true
  echo "--- relay :3000 ---"; ss -ltn 2>/dev/null | grep -q :3000 && echo LISTENING || echo DOWN
  echo "--- daemon ---"; pgrep -x buzz-node >/dev/null && echo UP || echo DOWN
  echo "--- advertised runtimes (adapters on daemon PATH) ---"
  for bin in codex-acp claude-agent-acp; do
    PATH="$DAEMON_PATH" command -v "$bin" >/dev/null 2>&1 && echo "$bin: present" || echo "$bin: MISSING"
  done
  echo "--- providers (node config) ---"; grep -o '"providers":[^]]*]' "$NODE_HOME/config.json" 2>/dev/null || echo "(none)"
  echo "--- vertex (claude auth) ---"
  if grep -q '"CLAUDE_CODE_USE_VERTEX"' "$NODE_HOME/config.json" 2>/dev/null; then
    python3 - "$NODE_HOME/config.json" <<'PY' 2>/dev/null || echo "(config parse failed)"
import json, os, sys
env = json.load(open(sys.argv[1])).get("agent_env") or {}
adc = env.get("GOOGLE_APPLICATION_CREDENTIALS", "")
print("  project:", env.get("ANTHROPIC_VERTEX_PROJECT_ID"),
      "region:", env.get("CLOUD_ML_REGION"), "model:", env.get("ANTHROPIC_MODEL"))
print("  ADC:", adc, "(present)" if adc and os.path.exists(adc) else "(MISSING)")
PY
  else
    echo "  (not configured; using ANTHROPIC_API_KEY or unset)"
  fi
  echo "--- agent procs ---"; pgrep -af 'codex-acp|claude-agent-acp' | grep -v pgrep || echo "(none)"
  echo "--- agent health (6s) ---"; cd "$REPO_DIR" && timeout 9 ./target/debug/examples/owner_driver observe "$RELAY_URL" 6 2>&1 | tail -4 || true
}

down() {
  log "stopping daemon + relay (containers left running; use '$PODMAN stop buzz-pg buzz-redis buzz-minio' to stop them)"
  pkill -9 -x buzz-node 2>/dev/null || true
  pkill -9 -x buzz-relay 2>/dev/null || true
}

case "${1:-}" in
  backend)  backend ;;
  relay)    relay ;;
  runtimes) runtimes ;;
  enroll)   enroll ;;
  secrets)  secrets ;;
  vertex)   vertex ;;
  up)       up ;;
  assign)   assign ;;
  status)   status ;;
  down)     down ;;
  all)      backend; relay; runtimes; enroll; secrets; vertex; up; sleep 6; status ;;
  *) die "usage: $0 {backend|relay|runtimes|enroll|secrets|vertex|up|assign|status|down|all}" ;;
esac
