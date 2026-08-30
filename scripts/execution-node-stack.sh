#!/usr/bin/env bash
# execution-node-stack.sh — bring up a full, self-contained execution-node
# stack on a single Linux host (relay backend + node daemon + a codex agent),
# with NO Docker required (uses podman) and NO desktop app (uses the
# owner_driver example to drive enroll/assign over the relay).
#
# This captures the exact, validated bring-up sequence — including the gotchas
# a first live run surfaced:
#   * headless nodes have no OS keychain -> BUZZ_NODE_FILE_KEYSTORE=1 (Fix A)
#   * the relay must run this branch (kinds 39500-39503 are registered — Fix C)
#   * codex has no ACP mode; the adapter is @agentclientprotocol/codex-acp
#   * the daemon needs buzz-acp + codex on its PATH
#   * rootless podman here uses the ROOT socket -> drive it with `sudo podman`
#
# Usage:
#   OPENAI_API_KEY=sk-... ./scripts/execution-node-stack.sh all
#   ./scripts/execution-node-stack.sh backend|relay|enroll|up|assign|status|down
#
# Idempotent: safe to re-run (e.g. after a reboot — `all` restarts everything;
# enrollment persists on disk so it is not redone).
set -euo pipefail

# ── Config (override via env) ──────────────────────────────────────────────
REPO_DIR="${REPO_DIR:-$HOME/os1}"
RELAY_URL="${RELAY_URL:-ws://localhost:3000}"
NODE_HOME="${NODE_HOME:-$HOME/.buzz-node}"
AGENT_COMMAND="${BUZZ_ACP_AGENT_COMMAND:-codex-acp}"   # ACP adapter binary
PODMAN="${PODMAN:-sudo podman}"                          # rootless here needs sudo
PG_IMAGE="${PG_IMAGE:-docker.io/library/postgres:16}"
REDIS_IMAGE="${REDIS_IMAGE:-docker.io/library/redis:7}"
MINIO_IMAGE="${MINIO_IMAGE:-docker.io/minio/minio}"
MC_IMAGE="${MC_IMAGE:-docker.io/minio/mc}"

# Node/codex binaries must be on the daemon's PATH:
#   - $REPO_DIR/target/debug            -> buzz-node, buzz-acp
#   - the fnm node bin (codex, codex-acp installed globally there)
FNM_NODE_BIN="${FNM_NODE_BIN:-$(dirname "$(readlink -f "$(command -v codex 2>/dev/null || true)" 2>/dev/null || true)" 2>/dev/null || true)}"
DAEMON_PATH="$REPO_DIR/target/debug:${FNM_NODE_BIN:+$FNM_NODE_BIN:}$PATH"

log() { printf '\033[36m==>\033[0m %s\n' "$*"; }
die() { printf '\033[31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

require_build() {
  [ -x "$REPO_DIR/target/debug/buzz-node" ] || die "build first: (cd $REPO_DIR && cargo build -p buzz-node -p buzz-acp -p buzz-relay --example owner_driver)"
  [ -x "$REPO_DIR/target/debug/buzz-relay" ] || die "buzz-relay not built (see above)"
  command -v "$AGENT_COMMAND" >/dev/null 2>&1 || log "WARNING: $AGENT_COMMAND not on PATH (npm i -g @agentclientprotocol/codex-acp)"
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
    rm -f "$NODE_HOME/config.json" "$NODE_HOME/secrets/node-key"
  fi
  cd "$REPO_DIR"
  log "enrolling node"
  : > /tmp/enroll.log
  BUZZ_NODE_FILE_KEYSTORE=1 setsid ./target/debug/buzz-node enroll --relay-url "$RELAY_URL" >/tmp/enroll.log 2>&1 </dev/null & disown
  for _ in $(seq 1 15); do grep -q 'Node pubkey:' /tmp/enroll.log && break; sleep 1; done
  local node_pk; node_pk="$(awk '/Node pubkey:/{print $3; exit}' /tmp/enroll.log)"
  [ -n "$node_pk" ] || { tail -5 /tmp/enroll.log; die "node did not announce (is the relay this branch? kinds 39500-39503)"; }
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

assign() {
  require_build
  [ -n "${OPENAI_API_KEY:-}" ] || die "set OPENAI_API_KEY to assign the codex agent"
  [ -f "$NODE_HOME/config.json" ] || die "enroll first"
  local node_pk; node_pk="$(grep -o '"node_pubkey": *"[0-9a-f]*"' "$NODE_HOME/config.json" | grep -o '[0-9a-f]\{64\}')"
  log "assigning codex agent ($AGENT_COMMAND) to node $node_pk"
  cd "$REPO_DIR"
  BUZZ_ACP_AGENT_COMMAND="$AGENT_COMMAND" OPENAI_API_KEY="$OPENAI_API_KEY" \
    ./target/debug/examples/owner_driver assign "$RELAY_URL" "$node_pk"
}

status() {
  echo "--- containers ---"; $PODMAN ps --format '{{.Names}} {{.Status}}' 2>/dev/null || true
  echo "--- relay :3000 ---"; ss -ltn 2>/dev/null | grep -q :3000 && echo LISTENING || echo DOWN
  echo "--- daemon ---"; pgrep -x buzz-node >/dev/null && echo UP || echo DOWN
  echo "--- agent procs ---"; pgrep -af codex-acp | grep -v pgrep || echo "(none)"
  echo "--- agent health (6s) ---"; cd "$REPO_DIR" && timeout 9 ./target/debug/examples/owner_driver observe "$RELAY_URL" 6 2>&1 | tail -4 || true
}

down() {
  log "stopping daemon + relay (containers left running; use '$PODMAN stop buzz-pg buzz-redis buzz-minio' to stop them)"
  pkill -9 -x buzz-node 2>/dev/null || true
  pkill -9 -x buzz-relay 2>/dev/null || true
}

case "${1:-}" in
  backend) backend ;;
  relay)   relay ;;
  enroll)  enroll ;;
  up)      up ;;
  assign)  assign ;;
  status)  status ;;
  down)    down ;;
  all)     backend; relay; enroll; up; assign; sleep 8; status ;;
  *) die "usage: $0 {backend|relay|enroll|up|assign|status|down|all}" ;;
esac
