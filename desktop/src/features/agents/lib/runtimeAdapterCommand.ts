import type { AcpRuntime, AcpRuntimeCatalogEntry } from "@/shared/api/types";
import type { NodeView } from "@/shared/api/nodesStore";

/**
 * Legacy wildcard runtime id. A node that advertises `"acp"` (older node
 * builds, before per-runtime advertisement) can host any ACP runtime, so it
 * matches every harness id.
 */
const LEGACY_ANY_RUNTIME_ID = "acp";

/**
 * Adapter command for a harness-catalog runtime id, for agents that will run on
 * a remote execution node rather than this Mac.
 *
 * On the local create path the command comes from the live catalog
 * (`AcpRuntimeCatalogEntry.command`, populated by `discoverAcpRuntimes`). But a
 * node-targeted agent may pick a runtime whose adapter is not installed
 * locally, so that field is `null` — the node supplies the binary. This map is
 * the fallback: id → the adapter command the node is expected to run.
 *
 * MIRRORS the first entry of `commands` for each runtime in the Rust
 * `KNOWN_ACP_RUNTIMES` table (`desktop/src-tauri/src/managed_agents/
 * discovery.rs`, lines 86-205). Keep the two in sync.
 */
const ADAPTER_COMMAND_BY_RUNTIME_ID: Readonly<Record<string, string>> = {
  codex: "codex-acp",
  claude: "claude-agent-acp",
  goose: "goose",
  "buzz-agent": "buzz-agent",
};

/**
 * The adapter command for a runtime id, or `null` for an unknown id (e.g. a
 * user's custom harness) — the caller keeps whatever command it already has
 * rather than inventing one.
 */
export function adapterCommandForRuntimeId(id: string): string | null {
  return ADAPTER_COMMAND_BY_RUNTIME_ID[id.trim()] ?? null;
}

/**
 * Whether a node advertising `runtimes` can host `runtimeId`. A node
 * advertising the legacy `"acp"` wildcard matches any runtime.
 */
export function nodeAdvertisesRuntime(
  runtimes: readonly string[],
  runtimeId: string,
): boolean {
  return (
    runtimes.includes(runtimeId) || runtimes.includes(LEGACY_ANY_RUNTIME_ID)
  );
}

/**
 * Build the `AcpRuntime` for a node-targeted create when the picked runtime is
 * not available on THIS Mac but the target node advertises it (fix a). The
 * synthetic entry reuses the local catalog entry's metadata (mcpCommand,
 * avatarUrl, id, …) but marks it available and fills `command` from
 * `adapterCommandForRuntimeId` — the node, not this desktop, runs the binary.
 *
 * Returns `null` when the id maps to no adapter command, the node isn't in the
 * roster or doesn't advertise the runtime, or the local catalog has no entry
 * to base the metadata on — the caller then refuses the create as before.
 */
export function nodeRuntimeForCreate({
  runtimeId,
  nodePubkey,
  nodes,
  catalogEntries,
}: {
  runtimeId: string;
  nodePubkey: string;
  nodes: readonly NodeView[];
  catalogEntries: readonly AcpRuntimeCatalogEntry[];
}): AcpRuntime | null {
  const command = adapterCommandForRuntimeId(runtimeId);
  if (!command) return null;
  const node = nodes.find((candidate) => candidate.nodePubkey === nodePubkey);
  if (!node || !nodeAdvertisesRuntime(node.runtimes, runtimeId)) return null;
  const catalogEntry = catalogEntries.find((entry) => entry.id === runtimeId);
  if (!catalogEntry) return null;
  return {
    ...catalogEntry,
    availability: "available" as const,
    command,
    binaryPath: catalogEntry.binaryPath ?? "",
  };
}
