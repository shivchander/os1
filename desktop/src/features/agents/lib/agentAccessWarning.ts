import { isAgentNodeHosted } from "@/shared/api/nodesStore";
import type { ManagedAgent, RespondToMode } from "@/shared/api/types";

/**
 * Where an agent's process runs, as far as the calling surface can tell.
 *
 * Deliberately coarser than `ManagedAgentBackend`: the warning copy only needs
 * to know "this machine" vs "somewhere else", so surfaces resolve their own
 * backend shape down to this before handing it over. `null` means the surface
 * genuinely cannot tell — see `agentAccessWarningText` for how that is
 * treated.
 */
export type AgentRunLocation = "local" | "remote";

/**
 * Resolve a running agent's actual run location. `null` when the agent is
 * unknown.
 *
 * Checks node-hosting FIRST: a node-hosted agent persists as
 * `backend:{type:"local"}` (see `instanceInputForDefinition.ts`'s `"node"`
 * `BackendIntent` branch) — its process and key live on the assigned
 * execution node, a machine the owner doesn't necessarily own the way "your
 * computer" implies, so `backend.type` alone would wrongly report "local"
 * and understate the respond-to warning's disclosure (Phase 4 fix-round-1
 * Important finding).
 */
export function runLocationForBackend(
  agent: Pick<ManagedAgent, "pubkey" | "backend"> | null | undefined,
): AgentRunLocation | null {
  if (!agent?.backend) return null;
  if (agent.backend.type === "local" && isAgentNodeHosted(agent.pubkey)) {
    return "remote";
  }
  return agent.backend.type === "local" ? "local" : "remote";
}

/**
 * Resolve the create flow's `WhereToRunDraft.runOn`, which is `"local"` or a
 * discovered provider id. An empty string is treated as unknown rather than as
 * a provider, since `runOn` is typed `"local" | string`.
 */
export function runLocationForRunOn(
  runOn: string | null | undefined,
): AgentRunLocation | null {
  if (!runOn) return null;
  return runOn === "local" ? "local" : "remote";
}

/**
 * Copy for the shared-access warning in the respond-to field, or `null` for
 * modes that share nothing.
 *
 * Both `anyone` and `allowlist` hand the host's access to someone other than
 * the owner, so both warn; only the audience phrase differs.
 *
 * An unknown run location falls back to the same "your computer" wording as
 * `local` rather than hedging with "computer or server". A remote host is
 * only reachable when a `buzz-backend-*` provider binary is installed or an
 * execution node is enrolled+online — without either, `WhereToRunSection`'s
 * "Run on" selector never renders and every agent is local — so hedging
 * would name a concept most owners have never been shown. When it *is*
 * remote the owner picked that host from the selector (provider) or it was
 * assigned to a node (`runLocationForBackend`'s `isAgentNodeHosted` check)
 * deliberately, so naming a server is meaningful there.
 */
export function agentAccessWarningText(
  mode: RespondToMode,
  runLocation?: AgentRunLocation | null,
): string | null {
  if (mode !== "anyone" && mode !== "allowlist") return null;
  const audience = mode === "anyone" ? "Anyone" : "Selected people";
  // The two locations differ in more than the noun: a local agent reaches the
  // owner's own files, while a remote host's files aren't theirs to describe —
  // only the accounts and tools provisioned there.
  const target =
    runLocation === "remote"
      ? "the server it runs on, including any accounts and tools available there"
      : "your computer, including files, accounts, and connected tools";
  return `${audience} can use this agent to access ${target}.`;
}
