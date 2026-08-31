import { publishAgentAssignment } from "@/shared/api/nodes";
import type { ManagedAgent } from "@/shared/api/types";
import type { BackendIntent } from "./instanceInputForDefinition";

/**
 * After a definition-create mints a new local `ManagedAgent` record with a
 * `node` `BackendIntent` (Run-on picker → an enrolled, online node), publish
 * the `AGENT_ASSIGNMENT` that tells that node to actually run it.
 *
 * The record itself was created with `spawnAfterCreate:false` (see
 * `buildInstanceInputForDefinition`'s `"node"` branch), so this call is the
 * *only* thing that starts the agent process — on the target node, never on
 * this desktop. No-ops when `backendIntent` is not a node target.
 *
 * Every surface that can produce a `node` `BackendIntent` (currently
 * `useAgentManagement.submitCreate` and `usePersonaActions.handleSubmit`)
 * must call this right after its create mutation resolves — mirrors
 * `buildInstanceInputForDefinition`'s single-mapping contract so the
 * assign-on-create behavior cannot drift per caller.
 *
 * We convey three things in `launch.env`:
 *
 * - The selected **runtime** as `BUZZ_ACP_AGENT_COMMAND` (the ACP adapter name,
 *   e.g. `codex-acp` / `claude-agent-acp`). The node's `AcpRuntime` always execs
 *   `buzz-acp`, which picks the underlying agent from this env var — it does NOT
 *   read `launch.command`. Without it, a node-hosted agent silently falls back to
 *   buzz-acp's default harness regardless of the runtime you chose. We send the
 *   command name (node-portable; the node resolves it on its own PATH) and rely
 *   on buzz-acp's `default_agent_args` to supply the correct per-adapter args
 *   (empty for codex-acp / claude-agent-acp), passing `BUZZ_ACP_AGENT_ARGS` only
 *   when the runtime declares explicit args.
 * - The agent's **system prompt** as `BUZZ_ACP_SYSTEM_PROMPT` so the harness
 *   honors its instructions (without it, the runtime uses its default identity).
 * - When a concrete, non-default **model** is pinned, `BUZZ_ACP_MODEL`.
 *
 * We deliberately send ONLY these fields — NOT the full persona/policy env
 * layering, which is still resolved only inside the Rust local-spawn path
 * (`start_local_agent_with_preflight` and friends); re-deriving that here is the
 * drift the spec warns against (see
 * `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` §9 "Config
 * without drift"). Provider credentials are supplied by the node itself
 * (`NodeConfig.agent_env` + the provider secret store), not from here.
 */
export async function publishNodeAssignmentForCreatedAgent(
  backendIntent: BackendIntent | null | undefined,
  createdAgent: ManagedAgent,
  ownerPubkey: string | null | undefined,
): Promise<void> {
  if (backendIntent?.type !== "node") return;
  if (!ownerPubkey) {
    throw new Error(
      "Could not resolve your identity to assign this agent to a node.",
    );
  }
  const env: Record<string, string> = {};
  // Runtime selection → the node's buzz-acp harness picker. Command name only
  // (node-portable); buzz-acp's `default_agent_args` supplies the correct empty
  // args for codex-acp / claude-agent-acp, so we set BUZZ_ACP_AGENT_ARGS only
  // when the runtime declares explicit args.
  if (createdAgent.agentCommand) {
    env.BUZZ_ACP_AGENT_COMMAND = createdAgent.agentCommand;
    if (createdAgent.agentArgs.length > 0) {
      env.BUZZ_ACP_AGENT_ARGS = createdAgent.agentArgs.join(",");
    }
  }
  if (createdAgent.systemPrompt) {
    env.BUZZ_ACP_SYSTEM_PROMPT = createdAgent.systemPrompt;
  }
  if (createdAgent.model) {
    env.BUZZ_ACP_MODEL = createdAgent.model;
  }
  await publishAgentAssignment({
    agentId: createdAgent.pubkey,
    nodePubkey: backendIntent.nodePubkey,
    launch: {
      command: createdAgent.agentCommand,
      args: createdAgent.agentArgs,
      env,
      policyEnv: {},
      ownerPubkey,
    },
    assigned: true,
  });
}
