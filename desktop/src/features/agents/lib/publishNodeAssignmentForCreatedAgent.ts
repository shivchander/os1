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
 * We convey the agent's own **system prompt** in `launch.env` as
 * `BUZZ_ACP_SYSTEM_PROMPT` so the node-hosted harness honors its instructions
 * (without it, codex/goose runs with its default identity). We deliberately
 * send ONLY the agent's instructions here — NOT the full persona/model/policy
 * env layering, which is still resolved only inside the Rust local-spawn path
 * (`start_local_agent_with_preflight` and friends); re-deriving that here is
 * the drift the spec warns against (see
 * `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` §9 "Config
 * without drift"). Provider credentials are supplied by the node itself
 * (`NodeConfig.agent_env` + the provider secret store), not from here.
 * `command`/`args` come straight off the just-created record.
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
  if (createdAgent.systemPrompt) {
    env.BUZZ_ACP_SYSTEM_PROMPT = createdAgent.systemPrompt;
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
