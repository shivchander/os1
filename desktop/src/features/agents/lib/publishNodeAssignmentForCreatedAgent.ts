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
 * `env`/`policyEnv` are sent empty: the effective launch env (persona/model
 * layering, policy-controlled overrides) is resolved today only inside the
 * Rust local-spawn path (`start_local_agent_with_preflight` and friends) —
 * there is no desktop-frontend resolver for it yet, and inventing one here
 * would re-derive logic the spec explicitly warns against duplicating (see
 * `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` §9 "Config
 * without drift"). `command`/`args` come straight off the just-created
 * record, which is the same value the record itself persists.
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
  await publishAgentAssignment({
    agentId: createdAgent.pubkey,
    nodePubkey: backendIntent.nodePubkey,
    launch: {
      command: createdAgent.agentCommand,
      args: createdAgent.agentArgs,
      env: {},
      policyEnv: {},
      ownerPubkey,
    },
    assigned: true,
  });
}
