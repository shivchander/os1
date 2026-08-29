import { invokeTauri } from "@/shared/api/tauri";

/**
 * Desktop-resolved launch contract for starting an agent process on a node.
 * Field names are camelCase on the wire — the Rust command's `LaunchInput`
 * carries `#[serde(rename_all = "camelCase")]` and maps these onto its own
 * snake_case fields (see `desktop-src-tauri/src/commands/nodes.rs`).
 */
export type NodeAssignmentLaunchInput = {
  command: string;
  args: string[];
  env: Record<string, string>;
  policyEnv: Record<string, string>;
  ownerPubkey: string | null;
};

/**
 * Owner-sign and publish a `NODE_ENROLLMENT` authorizing `nodePubkey`.
 * Resolves to the published event id (hex). Signing stays entirely native —
 * this call never sees a private key.
 */
export function publishNodeEnrollment(nodePubkey: string): Promise<string> {
  return invokeTauri<string>("publish_node_enrollment", { nodePubkey });
}

/**
 * Owner-sign and publish an `AGENT_ASSIGNMENT` assigning `agentId` to
 * `nodePubkey` (or unassigning it, when `assigned` is `false`). The agent's
 * nsec and launch env are NIP-44-encrypted to the node natively; only the
 * published event id crosses back over IPC.
 */
export function publishAgentAssignment(args: {
  agentId: string;
  nodePubkey: string;
  launch: NodeAssignmentLaunchInput;
  assigned: boolean;
}): Promise<string> {
  return invokeTauri<string>("publish_agent_assignment", args);
}
