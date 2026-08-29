import * as React from "react";

import {
  getAgentStatus,
  getNodesSnapshot,
  subscribeNodes,
} from "@/shared/api/nodesStore";
import { truncatePubkey } from "@/shared/lib/pubkey";
import { Badge, type BadgeProps } from "@/shared/ui/badge";

/**
 * "On <node> · <health>" for an agent that has (or had) an
 * `AGENT_ASSIGNMENT`. Reads `nodesStore` (shared/, not `features/nodes` —
 * `features/agents` may not import another feature's internals) via
 * `useSyncExternalStore`, never React Query: this is live relay-projected
 * state, not a request/response value.
 *
 * Renders nothing for an agent that has never been assigned to a node —
 * that is the common case (most agents still run as a local desktop child
 * process) and is not an error state worth a placeholder.
 */
export function AgentNodeStatusBadge({ agentPubkey }: { agentPubkey: string }) {
  // Subscribing to the whole store (rather than reading status via a getter
  // called only in render) is what makes this re-render when a later
  // AGENT_NODE_STATUS/NODE_ANNOUNCE arrives — getAgentStatus/getNodesSnapshot
  // alone are plain reads with no reactivity of their own.
  React.useSyncExternalStore(subscribeNodes, getNodesSnapshot);
  const status = getAgentStatus(agentPubkey);
  if (!status) return null;

  const node = getNodesSnapshot().find(
    (candidate) => candidate.nodePubkey === status.nodePubkey,
  );
  const nodeName = node?.name ?? truncatePubkey(status.nodePubkey);

  return (
    <div
      // text-2xs: this sits in the identity card's small fixed-footprint
      // footer strip alongside the label/model-label rows, not a roomy row
      // (AGENTS.md "text-2xs ... for the sub-text-xs ramp"). flex-wrap (not
      // truncate): the card is narrow enough that "On <name>" plus the health
      // pill on one forced line clipped even short node names — wrapping to a
      // second line reads better than an ellipsis on a name that would
      // otherwise fit.
      className="flex flex-wrap items-center gap-x-1 gap-y-0.5 text-2xs text-muted-foreground"
      data-testid={`agent-node-status-${agentPubkey}`}
    >
      <span className="min-w-0 truncate">
        On <span className="font-medium text-foreground">{nodeName}</span>
      </span>
      <Badge className="shrink-0" variant={healthBadgeVariant(status.health)}>
        {status.health}
      </Badge>
    </div>
  );
}

function healthBadgeVariant(health: string): BadgeProps["variant"] {
  switch (health) {
    case "running":
      return "success";
    case "starting":
      return "warning";
    case "crashed":
      return "destructive";
    default:
      return "secondary";
  }
}
