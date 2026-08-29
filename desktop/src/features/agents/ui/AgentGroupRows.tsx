import * as React from "react";

import { ensureNodesRelaySubscription } from "@/shared/api/nodesStore";
import type { ManagedAgent, PresenceLookup } from "@/shared/api/types";
import { normalizePubkey } from "@/shared/lib/pubkey";
import { ManagedAgentRow } from "./ManagedAgentRow";

export type AgentGroupRowsProps = {
  agents: ManagedAgent[];
  channelIdToName: Record<string, string>;
  channelsByPubkey: Record<string, { id: string; name: string }[]>;
  logContent: string | null;
  logError: Error | null;
  logLoading: boolean;
  personaLabelsById: Record<string, string>;
  presenceLoaded: boolean;
  presenceLookup: PresenceLookup;
  selectedLogAgentPubkey: string | null;
  onOpenProfile: (pubkey: string) => void;
  onSelectLogAgent: (pubkey: string | null) => void;
};

export function AgentGroupRows({
  agents,
  channelIdToName,
  channelsByPubkey,
  logContent,
  logError,
  logLoading,
  personaLabelsById,
  presenceLoaded,
  presenceLookup,
  selectedLogAgentPubkey,
  onOpenProfile,
  onSelectLogAgent,
}: AgentGroupRowsProps) {
  // Agent cards read node status/roster from nodesStore (AgentNodeStatusBadge,
  // AgentNodeControls); ensure its live subscription is running whenever the
  // agent list is on screen, even if the Nodes panel itself was never
  // visited. Idempotent — safe alongside NodesPanel's own call.
  React.useEffect(() => {
    void ensureNodesRelaySubscription();
  }, []);

  return (
    <div className="divide-y divide-border/50 border-t border-border/50">
      {agents.map((agent) => (
        <ManagedAgentRow
          agent={agent}
          channelIdToName={channelIdToName}
          channelNames={channelsByPubkey[normalizePubkey(agent.pubkey)] ?? []}
          isLogSelected={selectedLogAgentPubkey === agent.pubkey}
          key={agent.pubkey}
          logContent={
            selectedLogAgentPubkey === agent.pubkey ? logContent : null
          }
          logError={selectedLogAgentPubkey === agent.pubkey ? logError : null}
          logLoading={selectedLogAgentPubkey === agent.pubkey && logLoading}
          personaLabelsById={personaLabelsById}
          presenceLoaded={presenceLoaded}
          presenceLookup={presenceLookup}
          onOpenProfile={onOpenProfile}
          onSelectLogAgent={onSelectLogAgent}
        />
      ))}
    </div>
  );
}
