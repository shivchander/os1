import { useMutation } from "@tanstack/react-query";
import { ArrowRightLeft, Play, Square } from "lucide-react";
import * as React from "react";
import { toast } from "sonner";

import { useIdentityQuery } from "@/shared/api/hooks";
import { publishAgentAssignment } from "@/shared/api/nodes";
import {
  getAgentStatus,
  getNodesSnapshot,
  subscribeNodes,
} from "@/shared/api/nodesStore";
import type { ManagedAgent } from "@/shared/api/types";
import { Button } from "@/shared/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/shared/ui/dropdown-menu";

const RUNNING_HEALTH = new Set(["running", "starting"]);

/**
 * Compact icon-button trio (Start-or-Stop, Move) for an agent that already
 * has a node assignment (`nodesStore.getAgentStatus`) — sized to sit in the
 * identity card's small top-right `actions` corner alongside
 * `PersonaActionsMenu`, not a roomy row. Every action is an
 * `AGENT_ASSIGNMENT` edit — Start/Move publish `assigned:true` (to the last
 * known node, or a newly picked one); Stop publishes `assigned:false` to the
 * current node. There is no separate node control-plane command (spec §10
 * item 3: "Start / Stop / Move are assignment edits").
 *
 * Renders nothing for an agent with no assignment history — first-assign
 * happens through the Run-on picker at create time (`WhereToRunSection`) or
 * the Nodes panel roster, not from the agent card.
 */
export function AgentNodeControls({ agent }: { agent: ManagedAgent }) {
  // Re-render when the store changes — getAgentStatus/getNodesSnapshot below
  // are plain reads with no reactivity of their own.
  React.useSyncExternalStore(subscribeNodes, getNodesSnapshot);
  const identityQuery = useIdentityQuery();
  const status = getAgentStatus(agent.pubkey);

  const assignMutation = useMutation({
    mutationFn: (args: { nodePubkey: string; assigned: boolean }) =>
      publishAgentAssignment({
        agentId: agent.pubkey,
        nodePubkey: args.nodePubkey,
        // See publishNodeAssignmentForCreatedAgent.ts for why env/policyEnv
        // are empty: no desktop-frontend resolver for the effective launch
        // env exists yet. command/args are the record's own resolved values.
        launch: {
          command: agent.agentCommand,
          args: agent.agentArgs,
          env: {},
          policyEnv: {},
          ownerPubkey: identityQuery.data?.pubkey ?? null,
        },
        assigned: args.assigned,
      }),
    onSuccess: (_eventId, variables) => {
      toast.success(
        variables.assigned
          ? `${agent.name} assigned — it will start once the node picks up the change.`
          : `${agent.name} unassigned — the node will stop it.`,
      );
    },
    onError: (error: unknown) => {
      toast.error(
        error instanceof Error
          ? error.message
          : "Failed to update this agent's node assignment.",
      );
    },
  });

  if (!status) return null;

  const isRunning = RUNNING_HEALTH.has(status.health);
  const moveTargets = getNodesSnapshot().filter(
    (node) => node.online && node.nodePubkey !== status.nodePubkey,
  );
  const iconButtonClassName = "text-muted-foreground hover:text-foreground";

  return (
    <div className="flex items-center gap-0.5">
      {isRunning ? (
        <Button
          aria-label={`Stop ${agent.name}`}
          className={iconButtonClassName}
          data-testid={`agent-stop-${agent.pubkey}`}
          disabled={assignMutation.isPending}
          onClick={() =>
            assignMutation.mutate({
              nodePubkey: status.nodePubkey,
              assigned: false,
            })
          }
          size="icon-xs"
          type="button"
          variant="ghost"
        >
          <Square className="h-3.5 w-3.5" />
        </Button>
      ) : (
        <Button
          aria-label={`Start ${agent.name}`}
          className={iconButtonClassName}
          data-testid={`agent-start-${agent.pubkey}`}
          disabled={assignMutation.isPending}
          onClick={() =>
            assignMutation.mutate({
              nodePubkey: status.nodePubkey,
              assigned: true,
            })
          }
          size="icon-xs"
          type="button"
          variant="ghost"
        >
          <Play className="h-3.5 w-3.5" />
        </Button>
      )}
      <DropdownMenu modal={false}>
        <DropdownMenuTrigger asChild>
          <Button
            aria-label={`Move ${agent.name} to another node`}
            className={iconButtonClassName}
            data-testid={`agent-move-${agent.pubkey}`}
            disabled={assignMutation.isPending}
            size="icon-xs"
            type="button"
            variant="ghost"
          >
            <ArrowRightLeft className="h-3.5 w-3.5" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end">
          {moveTargets.length === 0 ? (
            <div className="px-2 py-1.5 text-xs text-muted-foreground">
              No other online nodes
            </div>
          ) : (
            moveTargets.map((node) => (
              <DropdownMenuItem
                key={node.nodePubkey}
                onSelect={() =>
                  assignMutation.mutate({
                    nodePubkey: node.nodePubkey,
                    assigned: true,
                  })
                }
              >
                {node.name}
              </DropdownMenuItem>
            ))
          )}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}
