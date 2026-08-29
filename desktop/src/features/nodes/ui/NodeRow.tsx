import type { NodeView } from "@/shared/api/nodesStore";
import { cn } from "@/shared/lib/cn";
import { Badge } from "@/shared/ui/badge";
import { PubKey } from "@/shared/ui/PubKey";

/**
 * One row in the Nodes panel roster. Private to `NodesPanel` — split into its
 * own file only to keep `NodesPanel.tsx` small (AGENTS.md "one public widget
 * per file; push private sub-widgets into sibling files").
 */
export function NodeRow({ node }: { node: NodeView }) {
  return (
    <li
      className="flex items-center justify-between gap-4 rounded-lg border border-border/70 bg-background/60 px-4 py-3"
      data-testid={`node-row-${node.nodePubkey}`}
    >
      <div className="flex min-w-0 items-center gap-3">
        <span
          aria-hidden="true"
          className={cn(
            "inline-flex h-2.5 w-2.5 shrink-0 rounded-full",
            node.online ? "bg-emerald-500" : "bg-muted-foreground/35",
          )}
          data-testid={`node-online-${node.nodePubkey}`}
        />
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <span className="truncate text-sm font-medium">{node.name}</span>
            <Badge variant="outline">{node.os}</Badge>
            <span className="text-2xs text-muted-foreground">
              {node.online ? "Online" : "Offline"}
            </span>
          </div>
          <div className="mt-1 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-2xs text-muted-foreground">
            <PubKey
              className="text-2xs"
              pubkey={node.nodePubkey}
              testId={`node-pubkey-${node.nodePubkey}`}
            />
            <span className="truncate">
              {node.runtimes.length > 0
                ? node.runtimes.join(", ")
                : "No runtimes reported"}
            </span>
          </div>
        </div>
      </div>
      <div className="shrink-0 text-right">
        <div
          className="text-sm font-medium"
          data-testid={`node-agent-count-${node.nodePubkey}`}
        >
          {node.agentCount}
        </div>
        <div className="text-2xs text-muted-foreground">
          {node.agentCount === 1 ? "agent running" : "agents running"}
        </div>
      </div>
    </li>
  );
}
