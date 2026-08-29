import { useMutation } from "@tanstack/react-query";
import * as React from "react";
import { toast } from "sonner";

import {
  ensureNodesRelaySubscription,
  getNodesSnapshot,
  subscribeNodes,
} from "@/shared/api/nodesStore";
import { NodeRow } from "@/features/nodes/ui/NodeRow";
import { publishNodeEnrollment } from "@/shared/api/nodes";
import { PageHeader } from "@/shared/ui/PageHeader";
import { Button } from "@/shared/ui/button";
import { Input } from "@/shared/ui/input";

/**
 * Execution-node roster + one-time enrollment approval. Live roster data
 * comes from `nodesStore` (a `relayClient.subscribeLive`-backed external
 * store — never React Query, per AGENTS.md "Live relay data → feature
 * store"); the enrollment publish is a one-shot command, so it goes through
 * a React Query mutation.
 */
export function NodesPanel() {
  React.useEffect(() => {
    void ensureNodesRelaySubscription();
  }, []);

  const nodes = React.useSyncExternalStore(subscribeNodes, getNodesSnapshot);
  const [pendingNodePubkey, setPendingNodePubkey] = React.useState("");

  const enrollMutation = useMutation({
    mutationFn: (nodePubkey: string) => publishNodeEnrollment(nodePubkey),
    onSuccess: () => {
      toast.success(
        "Enrollment published — the node will appear once it announces.",
      );
      setPendingNodePubkey("");
    },
    onError: (error: unknown) => {
      toast.error(
        error instanceof Error ? error.message : "Failed to enroll the node.",
      );
    },
  });

  const trimmedPubkey = pendingNodePubkey.trim();

  return (
    <div className="flex flex-col gap-6 p-6" data-testid="nodes-panel">
      <PageHeader
        description="Approve execution nodes and see which agents they're hosting."
        title="Nodes"
      />

      <form
        className="flex items-end gap-2"
        onSubmit={(event) => {
          event.preventDefault();
          if (!trimmedPubkey || enrollMutation.isPending) return;
          enrollMutation.mutate(trimmedPubkey);
        }}
      >
        <div className="flex-1 space-y-1.5">
          <label
            className="text-2xs font-semibold uppercase tracking-wide text-muted-foreground"
            htmlFor="node-enroll-pubkey"
          >
            Enroll a node
          </label>
          <Input
            data-testid="node-enroll-pubkey-input"
            id="node-enroll-pubkey"
            onChange={(event) => setPendingNodePubkey(event.target.value)}
            placeholder="Paste the node's public key"
            value={pendingNodePubkey}
          />
        </div>
        <Button
          data-testid="node-enroll-approve"
          disabled={!trimmedPubkey || enrollMutation.isPending}
          type="submit"
        >
          {enrollMutation.isPending ? "Approving…" : "Approve"}
        </Button>
      </form>

      {nodes.length === 0 ? (
        <p
          className="text-sm text-muted-foreground"
          data-testid="nodes-empty-state"
        >
          No execution nodes yet. Enroll one above once it has paired.
        </p>
      ) : (
        <ul className="flex flex-col gap-2" data-testid="nodes-roster">
          {nodes.map((node) => (
            <NodeRow key={node.nodePubkey} node={node} />
          ))}
        </ul>
      )}
    </div>
  );
}
