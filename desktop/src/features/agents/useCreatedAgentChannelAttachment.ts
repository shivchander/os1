import { toast } from "sonner";

import { attachManagedAgentToChannel } from "./channelAgents";
import type { Channel, CreateManagedAgentResponse } from "@/shared/api/types";

type TargetChannel = Pick<Channel, "id" | "name">;

type PresentCreatedAgentOptions = {
  /**
   * False when the caller already knows (synchronously, from the create
   * request's own backend intent) that this agent was just targeted at an
   * execution node — never from `nodesStore`, which cannot be trusted here.
   * `attachManagedAgentToChannel`'s own `isNodeHostedAgent` check races the
   * assignment publish: publishing only waits for the relay to accept the
   * event, not for that same event to echo back through this client's own
   * live subscription and update `nodesStore` (see
   * `publishNodeAssignmentForCreatedAgent`'s doc comment and
   * `run-on-node-picker.spec.ts`'s manual echo-seed for the same gap). A
   * node-targeted create's `attachManagedAgentToChannel` call lands
   * immediately after that publish — always before the echo — so
   * `nodesStore` would read "not (yet) assigned" every time, making that
   * check alone pass a node-hosted agent through here. Defaults to `true`
   * (unaffected: every other caller either targets no node or already has a
   * settled, pre-existing assignment nodesStore's own check correctly gates).
   */
  ensureRunning?: boolean;
};

async function attach(
  created: CreateManagedAgentResponse,
  targetChannel: TargetChannel,
  options: PresentCreatedAgentOptions,
) {
  const attached = await attachManagedAgentToChannel(targetChannel.id, {
    agent: created.agent,
    role: "bot",
    ensureRunning: options.ensureRunning ?? true,
  });
  created.agent = attached.agent;
}

function showAttachmentFailure(
  created: CreateManagedAgentResponse,
  targetChannel: TargetChannel,
  options: PresentCreatedAgentOptions,
  cause: unknown,
  toastId?: string | number,
) {
  const error = cause instanceof Error ? cause.message : "Failed to add agent.";
  const id = toast.warning("Agent created", {
    description: `${created.agent.name} couldn’t be added to #${targetChannel.name}. ${error}`,
    id: toastId,
    action: {
      label: "Try again",
      onClick: (event) => {
        event.preventDefault();
        toast.loading("Agent created", {
          description: `Adding ${created.agent.name} to #${targetChannel.name}…`,
          id,
        });
        void attach(created, targetChannel, options).then(
          () => {
            toast.success("Agent created", {
              description: `Added ${created.agent.name} to #${targetChannel.name}`,
              id,
            });
          },
          (retryCause: unknown) => {
            showAttachmentFailure(
              created,
              targetChannel,
              options,
              retryCause,
              id,
            );
          },
        );
      },
    },
  });
}

/** Keeps creation successful when its optional channel attachment fails. */
export function useCreatedAgentChannelAttachment() {
  async function presentCreatedAgent(
    created: CreateManagedAgentResponse,
    targetChannel?: TargetChannel | null,
    options: PresentCreatedAgentOptions = {},
  ) {
    if (created.spawnError || !targetChannel) {
      toast.success("Agent created");
      return;
    }

    try {
      await attach(created, targetChannel, options);
      toast.success("Agent created");
    } catch (cause) {
      showAttachmentFailure(created, targetChannel, options, cause);
    }
  }

  return { presentCreatedAgent };
}
