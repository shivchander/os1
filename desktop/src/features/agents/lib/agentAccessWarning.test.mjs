import assert from "node:assert/strict";
import test, { beforeEach } from "node:test";

import { ingestNodeEvent, resetNodesStore } from "@/shared/api/nodesStore";
import { KIND_AGENT_ASSIGNMENT } from "@/shared/constants/kinds";
import {
  agentAccessWarningText,
  runLocationForBackend,
  runLocationForRunOn,
} from "./agentAccessWarning.ts";

beforeEach(() => {
  resetNodesStore();
});

function assign(agentPubkey, state = "assigned") {
  ingestNodeEvent({
    id: `assign-${agentPubkey}-${state}`,
    kind: KIND_AGENT_ASSIGNMENT,
    pubkey: "owner1",
    created_at: 1,
    tags: [
      ["d", agentPubkey],
      ["node", "node1"],
      ["state", state],
    ],
    sig: "sig",
    content: "encrypted-marker",
  });
}

test("only the modes that share access warn", () => {
  assert.equal(agentAccessWarningText("owner-only", "local"), null);
  assert.ok(agentAccessWarningText("anyone", "local"));
  assert.ok(agentAccessWarningText("allowlist", "local"));
});

test("a local agent names this computer and what is reachable on it", () => {
  assert.equal(
    agentAccessWarningText("anyone", "local"),
    "Anyone can use this agent to access your computer, including files, accounts, and connected tools.",
  );
  assert.equal(
    agentAccessWarningText("allowlist", "local"),
    "Selected people can use this agent to access your computer, including files, accounts, and connected tools.",
  );
});

test("a provider-backed agent names the server, and not the owner's files", () => {
  // A remote host's files aren't the owner's to describe, so the tail narrows
  // to the accounts and tools provisioned there.
  assert.equal(
    agentAccessWarningText("anyone", "remote"),
    "Anyone can use this agent to access the server it runs on, including any accounts and tools available there.",
  );
  assert.equal(
    agentAccessWarningText("allowlist", "remote"),
    "Selected people can use this agent to access the server it runs on, including any accounts and tools available there.",
  );
  assert.doesNotMatch(
    agentAccessWarningText("anyone", "remote"),
    /your computer/,
  );
});

test("an unknown run location reads as local, not as a hedge", () => {
  // "computer or server" names a concept most owners have never been shown:
  // the Run on selector only renders when a buzz-backend-* provider exists.
  for (const unknown of [undefined, null]) {
    assert.equal(
      agentAccessWarningText("anyone", unknown),
      "Anyone can use this agent to access your computer, including files, accounts, and connected tools.",
    );
  }
});

test("every variant leads with the audience and stays jargon-free", () => {
  for (const mode of ["anyone", "allowlist"]) {
    for (const runLocation of [null, "local", "remote"]) {
      const text = agentAccessWarningText(mode, runLocation);
      assert.match(
        text,
        /^(Anyone|Selected people) can use this agent to access/,
      );
      assert.doesNotMatch(text, /respond-to|allowlist|pubkey|Nostr|harness/i);
    }
  }
});

test("runLocationForBackend maps the backend union", () => {
  assert.equal(
    runLocationForBackend({ pubkey: "a1", backend: { type: "local" } }),
    "local",
  );
  assert.equal(
    runLocationForBackend({
      pubkey: "a1",
      backend: { type: "provider", id: "blox", config: {} },
    }),
    "remote",
  );
  assert.equal(runLocationForBackend(null), null);
  assert.equal(runLocationForBackend(undefined), null);
});

// Phase 4 fix-round-1 Important finding: a node-hosted agent persists as
// backend:{type:"local"} (see instanceInputForDefinition.ts's "node"
// BackendIntent branch), so `backend.type` alone always read it as "local" —
// understating the respond-to warning ("access YOUR COMPUTER") for an agent
// whose key and process actually live on a separate execution node.
test("a node-hosted agent (backend:local + an active assignment) resolves as remote", () => {
  assign("a1", "assigned");
  assert.equal(
    runLocationForBackend({ pubkey: "a1", backend: { type: "local" } }),
    "remote",
  );
});

test("an unassigned agent with backend:local still resolves as local", () => {
  assign("a1", "assigned");
  assign("a1", "unassigned");
  assert.equal(
    runLocationForBackend({ pubkey: "a1", backend: { type: "local" } }),
    "local",
  );
});

test("runLocationForRunOn treats a provider id as remote", () => {
  assert.equal(runLocationForRunOn("local"), "local");
  assert.equal(runLocationForRunOn("blox"), "remote");
});

test("runLocationForRunOn treats a blank value as unknown", () => {
  // `runOn` is typed `"local" | string`, so a blank must not read as a
  // provider id and produce the server wording.
  assert.equal(runLocationForRunOn(""), null);
  assert.equal(runLocationForRunOn(null), null);
  assert.equal(runLocationForRunOn(undefined), null);
});
