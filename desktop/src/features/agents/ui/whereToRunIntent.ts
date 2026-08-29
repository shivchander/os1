import type { BackendIntent } from "../lib/instanceInputForDefinition";
import type { BackendProviderProbeResult } from "@/shared/api/types";
import { coerceConfigValues } from "./ProviderConfigFields";

/** Draft state of the optional remote-backend selector. */
export type WhereToRunDraft = {
  runOn: "local" | string;
  providerConfig: Record<string, string>;
  probedProvider: BackendProviderProbeResult | null;
};

/**
 * `runOnOptions` values for an enrolled execution node are `node:<pubkey>` —
 * a third namespace alongside `"local"` and a bare provider id. Prefixing
 * (rather than a sibling `WhereToRunDraft.nodePubkey` field) keeps the
 * selection in the single `runOn` value `PersonaDropdownField` already binds
 * to, so there is nothing else to keep in sync when the selection changes.
 */
const NODE_RUN_ON_PREFIX = "node:";

export function runOnValueForNode(nodePubkey: string): string {
  return `${NODE_RUN_ON_PREFIX}${nodePubkey}`;
}

/** The target node pubkey when `runOn` is a `node:*` value, else `null`. */
export function nodePubkeyFromRunOn(runOn: string): string | null {
  return runOn.startsWith(NODE_RUN_ON_PREFIX)
    ? runOn.slice(NODE_RUN_ON_PREFIX.length)
    : null;
}

export const emptyWhereToRunDraft: WhereToRunDraft = {
  runOn: "local",
  providerConfig: {},
  probedProvider: null,
};

/**
 * Fold a completed probe into the draft the user has *now* — not the draft
 * that existed when the probe started. Schema defaults prefill only the keys
 * the user has not touched: anything already in `providerConfig` (typed while
 * the probe was in flight) wins over the default. Overwriting instead of
 * merging is the "Typewriter Eraser" bug — every probe resolution silently
 * erased in-flight keystrokes.
 */
export function applyProbeResult(
  current: WhereToRunDraft,
  result: BackendProviderProbeResult,
): WhereToRunDraft {
  const defaults: Record<string, string> = {};
  const properties =
    (result.config_schema as Record<string, unknown> | undefined)?.properties ??
    {};
  for (const [key, property] of Object.entries(properties) as [
    string,
    Record<string, unknown>,
  ][]) {
    if (property.default != null) defaults[key] = String(property.default);
  }
  return {
    ...current,
    probedProvider: result,
    providerConfig: { ...defaults, ...current.providerConfig },
  };
}

export function providerConfigComplete(draft: WhereToRunDraft): boolean {
  if (draft.runOn === "local" || nodePubkeyFromRunOn(draft.runOn)) return true;
  if (!draft.probedProvider) return false;
  const schema = draft.probedProvider.config_schema as
    | Record<string, unknown>
    | undefined;
  const required: string[] = (schema?.required as string[] | undefined) ?? [];
  return required.every(
    (key) => (draft.providerConfig[key] ?? "").trim().length > 0,
  );
}

export function canSubmitWhereToRun(draft: WhereToRunDraft): boolean {
  return providerConfigComplete(draft);
}

export function resolveBackendIntent(
  draft: WhereToRunDraft,
): BackendIntent | null {
  const nodePubkey = nodePubkeyFromRunOn(draft.runOn);
  if (nodePubkey) return { type: "node", nodePubkey };
  if (draft.runOn === "local") return null;
  return {
    type: "provider",
    id: draft.runOn,
    config: coerceConfigValues(
      draft.providerConfig,
      draft.probedProvider?.config_schema,
    ),
  };
}
