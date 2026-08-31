import {
  AUTO_MODEL_DROPDOWN_VALUE,
  CUSTOM_PROVIDER_DROPDOWN_VALUE,
  type PersonaDropdownOption,
  type PersonaModelOption,
} from "./agentConfigOptions";
import { modelDropdownOptions as buildModelDropdownOptions } from "./relayMeshModelPicker";
import { MODEL_DISCOVERY_LOADING_VALUE } from "./usePersonaModelDiscovery";

/**
 * Provider dropdown options for the agent-definition dialog: the known LLM
 * providers (the empty-id "use defaults" sentinel dropped) plus the
 * Custom-provider entry. Extracted from `AgentDefinitionDialog` to keep that
 * file under the size ceiling.
 */
export function buildPersonaProviderDropdownOptions(
  providerOptions: readonly PersonaModelOption[],
): PersonaDropdownOption[] {
  return [
    ...providerOptions
      .filter((option) => option.id.trim().length > 0)
      .map((option) => ({ label: option.label, value: option.id })),
    { label: "Custom provider...", value: CUSTOM_PROVIDER_DROPDOWN_VALUE },
  ];
}

/**
 * Model dropdown options for the agent-definition dialog. Outside relay-mesh
 * (Buzz shared compute) the "Auto" entry is dropped so an explicit model is
 * required; inside relay-mesh it is relabelled "Automatic". Extracted from
 * `AgentDefinitionDialog` to keep that file under the size ceiling.
 */
export function buildPersonaModelDropdownOptions({
  discoveredModelOptions,
  isRelayMesh,
  modelDiscoveryLoading,
  modelOptions,
}: {
  discoveredModelOptions: readonly PersonaModelOption[] | null;
  isRelayMesh: boolean;
  modelDiscoveryLoading: boolean;
  modelOptions: readonly PersonaModelOption[];
}): PersonaDropdownOption[] {
  return buildModelDropdownOptions({
    allowCustom: !isRelayMesh,
    globalModel: undefined,
    loading: modelDiscoveryLoading && discoveredModelOptions === null,
    loadingValue: MODEL_DISCOVERY_LOADING_VALUE,
    options: modelOptions,
  })
    .filter(
      (option) => isRelayMesh || option.value !== AUTO_MODEL_DROPDOWN_VALUE,
    )
    .map((option) =>
      isRelayMesh && option.value === AUTO_MODEL_DROPDOWN_VALUE
        ? { ...option, label: "Automatic" }
        : option,
    );
}
