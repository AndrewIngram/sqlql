import type { ProviderSupportAnalysis } from "../provider/provider-support-analysis";
import type { PhysicalStep } from "../physical/physical";

export interface PhysicalPlanningState {
  providerSupport: ProviderSupportAnalysis;
  steps: PhysicalStep[];
}

export function createPhysicalPlanningState(
  providerSupport: ProviderSupportAnalysis,
): PhysicalPlanningState {
  return {
    providerSupport,
    steps: [],
  };
}

export function recordPhysicalStep(state: PhysicalPlanningState, step: PhysicalStep): string {
  state.steps.push(step);
  return step.id;
}
