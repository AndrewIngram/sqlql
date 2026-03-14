import { Result, type Result as BetterResult } from "better-result";

import { type RelNode, type TuplError } from "@tupl/foundation";
import { nextPhysicalStepId } from "../physical/planner-ids";
import type { PhysicalStep } from "../physical/physical";
import { getProviderSupportDecision } from "../provider/provider-support-analysis";
import { recordPhysicalStep, type PhysicalPlanningState } from "./physical-plan-state";

/**
 * Remote fragment planning owns maximal remote-fragment step creation from precomputed support
 * analysis. Capability discovery happens earlier in the planner so this module stays focused on
 * turning accepted provider-owned subtrees into physical steps.
 */
export async function tryPlanRemoteFragmentResult(
  node: RelNode,
  state: PhysicalPlanningState,
): Promise<BetterResult<string | null, TuplError>> {
  const support = getProviderSupportDecision(state.providerSupport, node);
  if (!support?.supported || !support.provider || !support.fragment) {
    return Result.ok(null);
  }

  const step: PhysicalStep = {
    id: nextPhysicalStepId("remote_fragment"),
    kind: "remote_fragment",
    dependsOn: [],
    summary: `Execute provider fragment (${support.provider})`,
    provider: support.provider,
    fragment: support.fragment,
  };

  return Result.ok(recordPhysicalStep(state, step));
}
