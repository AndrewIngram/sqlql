import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";

import type { QuerySessionInput } from "../contracts";
import type { PlanBuildState } from "./explain-shaping";
import { nextPlanId } from "./explain-shaping";
import { resolveSyncProviderCapabilityForRel } from "../provider-execution";

/**
 * Step families own plan-time decisions for remote fragment execution families.
 */
export function tryPlanRemoteFragmentStep<TContext>(
  state: PlanBuildState,
  input: QuerySessionInput<TContext>,
  node: RelNode,
  scopeId: string,
): string | null {
  if (node.kind === "scan" || node.kind === "sql") {
    return null;
  }

  const resolutionResult = resolveSyncProviderCapabilityForRel(input, node);
  if (Result.isError(resolutionResult)) {
    return null;
  }

  const resolution = resolutionResult.value;
  if (
    !resolution ||
    !resolution.fragment ||
    !resolution.provider ||
    !resolution.report?.supported
  ) {
    return null;
  }

  const id = nextPlanId(state, "remote_fragment");
  state.steps.push({
    id,
    kind: "remote_fragment",
    dependsOn: [],
    summary: `Execute provider fragment (${resolution.fragment.provider})`,
    phase: "fetch",
    operation: {
      name: "provider_fragment",
      details: {
        provider: resolution.fragment.provider,
      },
    },
    request: {
      fragment: resolution.fragment.kind,
    },
    outputs: node.output.map((column) => column.name),
    sqlOrigin: "SELECT",
    scopeId,
    ...(resolution.diagnostics.length > 0 ? { diagnostics: resolution.diagnostics } : {}),
  });
  return id;
}
