import type { RelNode } from "@tupl/foundation";

import type { QuerySessionInput } from "../contracts";
import { buildJoinStep, buildSetOpStep, buildWithStep } from "./execution-branch-builders";
import {
  buildAggregateStep,
  buildFilterStep,
  buildLimitOffsetStep,
  buildProjectStep,
  buildScanStep,
  buildSortStep,
  buildSqlStep,
  buildWindowStep,
} from "./execution-step-builders";
import type { PlanBuildState } from "./explain-shaping";
import { formatColumnRef } from "./step-routing";
import { tryPlanRemoteFragmentStep } from "./step-families";

/**
 * Execution graph owns recursive rel-to-step graph construction for plan/explain output.
 */
export function buildExecutionGraph<TContext>(
  state: PlanBuildState,
  input: QuerySessionInput<TContext>,
  rel: RelNode,
): void {
  const visit = (node: RelNode, scopeId = "scope_root"): string => {
    const remoteFragmentStepId = tryPlanRemoteFragmentStep(state, input, node, scopeId);
    if (remoteFragmentStepId) {
      return remoteFragmentStepId;
    }

    switch (node.kind) {
      case "scan":
        return buildScanStep(state, node, scopeId);
      case "filter":
        return buildFilterStep(state, node, scopeId, visit);
      case "project":
        return buildProjectStep(state, node, scopeId, visit);
      case "join":
        return buildJoinStep(state, input, node, scopeId, visit);
      case "aggregate":
        return buildAggregateStep(state, node, scopeId, visit, formatColumnRef);
      case "window":
        return buildWindowStep(state, node, scopeId, visit, formatColumnRef);
      case "sort":
        return buildSortStep(state, node, scopeId, visit, formatColumnRef);
      case "limit_offset":
        return buildLimitOffsetStep(state, node, scopeId, visit);
      case "set_op":
        return buildSetOpStep(state, node, scopeId, visit);
      case "with":
        return buildWithStep(state, node, scopeId, visit);
      case "sql":
        return buildSqlStep(state, node, scopeId);
    }
  };

  visit(rel, "scope_root");
}
