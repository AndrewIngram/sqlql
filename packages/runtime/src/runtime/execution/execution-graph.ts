import type { RelNode } from "@tupl/foundation";

import type { QuerySessionInput } from "../session/contracts";
import { buildJoinStep, buildSetOpStep, buildWithStep } from "./execution-branch-builders";
import {
  buildAggregateStep,
  buildCteRefStep,
  buildFilterStep,
  buildLimitOffsetStep,
  buildProjectStep,
  buildScanStep,
  buildSortStep,
  buildValuesStep,
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
      case "values":
        return buildValuesStep(state, node, scopeId);
      case "scan":
        return buildScanStep(state, node, scopeId);
      case "cte_ref":
        return buildCteRefStep(state, node, scopeId);
      case "filter":
        return buildFilterStep(state, node, scopeId, visit);
      case "project":
        return buildProjectStep(state, node, scopeId, visit);
      case "correlate": {
        const leftId = visit(node.left, scopeId);
        const rightId = visit(node.right, scopeId);
        const id = `correlate_${state.steps.length + 1}`;
        state.steps.push({
          id,
          kind: "projection",
          dependsOn: [leftId, rightId],
          summary: "Correlated subquery rewrite",
          phase: "transform",
          operation: {
            name: "correlate",
            details: {
              apply: node.apply.kind,
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "WHERE",
          scopeId,
        });
        return id;
      }
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
      case "repeat_union": {
        const seedId = visit(node.seed, scopeId);
        const iterativeId = visit(node.iterative, scopeId);
        const id = `repeat_union_${state.steps.length + 1}`;
        state.steps.push({
          id,
          kind: "projection",
          dependsOn: [seedId, iterativeId],
          summary: `Recursive CTE (${node.cteName})`,
          phase: "transform",
          operation: {
            name: "repeat_union",
            details: { cteName: node.cteName, mode: node.mode },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "WITH",
          scopeId,
        });
        return id;
      }
    }
  };

  visit(rel, "scope_root");
}
