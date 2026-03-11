import type { RelExpr, RelNode } from "@tupl/foundation";

import type {
  QueryExecutionPlan,
  QueryExecutionPlanScope,
  QueryExecutionPlanStep,
  TuplDiagnostic,
} from "./contracts";

export interface PlanBuildState {
  stepCounter: number;
  steps: QueryExecutionPlanStep[];
  scopes: QueryExecutionPlanScope[];
  whereSubqueryScopeCount: number;
  selectSubqueryScopeCount: number;
}

/**
 * Explain shaping owns plan-build state, scope creation, and subquery-scope labeling.
 */
export function createPlanBuildState(): PlanBuildState {
  return {
    stepCounter: 0,
    steps: [],
    scopes: [
      {
        id: "scope_root",
        kind: "root",
        label: "Root query",
      },
    ],
    whereSubqueryScopeCount: 0,
    selectSubqueryScopeCount: 0,
  };
}

export function nextPlanId(state: PlanBuildState, prefix: string): string {
  state.stepCounter += 1;
  return `${prefix}_${state.stepCounter}`;
}

export function finalizeExecutionPlan(
  state: PlanBuildState,
  diagnostics: TuplDiagnostic[],
): QueryExecutionPlan {
  return {
    steps: state.steps,
    scopes: state.scopes,
    ...(diagnostics.length > 0 ? { diagnostics } : {}),
  };
}

export function visitExprSubqueries(
  state: PlanBuildState,
  expr: RelExpr,
  owner: "WHERE" | "SELECT",
  parentScopeId: string,
  visitRelNode: (node: RelNode, scopeId: string) => string,
): string[] {
  switch (expr.kind) {
    case "literal":
    case "column":
      return [];
    case "function":
      return [
        ...new Set(
          expr.args.flatMap((arg) =>
            visitExprSubqueries(state, arg, owner, parentScopeId, visitRelNode),
          ),
        ),
      ];
    case "subquery": {
      const scopeId = nextPlanId(state, "scope_subquery");
      const label =
        owner === "WHERE"
          ? `Subquery WHERE #${++state.whereSubqueryScopeCount}`
          : `Subquery SELECT #${++state.selectSubqueryScopeCount}`;
      state.scopes.push({
        id: scopeId,
        kind: "subquery",
        label,
        parentId: parentScopeId,
      });
      return [visitRelNode(expr.rel, scopeId)];
    }
  }
}
