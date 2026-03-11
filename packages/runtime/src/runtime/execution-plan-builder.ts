import type { QueryExecutionPlan, QuerySessionInput, TuplDiagnostic } from "./contracts";
import type { RelNode } from "@tupl/foundation";
import { buildExecutionGraph } from "./execution-graph";
import { createPlanBuildState, finalizeExecutionPlan } from "./explain-shaping";

/**
 * Execution-plan builder owns session plan graph construction and visualization data.
 */
export function buildRelExecutionPlan<TContext>(
  input: QuerySessionInput<TContext>,
  rel: RelNode,
  diagnostics: TuplDiagnostic[] = [],
): QueryExecutionPlan {
  const state = createPlanBuildState();
  buildExecutionGraph(state, input, rel);
  return finalizeExecutionPlan(state, diagnostics);
}
