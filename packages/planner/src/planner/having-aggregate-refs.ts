import type { RelExpr, RelNode } from "@tupl/foundation";

import type { Binding } from "./planner-types";
import { getAggregateMetricSignature, parseAggregateMetric } from "./aggregate-lowering";

/**
 * Having aggregate refs own aggregate metric alias allocation and hidden metric registration.
 */
export function lowerHavingAggregateRef(
  expr: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): RelExpr | null {
  const metric = parseAggregateMetric(
    expr,
    deriveDefaultAggregateOutputName(expr),
    bindings,
    aliasToBinding,
  );
  if (!metric) {
    return null;
  }

  const signature = getAggregateMetricSignature(metric);
  let alias = aggregateMetricAliases.get(signature);
  if (!alias) {
    alias = `__having_metric_${aggregateMetricAliases.size + 1}`;
    aggregateMetricAliases.set(signature, alias);
    hiddenMetrics.push({
      ...metric,
      as: alias,
    });
  }

  return {
    kind: "column",
    ref: {
      column: alias,
    },
  };
}

function deriveDefaultAggregateOutputName(raw: unknown): string {
  const expr = raw as { name?: unknown };
  const fn = typeof expr.name === "string" ? expr.name.toLowerCase() : "agg";
  return `${fn}_value`;
}
