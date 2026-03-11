import type { RelExpr, RelNode } from "@tupl/foundation";

import type { Binding } from "./planner-types";
import { resolveColumnRef } from "./sql-expr-lowering";
import { lowerHavingAggregateRef } from "./aggregate/having-aggregate-refs";
import { lowerHavingBinaryExpr, lowerHavingFunctionExpr } from "./aggregate/having-function-exprs";

/**
 * Having lowering owns aggregate-aware expression lowering for HAVING clauses.
 */
export function lowerHavingExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): RelExpr | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const expr = raw as {
    type?: unknown;
    value?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
    name?: unknown;
    args?: { value?: unknown; expr?: unknown; distinct?: unknown };
    over?: unknown;
    ast?: unknown;
  };

  switch (expr.type) {
    case "string":
      return { kind: "literal", value: typeof expr.value === "string" ? expr.value : "" };
    case "number":
      return typeof expr.value === "number" ? { kind: "literal", value: expr.value } : null;
    case "bool":
      return typeof expr.value === "boolean" ? { kind: "literal", value: expr.value } : null;
    case "null":
      return { kind: "literal", value: null };
    case "column_ref": {
      const resolved = resolveColumnRef(expr, bindings, aliasToBinding);
      if (!resolved) {
        return null;
      }
      return {
        kind: "column",
        ref: {
          column: resolved.column,
        },
      };
    }
    case "aggr_func":
      return lowerHavingAggregateRef(
        expr,
        bindings,
        aliasToBinding,
        aggregateMetricAliases,
        hiddenMetrics,
      );
    case "binary_expr":
      return lowerHavingBinaryExpr(
        expr,
        bindings,
        aliasToBinding,
        aggregateMetricAliases,
        hiddenMetrics,
        lowerHavingExpr,
      );
    case "function":
      return lowerHavingFunctionExpr(
        expr,
        bindings,
        aliasToBinding,
        aggregateMetricAliases,
        hiddenMetrics,
        lowerHavingExpr,
      );
    default:
      if ("ast" in expr) {
        return null;
      }
      return null;
  }
}
