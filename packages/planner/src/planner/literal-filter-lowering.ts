import type { RelExpr } from "@tupl/foundation";

import type { Binding, InSubqueryFilter, LiteralFilter, ParsedWhereFilters } from "./planner-types";
import type { SqlExprLoweringContext } from "./sql-expr-lowering";
import { lowerSqlAstToRelExpr } from "./sql-expr-lowering";
import { flattenConjunctiveWhere, parseLiteralFilter } from "./expr/literal-filter-parser";
import { literalFilterToRelExpr } from "./expr/literal-filter-operators";

/**
 * Literal filter lowering owns pushdown-friendly predicate extraction and residual expr fallback.
 */
export function parseWhereFilters(
  where: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  lowerExprContext: SqlExprLoweringContext,
): ParsedWhereFilters | null {
  if (!where) {
    return {
      literals: [],
      inSubqueries: [],
    };
  }

  const parts = flattenConjunctiveWhere(where);
  if (parts == null) {
    const residualExpr = lowerSqlAstToRelExpr(where, bindings, aliasToBinding, lowerExprContext);
    if (!residualExpr) {
      return null;
    }
    return {
      literals: [],
      inSubqueries: [],
      residualExpr,
    };
  }

  const literals: LiteralFilter[] = [];
  const inSubqueries: InSubqueryFilter[] = [];
  const residualParts: RelExpr[] = [];
  for (const part of parts) {
    const parsed = parseLiteralFilter(part, bindings, aliasToBinding);
    if (!parsed) {
      const residual = lowerSqlAstToRelExpr(part, bindings, aliasToBinding, lowerExprContext);
      if (!residual) {
        return null;
      }
      residualParts.push(residual);
      continue;
    }
    if ("subquery" in parsed) {
      inSubqueries.push(parsed);
      continue;
    }
    literals.push(parsed);
  }

  const residualExpr = residualParts.reduce<RelExpr | null>(
    (acc, current) =>
      acc
        ? {
            kind: "function",
            name: "and",
            args: [acc, current],
          }
        : current,
    null,
  );

  return residualExpr
    ? {
        literals,
        inSubqueries,
        residualExpr,
      }
    : {
        literals,
        inSubqueries,
      };
}

export { literalFilterToRelExpr } from "./expr/literal-filter-operators";
