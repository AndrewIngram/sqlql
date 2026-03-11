import type { RelExpr } from "@tupl/foundation";

import { nextRelId } from "../planner-ids";
import type { Binding } from "../planner-types";
import type { SqlExprLoweringContext } from "../sql-expr-lowering";
import { isCorrelatedSubquery, parseSubqueryAst } from "../sql-expr-utils";

/**
 * Expr subquery lowering owns scalar and EXISTS subquery expression translation.
 */
export function lowerExistsSubqueryExpr(
  raw: unknown,
  bindings: Binding[],
  context: SqlExprLoweringContext,
): RelExpr | null {
  const subquery = parseSubqueryAst(raw);
  if (!subquery) {
    return null;
  }

  const outerAliases = new Set(bindings.map((binding) => binding.alias));
  if (isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  const rel = context.tryLowerSelect(subquery);
  if (!rel) {
    return null;
  }

  return {
    kind: "subquery",
    id: nextRelId("subquery_expr"),
    mode: "exists",
    rel,
  };
}

export function lowerScalarSubqueryExpr(
  raw: unknown,
  bindings: Binding[],
  context: SqlExprLoweringContext,
): RelExpr | null {
  const subquery = parseSubqueryAst(raw);
  if (!subquery) {
    return null;
  }

  const outerAliases = new Set(bindings.map((binding) => binding.alias));
  if (isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  const rel = context.tryLowerSelect(subquery);
  if (!rel || rel.output.length !== 1) {
    return null;
  }

  const outputColumn = rel.output[0]?.name;
  if (!outputColumn) {
    return null;
  }

  return {
    kind: "subquery",
    id: nextRelId("subquery_expr"),
    mode: "scalar",
    rel,
    outputColumn,
  };
}
