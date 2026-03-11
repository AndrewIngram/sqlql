import type { RelExpr } from "@tupl/foundation";
import type { SelectAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";

import type { Binding } from "./planner-types";
import {
  collectRelExprRefs,
  collectTablesFromSelectAst,
  isCorrelatedSubquery,
  mapBinaryOperatorToRelFunction,
  parseLimitAndOffset,
  parseLiteral,
  parseNamedWindowSpecifications,
  parsePositiveOrdinalLiteral,
  parseSubqueryAst,
  parseWindowOver,
  readWindowFunctionName,
  resolveColumnRef,
  supportsRankWindowArgs,
  toRawColumnRef,
  tryParseLiteralExpressionList,
} from "./sql-expr-utils";
import {
  lowerBinaryExprToRelExpr,
  lowerColumnRefExpr,
  lowerFunctionExprToRelExpr,
} from "./expr-function-lowering";
import { lowerScalarSubqueryExpr } from "./expr-subquery-lowering";

export interface SqlExprLoweringContext {
  schema: SchemaDefinition;
  cteNames: Set<string>;
  tryLowerSelect(ast: SelectAst): import("@tupl/foundation").RelNode | null;
}

/**
 * SQL expression lowering owns translation from parser AST fragments into RelExpr.
 * Callers provide a structured-select callback so subqueries stay planner-owned without
 * coupling this module back to the higher-level select lowering implementation.
 */
export function lowerSqlAstToRelExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
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
    args?: { value?: unknown };
    ast?: unknown;
  };

  if ("ast" in expr) {
    return lowerScalarSubqueryExpr(raw, bindings, context);
  }

  switch (expr.type) {
    case "string":
      return { kind: "literal", value: typeof expr.value === "string" ? expr.value : "" };
    case "number":
      return typeof expr.value === "number" ? { kind: "literal", value: expr.value } : null;
    case "bool":
      return typeof expr.value === "boolean" ? { kind: "literal", value: expr.value } : null;
    case "null":
      return { kind: "literal", value: null };
    case "column_ref":
      return lowerColumnRefExpr(expr, bindings, aliasToBinding);
    case "binary_expr":
      return lowerBinaryExprToRelExpr(
        expr,
        bindings,
        aliasToBinding,
        context,
        lowerSqlAstToRelExpr,
      );
    case "function":
      return lowerFunctionExprToRelExpr(
        expr,
        bindings,
        aliasToBinding,
        context,
        lowerSqlAstToRelExpr,
      );
    default:
      return null;
  }
}

export {
  collectRelExprRefs,
  collectTablesFromSelectAst,
  isCorrelatedSubquery,
  mapBinaryOperatorToRelFunction,
  parseLimitAndOffset,
  parseLiteral,
  parseNamedWindowSpecifications,
  parsePositiveOrdinalLiteral,
  parseSubqueryAst,
  parseWindowOver,
  readWindowFunctionName,
  resolveColumnRef,
  supportsRankWindowArgs,
  toRawColumnRef,
  tryParseLiteralExpressionList,
};
