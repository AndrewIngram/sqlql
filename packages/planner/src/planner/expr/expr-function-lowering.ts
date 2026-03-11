import type { RelExpr } from "@tupl/foundation";

import type { Binding } from "../planner-types";
import type { SqlExprLoweringContext } from "../sql-expr-lowering";
import { lowerExistsSubqueryExpr } from "./expr-subquery-lowering";
import { mapBinaryOperatorToRelFunction, parseLiteral, resolveColumnRef } from "../sql-expr-utils";

type LowerExprFn = (
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
) => RelExpr | null;

/**
 * Expr function lowering owns binary/function expression lowering and argument parsing.
 */
export function lowerColumnRefExpr(
  expr: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): RelExpr | null {
  const resolved = resolveColumnRef(expr, bindings, aliasToBinding);
  if (!resolved) {
    return null;
  }

  return {
    kind: "column",
    ref: {
      alias: resolved.alias,
      column: resolved.column,
    },
  };
}

export function lowerBinaryExprToRelExpr(
  expr: {
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
  lowerExpr: LowerExprFn,
): RelExpr | null {
  const operator = typeof expr.operator === "string" ? expr.operator.toUpperCase() : null;
  if (!operator) {
    return null;
  }

  if (operator === "BETWEEN") {
    const range = expr.right as { type?: unknown; value?: unknown } | undefined;
    if (range?.type !== "expr_list" || !Array.isArray(range.value) || range.value.length !== 2) {
      return null;
    }
    const left = lowerExpr(expr.left, bindings, aliasToBinding, context);
    const low = lowerExpr(range.value[0], bindings, aliasToBinding, context);
    const high = lowerExpr(range.value[1], bindings, aliasToBinding, context);
    if (!left || !low || !high) {
      return null;
    }
    return {
      kind: "function",
      name: "between",
      args: [left, low, high],
    };
  }

  if (operator === "IN" || operator === "NOT IN") {
    const left = lowerExpr(expr.left, bindings, aliasToBinding, context);
    const values = parseExprListToRelExprArgs(
      expr.right,
      bindings,
      aliasToBinding,
      context,
      lowerExpr,
    );
    if (!left || !values) {
      return null;
    }
    return {
      kind: "function",
      name: operator === "NOT IN" ? "not_in" : "in",
      args: [left, ...values],
    };
  }

  if (operator === "IS" || operator === "IS NOT") {
    const left = lowerExpr(expr.left, bindings, aliasToBinding, context);
    const rightLiteral = parseLiteral(expr.right);
    if (!left || rightLiteral !== null) {
      return null;
    }
    return {
      kind: "function",
      name: operator === "IS NOT" ? "is_not_null" : "is_null",
      args: [left],
    };
  }

  const left = lowerExpr(expr.left, bindings, aliasToBinding, context);
  const right = lowerExpr(expr.right, bindings, aliasToBinding, context);
  if (!left || !right) {
    return null;
  }

  const mapped = mapBinaryOperatorToRelFunction(operator);
  if (!mapped) {
    return null;
  }

  return {
    kind: "function",
    name: mapped,
    args: [left, right],
  };
}

export function lowerFunctionExprToRelExpr(
  expr: {
    name?: unknown;
    args?: { value?: unknown };
    over?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
  lowerExpr: LowerExprFn,
): RelExpr | null {
  if (expr.over) {
    return null;
  }

  const rawName = (expr.name as { name?: Array<{ value?: unknown }> } | undefined)?.name?.[0]
    ?.value;
  if (typeof rawName !== "string") {
    return null;
  }

  const normalized = rawName.toLowerCase();
  if (normalized === "exists") {
    const values = Array.isArray(expr.args?.value) ? expr.args?.value : [expr.args?.value];
    if (values.length !== 1) {
      return null;
    }
    return lowerExistsSubqueryExpr(values[0], bindings, context);
  }

  const args = parseFunctionArgsToRelExpr(
    expr.args?.value,
    bindings,
    aliasToBinding,
    context,
    lowerExpr,
  );
  if (!args) {
    return null;
  }

  if (normalized === "not") {
    return args.length === 1 ? { kind: "function", name: "not", args } : null;
  }

  if (
    normalized !== "lower" &&
    normalized !== "upper" &&
    normalized !== "trim" &&
    normalized !== "length" &&
    normalized !== "substr" &&
    normalized !== "substring" &&
    normalized !== "coalesce" &&
    normalized !== "nullif" &&
    normalized !== "abs" &&
    normalized !== "round" &&
    normalized !== "cast" &&
    normalized !== "case"
  ) {
    return null;
  }

  return {
    kind: "function",
    name: normalized === "substring" ? "substr" : normalized,
    args,
  };
}

function parseFunctionArgsToRelExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
  lowerExpr: LowerExprFn,
): RelExpr[] | null {
  if (raw == null) {
    return [];
  }

  const values = Array.isArray(raw) ? raw : [raw];
  const args: RelExpr[] = [];
  for (const value of values) {
    const arg = lowerExpr(value, bindings, aliasToBinding, context);
    if (!arg) {
      return null;
    }
    args.push(arg);
  }
  return args;
}

function parseExprListToRelExprArgs(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
  lowerExpr: LowerExprFn,
): RelExpr[] | null {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return null;
  }

  return parseFunctionArgsToRelExpr(expr.value, bindings, aliasToBinding, context, lowerExpr);
}
