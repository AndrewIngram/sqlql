import type { InSubqueryFilter, LiteralFilter } from "./planner-types";
import type { Binding } from "./planner-types";
import {
  parseLiteral,
  parseSubqueryAst,
  resolveColumnRef,
  tryParseLiteralExpressionList,
} from "./sql-expr-lowering";
import { invertOperator, tryNormalizeBinaryOperator } from "./literal-filter-operators";

/**
 * Literal filter parser owns pushdown-friendly predicate parsing from SQL AST fragments.
 */
export function parseLiteralFilter(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): LiteralFilter | InSubqueryFilter | null {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr.type !== "binary_expr") {
    return null;
  }

  const operator = tryNormalizeBinaryOperator(expr.operator);
  if (!operator) {
    return null;
  }

  if (operator === "in") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const subquery = parseSubqueryAst(expr.right);
    if (col && subquery) {
      return {
        alias: col.alias,
        column: col.column,
        subquery,
      };
    }

    const values = tryParseLiteralExpressionList(expr.right);
    if (!col || !values) {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: "in",
        column: col.column,
        values,
      },
    };
  }

  if (operator === "not_in") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const values = tryParseLiteralExpressionList(expr.right);
    if (!col || !values) {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: "not_in",
        column: col.column,
        values,
      },
    };
  }

  if (operator === "is_null" || operator === "is_not_null") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const value = parseLiteral(expr.right);
    if (!col || value !== null) {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: operator,
        column: col.column,
      },
    };
  }

  if (operator === "like" || operator === "not_like") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const value = parseLiteral(expr.right);
    if (!col || typeof value !== "string") {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: operator,
        column: col.column,
        value,
      },
    };
  }

  const leftCol = resolveColumnRef(expr.left, bindings, aliasToBinding);
  const rightCol = resolveColumnRef(expr.right, bindings, aliasToBinding);

  if (leftCol && rightCol) {
    return null;
  }

  if (leftCol) {
    const value = parseLiteral(expr.right);
    if (value === undefined) {
      return null;
    }

    return {
      alias: leftCol.alias,
      clause: {
        op: operator,
        column: leftCol.column,
        value,
      },
    };
  }

  if (rightCol) {
    const value = parseLiteral(expr.left);
    if (value === undefined) {
      return null;
    }

    return {
      alias: rightCol.alias,
      clause: {
        op: invertOperator(operator),
        column: rightCol.column,
        value,
      },
    };
  }

  return null;
}

export function flattenConjunctiveWhere(where: unknown): unknown[] | null {
  if (!where) {
    return [];
  }

  const expr = where as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr.type === "binary_expr" && expr.operator === "AND") {
    const left = flattenConjunctiveWhere(expr.left);
    const right = flattenConjunctiveWhere(expr.right);
    if (!left || !right) {
      return null;
    }

    return [...left, ...right];
  }

  if (expr.type === "binary_expr" && expr.operator === "OR") {
    return null;
  }

  return [expr];
}
