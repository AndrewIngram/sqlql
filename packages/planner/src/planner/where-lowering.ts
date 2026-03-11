import type { RelExpr } from "@tupl/foundation";
import type { ScanFilterClause } from "@tupl/foundation";
import { resolveColumnDefinition, type SchemaDefinition } from "@tupl/schema-model";
import type {
  Binding,
  InSubqueryFilter,
  LiteralFilter,
  ParsedJoin,
  ParsedWhereFilters,
} from "./planner-types";
import type { SqlExprLoweringContext } from "./sql-expr-lowering";
import {
  lowerSqlAstToRelExpr,
  parseLiteral,
  parseSubqueryAst,
  resolveColumnRef,
  tryParseLiteralExpressionList,
} from "./sql-expr-lowering";

/**
 * Where lowering owns literal-filter extraction, pushdown eligibility, and residual expression parsing.
 */
export function validateEnumLiteralFilters(
  filters: LiteralFilter[],
  bindings: Binding[],
  schema: SchemaDefinition,
): void {
  const tableByAlias = new Map(bindings.map((binding) => [binding.alias, binding.table]));

  for (const filter of filters) {
    const tableName = tableByAlias.get(filter.alias);
    if (!tableName) {
      continue;
    }
    const definition = schema.tables[tableName]?.columns[filter.clause.column];
    if (!definition || typeof definition === "string") {
      continue;
    }
    const resolved = resolveColumnDefinition(definition);
    if (!resolved.enum) {
      continue;
    }

    if (filter.clause.op === "eq") {
      if (typeof filter.clause.value === "string" && !resolved.enum.includes(filter.clause.value)) {
        throw new Error(`Invalid enum value for ${tableName}.${filter.clause.column}`);
      }
      continue;
    }

    if (filter.clause.op === "in") {
      for (const value of filter.clause.values) {
        if (value == null) {
          continue;
        }
        if (typeof value !== "string" || !resolved.enum.includes(value)) {
          throw new Error(`Invalid enum value for ${tableName}.${filter.clause.column}`);
        }
      }
    }
  }
}

export function combineAndExprs(exprs: RelExpr[]): RelExpr | undefined {
  return exprs.reduce<RelExpr | undefined>(
    (acc, current) =>
      acc
        ? {
            kind: "function",
            name: "and",
            args: [acc, current],
          }
        : current,
    undefined,
  );
}

export function literalFilterToRelExpr(filter: LiteralFilter): RelExpr {
  const source: RelExpr = {
    kind: "column",
    ref: {
      alias: filter.alias,
      column: filter.clause.column,
    },
  };

  switch (filter.clause.op) {
    case "in":
    case "not_in":
      return {
        kind: "function",
        name: filter.clause.op,
        args: [
          source,
          ...filter.clause.values.map((value) => ({
            kind: "literal" as const,
            value: toRelLiteralValue(value),
          })),
        ],
      };
    case "is_null":
    case "is_not_null":
      return {
        kind: "function",
        name: filter.clause.op,
        args: [source],
      };
    default:
      return {
        kind: "function",
        name: filter.clause.op,
        args: [
          source,
          {
            kind: "literal",
            value: toRelLiteralValue(filter.clause.value),
          },
        ],
      };
  }
}

export function getPushableWhereAliases(rootAlias: string, joins: ParsedJoin[]): Set<string> {
  const reachable = new Set<string>([rootAlias]);

  for (const join of joins) {
    if (join.joinType !== "inner") {
      continue;
    }
    if (reachable.has(join.leftAlias)) {
      reachable.add(join.rightAlias);
      continue;
    }
    if (reachable.has(join.rightAlias)) {
      reachable.add(join.leftAlias);
    }
  }

  return reachable;
}

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

function parseLiteralFilter(
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

function flattenConjunctiveWhere(where: unknown): unknown[] | null {
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

function tryNormalizeBinaryOperator(raw: unknown): Exclude<ScanFilterClause["op"], never> | null {
  switch (raw) {
    case "=":
      return "eq";
    case "!=":
    case "<>":
      return "neq";
    case ">":
      return "gt";
    case ">=":
      return "gte";
    case "<":
      return "lt";
    case "<=":
      return "lte";
    case "IN":
      return "in";
    case "NOT IN":
      return "not_in";
    case "LIKE":
      return "like";
    case "NOT LIKE":
      return "not_like";
    case "IS DISTINCT FROM":
      return "is_distinct_from";
    case "IS NOT DISTINCT FROM":
      return "is_not_distinct_from";
    case "IS":
      return "is_null";
    case "IS NOT":
      return "is_not_null";
    default:
      return null;
  }
}

function invertOperator(
  op: Exclude<
    ScanFilterClause["op"],
    "in" | "not_in" | "like" | "not_like" | "is_null" | "is_not_null"
  >,
): Exclude<
  ScanFilterClause["op"],
  "in" | "not_in" | "like" | "not_like" | "is_null" | "is_not_null"
> {
  switch (op) {
    case "eq":
      return "eq";
    case "neq":
      return "neq";
    case "is_distinct_from":
      return "is_distinct_from";
    case "is_not_distinct_from":
      return "is_not_distinct_from";
    case "gt":
      return "lt";
    case "gte":
      return "lte";
    case "lt":
      return "gt";
    case "lte":
      return "gte";
  }
}

function toRelLiteralValue(value: unknown): string | number | boolean | null {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  throw new Error(`Unsupported literal filter value: ${JSON.stringify(value)}`);
}
