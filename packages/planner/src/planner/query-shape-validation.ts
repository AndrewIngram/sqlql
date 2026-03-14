import { Result } from "better-result";

import type { SelectAst } from "./sqlite-parser/ast";
import { toUnsupportedQueryShapeError } from "./planner-errors";
import { isCorrelatedSubquery, parseSubqueryAst } from "./sql-expr-lowering";
import {
  parseSupportedCorrelatedExistsSubquery,
  parseSupportedCorrelatedInSubquery,
  parseSupportedCorrelatedScalarAggregateProjectionSubquery,
  parseSupportedCorrelatedScalarAggregateSubquery,
} from "./subqueries/analysis";

/**
 * Query-shape validation owns the unsupported SQL checks that run before relational lowering.
 */
export function validateQueryShapeResult(ast: SelectAst) {
  const reason = findUnsupportedQueryShape(ast, new Set<string>());
  if (reason) {
    return Result.err(toUnsupportedQueryShapeError(reason));
  }

  return Result.ok(ast);
}

function findUnsupportedQueryShape(ast: SelectAst, cteNames: Set<string>): string | null {
  const windowReason = findUnsupportedWindowShape(ast);
  if (windowReason) {
    return windowReason;
  }

  const withClauses = Array.isArray(ast.with) ? ast.with : [];

  const scopedCteNames = new Set(cteNames);
  for (const clause of withClauses) {
    const rawName = clause.name;
    const cteName =
      typeof rawName === "string"
        ? rawName
        : rawName && typeof rawName === "object" && typeof rawName.value === "string"
          ? rawName.value
          : null;
    if (cteName) {
      scopedCteNames.add(cteName);
    }
  }

  for (const clause of withClauses) {
    const nested = clause.stmt?.ast;
    if (nested) {
      const reason = findUnsupportedQueryShape(nested, scopedCteNames);
      if (reason) {
        return reason;
      }
    }
  }

  const from = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  for (const entry of from) {
    const nested = entry.stmt?.ast;
    if (nested) {
      const reason = findUnsupportedQueryShape(nested, scopedCteNames);
      if (reason) {
        return reason;
      }
    }
  }

  const outerAliases = new Set<string>(
    from.flatMap((entry) => {
      const alias = typeof entry.as === "string" && entry.as ? entry.as : entry.table;
      return typeof alias === "string" ? [alias] : [];
    }),
  );

  const expressionFields = [
    ast.columns,
    ast.where,
    ast.groupby,
    ast.having,
    ast.orderby,
    ast.limit,
    ast.window,
  ];
  for (const field of expressionFields) {
    const reason = findUnsupportedSubqueryShape(field, outerAliases, scopedCteNames);
    if (reason) {
      return reason;
    }
  }

  if (ast._next) {
    return findUnsupportedQueryShape(ast._next, scopedCteNames);
  }

  return null;
}

function findUnsupportedWindowShape(ast: SelectAst): string | null {
  const hasWindow = hasWindowExpression(ast.columns);
  if (hasWindow && (ast.groupby || ast.having)) {
    return "Window functions cannot be mixed with GROUP BY/HAVING.";
  }

  return findUnsupportedWindowShapeInValue(ast.columns);
}

function findUnsupportedWindowShapeInValue(value: unknown): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const reason = findUnsupportedWindowShapeInValue(item);
      if (reason) {
        return reason;
      }
    }
    return null;
  }

  const record = value as Record<string, unknown>;
  const exprType = record.type;
  if (exprType === "function" || exprType === "aggr_func") {
    const overReason = validateSupportedWindowOver(record);
    if (overReason) {
      return overReason;
    }
  }

  for (const nested of Object.values(record)) {
    const reason = findUnsupportedWindowShapeInValue(nested);
    if (reason) {
      return reason;
    }
  }

  return null;
}

function validateSupportedWindowOver(expr: Record<string, unknown>): string | null {
  if (!expr.over || typeof expr.over !== "object") {
    return null;
  }

  const rawName =
    expr.type === "aggr_func"
      ? typeof expr.name === "string"
        ? expr.name
        : null
      : (expr.name as { name?: Array<{ value?: unknown }> } | undefined)?.name?.[0]?.value;
  if (typeof rawName !== "string") {
    return "Unsupported window function.";
  }

  const normalized = rawName.toLowerCase();
  if (
    normalized !== "row_number" &&
    normalized !== "rank" &&
    normalized !== "dense_rank" &&
    normalized !== "lead" &&
    normalized !== "lag" &&
    normalized !== "first_value" &&
    normalized !== "count" &&
    normalized !== "sum" &&
    normalized !== "avg" &&
    normalized !== "min" &&
    normalized !== "max"
  ) {
    return `Unsupported window function: ${rawName.toUpperCase()}`;
  }

  return null;
}

function hasWindowExpression(rawColumns: unknown): boolean {
  if (rawColumns === "*") {
    return false;
  }

  const columns = Array.isArray(rawColumns) ? rawColumns : [];
  return columns.some((entry) => {
    const expr = (entry as { expr?: { over?: unknown } }).expr;
    return !!expr?.over;
  });
}

function findUnsupportedSubqueryShape(
  value: unknown,
  outerAliases: Set<string>,
  cteNames: Set<string>,
): string | null {
  const correlatedExists = parseSupportedCorrelatedExistsSubquery(value, outerAliases);
  if (correlatedExists) {
    return findUnsupportedQueryShape(correlatedExists.rewrittenSubquery, cteNames);
  }

  const correlatedIn = parseSupportedCorrelatedInSubquery(value, outerAliases);
  if (correlatedIn) {
    return findUnsupportedQueryShape(correlatedIn.rewrittenSubquery, cteNames);
  }

  const correlatedScalarAggregate = parseSupportedCorrelatedScalarAggregateSubquery(
    value,
    outerAliases,
  );
  if (correlatedScalarAggregate) {
    return findUnsupportedQueryShape(correlatedScalarAggregate.rewrittenSubquery, cteNames);
  }

  const correlatedScalarProjection = parseSupportedCorrelatedScalarAggregateProjectionSubquery(
    value,
    outerAliases,
  );
  if (correlatedScalarProjection) {
    return findUnsupportedQueryShape(correlatedScalarProjection.rewrittenSubquery, cteNames);
  }

  const subquery = parseSubqueryAst(value);
  if (subquery) {
    if (isCorrelatedSubquery(subquery, outerAliases)) {
      return "Correlated subqueries are not yet supported.";
    }
    return findUnsupportedQueryShape(subquery, cteNames);
  }

  if (!value || typeof value !== "object") {
    return null;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const reason = findUnsupportedSubqueryShape(item, outerAliases, cteNames);
      if (reason) {
        return reason;
      }
    }
    return null;
  }

  for (const nested of Object.values(value)) {
    const reason = findUnsupportedSubqueryShape(nested, outerAliases, cteNames);
    if (reason) {
      return reason;
    }
  }

  return null;
}
