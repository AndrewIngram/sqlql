import { Result } from "better-result";

import type { CteAst, SelectAst } from "./sqlite-parser/ast";
import { toUnsupportedQueryShapeError } from "./planner-errors";
import { isCorrelatedSubquery, parseSubqueryAst } from "./sql-expr-lowering";
import {
  parseSupportedCorrelatedExistsSubquery,
  parseSupportedCorrelatedInSubquery,
  parseSupportedCorrelatedScalarAggregateProjectionSubquery,
  parseSupportedCorrelatedScalarAggregateSubquery,
} from "./subqueries/analysis";
import {
  parseNamedWindowSpecifications,
  parseWindowFrameClause,
  parseWindowOver,
} from "./windows/window-specifications";

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
    const nested = clause.stmt?.ast;
    if (nested) {
      const visibleCteNames = new Set(scopedCteNames);
      const cteName = getCteName(clause);
      if (cteName && clause.recursive) {
        visibleCteNames.add(cteName);
      }
      const reason = findUnsupportedQueryShape(nested, visibleCteNames);
      if (reason) {
        return reason;
      }
    }
    const cteName = getCteName(clause);
    if (cteName) {
      scopedCteNames.add(cteName);
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
  return findUnsupportedWindowShapeInValue(ast.columns, parseNamedWindowSpecifications(ast.window));
}

function findUnsupportedWindowShapeInValue(
  value: unknown,
  windowDefinitions: ReturnType<typeof parseNamedWindowSpecifications>,
): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const reason = findUnsupportedWindowShapeInValue(item, windowDefinitions);
      if (reason) {
        return reason;
      }
    }
    return null;
  }

  const record = value as Record<string, unknown>;
  const exprType = record.type;
  if (exprType === "function" || exprType === "aggr_func") {
    const overReason = validateSupportedWindowOver(record, windowDefinitions);
    if (overReason) {
      return overReason;
    }
  }

  for (const nested of Object.values(record)) {
    const reason = findUnsupportedWindowShapeInValue(nested, windowDefinitions);
    if (reason) {
      return reason;
    }
  }

  return null;
}

function validateSupportedWindowOver(
  expr: Record<string, unknown>,
  windowDefinitions: ReturnType<typeof parseNamedWindowSpecifications>,
): string | null {
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

  const spec = parseWindowOver(expr.over, windowDefinitions);
  const frame = parseWindowFrameClause(spec?.window_frame_clause?.raw);
  if (frame && frame.mode !== "rows") {
    return `Unsupported window frame mode: ${frame.mode.toUpperCase()}`;
  }

  return null;
}

function getCteName(clause: CteAst): string | null {
  const rawName = clause.name;
  return typeof rawName === "string"
    ? rawName
    : rawName && typeof rawName === "object" && typeof rawName.value === "string"
      ? rawName.value
      : null;
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
