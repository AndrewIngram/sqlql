import type { RelColumnRef, RelNode } from "@tupl/foundation";
import type { SelectColumnAst } from "./sqlite-parser/ast";
import type {
  Binding,
  ParsedAggregateProjection,
  ParsedAggregateMetricProjection,
  ParsedGroupByTerm,
} from "./planner-types";
import type { SqlExprLoweringContext } from "./sql-expr-lowering";
import {
  lowerSqlAstToRelExpr,
  parsePositiveOrdinalLiteral,
  resolveColumnRef,
} from "./sql-expr-lowering";
import { parseWindowOver } from "./sql-expr-utils";
import { lowerHavingExpr } from "./having-lowering";
import {
  parseOrderBy,
  resolveAggregateGroupBy,
  resolveAggregateOrderBy,
  resolveNonAggregateOrderBy,
  validateAggregateProjectionGroupBy,
} from "./aggregate-ordering";

/**
 * Aggregate lowering owns GROUP BY, HAVING, aggregate metrics, and ORDER BY resolution
 * for aggregate projections.
 */
export function hasAggregateProjection(rawColumns: unknown): boolean {
  if (rawColumns === "*") {
    return false;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  return columns.some((entry) => {
    const expr = entry.expr as { type?: unknown; over?: unknown };
    return expr.type === "aggr_func" && !expr.over;
  });
}

export function getGroupByColumns(rawGroupBy: unknown): unknown[] {
  if (!rawGroupBy || typeof rawGroupBy !== "object") {
    return [];
  }

  const groupBy = rawGroupBy as { columns?: unknown };
  return Array.isArray(groupBy.columns) ? groupBy.columns : [];
}

export function parseGroupBy(
  rawGroupBy: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): ParsedGroupByTerm[] | null {
  const refs: ParsedGroupByTerm[] = [];

  for (const entry of getGroupByColumns(rawGroupBy)) {
    const ordinal = parsePositiveOrdinalLiteral(entry, "GROUP BY");
    if (ordinal != null) {
      refs.push({
        kind: "ordinal",
        position: ordinal,
      });
      continue;
    }

    const resolved = resolveColumnRef(entry, bindings, aliasToBinding);
    if (!resolved) {
      return null;
    }

    refs.push({
      kind: "ref",
      ref: {
        alias: resolved.alias,
        column: resolved.column,
      },
    });
  }

  return refs;
}

export function parseAggregateProjections(
  rawColumns: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
): ParsedAggregateProjection[] | null {
  if (rawColumns === "*") {
    return null;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  if (columns.length === 0) {
    return null;
  }

  const out: ParsedAggregateProjection[] = [];

  for (const entry of columns) {
    if (isWindowProjection(entry)) {
      continue;
    }

    const exprType = (entry.expr as { type?: unknown })?.type;
    if (exprType === "aggr_func") {
      const output =
        typeof entry.as === "string" && entry.as.length > 0
          ? entry.as
          : deriveDefaultAggregateOutputName(entry.expr);
      const metric = parseAggregateMetric(entry.expr, output, bindings, aliasToBinding);
      if (!metric) {
        return null;
      }

      out.push({
        kind: "metric",
        output,
        metric,
      });
      continue;
    }

    const column = resolveColumnRef(entry.expr, bindings, aliasToBinding);
    const output =
      typeof entry.as === "string" && entry.as.length > 0 ? entry.as : (column?.column ?? "expr");
    if (column) {
      out.push({
        kind: "group",
        source: {
          alias: column.alias,
          column: column.column,
        },
        output,
      });
      continue;
    }

    const expr = lowerSqlAstToRelExpr(entry.expr, bindings, aliasToBinding, context);
    if (!expr) {
      return null;
    }

    out.push({
      kind: "group",
      expr,
      output,
    });
  }

  return out;
}

function isWindowProjection(entry: SelectColumnAst): boolean {
  const expr = entry.expr as { type?: unknown; over?: unknown };
  if (expr.type !== "function" && expr.type !== "aggr_func") {
    return false;
  }

  return parseWindowOver(expr.over, new Map()) != null;
}

export function parseAggregateMetric(
  raw: unknown,
  output: string,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): ParsedAggregateMetricProjection["metric"] | null {
  const expr = raw as {
    type?: unknown;
    name?: unknown;
    args?: {
      expr?: unknown;
      distinct?: unknown;
    };
  };

  if (expr.type !== "aggr_func" || typeof expr.name !== "string") {
    return null;
  }

  const fn = expr.name.toLowerCase();
  if (fn !== "count" && fn !== "sum" && fn !== "avg" && fn !== "min" && fn !== "max") {
    return null;
  }

  const distinct = expr.args?.distinct === "DISTINCT";
  const arg = expr.args?.expr;
  const column = parseAggregateMetricColumn(arg, bindings, aliasToBinding);

  if (fn === "count") {
    if (column === null) {
      return null;
    }
    if (!column && distinct) {
      return null;
    }

    return {
      fn,
      as: output,
      ...(column ? { column } : {}),
      ...(distinct ? { distinct: true } : {}),
    };
  }

  if (!column) {
    return null;
  }

  return {
    fn,
    as: output,
    column,
    ...(distinct ? { distinct: true } : {}),
  };
}

function parseAggregateMetricColumn(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): RelColumnRef | null | undefined {
  if (!raw) {
    return undefined;
  }

  const maybeStar = raw as { type?: unknown; value?: unknown };
  if (maybeStar.type === "star" || maybeStar.value === "*") {
    return undefined;
  }

  const resolved = resolveColumnRef(raw, bindings, aliasToBinding);
  if (!resolved) {
    return null;
  }

  return {
    alias: resolved.alias,
    column: resolved.column,
  };
}

function deriveDefaultAggregateOutputName(raw: unknown): string {
  const expr = raw as { name?: unknown };
  const fn = typeof expr.name === "string" ? expr.name.toLowerCase() : "agg";
  return `${fn}_value`;
}

export function getAggregateMetricSignature(
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
): string {
  const ref = metric.column;
  return `${metric.fn}|${metric.distinct ? "distinct" : "all"}|${ref?.alias ?? ref?.table ?? ""}.${ref?.column ?? "*"}`;
}

export {
  lowerHavingExpr,
  parseOrderBy,
  resolveAggregateGroupBy,
  resolveAggregateOrderBy,
  resolveNonAggregateOrderBy,
  validateAggregateProjectionGroupBy,
};
