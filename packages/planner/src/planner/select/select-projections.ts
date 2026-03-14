import type {
  RelColumnRef,
  RelNode,
  RelProjectExprMapping,
  RelProjectNode,
} from "@tupl/foundation";
import type { SelectColumnAst, WindowSpecificationAst } from "../sqlite-parser/ast";
import type { Binding, SelectProjection, SelectWindowProjection } from "../planner-types";
import {
  lowerSqlAstToRelExpr,
  parseNamedWindowSpecifications,
  parseWindowFrameClause,
  parseWindowOver,
  readWindowFunctionArgs,
  readWindowFunctionName,
  resolveColumnRef,
  supportsRankWindowArgs,
  type SqlExprLoweringContext,
} from "../sql-expr-lowering";
import { nextRelId } from "../physical/planner-ids";
import { parseAggregateMetric } from "../aggregate-lowering";
import { parseSupportedCorrelatedScalarAggregateProjectionSubquery } from "../subqueries/analysis";
import { parseRelColumnRef } from "./select-from-lowering";

/**
 * Select projections own projection, window-function, and project materialization lowering.
 */
export function parseProjection(
  rawColumns: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  windowDefinitions: Map<string, WindowSpecificationAst>,
  lowerExprContext: SqlExprLoweringContext,
): SelectProjection[] | null {
  if (rawColumns === "*") {
    return null;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  if (columns.length === 0) {
    return null;
  }

  const out: SelectProjection[] = [];
  const outerAliases = new Set(bindings.map((binding) => binding.alias));

  for (const entry of columns) {
    const column = resolveColumnRef(entry.expr, bindings, aliasToBinding);
    if (column) {
      out.push({
        kind: "column",
        source: {
          alias: column.alias,
          column: column.column,
        },
        output: typeof entry.as === "string" && entry.as.length > 0 ? entry.as : column.column,
      });
      continue;
    }

    const windowProjection = parseWindowProjection(
      entry,
      bindings,
      aliasToBinding,
      windowDefinitions,
      lowerExprContext,
    );
    if (windowProjection) {
      out.push(windowProjection);
      continue;
    }

    const correlatedScalarProjection = parseSupportedCorrelatedScalarAggregateProjectionSubquery(
      entry.expr,
      outerAliases,
    );
    if (correlatedScalarProjection) {
      out.push({
        kind: "correlated_scalar",
        output: typeof entry.as === "string" && entry.as.length > 0 ? entry.as : "expr",
        projection: {
          ...correlatedScalarProjection,
          subquery: correlatedScalarProjection.rewrittenSubquery,
        },
      });
      continue;
    }

    const expr = lowerSqlAstToRelExpr(entry.expr, bindings, aliasToBinding, lowerExprContext);
    if (!expr) {
      return null;
    }

    out.push({
      kind: "expr",
      expr,
      output: typeof entry.as === "string" && entry.as.length > 0 ? entry.as : "expr",
    });
  }

  return out;
}

export function toParsedOrderSource(
  ref: RelColumnRef | null | undefined,
  fallbackColumn: string,
): { alias?: string; column: string } {
  if (!ref) {
    return {
      column: fallbackColumn,
    };
  }
  return ref.alias
    ? {
        alias: ref.alias,
        column: ref.column,
      }
    : {
        column: ref.column,
      };
}

export function appendProjectExpressions(
  input: RelNode,
  mappings: RelProjectExprMapping[],
): RelProjectNode {
  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input,
    columns: [
      ...input.output.map((column) => ({
        kind: "column" as const,
        source: parseRelColumnRef(column.name),
        output: column.name,
      })),
      ...mappings,
    ],
    output: [...input.output, ...mappings.map((mapping) => ({ name: mapping.output }))],
  };
}

export function parseNamedWindows(windowClause: unknown): Map<string, WindowSpecificationAst> {
  return parseNamedWindowSpecifications(windowClause as any);
}

function parseWindowProjection(
  entry: SelectColumnAst,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  windowDefinitions: Map<string, WindowSpecificationAst>,
  lowerExprContext: SqlExprLoweringContext,
): SelectWindowProjection | null {
  const expr = entry.expr as {
    type?: unknown;
    name?: unknown;
    over?: unknown;
    args?: unknown;
  };
  if (expr.type !== "function" && expr.type !== "aggr_func") {
    return null;
  }

  const over = parseWindowOver(expr.over, windowDefinitions);
  if (!over) {
    return null;
  }

  const name = readWindowFunctionName(expr);
  if (!name) {
    return null;
  }

  const partitionBy: RelColumnRef[] = [];
  for (const term of over.partitionby ?? []) {
    const resolved = resolveColumnRef(term.expr, bindings, aliasToBinding);
    if (!resolved) {
      return null;
    }
    partitionBy.push({
      alias: resolved.alias,
      column: resolved.column,
    });
  }

  const orderBy: Array<{ source: RelColumnRef; direction: "asc" | "desc" }> = [];
  for (const term of over.orderby ?? []) {
    const resolved = resolveColumnRef(term.expr, bindings, aliasToBinding);
    if (!resolved) {
      return null;
    }
    orderBy.push({
      source: {
        alias: resolved.alias,
        column: resolved.column,
      },
      direction: term.type === "DESC" ? "desc" : "asc",
    });
  }

  const output = typeof entry.as === "string" && entry.as.length > 0 ? entry.as : name;
  const args = readWindowFunctionArgs(expr.args);
  const frame = parseWindowFrameClause(over.window_frame_clause?.raw);
  if (over.window_frame_clause && !frame) {
    return null;
  }

  if (name === "dense_rank" || name === "rank" || name === "row_number") {
    if (!supportsRankWindowArgs(expr.args)) {
      return null;
    }

    return {
      kind: "window",
      output,
      function: {
        fn: name,
        as: output,
        partitionBy,
        orderBy,
        ...(frame ? { frame } : {}),
      },
    };
  }

  if (name === "first_value") {
    const value = args[0]
      ? lowerSqlAstToRelExpr(args[0], bindings, aliasToBinding, lowerExprContext)
      : null;
    if (!value) {
      return null;
    }

    return {
      kind: "window",
      output,
      function: {
        fn: name,
        as: output,
        partitionBy,
        value,
        orderBy,
        ...(frame ? { frame } : {}),
      },
    };
  }

  if (name === "lead" || name === "lag") {
    const value = args[0]
      ? lowerSqlAstToRelExpr(args[0], bindings, aliasToBinding, lowerExprContext)
      : null;
    if (!value) {
      return null;
    }

    const rawOffset = args[1];
    const offsetExpr =
      rawOffset != null
        ? lowerSqlAstToRelExpr(rawOffset, bindings, aliasToBinding, lowerExprContext)
        : null;
    const offsetValue =
      offsetExpr && offsetExpr.kind === "literal" && typeof offsetExpr.value === "number"
        ? offsetExpr.value
        : null;
    if (rawOffset != null) {
      if (offsetValue == null) {
        return null;
      }
      if (!Number.isInteger(offsetValue) || offsetValue < 0) {
        return null;
      }
    }

    const defaultExpr =
      args[2] != null
        ? lowerSqlAstToRelExpr(args[2], bindings, aliasToBinding, lowerExprContext)
        : null;
    if (args[2] != null && !defaultExpr) {
      return null;
    }

    return {
      kind: "window",
      output,
      function: {
        fn: name,
        as: output,
        partitionBy,
        value,
        orderBy,
        ...(offsetValue != null ? { offset: offsetValue } : {}),
        ...(defaultExpr ? { defaultExpr } : {}),
        ...(frame ? { frame } : {}),
      },
    };
  }

  if (expr.type !== "aggr_func") {
    return null;
  }

  const metric = parseAggregateMetric(expr, output, bindings, aliasToBinding);
  if (!metric) {
    return null;
  }

  return {
    kind: "window",
    output,
    function: {
      fn: name,
      as: output,
      partitionBy,
      ...(metric.column ? { column: metric.column } : {}),
      ...(metric.distinct ? { distinct: true } : {}),
      orderBy,
      ...(frame ? { frame } : {}),
    },
  };
}
