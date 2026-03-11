import type { RelColumnRef, RelNode, RelProjectExprMapping } from "@tupl/foundation";
import type { OrderByTermAst, SelectColumnAst } from "./sqlite-parser/ast";
import type {
  Binding,
  ParsedAggregateGroupProjection,
  ParsedAggregateProjection,
  ParsedAggregateMetricProjection,
  ParsedGroupByTerm,
  ParsedOrderByTerm,
  ResolvedOrderTerm,
  SelectExprProjection,
  SelectProjection,
} from "./planner-types";
import type { SqlExprLoweringContext } from "./sql-expr-lowering";
import {
  lowerSqlAstToRelExpr,
  mapBinaryOperatorToRelFunction,
  parsePositiveOrdinalLiteral,
  parseLiteral,
  resolveColumnRef,
  toRawColumnRef,
} from "./sql-expr-lowering";
import { nextSyntheticColumnName } from "./planner-ids";

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

export function lowerHavingExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr | null {
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
    args?: { value?: unknown; expr?: unknown; distinct?: unknown };
    ast?: unknown;
  };

  switch (expr.type) {
    case "string":
      return { kind: "literal", value: typeof expr.value === "string" ? expr.value : "" };
    case "number":
      return typeof expr.value === "number" ? { kind: "literal", value: expr.value } : null;
    case "bool":
      return typeof expr.value === "boolean" ? { kind: "literal", value: expr.value } : null;
    case "null":
      return { kind: "literal", value: null };
    case "column_ref": {
      const resolved = resolveColumnRef(expr, bindings, aliasToBinding);
      if (!resolved) {
        return null;
      }
      return {
        kind: "column",
        ref: {
          column: resolved.column,
        },
      };
    }
    case "aggr_func": {
      const metric = parseAggregateMetric(
        expr,
        deriveDefaultAggregateOutputName(expr),
        bindings,
        aliasToBinding,
      );
      if (!metric) {
        return null;
      }
      const signature = getAggregateMetricSignature(metric);
      let alias = aggregateMetricAliases.get(signature);
      if (!alias) {
        alias = `__having_metric_${aggregateMetricAliases.size + 1}`;
        aggregateMetricAliases.set(signature, alias);
        hiddenMetrics.push({
          ...metric,
          as: alias,
        });
      }
      return {
        kind: "column",
        ref: {
          column: alias,
        },
      };
    }
    case "binary_expr":
      return lowerHavingBinaryExpr(
        expr,
        bindings,
        aliasToBinding,
        aggregateMetricAliases,
        hiddenMetrics,
      );
    case "function":
      return lowerHavingFunctionExpr(
        expr,
        bindings,
        aliasToBinding,
        aggregateMetricAliases,
        hiddenMetrics,
      );
    default:
      if ("ast" in expr) {
        return null;
      }
      return null;
  }
}

function lowerHavingBinaryExpr(
  expr: {
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr | null {
  const operator = typeof expr.operator === "string" ? expr.operator.toUpperCase() : null;
  if (!operator) {
    return null;
  }

  if (operator === "BETWEEN") {
    const range = expr.right as { type?: unknown; value?: unknown } | undefined;
    if (range?.type !== "expr_list" || !Array.isArray(range.value) || range.value.length !== 2) {
      return null;
    }
    const left = lowerHavingExpr(
      expr.left,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const low = lowerHavingExpr(
      range.value[0],
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const high = lowerHavingExpr(
      range.value[1],
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
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
    const left = lowerHavingExpr(
      expr.left,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const values = parseHavingExprListToRelExprArgs(
      expr.right,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
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
    const left = lowerHavingExpr(
      expr.left,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
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

  const left = lowerHavingExpr(
    expr.left,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
  const right = lowerHavingExpr(
    expr.right,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
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

function lowerHavingFunctionExpr(
  expr: {
    name?: unknown;
    args?: { value?: unknown };
    over?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr | null {
  if (expr.over) {
    return null;
  }

  const rawName = (expr.name as { name?: Array<{ value?: unknown }> } | undefined)?.name?.[0]
    ?.value;
  if (typeof rawName !== "string") {
    return null;
  }

  const normalized = rawName.toLowerCase();
  const args = parseHavingFunctionArgsToRelExpr(
    expr.args?.value,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
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

function parseHavingFunctionArgsToRelExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr[] | null {
  if (raw == null) {
    return [];
  }
  const values = Array.isArray(raw) ? raw : [raw];
  const args: import("@tupl/foundation").RelExpr[] = [];
  for (const value of values) {
    const arg = lowerHavingExpr(
      value,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    if (!arg) {
      return null;
    }
    args.push(arg);
  }
  return args;
}

function parseHavingExprListToRelExprArgs(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr[] | null {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return null;
  }
  return parseHavingFunctionArgsToRelExpr(
    expr.value,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
}

export function validateAggregateProjectionGroupBy(
  projections: ParsedAggregateProjection[],
  groupBy: RelColumnRef[],
): boolean {
  const groupBySet = new Set(groupBy.map((ref) => `${ref.alias ?? ""}.${ref.column}`));

  for (const projection of projections) {
    if (projection.kind !== "group") {
      continue;
    }

    if (!projection.source) {
      return false;
    }

    const key = `${projection.source.alias ?? ""}.${projection.source.column}`;
    if (!groupBySet.has(key)) {
      return false;
    }
  }

  return true;
}

export function parseOrderBy(
  rawOrderBy: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  outputs: Set<string>,
): ParsedOrderByTerm[] | null {
  const orderBy = Array.isArray(rawOrderBy) ? (rawOrderBy as OrderByTermAst[]) : [];
  const out: ParsedOrderByTerm[] = [];

  for (const term of orderBy) {
    const ordinal = parsePositiveOrdinalLiteral(term.expr, "ORDER BY");
    if (ordinal != null) {
      out.push({
        kind: "ordinal",
        position: ordinal,
        direction: term.type === "DESC" ? "desc" : "asc",
      });
      continue;
    }

    const rawRef = toRawColumnRef(term.expr);
    if (rawRef && !rawRef.table && outputs.has(rawRef.column)) {
      out.push({
        kind: "output",
        output: rawRef.column,
        direction: term.type === "DESC" ? "desc" : "asc",
      });
      continue;
    }

    const resolved = resolveColumnRef(term.expr, bindings, aliasToBinding);
    if (!resolved?.alias) {
      return null;
    }

    out.push({
      kind: "ref",
      source: {
        alias: resolved.alias,
        column: resolved.column,
      },
      direction: term.type === "DESC" ? "desc" : "asc",
    });
  }

  return out;
}

function materializeSelectExprProjection(
  projection: SelectExprProjection,
  prefix: string,
): RelProjectExprMapping | null {
  if (projection.source) {
    return null;
  }

  const column = nextSyntheticColumnName(prefix);
  projection.source = { column };
  return {
    kind: "expr",
    expr: projection.expr,
    output: column,
  };
}

function materializeAggregateGroupProjection(
  projection: ParsedAggregateGroupProjection,
  prefix: string,
): RelProjectExprMapping | null {
  if (projection.source || !projection.expr) {
    return null;
  }

  const column = nextSyntheticColumnName(prefix);
  projection.source = { column };
  return {
    kind: "expr",
    expr: projection.expr,
    output: column,
  };
}

export function resolveAggregateGroupBy(
  groupByTerms: ParsedGroupByTerm[],
  projections: ParsedAggregateProjection[],
): {
  groupBy: RelColumnRef[];
  materializations: RelProjectExprMapping[];
} {
  const groupBy: RelColumnRef[] = [];
  const materializations: RelProjectExprMapping[] = [];

  for (const term of groupByTerms) {
    if (term.kind === "ref") {
      groupBy.push(term.ref);
      continue;
    }

    const projection = projections[term.position - 1];
    if (!projection) {
      throw new Error(`GROUP BY ordinal ${term.position} is out of range.`);
    }
    if (projection.kind === "metric") {
      throw new Error(`GROUP BY ordinal ${term.position} cannot reference an aggregate output.`);
    }

    const materialization = materializeAggregateGroupProjection(projection, "group_by");
    if (materialization) {
      materializations.push(materialization);
    }
    if (!projection.source) {
      throw new Error(`GROUP BY ordinal ${term.position} could not be resolved.`);
    }

    groupBy.push(projection.source);
  }

  return {
    groupBy,
    materializations,
  };
}

export function resolveNonAggregateOrderBy(
  orderByTerms: ParsedOrderByTerm[],
  projections: SelectProjection[],
  toParsedOrderSource: (
    ref: RelColumnRef | null | undefined,
    fallbackColumn: string,
  ) => ResolvedOrderTerm["source"],
): {
  orderBy: ResolvedOrderTerm[];
  materializations: RelProjectExprMapping[];
} {
  const projectionsByOutput = new Map(
    projections.map((projection) => [projection.output, projection] as const),
  );
  const materializations: RelProjectExprMapping[] = [];
  const orderBy: ResolvedOrderTerm[] = [];

  const resolveProjectionSource = (
    projection: SelectProjection,
    ordinal?: number,
  ): ResolvedOrderTerm["source"] => {
    if (projection.kind === "column") {
      return toParsedOrderSource(projection.source, projection.output);
    }
    if (projection.kind === "window") {
      return { column: projection.function.as };
    }

    const materialization = materializeSelectExprProjection(projection, "order_by");
    if (materialization) {
      materializations.push(materialization);
    }
    if (!projection.source) {
      throw new Error(
        ordinal != null
          ? `ORDER BY ordinal ${ordinal} could not be resolved.`
          : `ORDER BY expression "${projection.output}" could not be resolved.`,
      );
    }
    return { column: projection.source.column };
  };

  for (const term of orderByTerms) {
    if (term.kind === "ref") {
      orderBy.push({
        source: term.source,
        direction: term.direction,
      });
      continue;
    }

    const projection =
      term.kind === "ordinal"
        ? projections[term.position - 1]
        : projectionsByOutput.get(term.output);
    if (!projection) {
      if (term.kind === "ordinal") {
        throw new Error(`ORDER BY ordinal ${term.position} is out of range.`);
      }
      throw new Error(`Unknown ORDER BY output "${term.output}".`);
    }

    orderBy.push({
      source: resolveProjectionSource(
        projection,
        term.kind === "ordinal" ? term.position : undefined,
      ),
      direction: term.direction,
    });
  }

  return {
    orderBy,
    materializations,
  };
}

export function resolveAggregateOrderBy(
  orderByTerms: ParsedOrderByTerm[],
  projections: ParsedAggregateProjection[],
): ResolvedOrderTerm[] {
  const projectionsByOutput = new Map(
    projections.map((projection) => [projection.output, projection] as const),
  );
  const groupOutputsBySource = new Map<string, string>();

  for (const projection of projections) {
    if (projection.kind !== "group" || !projection.source) {
      continue;
    }
    groupOutputsBySource.set(
      `${projection.source.alias ?? ""}.${projection.source.column}`,
      projection.source.column,
    );
  }

  const resolveProjectionSource = (
    projection: ParsedAggregateProjection,
    ordinal?: number,
  ): ResolvedOrderTerm["source"] => {
    if (projection.kind === "metric") {
      return { column: projection.metric.as };
    }
    if (!projection.source) {
      throw new Error(
        ordinal != null
          ? `ORDER BY ordinal ${ordinal} could not be resolved.`
          : `ORDER BY expression "${projection.output}" could not be resolved.`,
      );
    }
    return { column: projection.source.column };
  };

  return orderByTerms.map((term) => {
    if (term.kind === "ref") {
      const key = `${term.source.alias ?? ""}.${term.source.column}`;
      return {
        source: { column: groupOutputsBySource.get(key) ?? term.source.column },
        direction: term.direction,
      };
    }

    const projection =
      term.kind === "ordinal"
        ? projections[term.position - 1]
        : projectionsByOutput.get(term.output);
    if (!projection) {
      if (term.kind === "ordinal") {
        throw new Error(`ORDER BY ordinal ${term.position} is out of range.`);
      }
      throw new Error(`Unknown ORDER BY output "${term.output}".`);
    }

    return {
      source: resolveProjectionSource(
        projection,
        term.kind === "ordinal" ? term.position : undefined,
      ),
      direction: term.direction,
    };
  });
}
