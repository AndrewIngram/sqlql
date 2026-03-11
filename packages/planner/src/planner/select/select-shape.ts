import type { RelColumnRef, RelNode, RelProjectExprMapping } from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";

import {
  getAggregateMetricSignature,
  hasAggregateProjection,
  lowerHavingExpr,
  parseAggregateProjections,
  parseGroupBy,
  parseOrderBy,
  resolveAggregateGroupBy,
  resolveAggregateOrderBy,
  resolveNonAggregateOrderBy,
  validateAggregateProjectionGroupBy,
} from "../aggregate-lowering";
import type {
  Binding,
  ParsedAggregateMetricProjection,
  ParsedAggregateProjection,
  ResolvedOrderTerm,
  SelectProjection,
  SelectWindowProjection,
} from "../planner-types";
import type { FromEntryAst, SelectAst } from "../sqlite-parser/ast";
import type { SqlExprLoweringContext } from "../sql-expr-lowering";
import { parseLimitAndOffset } from "../sql-expr-lowering";
import { parseNamedWindows, parseProjection, toParsedOrderSource } from "./select-projections";
import { parseJoins } from "./select-from-lowering";
import { parseWhereFilters, validateEnumLiteralFilters } from "../where-lowering";

export interface PreparedSimpleSelect {
  bindings: Binding[];
  aggregateMode: boolean;
  safeAggregateProjections: ParsedAggregateProjection[];
  safeProjections: SelectProjection[];
  aggregateGroupByResolution: {
    groupBy: RelColumnRef[];
    materializations: RelProjectExprMapping[];
  };
  effectiveGroupBy: RelColumnRef[];
  allAggregateMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"];
  havingExpr: import("@tupl/foundation").RelExpr | null;
  orderBy: ResolvedOrderTerm[];
  orderByMaterializations: RelProjectExprMapping[];
  limit?: number;
  offset?: number;
  joins: NonNullable<ReturnType<typeof parseJoins>>;
  whereFilters: NonNullable<ReturnType<typeof parseWhereFilters>>;
  windowFunctions: SelectWindowProjection["function"][];
  rootBinding: Binding;
}

/**
 * Select-shape preparation owns the analysis pass that turns a parsed SELECT into
 * binding state, aggregate/window mode, and the normalized projection/filter metadata
 * needed by lower phases.
 */
export function prepareSimpleSelectLowering(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
  tryLowerSelect: (ast: SelectAst) => RelNode | null,
): PreparedSimpleSelect | null {
  if (ast.type !== "select" || ast.with || ast.set_op || ast._next) {
    return null;
  }

  const from = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (from.length === 0) {
    return null;
  }

  if (
    from.some(
      (entry) => typeof (entry as FromEntryAst).table !== "string" || (entry as FromEntryAst).stmt,
    )
  ) {
    return null;
  }

  const bindings: Binding[] = from.map((entry, index) => {
    const table = (entry as FromEntryAst).table;
    if (typeof table !== "string" || (!schema.tables[table] && !cteNames.has(table))) {
      throw new Error(`Unknown table: ${String(table)}`);
    }

    const alias =
      typeof (entry as FromEntryAst).as === "string" && (entry as FromEntryAst).as
        ? ((entry as FromEntryAst).as as string)
        : table;

    return { table, alias, index };
  });

  const aliasToBinding = new Map(bindings.map((binding) => [binding.alias, binding]));
  const lowerExprContext: SqlExprLoweringContext = {
    schema,
    cteNames,
    tryLowerSelect,
  };

  const joins = parseJoins(from, bindings, aliasToBinding);
  if (joins == null) {
    return null;
  }

  const whereFilters = parseWhereFilters(ast.where, bindings, aliasToBinding, lowerExprContext);
  if (!whereFilters) {
    return null;
  }
  validateEnumLiteralFilters(whereFilters.literals, bindings, schema);

  const distinctMode = ast.distinct === "DISTINCT";
  const aggregateMode = Boolean(ast.groupby || hasAggregateProjection(ast.columns) || distinctMode);

  const projections = aggregateMode
    ? null
    : parseProjection(
        ast.columns,
        bindings,
        aliasToBinding,
        parseNamedWindows(ast.window),
        lowerExprContext,
      );
  if (!aggregateMode && projections == null) {
    return null;
  }

  const aggregateProjections = aggregateMode
    ? parseAggregateProjections(ast.columns, bindings, aliasToBinding, lowerExprContext)
    : null;
  if (aggregateMode && aggregateProjections == null) {
    return null;
  }

  const safeAggregateProjections = aggregateMode ? (aggregateProjections ?? []) : [];
  const safeProjections = aggregateMode ? [] : (projections ?? []);
  const groupByTerms = aggregateMode ? parseGroupBy(ast.groupby, bindings, aliasToBinding) : [];
  if (aggregateMode && groupByTerms == null) {
    return null;
  }

  const windowFunctions = safeProjections
    .filter((projection): projection is SelectWindowProjection => projection.kind === "window")
    .map((projection) => projection.function);

  const aggregateGroupByResolution = aggregateMode
    ? resolveAggregateGroupBy(groupByTerms ?? [], safeAggregateProjections)
    : { groupBy: [], materializations: [] };
  let effectiveGroupBy = aggregateGroupByResolution.groupBy;

  if (distinctMode && effectiveGroupBy.length === 0) {
    const distinctGroupBy: RelColumnRef[] = [];
    for (const projection of safeAggregateProjections) {
      if (projection.kind !== "group" || !projection.source) {
        return null;
      }
      distinctGroupBy.push(projection.source);
    }
    if (distinctGroupBy.length === 0) {
      return null;
    }
    effectiveGroupBy = distinctGroupBy;
  }

  if (
    aggregateMode &&
    !validateAggregateProjectionGroupBy(safeAggregateProjections, effectiveGroupBy)
  ) {
    return null;
  }

  const aggregateMetrics = safeAggregateProjections
    .filter(
      (projection): projection is ParsedAggregateMetricProjection => projection.kind === "metric",
    )
    .map((projection) => projection.metric);
  const aggregateMetricAliases = new Map<string, string>(
    aggregateMetrics.map((metric) => [getAggregateMetricSignature(metric), metric.as]),
  );
  const hiddenHavingMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"] = [];
  const havingExpr =
    aggregateMode && ast.having
      ? lowerHavingExpr(
          ast.having,
          bindings,
          aliasToBinding,
          aggregateMetricAliases,
          hiddenHavingMetrics,
        )
      : null;
  if (ast.having && (!aggregateMode || !havingExpr)) {
    return null;
  }
  const allAggregateMetrics = [...aggregateMetrics, ...hiddenHavingMetrics];

  const orderByTerms = parseOrderBy(
    ast.orderby,
    bindings,
    aliasToBinding,
    new Set(
      (aggregateMode ? safeAggregateProjections : safeProjections).map(
        (projection) => projection.output,
      ),
    ),
  );
  if (orderByTerms == null) {
    return null;
  }

  const { orderBy, materializations: orderByMaterializations } = aggregateMode
    ? {
        orderBy: resolveAggregateOrderBy(orderByTerms, safeAggregateProjections),
        materializations: [] as RelProjectExprMapping[],
      }
    : resolveNonAggregateOrderBy(orderByTerms, safeProjections, toParsedOrderSource);

  const rootBinding = bindings[0];
  if (!rootBinding) {
    return null;
  }

  const { limit, offset } = parseLimitAndOffset(ast.limit);

  return {
    bindings,
    aggregateMode,
    safeAggregateProjections,
    safeProjections,
    aggregateGroupByResolution,
    effectiveGroupBy,
    allAggregateMetrics,
    havingExpr,
    orderBy,
    orderByMaterializations,
    ...(limit != null ? { limit } : {}),
    ...(offset != null ? { offset } : {}),
    joins,
    whereFilters,
    windowFunctions,
    rootBinding,
  };
}
