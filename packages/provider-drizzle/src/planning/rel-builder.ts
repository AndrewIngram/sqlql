import type {
  SqlRelationalBackend,
  SqlRelationalOrderTerm,
  SqlRelationalWithSelection,
} from "@tupl/provider-kit";
import type { RelNode } from "@tupl/foundation";
import { asc, desc, eq, sql, type AnyColumn, type SQL } from "drizzle-orm";

import { executeDrizzleQueryBuilder, toSqlConditionFromSource } from "../backend/query-helpers";
import type {
  DrizzleExecutableBuilder,
  DrizzleQueryExecutor,
  ResolvedEntityConfig,
} from "../types";
import {
  buildSingleQueryPlan,
  buildSqlExpressionFromRelExpr,
  createScanBinding,
  resolveColumnRefFromAliasMap,
  resolveDrizzleRelCompileStrategy,
  resolveJoinKeyColumnRefFromAliasMap,
  toAliasColumnRef,
  type JoinStep,
  type ScanBinding,
  type SingleQueryPlan,
  UnsupportedSingleQueryPlanError,
} from "./rel-strategy";

interface DrizzleRelationalQuery {
  builder: DrizzleExecutableBuilder;
  pendingWhere: SQL[];
}

/**
 * Drizzle backend hooks keep the custom pieces that matter for third-party authors:
 * projected scan planning, expression-aware source resolution, and Drizzle-specific builder calls.
 * Strategy recursion and shared rel assembly remain in provider-kit.
 */
export const drizzleSqlRelationalBackend: SqlRelationalBackend<
  any,
  ResolvedEntityConfig<any>,
  ScanBinding<any>,
  DrizzleQueryExecutor,
  DrizzleRelationalQuery
> = {
  createScanBinding,
  buildSingleQueryPlan,
  resolveRelCompileStrategy: (node, resolvedEntities) =>
    resolveDrizzleRelCompileStrategy(node, resolvedEntities),
  createRootQuery({ runtime, plan }) {
    const singleQueryPlan = plan as SingleQueryPlan<any>;
    const preferDistinctSelection =
      !!singleQueryPlan.pipeline.aggregate &&
      singleQueryPlan.pipeline.aggregate.metrics.length === 0 &&
      singleQueryPlan.pipeline.aggregate.groupBy.length > 0;
    const dbWithSelectDistinct = runtime as {
      select: (selection: Record<string, unknown>) => {
        from: (table: object) => DrizzleExecutableBuilder;
      };
      selectDistinct?: (selection: Record<string, unknown>) => {
        from: (table: object) => DrizzleExecutableBuilder;
      };
    };
    const selectFn =
      preferDistinctSelection && typeof dbWithSelectDistinct.selectDistinct === "function"
        ? dbWithSelectDistinct.selectDistinct.bind(dbWithSelectDistinct)
        : dbWithSelectDistinct.select.bind(dbWithSelectDistinct);

    const builder = selectFn(buildSingleQuerySelection(singleQueryPlan)).from(
      singleQueryPlan.joinPlan.root.sourceTable,
    ) as DrizzleExecutableBuilder;

    return {
      builder,
      pendingWhere: [],
    };
  },
  applyRegularJoin({ query, join, aliases }) {
    ensureJoinMethodsAvailable(
      query.builder as {
        innerJoin?: unknown;
        leftJoin?: unknown;
        rightJoin?: unknown;
        fullJoin?: unknown;
      },
      [join],
    );

    const leftColumn = resolveJoinKeyColumnRefFromAliasMap(aliases, {
      alias: join.leftKey.alias,
      column: join.leftKey.column,
    });
    const rightColumn = resolveJoinKeyColumnRefFromAliasMap(aliases, {
      alias: join.rightKey.alias,
      column: join.rightKey.column,
    });
    const onClause = eq(leftColumn, rightColumn);

    const builder = query.builder as DrizzleExecutableBuilder & {
      innerJoin: (table: object, on: SQL) => unknown;
      leftJoin: (table: object, on: SQL) => unknown;
      rightJoin: (table: object, on: SQL) => unknown;
      fullJoin: (table: object, on: SQL) => unknown;
    };

    query.builder = (
      join.joinType === "inner"
        ? builder.innerJoin(join.right.sourceTable, onClause)
        : join.joinType === "left"
          ? builder.leftJoin(join.right.sourceTable, onClause)
          : join.joinType === "right"
            ? builder.rightJoin(join.right.sourceTable, onClause)
            : builder.fullJoin(join.right.sourceTable, onClause)
    ) as DrizzleExecutableBuilder;

    return query;
  },
  applySemiJoin({ query, leftKey, subquery, aliases }) {
    const leftColumn = resolveJoinKeyColumnRefFromAliasMap(aliases, {
      alias: leftKey.alias,
      column: leftKey.column,
    });

    query.pendingWhere.push(
      sql`${leftColumn} in (${asDrizzleSubquerySql(finalizeDrizzleQuery(subquery).builder)})`,
    );
    return query;
  },
  applyWhereClause({ query, clause, plan }) {
    query.pendingWhere.push(
      toSqlConditionFromRelFilterClause(clause, plan as SingleQueryPlan<any>),
    );
    return query;
  },
  applySelection({ query }) {
    return finalizeDrizzleQuery(query);
  },
  applyGroupBy({ query, groupBy, aliases }) {
    query = finalizeDrizzleQuery(query);
    const builder = query.builder as DrizzleExecutableBuilder & {
      groupBy?: (...columns: AnyColumn[]) => unknown;
    };
    if (typeof builder.groupBy !== "function") {
      return query;
    }

    const groupByColumns = groupBy.map((columnRef) =>
      resolveColumnRefFromAliasMap(
        aliases,
        toAliasColumnRef(columnRef.alias ?? columnRef.table, columnRef.column),
      ),
    );
    query.builder = builder.groupBy(...(groupByColumns as AnyColumn[])) as DrizzleExecutableBuilder;
    return query;
  },
  applyOrderBy({ query, plan, orderBy }) {
    query = finalizeDrizzleQuery(query);
    if (typeof query.builder.orderBy !== "function") {
      return query;
    }

    const clauses = orderBy.map((term) => {
      const source = resolveOrderSource(term, plan);
      return term.direction === "asc" ? asc(source) : desc(source);
    });
    if (clauses.length > 0) {
      query.builder = query.builder.orderBy(...clauses) as DrizzleExecutableBuilder;
    }
    return query;
  },
  applyLimit({ query, limit }) {
    query = finalizeDrizzleQuery(query);
    if (typeof query.builder.limit === "function") {
      query.builder = query.builder.limit(limit) as DrizzleExecutableBuilder;
    }
    return query;
  },
  applyOffset({ query, offset }) {
    query = finalizeDrizzleQuery(query);
    if (typeof query.builder.offset === "function") {
      query.builder = query.builder.offset(offset) as DrizzleExecutableBuilder;
    }
    return query;
  },
  applySetOp({ left, right, wrapper }) {
    const leftBuilder = finalizeDrizzleQuery(left).builder;
    const rightBuilder = finalizeDrizzleQuery(right).builder;
    const methodName =
      wrapper.setOp.op === "union_all"
        ? "unionAll"
        : wrapper.setOp.op === "union"
          ? "union"
          : wrapper.setOp.op === "intersect"
            ? "intersect"
            : "except";
    const applySetOp = (leftBuilder as unknown as Record<string, unknown>)[methodName];
    if (typeof applySetOp !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        `Drizzle query builder does not support ${methodName} for single-query pushdown.`,
      );
    }

    return {
      builder: applySetOp.call(leftBuilder, rightBuilder) as DrizzleExecutableBuilder,
      pendingWhere: [],
    };
  },
  buildWithQuery({ body, ctes, projection, orderBy, runtime }) {
    const dbWithCtes = runtime as {
      $with?: (name: string) => { as: (query: DrizzleExecutableBuilder) => unknown };
      with?: (...cteRefs: unknown[]) => {
        select: (selection: Record<string, unknown>) => {
          from: (source: unknown) => DrizzleExecutableBuilder;
        };
      };
    };
    if (typeof dbWithCtes.$with !== "function" || typeof dbWithCtes.with !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle database instance does not support CTE builders required for WITH pushdown.",
      );
    }

    const cteBindings = new Map<string, unknown>();
    const cteRefs: unknown[] = [];
    for (const cte of ctes) {
      const cteRef = dbWithCtes.$with(cte.name).as(finalizeDrizzleQuery(cte.query).builder);
      cteBindings.set(cte.name, cteRef);
      cteRefs.push(cteRef);
    }

    const source = cteBindings.get(body.cteScan.table);
    if (!source) {
      throw new UnsupportedSingleQueryPlanError(
        `Unknown CTE "${body.cteScan.table}" in WITH body.`,
      );
    }
    const scanAlias = body.cteScan.alias ?? body.cteScan.table;

    const windowExpressions = new Map<string, unknown>();
    for (const fn of body.window?.functions ?? []) {
      windowExpressions.set(
        fn.as,
        buildWindowFunctionSql(fn, source as Record<string, unknown>, scanAlias),
      );
    }

    let builder = dbWithCtes
      .with(...cteRefs)
      .select(
        buildWithSelectionRecord(
          projection,
          source as Record<string, unknown>,
          windowExpressions,
          scanAlias,
        ),
      )
      .from(source) as DrizzleExecutableBuilder;

    const whereClauses: SQL[] = [];
    for (const clause of body.cteScan.where ?? []) {
      whereClauses.push(
        toSqlConditionFromSource(
          clause,
          resolveWithBodySourceColumn(
            source as Record<string, unknown>,
            toInlineColumnRef(clause.column),
            scanAlias,
          ),
        ),
      );
    }
    for (const filter of body.filters) {
      for (const clause of filter.where ?? []) {
        whereClauses.push(
          toSqlConditionFromSource(
            clause,
            resolveWithBodySourceColumn(
              source as Record<string, unknown>,
              toInlineColumnRef(clause.column),
              scanAlias,
            ),
          ),
        );
      }
    }

    if (whereClauses.length > 0) {
      if (typeof builder.where !== "function") {
        throw new UnsupportedSingleQueryPlanError(
          "Drizzle query builder does not support WHERE on WITH fragments.",
        );
      }
      builder = builder.where(sql.join(whereClauses, sql` and `)) as DrizzleExecutableBuilder;
    }

    if (orderBy.length > 0) {
      if (typeof builder.orderBy !== "function") {
        throw new UnsupportedSingleQueryPlanError(
          "Drizzle query builder does not support ORDER BY on WITH fragments.",
        );
      }
      const clauses = orderBy.map((term) => {
        const sourceColumn =
          term.kind === "output"
            ? sql.identifier(term.column)
            : resolveWithBodySourceColumn(
                source as Record<string, unknown>,
                term.source,
                scanAlias,
              );
        return term.direction === "asc" ? asc(sourceColumn) : desc(sourceColumn);
      });
      builder = builder.orderBy(...clauses) as DrizzleExecutableBuilder;
    }

    return {
      builder,
      pendingWhere: [],
    };
  },
  async executeQuery({ query, runtime }) {
    return executeDrizzleQueryBuilder(finalizeDrizzleQuery(query).builder, runtime);
  },
};

function finalizeDrizzleQuery(query: DrizzleRelationalQuery): DrizzleRelationalQuery {
  if (query.pendingWhere.length === 0) {
    return query;
  }
  if (typeof query.builder.where !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      "Drizzle query builder does not support WHERE for this relational fragment.",
    );
  }

  query.builder = query.builder.where(
    sql.join(query.pendingWhere, sql` and `),
  ) as DrizzleExecutableBuilder;
  query.pendingWhere = [];
  return query;
}

function buildSingleQuerySelection<TContext>(
  plan: SingleQueryPlan<TContext>,
): Record<string, unknown> {
  const selection: Record<string, unknown> = {};

  if (plan.pipeline.aggregate) {
    const groupSources = new Map<string, AnyColumn | SQL>();
    const groupSourcesByKey = new Map<string, AnyColumn | SQL>();
    plan.pipeline.aggregate.groupBy.forEach((groupBy, index) => {
      const source = resolveColumnRefFromAliasMap(
        plan.joinPlan.aliases,
        toAliasColumnRef(groupBy.alias ?? groupBy.table, groupBy.column),
      );
      const outputName = plan.pipeline.aggregate!.output[index]?.name ?? groupBy.column;
      groupSources.set(outputName, source);
      groupSources.set(groupBy.column, source);
      const keyAlias = groupBy.alias ?? groupBy.table ?? "";
      groupSourcesByKey.set(`${keyAlias}.${groupBy.column}`, source);
    });

    const metricSources = new Map<string, SQL>();
    plan.pipeline.aggregate.metrics.forEach((metric, index) => {
      const outputName =
        plan.pipeline.aggregate!.output[plan.pipeline.aggregate!.groupBy.length + index]?.name ??
        metric.as;
      const source = buildAggregateMetricSql(metric, plan.joinPlan.aliases);
      metricSources.set(outputName, source);
      metricSources.set(metric.as, source);
    });

    if (plan.pipeline.project) {
      for (const rawMapping of plan.pipeline.project.columns) {
        if (!("source" in rawMapping)) {
          throw new UnsupportedSingleQueryPlanError(
            "Computed aggregate projections are not supported in Drizzle single-query pushdown.",
          );
        }

        const metricSource = metricSources.get(rawMapping.source.column);
        if (metricSource) {
          selection[rawMapping.output] = metricSource.as(rawMapping.output);
          continue;
        }

        const projectedSource = resolveAggregateProjectionSource(
          rawMapping,
          groupSources,
          groupSourcesByKey,
        );
        const qualifiedSource = rawMapping.source.alias ?? rawMapping.source.table;
        selection[rawMapping.output] =
          qualifiedSource || rawMapping.source.column !== rawMapping.output
            ? sql`${projectedSource}`.as(rawMapping.output)
            : projectedSource;
      }
      return selection;
    }

    plan.pipeline.aggregate.groupBy.forEach((groupBy, index) => {
      const outputName = plan.pipeline.aggregate!.output[index]?.name ?? groupBy.column;
      const source = groupSources.get(outputName);
      if (source) {
        selection[outputName] = source;
      }
    });
    plan.pipeline.aggregate.metrics.forEach((metric, index) => {
      const outputName =
        plan.pipeline.aggregate!.output[plan.pipeline.aggregate!.groupBy.length + index]?.name ??
        metric.as;
      const metricSource = metricSources.get(outputName);
      if (metricSource) {
        selection[outputName] = metricSource.as(outputName);
      }
    });
    return selection;
  }

  if (plan.pipeline.project) {
    for (const rawMapping of plan.pipeline.project.columns) {
      const resolved =
        "source" in rawMapping
          ? resolveColumnRefFromAliasMap(
              plan.joinPlan.aliases,
              toAliasColumnRef(
                rawMapping.source.alias ?? rawMapping.source.table,
                rawMapping.source.column,
              ),
            )
          : buildSqlExpressionFromRelExpr(rawMapping.expr, plan.joinPlan.aliases);
      selection[rawMapping.output] =
        "source" in rawMapping ? resolved : (resolved as SQL).as(rawMapping.output);
    }
    return selection;
  }

  for (const binding of plan.joinPlan.aliases.values()) {
    for (const column of binding.outputColumns) {
      selection[`${binding.alias}.${column}`] = resolveColumnRefFromAliasMap(
        plan.joinPlan.aliases,
        {
          alias: binding.alias,
          column,
        },
      );
    }
  }

  return selection;
}

function resolveAggregateProjectionSource(
  rawMapping: Extract<RelNode, { kind: "project" }>["columns"][number],
  groupSources: Map<string, AnyColumn | SQL>,
  groupSourcesByKey: Map<string, AnyColumn | SQL>,
): AnyColumn | SQL {
  if (!("source" in rawMapping)) {
    throw new UnsupportedSingleQueryPlanError(
      "Computed aggregate projections are not supported in Drizzle single-query pushdown.",
    );
  }

  const qualifiedSource = rawMapping.source.alias ?? rawMapping.source.table;
  if (qualifiedSource) {
    const groupSource = groupSourcesByKey.get(`${qualifiedSource}.${rawMapping.source.column}`);
    if (groupSource) {
      return groupSource;
    }
  }

  const groupSource = groupSources.get(rawMapping.source.column);
  if (groupSource) {
    return groupSource;
  }

  throw new UnsupportedSingleQueryPlanError(
    `Aggregate projection source "${rawMapping.source.column}" is not available in grouped output.`,
  );
}

function buildAggregateMetricSql<TContext>(
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  aliases: Map<string, ScanBinding<TContext>>,
): SQL {
  if (metric.fn === "count" && !metric.column) {
    return sql`count(*)`;
  }

  if (!metric.column) {
    throw new UnsupportedSingleQueryPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const source = resolveColumnRefFromAliasMap(aliases, {
    ...toAliasColumnRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  switch (metric.fn) {
    case "count":
      return metric.distinct ? sql`count(distinct ${source})` : sql`count(${source})`;
    case "sum":
      return metric.distinct ? sql`sum(distinct ${source})` : sql`sum(${source})`;
    case "avg":
      return metric.distinct ? sql`avg(distinct ${source})` : sql`avg(${source})`;
    case "min":
      return sql`min(${source})`;
    case "max":
      return sql`max(${source})`;
  }
}

function resolveOrderSource(
  term: SqlRelationalOrderTerm,
  plan: SingleQueryPlan<any> | { setOp?: unknown } | { cteScan?: unknown },
): AnyColumn | SQL {
  if ("joinPlan" in plan) {
    return resolveSingleQuerySortSource(term, plan);
  }

  if (term.kind !== "output") {
    throw new UnsupportedSingleQueryPlanError(
      "Set-op ORDER BY columns must be unqualified output columns.",
    );
  }
  return sql`${sql.identifier(term.column)}`;
}

function resolveSingleQuerySortSource<TContext>(
  term: SqlRelationalOrderTerm,
  plan: SingleQueryPlan<TContext>,
): AnyColumn | SQL {
  if (term.kind === "qualified") {
    return resolveColumnRefFromAliasMap(
      plan.joinPlan.aliases,
      toAliasColumnRef(term.source.alias ?? term.source.table, term.source.column),
    );
  }

  if (!plan.pipeline.aggregate) {
    const projected = resolveProjectedSelectionSource(term.column, plan);
    if (projected) {
      return projected;
    }
    return resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      column: term.column,
    });
  }

  const metric = plan.pipeline.aggregate.metrics.find((entry) => entry.as === term.column);
  if (metric) {
    return buildAggregateMetricSql(metric, plan.joinPlan.aliases);
  }

  const groupBy = plan.pipeline.aggregate.groupBy.find((entry, index) => {
    const outputName = plan.pipeline.aggregate!.output[index]?.name ?? entry.column;
    return outputName === term.column || entry.column === term.column;
  });
  if (groupBy) {
    return resolveColumnRefFromAliasMap(
      plan.joinPlan.aliases,
      toAliasColumnRef(groupBy.alias ?? groupBy.table, groupBy.column),
    );
  }

  throw new UnsupportedSingleQueryPlanError(
    `Unsupported ORDER BY reference "${term.column}" in aggregate rel fragment.`,
  );
}

function resolveProjectedSelectionSource<TContext>(
  output: string,
  plan: SingleQueryPlan<TContext>,
): SQL | AnyColumn | null {
  const mapping = plan.pipeline.project?.columns.find((column) => column.output === output);
  if (!mapping) {
    return null;
  }

  if ("source" in mapping) {
    return resolveColumnRefFromAliasMap(
      plan.joinPlan.aliases,
      toAliasColumnRef(mapping.source.alias ?? mapping.source.table, mapping.source.column),
    );
  }

  return buildSqlExpressionFromRelExpr(mapping.expr, plan.joinPlan.aliases);
}

function toSqlConditionFromRelFilterClause<TContext>(
  clause: import("@tupl/provider-kit").ScanFilterClause,
  plan: SingleQueryPlan<TContext>,
): SQL {
  const source = resolveFilterSource(clause.column, plan);
  return toSqlConditionFromSource(clause, source);
}

function resolveFilterSource<TContext>(
  column: string,
  plan: SingleQueryPlan<TContext>,
): AnyColumn | SQL {
  if (!plan.pipeline.aggregate) {
    const projected = resolveProjectedSelectionSource(column, plan);
    if (projected) {
      return projected;
    }
  }

  return resolveColumnRefFromFilterColumn(plan.joinPlan.aliases, column);
}

function resolveColumnRefFromFilterColumn<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  column: string,
): AnyColumn | SQL {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    const alias = column.slice(0, idx);
    const name = column.slice(idx + 1);
    return resolveColumnRefFromAliasMap(aliases, { alias, column: name });
  }

  return resolveColumnRefFromAliasMap(aliases, { column });
}

function ensureJoinMethodsAvailable<TContext>(
  builder: {
    innerJoin?: unknown;
    leftJoin?: unknown;
    rightJoin?: unknown;
    fullJoin?: unknown;
  },
  joins: JoinStep<TContext>[],
): void {
  for (const join of joins) {
    if (join.joinType === "semi") {
      continue;
    }
    const methodName =
      join.joinType === "inner"
        ? "innerJoin"
        : join.joinType === "left"
          ? "leftJoin"
          : join.joinType === "right"
            ? "rightJoin"
            : "fullJoin";

    if (typeof builder[methodName] !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        `Drizzle query builder does not support ${methodName} for single-query pushdown.`,
      );
    }
  }
}

function buildWithSelectionRecord(
  projection: SqlRelationalWithSelection[],
  source: Record<string, unknown>,
  windowExpressions: Map<string, unknown>,
  scanAlias: string,
): Record<string, unknown> {
  const selection: Record<string, unknown> = {};
  for (const entry of projection) {
    if (entry.kind === "window") {
      selection[entry.output] =
        windowExpressions.get(entry.output) ?? windowExpressions.get(entry.window.as);
      continue;
    }
    selection[entry.output] = resolveWithBodyProjectionSource(
      entry.source,
      source,
      windowExpressions,
      scanAlias,
    );
  }
  return selection;
}

function resolveWithBodyProjectionSource(
  ref: { alias?: string; table?: string; column: string },
  source: Record<string, unknown>,
  windowExpressions: Map<string, unknown>,
  scanAlias: string,
): unknown {
  if (windowExpressions.has(ref.column)) {
    return windowExpressions.get(ref.column)!;
  }
  return resolveWithBodySourceColumn(source, ref, scanAlias);
}

function resolveWithBodySourceColumn(
  source: Record<string, unknown>,
  ref: { alias?: string; table?: string; column: string },
  scanAlias: string,
): AnyColumn {
  const refAlias = ref.alias ?? ref.table;
  if (refAlias && refAlias !== scanAlias) {
    throw new UnsupportedSingleQueryPlanError(
      `WITH body column "${refAlias}.${ref.column}" must reference alias "${scanAlias}".`,
    );
  }
  const column = source[ref.column];
  if (!column || typeof column !== "object") {
    throw new UnsupportedSingleQueryPlanError(`Unknown WITH body column "${ref.column}".`);
  }
  return column as AnyColumn;
}

function buildWindowFunctionSql(
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
  source: Record<string, unknown>,
  scanAlias: string,
): unknown {
  const call =
    fn.fn === "dense_rank" ? sql`dense_rank()` : fn.fn === "rank" ? sql`rank()` : sql`row_number()`;
  const partitionBy = fn.partitionBy.map((ref) =>
    resolveWithBodySourceColumn(source, ref, scanAlias),
  );
  const orderBy = fn.orderBy.map((term) => {
    const column = resolveWithBodySourceColumn(source, term.source, scanAlias);
    return sql`${column} ${term.direction === "asc" ? sql`asc` : sql`desc`}`;
  });
  const overParts: SQL[] = [];
  if (partitionBy.length > 0) {
    overParts.push(sql`partition by ${sql.join(partitionBy, sql`, `)}`);
  }
  if (orderBy.length > 0) {
    overParts.push(sql`order by ${sql.join(orderBy, sql`, `)}`);
  }
  return sql`${call} over (${sql.join(overParts, sql` `)})`.as(fn.as);
}

function toInlineColumnRef(column: string): { alias?: string; column: string } {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    return {
      alias: column.slice(0, idx),
      column: column.slice(idx + 1),
    };
  }
  return { column };
}

function asDrizzleSubquerySql(subquery: unknown): SQL {
  if (!subquery || typeof subquery !== "object") {
    throw new UnsupportedSingleQueryPlanError("SEMI join subquery must be a Drizzle query object.");
  }
  const maybe = subquery as { getSQL?: unknown };
  if (typeof maybe.getSQL !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      "SEMI join subquery does not expose getSQL(), so it cannot be embedded as an IN subquery.",
    );
  }
  return sql`${subquery as { getSQL: () => SQL }}`;
}
