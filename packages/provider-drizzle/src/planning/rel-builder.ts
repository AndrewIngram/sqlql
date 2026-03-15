import { asc, desc, eq, sql, type AnyColumn, type SQL } from "drizzle-orm";
import type { RelNode } from "@tupl/foundation";
import type { ScanFilterClause } from "@tupl/provider-kit";
import type {
  SqlRelationalOrderTerm,
  SqlRelationalQueryTranslationBackend,
  SqlRelationalSelection,
  SqlRelationalWithSelection,
} from "@tupl/provider-kit/relational-sql";

import { executeDrizzleQueryBuilder, toSqlConditionFromSource } from "../backend/query-helpers";
import type {
  DrizzleExecutableBuilder,
  DrizzleProviderTableConfig,
  DrizzleQueryExecutor,
} from "../types";
import {
  buildSqlExpressionFromRelExpr,
  resolveColumnRefFromAliasMap,
  resolveJoinKeyColumnRefFromAliasMap,
  resolveProjectedSqlExpression,
  toAliasColumnRef,
  type JoinStep,
  type ScanBinding,
  type SingleQueryPlan,
  UnsupportedSingleQueryPlanError,
} from "./rel-strategy";

/**
 * Drizzle query translation owns Drizzle-specific builder primitives.
 * Provider-kit owns recursive rel traversal, strategy dispatch, and shared SQL-relational orchestration.
 */
export interface DrizzleTranslatedQuery {
  builder: DrizzleExecutableBuilder;
  whereClauses: SQL[];
}

export const drizzleQueryTranslationBackend: SqlRelationalQueryTranslationBackend<
  unknown,
  {
    entity: string;
    table: string;
    config: DrizzleProviderTableConfig<unknown>;
  },
  ScanBinding<unknown>,
  DrizzleQueryExecutor,
  DrizzleTranslatedQuery
> = {
  createRootQuery({ runtime, plan, selection }) {
    const preferDistinctSelection =
      !!plan.pipeline.aggregate &&
      plan.pipeline.aggregate.metrics.length === 0 &&
      plan.pipeline.aggregate.groupBy.length > 0;
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

    return {
      builder: selectFn(buildSelectionRecord(selection, plan.joinPlan.aliases)).from(
        plan.joinPlan.root.sourceTable,
      ) as DrizzleExecutableBuilder,
      whereClauses: [],
    };
  },
  applyRegularJoin({ query, join, aliases }) {
    const joinable = query.builder as DrizzleExecutableBuilder & {
      innerJoin?: (table: object, on: SQL) => unknown;
      leftJoin?: (table: object, on: SQL) => unknown;
      rightJoin?: (table: object, on: SQL) => unknown;
      fullJoin?: (table: object, on: SQL) => unknown;
    };
    ensureJoinMethodsAvailable(joinable, [join]);
    const leftColumn = resolveJoinKeyColumnRefFromAliasMap(aliases, {
      alias: join.leftKey.alias,
      column: join.leftKey.column,
    });
    const rightColumn = resolveJoinKeyColumnRefFromAliasMap(aliases, {
      alias: join.rightKey.alias,
      column: join.rightKey.column,
    });
    const onClause = eq(leftColumn, rightColumn);
    return {
      ...query,
      builder: (join.joinType === "inner"
        ? joinable.innerJoin!(join.right.sourceTable, onClause)
        : join.joinType === "left"
          ? joinable.leftJoin!(join.right.sourceTable, onClause)
          : join.joinType === "right"
            ? joinable.rightJoin!(join.right.sourceTable, onClause)
            : joinable.fullJoin!(join.right.sourceTable, onClause)) as DrizzleExecutableBuilder,
    };
  },
  applySemiJoin({ query, leftKey, subquery, aliases }) {
    const leftColumn = resolveJoinKeyColumnRefFromAliasMap(aliases, leftKey);
    return {
      ...query,
      whereClauses: [
        ...query.whereClauses,
        sql`${leftColumn} in (${asDrizzleSubquerySql(subquery.builder)})`,
      ],
    };
  },
  applyWhereClause({ query, clause, plan }) {
    const singleQueryPlan = plan as SingleQueryPlan<unknown>;
    return {
      ...query,
      whereClauses: [
        ...query.whereClauses,
        toSqlConditionFromRelFilterClause(clause, singleQueryPlan),
      ],
    };
  },
  applySelection({ query }) {
    return query;
  },
  applyGroupBy({ query, groupBy, aliases }) {
    const withWhere = ensureWhereApplied(query);
    const groupable = withWhere.builder as DrizzleExecutableBuilder & {
      groupBy?: (...columns: AnyColumn[]) => unknown;
    };
    if (typeof groupable.groupBy !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support GROUP BY on single-query fragments.",
      );
    }
    const groupByColumns = groupBy.map((columnRef) =>
      resolveColumnRefFromAliasMap(
        aliases,
        toAliasColumnRef(columnRef.alias ?? columnRef.table, columnRef.column),
      ),
    );
    return {
      builder: groupable.groupBy(...(groupByColumns as AnyColumn[])) as DrizzleExecutableBuilder,
      whereClauses: [],
    };
  },
  applyOrderBy({ query, plan, orderBy, aliases }) {
    const withWhere = ensureWhereApplied(query);
    if (typeof withWhere.builder.orderBy !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support ORDER BY on this fragment.",
      );
    }
    const orderByClauses = orderBy.map((term) => {
      const source = resolveOrderSource(term, plan, aliases);
      return term.direction === "asc" ? asc(source) : desc(source);
    });
    return orderByClauses.length > 0
      ? {
          builder: withWhere.builder.orderBy(...orderByClauses) as DrizzleExecutableBuilder,
          whereClauses: [],
        }
      : withWhere;
  },
  applyLimit({ query, limit }) {
    const withWhere = ensureWhereApplied(query);
    if (typeof withWhere.builder.limit !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support LIMIT on this fragment.",
      );
    }
    return {
      builder: withWhere.builder.limit(limit) as DrizzleExecutableBuilder,
      whereClauses: [],
    };
  },
  applyOffset({ query, offset }) {
    const withWhere = ensureWhereApplied(query);
    if (typeof withWhere.builder.offset !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support OFFSET on this fragment.",
      );
    }
    return {
      builder: withWhere.builder.offset(offset) as DrizzleExecutableBuilder,
      whereClauses: [],
    };
  },
  applySetOp({ left, right, wrapper }) {
    const leftQuery = ensureWhereApplied(left);
    const rightQuery = ensureWhereApplied(right);
    const methodName =
      wrapper.setOp.op === "union_all"
        ? "unionAll"
        : wrapper.setOp.op === "union"
          ? "union"
          : wrapper.setOp.op === "intersect"
            ? "intersect"
            : "except";
    const applySetOp = (leftQuery.builder as unknown as Record<string, unknown>)[methodName];
    if (typeof applySetOp !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        `Drizzle query builder does not support ${methodName} for single-query pushdown.`,
      );
    }
    return {
      builder: applySetOp.call(leftQuery.builder, rightQuery.builder) as DrizzleExecutableBuilder,
      whereClauses: [],
    };
  },
  buildWithQuery({ body, ctes, projection, orderBy, runtime }) {
    const dbWithCtes = runtime as {
      $with?: (name: string) => { as: (query: DrizzleExecutableBuilder) => unknown };
      with?: (...ctes: unknown[]) => {
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
      const cteRef = dbWithCtes.$with(cte.name).as(ensureWhereApplied(cte.query).builder);
      cteBindings.set(cte.name, cteRef);
      cteRefs.push(cteRef);
    }

    const source = cteBindings.get(body.cteRef.name);
    if (!source) {
      throw new UnsupportedSingleQueryPlanError(`Unknown CTE "${body.cteRef.name}" in WITH body.`);
    }
    const scanAlias = body.cteRef.alias ?? body.cteRef.name;

    const windowExpressions = new Map<string, unknown>();
    for (const fn of body.window?.functions ?? []) {
      windowExpressions.set(
        fn.as,
        buildWindowFunctionSql(fn, source as Record<string, unknown>, scanAlias),
      );
    }

    const selection = buildWithSelectionRecord(
      projection,
      source as Record<string, unknown>,
      windowExpressions,
      scanAlias,
    );

    let builder = dbWithCtes
      .with(...cteRefs)
      .select(selection)
      .from(source) as DrizzleExecutableBuilder;

    const whereClauses: SQL[] = [];
    for (const clause of body.cteRef.where ?? []) {
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
      const orderByClauses = orderBy.map((term) => {
        const sourceColumn =
          term.kind === "qualified"
            ? resolveWithBodySourceColumn(source as Record<string, unknown>, term.source, scanAlias)
            : windowExpressions.has(term.column)
              ? sql`${sql.identifier(term.column)}`
              : sql`${sql.identifier(term.column)}`;
        return term.direction === "asc" ? asc(sourceColumn) : desc(sourceColumn);
      });
      builder = builder.orderBy(...orderByClauses) as DrizzleExecutableBuilder;
    }

    return {
      builder,
      whereClauses: [],
    };
  },
  async executeQuery({ query, runtime }) {
    return executeDrizzleQueryBuilder(ensureWhereApplied(query).builder, runtime);
  },
};

function ensureWhereApplied(query: DrizzleTranslatedQuery): DrizzleTranslatedQuery {
  if (query.whereClauses.length === 0) {
    return query;
  }

  if (typeof query.builder.where !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      "Drizzle query builder does not support WHERE on single-query fragments.",
    );
  }

  return {
    builder: query.builder.where(
      sql.join(query.whereClauses, sql` and `),
    ) as DrizzleExecutableBuilder,
    whereClauses: [],
  };
}

function buildSelectionRecord<TContext>(
  selection: SqlRelationalSelection[],
  aliases: Map<string, ScanBinding<TContext>>,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};

  for (const entry of selection) {
    switch (entry.kind) {
      case "column":
        out[entry.output] = resolveColumnRefFromAliasMap(
          aliases,
          toAliasColumnRef(entry.source.alias ?? entry.source.table, entry.source.column),
        );
        break;
      case "metric":
        out[entry.output] = buildAggregateMetricSql(entry.metric, aliases);
        break;
      case "expr":
        out[entry.output] = buildSqlExpressionFromRelExpr(entry.expr, aliases);
        break;
    }
  }

  return out;
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
        windowExpressions.get(entry.window.as) ?? windowExpressions.get(entry.output);
      continue;
    }

    if (windowExpressions.has(entry.source.column)) {
      selection[entry.output] = windowExpressions.get(entry.source.column)!;
      continue;
    }

    selection[entry.output] = resolveWithBodySourceColumn(source, entry.source, scanAlias);
  }

  return selection;
}

function resolveOrderSource<TContext>(
  term: SqlRelationalOrderTerm,
  plan: SingleQueryPlan<TContext> | object,
  aliases: Map<string, ScanBinding<TContext>>,
): AnyColumn | SQL {
  if (term.kind === "qualified") {
    const alias = term.source.alias ?? term.source.table;
    if (!alias) {
      throw new UnsupportedSingleQueryPlanError(
        `Qualified ORDER BY column "${term.source.column}" is missing an alias.`,
      );
    }
    return resolveColumnRefFromAliasMap(aliases, {
      alias,
      column: term.source.column,
    });
  }

  if (!("pipeline" in plan)) {
    return sql`${sql.identifier(term.column)}`;
  }

  if (!plan.pipeline.aggregate) {
    const projected = resolveProjectedSelectionSource(term.column, plan);
    if (projected) {
      return projected;
    }
    return sql`${sql.identifier(term.column)}`;
  }

  const metric = plan.pipeline.aggregate.metrics.find((entry) => entry.as === term.column);
  if (metric) {
    return buildAggregateMetricSql(metric, aliases);
  }

  const groupBy = plan.pipeline.aggregate.groupBy.find((entry, index) => {
    const outputName = plan.pipeline.aggregate!.output[index]?.name ?? entry.column;
    return outputName === term.column || entry.column === term.column;
  });
  if (groupBy) {
    return resolveColumnRefFromAliasMap(
      aliases,
      toAliasColumnRef(groupBy.alias ?? groupBy.table, groupBy.column),
    );
  }

  return sql`${sql.identifier(term.column)}`;
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

function resolveProjectedSelectionSource<TContext>(
  output: string,
  plan: SingleQueryPlan<TContext>,
): SQL | AnyColumn | null {
  const mapping = plan.pipeline.project?.columns.find((column) => column.output === output);
  if (!mapping) {
    return null;
  }

  return resolveProjectedSqlExpression(mapping, plan.joinPlan.aliases, false);
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

function toSqlConditionFromRelFilterClause<TContext>(
  clause: ScanFilterClause,
  plan: SingleQueryPlan<TContext>,
): SQL {
  const source = resolveFilterSource(clause.column, plan);
  return toSqlConditionFromSource(clause, source);
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
