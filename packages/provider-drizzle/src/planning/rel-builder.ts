import { asc, desc, eq, sql, type AnyColumn, type SQL } from "drizzle-orm";
import type { RelNode } from "@tupl/foundation";
import { unwrapSetOpRel, unwrapWithBodyRel } from "@tupl/provider-kit/shapes";
import type { QueryRow, ScanFilterClause } from "@tupl/provider-kit";

import {
  executeDrizzleQueryBuilder,
  normalizeScope,
  toSqlConditionFromSource,
} from "../backend/query-helpers";
import type {
  CreateDrizzleProviderOptions,
  DrizzleExecutableBuilder,
  DrizzleProviderTableConfig,
  DrizzleQueryExecutor,
} from "../types";
import {
  buildSingleQueryPlan,
  requireColumnProjectMapping,
  resolveColumnRefFromAliasMap,
  resolveDrizzleRelCompileStrategy,
  resolveJoinKeyColumnRefFromAliasMap,
  resolveProjectedSqlExpression,
  toAliasColumnRef,
  type DrizzleRelCompileStrategy,
  type JoinStep,
  type ScanBinding,
  type SemiJoinStep,
  type SingleQueryPlan,
  UnsupportedSingleQueryPlanError,
} from "./rel-strategy";

export async function executeDrizzleRelSingleQuery<TContext>(
  rel: RelNode,
  strategy: DrizzleRelCompileStrategy,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  switch (strategy) {
    case "basic":
      return executeDrizzleBasicRelSingleQuery(rel, options, context, db);
    case "set_op":
      return executeDrizzleSetOpRelSingleQuery(rel, options, context, db);
    case "with":
      return executeDrizzleWithRelSingleQuery(rel, options, context, db);
  }
}

export async function buildDrizzleRelBuilderForStrategy<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{ builder: DrizzleExecutableBuilder }> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const strategy = resolveDrizzleRelCompileStrategy(rel, tableConfigs);
  if (!strategy) {
    throw new UnsupportedSingleQueryPlanError(
      `Rel node "${rel.kind}" is not supported in Drizzle single-query pushdown.`,
    );
  }
  switch (strategy) {
    case "basic":
      return buildDrizzleBasicRelSingleQueryBuilder(rel, options, context, db);
    case "set_op":
      return buildDrizzleSetOpRelSingleQueryBuilder(rel, options, context, db);
    case "with":
      return buildDrizzleWithRelSingleQueryBuilder(rel, options, context, db);
  }
}

async function executeDrizzleBasicRelSingleQuery<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  const { builder } = await buildDrizzleBasicRelSingleQueryBuilder(rel, options, context, db);
  return executeDrizzleQueryBuilder(builder, db);
}

export async function buildDrizzleBasicRelSingleQueryBuilder<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{ builder: DrizzleExecutableBuilder }> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const plan = buildSingleQueryPlan(rel, tableConfigs);
  const selection = buildSingleQuerySelection(plan);
  const preferDistinctSelection =
    !!plan.pipeline.aggregate &&
    plan.pipeline.aggregate.metrics.length === 0 &&
    plan.pipeline.aggregate.groupBy.length > 0;
  const dbWithSelectDistinct = db as {
    select: (selection: Record<string, unknown>) => {
      from: (table: object) => {
        innerJoin: (table: object, on: SQL) => unknown;
        leftJoin: (table: object, on: SQL) => unknown;
        rightJoin: (table: object, on: SQL) => unknown;
        fullJoin: (table: object, on: SQL) => unknown;
        where: (condition: SQL) => unknown;
        groupBy: (...columns: AnyColumn[]) => unknown;
        orderBy: (...clauses: SQL[]) => unknown;
        limit: (value: number) => unknown;
        offset: (value: number) => unknown;
        execute: () => Promise<QueryRow[]>;
      };
    };
    selectDistinct?: (selection: Record<string, unknown>) => {
      from: (table: object) => {
        innerJoin: (table: object, on: SQL) => unknown;
        leftJoin: (table: object, on: SQL) => unknown;
        rightJoin: (table: object, on: SQL) => unknown;
        fullJoin: (table: object, on: SQL) => unknown;
        where: (condition: SQL) => unknown;
        groupBy: (...columns: AnyColumn[]) => unknown;
        orderBy: (...clauses: SQL[]) => unknown;
        limit: (value: number) => unknown;
        offset: (value: number) => unknown;
        execute: () => Promise<QueryRow[]>;
      };
    };
  };

  const selectFn =
    preferDistinctSelection && typeof dbWithSelectDistinct.selectDistinct === "function"
      ? dbWithSelectDistinct.selectDistinct.bind(dbWithSelectDistinct)
      : dbWithSelectDistinct.select.bind(dbWithSelectDistinct);

  let builder = selectFn(selection).from(plan.joinPlan.root.table) as DrizzleExecutableBuilder & {
    innerJoin: (table: object, on: SQL) => unknown;
    leftJoin: (table: object, on: SQL) => unknown;
    rightJoin: (table: object, on: SQL) => unknown;
    fullJoin: (table: object, on: SQL) => unknown;
    groupBy: (...columns: AnyColumn[]) => unknown;
  };

  ensureJoinMethodsAvailable(builder, plan.joinPlan.joins);

  const whereClauses: SQL[] = [];
  for (const joinStep of plan.joinPlan.joins) {
    if (joinStep.joinType === "semi") {
      const leftColumn = resolveJoinKeyColumnRefFromAliasMap(plan.joinPlan.aliases, {
        alias: joinStep.leftKey.alias,
        column: joinStep.leftKey.column,
      });
      const { subquery } = await buildSemiJoinSubquery(joinStep, options, context, db);
      whereClauses.push(sql`${leftColumn} in (${asDrizzleSubquerySql(subquery)})`);
      continue;
    }
    const leftColumn = resolveJoinKeyColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias: joinStep.leftKey.alias,
      column: joinStep.leftKey.column,
    });
    const rightColumn = resolveJoinKeyColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias: joinStep.rightKey.alias,
      column: joinStep.rightKey.column,
    });
    const onClause = eq(leftColumn, rightColumn);
    builder = (
      joinStep.joinType === "inner"
        ? builder.innerJoin(joinStep.right.table, onClause)
        : joinStep.joinType === "left"
          ? builder.leftJoin(joinStep.right.table, onClause)
          : joinStep.joinType === "right"
            ? builder.rightJoin(joinStep.right.table, onClause)
            : builder.fullJoin(joinStep.right.table, onClause)
    ) as typeof builder;
  }

  for (const binding of plan.joinPlan.aliases.values()) {
    whereClauses.push(
      ...normalizeScope(
        binding.tableConfig.scope ? await binding.tableConfig.scope(context) : undefined,
      ),
    );
    for (const clause of binding.scan.where ?? []) {
      whereClauses.push(toSqlCondition(clause, binding.scanColumns, binding.tableName));
    }
  }

  for (const filterNode of plan.pipeline.filters) {
    for (const clause of filterNode.where ?? []) {
      whereClauses.push(toSqlConditionFromRelFilterClause(clause, plan));
    }
  }

  const where = sql.join(whereClauses, sql` and `);
  if (whereClauses.length > 0 && typeof builder.where === "function") {
    builder = builder.where(where) as typeof builder;
  }

  if (
    plan.pipeline.aggregate &&
    plan.pipeline.aggregate.groupBy.length > 0 &&
    !preferDistinctSelection
  ) {
    const groupByColumns = plan.pipeline.aggregate.groupBy.map((columnRef) =>
      resolveColumnRefFromAliasMap(
        plan.joinPlan.aliases,
        toAliasColumnRef(columnRef.alias ?? columnRef.table, columnRef.column),
      ),
    );
    builder = builder.groupBy(...(groupByColumns as AnyColumn[])) as typeof builder;
  }

  if (plan.pipeline.sort && typeof builder.orderBy === "function") {
    const orderBy = plan.pipeline.sort.orderBy.map((term) => {
      const source = resolveSingleQuerySortSource(term, plan);
      return term.direction === "asc" ? asc(source) : desc(source);
    });
    if (orderBy.length > 0) {
      builder = builder.orderBy(...orderBy) as typeof builder;
    }
  }

  if (plan.pipeline.limitOffset?.limit != null && typeof builder.limit === "function") {
    builder = builder.limit(plan.pipeline.limitOffset.limit) as typeof builder;
  }
  if (plan.pipeline.limitOffset?.offset != null && typeof builder.offset === "function") {
    builder = builder.offset(plan.pipeline.limitOffset.offset) as typeof builder;
  }

  return { builder };
}

async function executeDrizzleSetOpRelSingleQuery<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  const { builder } = await buildDrizzleSetOpRelSingleQueryBuilder(rel, options, context, db);
  return executeDrizzleQueryBuilder(builder, db);
}

async function executeDrizzleWithRelSingleQuery<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  const { builder } = await buildDrizzleWithRelSingleQueryBuilder(rel, options, context, db);
  return executeDrizzleQueryBuilder(builder, db);
}

async function buildSemiJoinSubquery<TContext>(
  joinStep: SemiJoinStep,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{ subquery: DrizzleExecutableBuilder }> {
  if (joinStep.right.output.length !== 1) {
    throw new UnsupportedSingleQueryPlanError(
      "SEMI join subquery must project exactly one output column.",
    );
  }
  return {
    subquery: (await buildDrizzleRelBuilderForStrategy(joinStep.right, options, context, db))
      .builder,
  };
}

export async function buildDrizzleSetOpRelSingleQueryBuilder<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{ builder: DrizzleExecutableBuilder }> {
  const wrapper = unwrapSetOpRel(rel);
  if (!wrapper) {
    throw new UnsupportedSingleQueryPlanError("Expected set-op relational shape.");
  }

  const left = (await buildDrizzleRelBuilderForStrategy(wrapper.setOp.left, options, context, db))
    .builder;
  const right = (await buildDrizzleRelBuilderForStrategy(wrapper.setOp.right, options, context, db))
    .builder;
  const methodName =
    wrapper.setOp.op === "union_all"
      ? "unionAll"
      : wrapper.setOp.op === "union"
        ? "union"
        : wrapper.setOp.op === "intersect"
          ? "intersect"
          : "except";
  const applySetOp = (left as unknown as Record<string, unknown>)[methodName];
  if (typeof applySetOp !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      `Drizzle query builder does not support ${methodName} for single-query pushdown.`,
    );
  }
  let builder = applySetOp.call(left, right) as DrizzleExecutableBuilder;

  if (wrapper.project) {
    for (const rawMapping of wrapper.project.columns) {
      const mapping = requireColumnProjectMapping(rawMapping);
      if (
        (mapping.source.alias || mapping.source.table) &&
        mapping.source.column !== mapping.output
      ) {
        throw new UnsupportedSingleQueryPlanError(
          "Set-op projections with qualified or renamed columns are not supported in single-query pushdown.",
        );
      }
    }
  }

  if (wrapper.sort) {
    if (typeof builder.orderBy !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support ORDER BY on set-op fragments.",
      );
    }
    const orderByClauses = wrapper.sort.orderBy.map((term) => {
      if (term.source.alias || term.source.table) {
        throw new UnsupportedSingleQueryPlanError(
          "Set-op ORDER BY columns must be unqualified output columns.",
        );
      }
      const identifier = sql.identifier(term.source.column);
      return term.direction === "asc" ? asc(identifier) : desc(identifier);
    });
    if (orderByClauses.length > 0) {
      builder = builder.orderBy(...orderByClauses) as DrizzleExecutableBuilder;
    }
  }

  if (wrapper.limitOffset?.limit != null) {
    if (typeof builder.limit !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support LIMIT on set-op fragments.",
      );
    }
    builder = builder.limit(wrapper.limitOffset.limit) as DrizzleExecutableBuilder;
  }
  if (wrapper.limitOffset?.offset != null) {
    if (typeof builder.offset !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support OFFSET on set-op fragments.",
      );
    }
    builder = builder.offset(wrapper.limitOffset.offset) as DrizzleExecutableBuilder;
  }

  return { builder };
}

export async function buildDrizzleWithRelSingleQueryBuilder<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<{ builder: DrizzleExecutableBuilder }> {
  if (rel.kind !== "with") {
    throw new UnsupportedSingleQueryPlanError(`Expected with node, received "${rel.kind}".`);
  }
  const dbWithCtes = db as {
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
  for (const cte of rel.ctes) {
    const query = (await buildDrizzleRelBuilderForStrategy(cte.query, options, context, db))
      .builder;
    const cteRef = dbWithCtes.$with(cte.name).as(query);
    cteBindings.set(cte.name, cteRef);
    cteRefs.push(cteRef);
  }

  const body = unwrapWithBodyRel(rel.body);
  if (!body) {
    throw new UnsupportedSingleQueryPlanError(
      "Unsupported WITH body shape for single-query pushdown.",
    );
  }
  const source = cteBindings.get(body.cteScan.table);
  if (!source) {
    throw new UnsupportedSingleQueryPlanError(`Unknown CTE "${body.cteScan.table}" in WITH body.`);
  }
  const scanAlias = body.cteScan.alias ?? body.cteScan.table;

  const windowExpressions = new Map<string, unknown>();
  for (const fn of body.window?.functions ?? []) {
    windowExpressions.set(
      fn.as,
      buildWindowFunctionSql(fn, source as Record<string, unknown>, scanAlias),
    );
  }

  const selection: Record<string, unknown> = {};
  if (body.project) {
    for (const rawMapping of body.project.columns) {
      const mapping = requireColumnProjectMapping(rawMapping);
      selection[mapping.output] = resolveWithBodyProjectionSource(
        mapping,
        source as Record<string, unknown>,
        windowExpressions,
        scanAlias,
      );
    }
  } else {
    for (const column of body.cteScan.select) {
      selection[column] = resolveWithBodySourceColumn(
        source as Record<string, unknown>,
        {
          alias: scanAlias,
          column,
        },
        scanAlias,
      );
    }
    for (const [name, exprSql] of windowExpressions.entries()) {
      selection[name] = exprSql;
    }
  }

  let builder = dbWithCtes
    .with(...cteRefs)
    .select(selection)
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

  if (body.sort) {
    if (typeof builder.orderBy !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support ORDER BY on WITH fragments.",
      );
    }
    const orderBy = body.sort.orderBy.map((term) => {
      const sourceColumn = windowExpressions.has(term.source.column)
        ? sql.identifier(term.source.column)
        : resolveWithBodySourceColumn(source as Record<string, unknown>, term.source, scanAlias);
      return term.direction === "asc" ? asc(sourceColumn) : desc(sourceColumn);
    });
    if (orderBy.length > 0) {
      builder = builder.orderBy(...orderBy) as DrizzleExecutableBuilder;
    }
  }

  if (body.limitOffset?.limit != null) {
    if (typeof builder.limit !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support LIMIT on WITH fragments.",
      );
    }
    builder = builder.limit(body.limitOffset.limit) as DrizzleExecutableBuilder;
  }
  if (body.limitOffset?.offset != null) {
    if (typeof builder.offset !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Drizzle query builder does not support OFFSET on WITH fragments.",
      );
    }
    builder = builder.offset(body.limitOffset.offset) as DrizzleExecutableBuilder;
  }

  return { builder };
}

function resolveWithBodyProjectionSource(
  rawMapping: Extract<RelNode, { kind: "project" }>["columns"][number],
  source: Record<string, unknown>,
  windowExpressions: Map<string, unknown>,
  scanAlias: string,
): unknown {
  const mapping = requireColumnProjectMapping(rawMapping);
  if (windowExpressions.has(mapping.source.column)) {
    return windowExpressions.get(mapping.source.column)!;
  }
  return resolveWithBodySourceColumn(source, mapping.source, scanAlias);
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

function resolveSingleQuerySortSource<TContext>(
  term: Extract<RelNode, { kind: "sort" }>["orderBy"][number],
  plan: SingleQueryPlan<TContext>,
): AnyColumn | SQL {
  const alias = term.source.alias ?? term.source.table;
  if (alias) {
    return resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias,
      column: term.source.column,
    });
  }

  if (!plan.pipeline.aggregate) {
    const projected = resolveProjectedSelectionSource(term.source.column, plan);
    if (projected) {
      return projected;
    }
    return resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      column: term.source.column,
    });
  }

  const metric = plan.pipeline.aggregate.metrics.find((entry) => entry.as === term.source.column);
  if (metric) {
    return buildAggregateMetricSql(metric, plan.joinPlan.aliases);
  }

  const groupBy = plan.pipeline.aggregate.groupBy.find((entry, index) => {
    const outputName = plan.pipeline.aggregate!.output[index]?.name ?? entry.column;
    return outputName === term.source.column || entry.column === term.source.column;
  });
  if (groupBy) {
    return resolveColumnRefFromAliasMap(
      plan.joinPlan.aliases,
      toAliasColumnRef(groupBy.alias ?? groupBy.table, groupBy.column),
    );
  }

  throw new UnsupportedSingleQueryPlanError(
    `Unsupported ORDER BY reference "${term.source.column}" in aggregate rel fragment.`,
  );
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
        const mapping = requireColumnProjectMapping(rawMapping);
        const metricSource = metricSources.get(mapping.source.column);
        if (metricSource) {
          selection[mapping.output] = metricSource.as(mapping.output);
          continue;
        }
        const qualifiedSource = mapping.source.alias ?? mapping.source.table;
        if (qualifiedSource) {
          const groupSource = groupSourcesByKey.get(`${qualifiedSource}.${mapping.source.column}`);
          if (groupSource) {
            selection[mapping.output] = sql`${groupSource}`.as(mapping.output);
            continue;
          }
        }
        const groupSource = groupSources.get(mapping.source.column);
        if (groupSource) {
          selection[mapping.output] = sql`${groupSource}`.as(mapping.output);
          continue;
        }
        throw new UnsupportedSingleQueryPlanError(
          `Aggregate projection source "${mapping.source.column}" is not available in grouped output.`,
        );
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
      const resolved = resolveProjectedSqlExpression(rawMapping, plan.joinPlan.aliases, true);
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

function toSqlCondition<TColumn extends string>(
  clause: ScanFilterClause<TColumn>,
  columns: import("../types").DrizzleColumnMap<TColumn>,
  tableName: string,
): SQL {
  const source = columns[clause.column as TColumn];
  if (!source) {
    throw new Error(`Unsupported filter column "${clause.column}" for table "${tableName}".`);
  }
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
