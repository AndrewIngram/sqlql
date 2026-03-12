import type { RelNode } from "@tupl/foundation";
import { unwrapSetOpRel, unwrapWithBodyRel } from "@tupl/provider-kit/shapes";

import {
  applyWhereClause,
  applyWindowFunction,
  createJoinSource,
  resolveQualifiedColumnRef,
  resolveWithBodyColumnRef,
  toRef,
} from "../backend/query-helpers";
import type { KnexLike, KnexLikeQueryBuilder, ResolvedEntityConfig } from "../types";
import {
  buildSingleQueryPlan,
  requireColumnProjectMapping,
  resolveObjectionRelCompileStrategy,
  type ObjectionRelCompileStrategy,
  type ScanBinding,
  type SemiJoinStep,
  type SingleQueryPlan,
  UnsupportedSingleQueryPlanError,
} from "./rel-strategy";

export async function buildObjectionRelBuilderForStrategy<TContext>(
  knex: KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  strategy: ObjectionRelCompileStrategy,
  context: TContext,
): Promise<KnexLikeQueryBuilder> {
  switch (strategy) {
    case "basic":
      return buildObjectionBasicRelSingleQueryBuilder(knex, entityConfigs, rel, context);
    case "set_op":
      return buildObjectionSetOpRelSingleQueryBuilder(knex, entityConfigs, rel, context);
    case "with":
      return buildObjectionWithRelSingleQueryBuilder(knex, entityConfigs, rel, context);
  }
}

export async function buildObjectionBasicRelSingleQueryBuilder<TContext>(
  knex: KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  context: TContext,
): Promise<KnexLikeQueryBuilder> {
  const plan = buildSingleQueryPlan(rel, entityConfigs);

  const rootSource = createJoinSource(plan.joinPlan.root, context);
  let query = knex.queryBuilder().from(rootSource);

  for (const joinStep of plan.joinPlan.joins) {
    if (joinStep.joinType === "semi") {
      const leftRef = `${joinStep.leftKey.alias}.${joinStep.leftKey.column}`;
      const subquery = await buildObjectionSemiJoinSubquery(knex, entityConfigs, joinStep, context);
      query = query.whereIn(leftRef, subquery);
      continue;
    }

    const joinMethod =
      joinStep.joinType === "inner"
        ? "innerJoin"
        : joinStep.joinType === "left"
          ? "leftJoin"
          : joinStep.joinType === "right"
            ? "rightJoin"
            : "fullJoin";

    const fn = (query as unknown as Record<string, unknown>)[joinMethod];
    if (typeof fn !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        `Knex query builder does not support ${joinMethod} in this dialect.`,
      );
    }

    const rightSource = createJoinSource(joinStep.right, context);
    query = (fn as (...args: unknown[]) => KnexLikeQueryBuilder).call(
      query,
      rightSource,
      `${joinStep.leftKey.alias}.${joinStep.leftKey.column}`,
      `${joinStep.rightKey.alias}.${joinStep.rightKey.column}`,
    );
  }

  for (const binding of plan.joinPlan.aliases.values()) {
    for (const clause of binding.scan.where ?? []) {
      query = applyWhereClause(query, clause, plan.joinPlan.aliases);
    }
  }
  for (const filter of plan.pipeline.filters) {
    for (const clause of filter.where ?? []) {
      query = applyWhereClause(query, clause, plan.joinPlan.aliases);
    }
  }

  query = query.clearSelect?.() ?? query;
  applySelection(query, plan);

  if (plan.pipeline.aggregate && plan.pipeline.aggregate.groupBy.length > 0) {
    query = query.groupBy(
      ...plan.pipeline.aggregate.groupBy.map((ref) =>
        resolveQualifiedColumnRef(plan.joinPlan.aliases, {
          ...toRef(ref.alias ?? ref.table, ref.column),
        }),
      ),
    );
  }

  if (plan.pipeline.sort) {
    for (const term of plan.pipeline.sort.orderBy) {
      query = query.orderBy(resolveSortRef(plan, term), term.direction);
    }
  }

  if (plan.pipeline.limitOffset?.limit != null) {
    query = query.limit(plan.pipeline.limitOffset.limit);
  }
  if (plan.pipeline.limitOffset?.offset != null) {
    query = query.offset(plan.pipeline.limitOffset.offset);
  }

  return query;
}

export async function buildObjectionSetOpRelSingleQueryBuilder<TContext>(
  knex: KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  context: TContext,
): Promise<KnexLikeQueryBuilder> {
  const wrapper = unwrapSetOpRel(rel);
  if (!wrapper) {
    throw new UnsupportedSingleQueryPlanError("Expected set-op relational shape.");
  }

  const leftStrategy = resolveObjectionRelCompileStrategy(wrapper.setOp.left, entityConfigs);
  const rightStrategy = resolveObjectionRelCompileStrategy(wrapper.setOp.right, entityConfigs);
  if (!leftStrategy || !rightStrategy) {
    throw new UnsupportedSingleQueryPlanError(
      "Set-op branches are not supported for single-query pushdown.",
    );
  }

  const left = await buildObjectionRelBuilderForStrategy(
    knex,
    entityConfigs,
    wrapper.setOp.left,
    leftStrategy,
    context,
  );
  const right = await buildObjectionRelBuilderForStrategy(
    knex,
    entityConfigs,
    wrapper.setOp.right,
    rightStrategy,
    context,
  );

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
      `Knex query builder does not support ${methodName} for single-query pushdown.`,
    );
  }

  let query = applySetOp.call(left, [right]) as KnexLikeQueryBuilder;

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
    for (const term of wrapper.sort.orderBy) {
      if (term.source.alias || term.source.table) {
        throw new UnsupportedSingleQueryPlanError(
          "Set-op ORDER BY columns must be unqualified output columns.",
        );
      }
      query = query.orderBy(term.source.column, term.direction);
    }
  }

  if (wrapper.limitOffset?.limit != null) {
    query = query.limit(wrapper.limitOffset.limit);
  }
  if (wrapper.limitOffset?.offset != null) {
    query = query.offset(wrapper.limitOffset.offset);
  }

  return query;
}

export async function buildObjectionWithRelSingleQueryBuilder<TContext>(
  knex: KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  context: TContext,
): Promise<KnexLikeQueryBuilder> {
  if (rel.kind !== "with") {
    throw new UnsupportedSingleQueryPlanError(`Expected with node, received "${rel.kind}".`);
  }

  let query = knex.queryBuilder();

  for (const cte of rel.ctes) {
    const strategy = resolveObjectionRelCompileStrategy(cte.query, entityConfigs);
    if (!strategy) {
      throw new UnsupportedSingleQueryPlanError(
        `CTE "${cte.name}" is not supported for single-query pushdown.`,
      );
    }

    const cteQuery = await buildObjectionRelBuilderForStrategy(
      knex,
      entityConfigs,
      cte.query,
      strategy,
      context,
    );

    const withFn = (query as { with?: unknown }).with;
    if (typeof withFn !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        "Knex query builder does not support CTE builders required for WITH pushdown.",
      );
    }

    query = withFn.call(query, cte.name, cteQuery) as KnexLikeQueryBuilder;
  }

  const body = unwrapWithBodyRel(rel.body);
  if (!body) {
    throw new UnsupportedSingleQueryPlanError(
      "Unsupported WITH body shape for single-query pushdown.",
    );
  }

  const scanAlias = body.cteScan.alias ?? body.cteScan.table;
  const fromSource = body.cteScan.alias
    ? ({ [body.cteScan.alias]: body.cteScan.table } as Record<string, string>)
    : body.cteScan.table;
  query = query.from(fromSource);

  const aliases = new Map<string, ScanBinding<TContext>>([
    [
      scanAlias,
      {
        alias: scanAlias,
        entity: body.cteScan.table,
        table: body.cteScan.table,
        scan: body.cteScan,
        config: {},
      },
    ],
  ]);

  for (const clause of body.cteScan.where ?? []) {
    query = applyWhereClause(query, clause, aliases);
  }
  for (const filter of body.filters) {
    for (const clause of filter.where ?? []) {
      query = applyWhereClause(query, clause, aliases);
    }
  }

  for (const fn of body.window?.functions ?? []) {
    query = applyWindowFunction(query, fn, scanAlias);
  }

  query = query.clearSelect?.() ?? query;

  const windowAliases = new Set((body.window?.functions ?? []).map((fn) => fn.as));
  const projection: Array<{
    source: { alias?: string; table?: string; column: string };
    output: string;
  }> = body.project?.columns.map((mapping) => requireColumnProjectMapping(mapping)) ?? [
    ...body.cteScan.select.map((column) => ({
      source: { column },
      output: column,
    })),
    ...[...windowAliases].map((column) => ({
      source: { column },
      output: column,
    })),
  ];

  for (const mapping of projection) {
    if (
      !mapping.source.alias &&
      !mapping.source.table &&
      windowAliases.has(mapping.source.column)
    ) {
      query = query.select({ [mapping.output]: mapping.source.column });
      continue;
    }

    const source = resolveWithBodyColumnRef(mapping.source, scanAlias);
    query = query.select({ [mapping.output]: source });
  }

  if (body.sort) {
    for (const term of body.sort.orderBy) {
      const source =
        !term.source.alias && !term.source.table && windowAliases.has(term.source.column)
          ? term.source.column
          : resolveWithBodyColumnRef(term.source, scanAlias);
      query = query.orderBy(source, term.direction);
    }
  }

  if (body.limitOffset?.limit != null) {
    query = query.limit(body.limitOffset.limit);
  }
  if (body.limitOffset?.offset != null) {
    query = query.offset(body.limitOffset.offset);
  }

  return query;
}

async function buildObjectionSemiJoinSubquery<TContext>(
  knex: KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  joinStep: SemiJoinStep,
  context: TContext,
): Promise<KnexLikeQueryBuilder> {
  if (joinStep.right.output.length !== 1) {
    throw new UnsupportedSingleQueryPlanError(
      "SEMI join subquery must project exactly one output column.",
    );
  }

  const strategy = resolveObjectionRelCompileStrategy(joinStep.right, entityConfigs);
  if (!strategy) {
    throw new UnsupportedSingleQueryPlanError(
      "SEMI join right-hand rel fragment is not supported for single-query pushdown.",
    );
  }

  return buildObjectionRelBuilderForStrategy(
    knex,
    entityConfigs,
    joinStep.right,
    strategy,
    context,
  );
}

function applySelection<TContext>(
  query: KnexLikeQueryBuilder,
  plan: SingleQueryPlan<TContext>,
): void {
  if (!plan.pipeline.aggregate) {
    const project = plan.pipeline.project;
    if (!project) {
      throw new UnsupportedSingleQueryPlanError(
        "Non-aggregate rel fragment requires a project node.",
      );
    }

    for (const rawMapping of project.columns) {
      const mapping = requireColumnProjectMapping(rawMapping);
      const source = resolveQualifiedColumnRef(plan.joinPlan.aliases, {
        ...toRef(mapping.source.alias ?? mapping.source.table, mapping.source.column),
      });
      query.select({ [mapping.output]: source });
    }
    return;
  }

  const metricByAs = new Map(plan.pipeline.aggregate.metrics.map((metric) => [metric.as, metric]));
  const groupByByColumn = new Map<string, (typeof plan.pipeline.aggregate.groupBy)[number]>();
  plan.pipeline.aggregate.groupBy.forEach((groupBy, index) => {
    groupByByColumn.set(groupBy.column, groupBy);
    const outputName = plan.pipeline.aggregate!.output[index]?.name ?? groupBy.column;
    groupByByColumn.set(outputName, groupBy);
  });

  const projection = plan.pipeline.project?.columns ?? [
    ...plan.pipeline.aggregate.groupBy.map((groupBy, index) => ({
      source: {
        column: plan.pipeline.aggregate!.output[index]?.name ?? groupBy.column,
      },
      output: plan.pipeline.aggregate!.output[index]?.name ?? groupBy.column,
    })),
    ...plan.pipeline.aggregate.metrics.map((metric, index) => ({
      source: {
        column:
          plan.pipeline.aggregate!.output[plan.pipeline.aggregate!.groupBy.length + index]?.name ??
          metric.as,
      },
      output:
        plan.pipeline.aggregate!.output[plan.pipeline.aggregate!.groupBy.length + index]?.name ??
        metric.as,
    })),
  ];

  for (const rawMapping of projection) {
    const mapping = requireColumnProjectMapping(rawMapping);
    const metric = metricByAs.get(mapping.source.column);
    if (metric) {
      applyMetricSelection(query, plan.joinPlan.aliases, metric, mapping.output);
      continue;
    }

    const groupBy = groupByByColumn.get(mapping.source.column);
    if (!groupBy) {
      throw new UnsupportedSingleQueryPlanError(
        `Unknown aggregate projection source "${mapping.source.column}".`,
      );
    }

    const source = resolveQualifiedColumnRef(plan.joinPlan.aliases, {
      ...toRef(groupBy.alias ?? groupBy.table, groupBy.column),
    });
    query.select({ [mapping.output]: source });
  }
}

function applyMetricSelection<TContext>(
  query: KnexLikeQueryBuilder,
  aliases: Map<string, ScanBinding<TContext>>,
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  output: string,
): void {
  if (metric.fn === "count" && !metric.column) {
    query.count({ [output]: "*" });
    return;
  }

  if (!metric.column) {
    throw new UnsupportedSingleQueryPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const source = resolveQualifiedColumnRef(aliases, {
    ...toRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  if (metric.fn === "count" && metric.distinct) {
    query.countDistinct({ [output]: source });
    return;
  }

  if (metric.fn === "sum" && metric.distinct) {
    throw new UnsupportedSingleQueryPlanError(
      "Knex sum(distinct ...) is not supported in this adapter yet.",
    );
  }

  if (metric.fn === "avg" && metric.distinct) {
    throw new UnsupportedSingleQueryPlanError(
      "Knex avg(distinct ...) is not supported in this adapter yet.",
    );
  }

  switch (metric.fn) {
    case "count":
      query.count({ [output]: source });
      break;
    case "sum":
      query.sum({ [output]: source });
      break;
    case "avg":
      query.avg({ [output]: source });
      break;
    case "min":
      query.min({ [output]: source });
      break;
    case "max":
      query.max({ [output]: source });
      break;
  }
}

function resolveSortRef<TContext>(
  plan: SingleQueryPlan<TContext>,
  term: Extract<RelNode, { kind: "sort" }>["orderBy"][number],
): string {
  if (term.source.alias || term.source.table) {
    return resolveQualifiedColumnRef(plan.joinPlan.aliases, {
      ...toRef(term.source.alias ?? term.source.table, term.source.column),
    });
  }

  if (plan.pipeline.aggregate) {
    const groupBy = plan.pipeline.aggregate.groupBy.find((entry, index) => {
      const outputName = plan.pipeline.aggregate!.output[index]?.name ?? entry.column;
      return outputName === term.source.column || entry.column === term.source.column;
    });
    if (groupBy) {
      return resolveQualifiedColumnRef(plan.joinPlan.aliases, {
        ...toRef(groupBy.alias ?? groupBy.table, groupBy.column),
      });
    }
  }

  return term.source.column;
}
