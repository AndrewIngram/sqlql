import type { RelNode } from "@tupl/foundation";
import { unwrapSetOpRel, unwrapWithBodyRel } from "@tupl/provider-kit/shapes";

import {
  applyBase,
  applyWhereClause,
  resolveQualifiedColumnRef,
  toRef,
} from "../backend/query-helpers";
import type { KyselyDatabaseLike, KyselyQueryBuilderLike, ResolvedEntityConfig } from "../types";
import {
  buildSingleQueryPlan,
  requireColumnProjectMapping,
  resolveKyselyRelCompileStrategy,
  type KyselyRelCompileStrategy,
  type ScanBinding,
  type SemiJoinStep,
  type SingleQueryPlan,
  UnsupportedSingleQueryPlanError,
} from "./rel-strategy";

type SelectionEntry = {
  output: string;
  toExpression: (eb: any) => unknown;
};

export async function buildKyselyRelBuilderForStrategy<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  strategy: KyselyRelCompileStrategy,
  context: TContext,
): Promise<KyselyQueryBuilderLike> {
  switch (strategy) {
    case "basic":
      return buildKyselyBasicRelSingleQueryBuilder(db, entityConfigs, rel, context);
    case "set_op":
      return buildKyselySetOpRelSingleQueryBuilder(db, entityConfigs, rel, context);
    case "with":
      return buildKyselyWithRelSingleQueryBuilder(db, entityConfigs, rel, context);
  }
}

export async function buildKyselyBasicRelSingleQueryBuilder<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  context: TContext,
): Promise<KyselyQueryBuilderLike> {
  const plan = buildSingleQueryPlan(rel, entityConfigs);

  const rootFrom = `${plan.joinPlan.root.table} as ${plan.joinPlan.root.alias}`;
  let query = db.selectFrom(rootFrom);
  query = await applyBase(query, db, plan.joinPlan.root, context, plan.joinPlan.root.alias);

  for (const joinStep of plan.joinPlan.joins) {
    if (joinStep.joinType === "semi") {
      const leftRef = `${joinStep.leftKey.alias}.${joinStep.leftKey.column}`;
      const subquery = await buildKyselySemiJoinSubquery(db, entityConfigs, joinStep, context);
      query = query.where(leftRef, "in", subquery);
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
        `Kysely query builder does not support ${joinMethod} in this dialect.`,
      );
    }

    query = (fn as (...args: unknown[]) => KyselyQueryBuilderLike).call(
      query,
      `${joinStep.right.table} as ${joinStep.right.alias}`,
      `${joinStep.leftKey.alias}.${joinStep.leftKey.column}`,
      `${joinStep.rightKey.alias}.${joinStep.rightKey.column}`,
    );

    query = await applyBase(query, db, joinStep.right, context, joinStep.right.alias);
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

  const selection = buildSelection(plan);
  query = query.select((eb: any) => selection.map((entry) => entry.toExpression(eb)));

  if (plan.pipeline.aggregate && plan.pipeline.aggregate.groupBy.length > 0) {
    query =
      query.groupBy?.(
        plan.pipeline.aggregate.groupBy.map((ref) =>
          resolveQualifiedColumnRef(plan.joinPlan.aliases, {
            ...toRef(ref.alias ?? ref.table, ref.column),
          }),
        ),
      ) ?? query;
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

export async function buildKyselySetOpRelSingleQueryBuilder<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  context: TContext,
): Promise<KyselyQueryBuilderLike> {
  const wrapper = unwrapSetOpRel(rel);
  if (!wrapper) {
    throw new UnsupportedSingleQueryPlanError("Expected set-op relational shape.");
  }

  const leftStrategy = resolveKyselyRelCompileStrategy(wrapper.setOp.left, entityConfigs);
  const rightStrategy = resolveKyselyRelCompileStrategy(wrapper.setOp.right, entityConfigs);
  if (!leftStrategy || !rightStrategy) {
    throw new UnsupportedSingleQueryPlanError(
      "Set-op branches are not supported for single-query pushdown.",
    );
  }

  const left = await buildKyselyRelBuilderForStrategy(
    db,
    entityConfigs,
    wrapper.setOp.left,
    leftStrategy,
    context,
  );
  const right = await buildKyselyRelBuilderForStrategy(
    db,
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
      `Kysely query builder does not support ${methodName} for single-query pushdown.`,
    );
  }

  let query = applySetOp.call(left, right) as KyselyQueryBuilderLike;

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

export async function buildKyselyWithRelSingleQueryBuilder<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  context: TContext,
): Promise<KyselyQueryBuilderLike> {
  if (rel.kind !== "with") {
    throw new UnsupportedSingleQueryPlanError(`Expected with node, received "${rel.kind}".`);
  }
  if (typeof db.with !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      "Kysely database instance does not support CTE builders required for WITH pushdown.",
    );
  }

  let withDb = db;
  for (const cte of rel.ctes) {
    const strategy = resolveKyselyRelCompileStrategy(cte.query, entityConfigs);
    if (!strategy) {
      throw new UnsupportedSingleQueryPlanError(
        `CTE "${cte.name}" is not supported for single-query pushdown.`,
      );
    }
    const cteQuery = await buildKyselyRelBuilderForStrategy(
      db,
      entityConfigs,
      cte.query,
      strategy,
      context,
    );
    withDb = withDb.with!(cte.name, () => cteQuery);
  }

  const body = unwrapWithBodyRel(rel.body);
  if (!body) {
    throw new UnsupportedSingleQueryPlanError(
      "Unsupported WITH body shape for single-query pushdown.",
    );
  }

  const scanAlias = body.cteScan.alias ?? body.cteScan.table;
  const from = body.cteScan.alias
    ? `${body.cteScan.table} as ${body.cteScan.alias}`
    : body.cteScan.table;
  let query = withDb.selectFrom(from);

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

  const windowByAlias = new Map((body.window?.functions ?? []).map((fn) => [fn.as, fn] as const));

  const projection = body.project?.columns ?? [
    ...body.cteScan.select.map((column) => ({
      source: { column },
      output: column,
    })),
    ...[...windowByAlias.values()].map((fn) => ({
      source: { column: fn.as },
      output: fn.as,
    })),
  ];

  query = query.select((eb: any) =>
    projection.map((rawMapping) => {
      const mapping = requireColumnProjectMapping(rawMapping);
      const windowFn = windowByAlias.get(mapping.source.column);
      if (windowFn) {
        return buildWindowExpression(eb, windowFn, scanAlias).as(mapping.output);
      }

      const source = resolveWithBodyColumnRef(mapping.source, scanAlias);
      return eb.ref(source).as(mapping.output);
    }),
  );

  if (body.sort) {
    for (const term of body.sort.orderBy) {
      query = query.orderBy(resolveWithBodySortRef(term, scanAlias, windowByAlias), term.direction);
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

async function buildKyselySemiJoinSubquery<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  joinStep: SemiJoinStep,
  context: TContext,
): Promise<KyselyQueryBuilderLike> {
  if (joinStep.right.output.length !== 1) {
    throw new UnsupportedSingleQueryPlanError(
      "SEMI join subquery must project exactly one output column.",
    );
  }

  const strategy = resolveKyselyRelCompileStrategy(joinStep.right, entityConfigs);
  if (!strategy) {
    throw new UnsupportedSingleQueryPlanError(
      "SEMI join right-hand rel fragment is not supported for single-query pushdown.",
    );
  }

  return buildKyselyRelBuilderForStrategy(db, entityConfigs, joinStep.right, strategy, context);
}

function buildWindowExpression(
  eb: any,
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
  scanAlias: string,
): any {
  const aggregate = eb.fn.agg(fn.fn, []);

  return aggregate.over((ob: any) => {
    let over = ob;

    if (fn.partitionBy.length > 0) {
      over = over.partitionBy(
        fn.partitionBy.map((ref) => resolveWithBodyColumnRef(ref, scanAlias)),
      );
    }

    for (const term of fn.orderBy) {
      over = over.orderBy(resolveWithBodyColumnRef(term.source, scanAlias), term.direction);
    }

    return over;
  });
}

function resolveWithBodySortRef(
  term: Extract<RelNode, { kind: "sort" }>["orderBy"][number],
  scanAlias: string,
  windowByAlias: Map<string, Extract<RelNode, { kind: "window" }>["functions"][number]>,
): string {
  const refAlias = term.source.alias ?? term.source.table;
  if (!refAlias && windowByAlias.has(term.source.column)) {
    return term.source.column;
  }
  return resolveWithBodyColumnRef(term.source, scanAlias);
}

function resolveWithBodyColumnRef(
  ref: { alias?: string; table?: string; column: string },
  scanAlias: string,
): string {
  const refAlias = ref.alias ?? ref.table;
  if (refAlias && refAlias !== scanAlias) {
    throw new UnsupportedSingleQueryPlanError(
      `WITH body column "${refAlias}.${ref.column}" must reference alias "${scanAlias}".`,
    );
  }
  return `${scanAlias}.${ref.column}`;
}

function buildSelection<TContext>(plan: SingleQueryPlan<TContext>): SelectionEntry[] {
  if (!plan.pipeline.aggregate) {
    if (!plan.pipeline.project) {
      throw new UnsupportedSingleQueryPlanError(
        "Non-aggregate rel fragment requires a project node.",
      );
    }

    return plan.pipeline.project.columns.map((rawMapping) => {
      const mapping = requireColumnProjectMapping(rawMapping);
      const source = resolveQualifiedColumnRef(plan.joinPlan.aliases, {
        ...toRef(mapping.source.alias ?? mapping.source.table, mapping.source.column),
      });

      return {
        output: mapping.output,
        toExpression: (eb: any) => eb.ref(source).as(mapping.output),
      } satisfies SelectionEntry;
    });
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

  return projection.map((rawMapping) => {
    const mapping = requireColumnProjectMapping(rawMapping);
    const metric = metricByAs.get(mapping.source.column);
    if (metric) {
      return {
        output: mapping.output,
        toExpression: (eb: any) =>
          buildMetricExpression(eb, metric, plan.joinPlan.aliases).as(mapping.output),
      } satisfies SelectionEntry;
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

    return {
      output: mapping.output,
      toExpression: (eb: any) => eb.ref(source).as(mapping.output),
    } satisfies SelectionEntry;
  });
}

function buildMetricExpression<TContext>(
  eb: any,
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  aliases: Map<string, ScanBinding<TContext>>,
): any {
  if (metric.fn === "count" && !metric.column) {
    return eb.fn.countAll();
  }

  if (!metric.column) {
    throw new UnsupportedSingleQueryPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const ref = resolveQualifiedColumnRef(aliases, {
    ...toRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  const fn = (eb as { fn?: Record<string, (value: unknown) => any> }).fn;
  if (!fn) {
    throw new UnsupportedSingleQueryPlanError(
      "Kysely expression builder does not expose fn helpers.",
    );
  }

  const fnImpl = fn[metric.fn];
  if (typeof fnImpl !== "function") {
    throw new UnsupportedSingleQueryPlanError(`Unsupported aggregate function: ${metric.fn}.`);
  }

  let expression = fnImpl(eb.ref(ref));
  if (metric.distinct && expression && typeof expression.distinct === "function") {
    expression = expression.distinct();
  }

  return expression;
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
