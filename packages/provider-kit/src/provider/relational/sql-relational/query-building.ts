import { type RelNode } from "@tupl/foundation";

import {
  unwrapSetOpRel,
  unwrapWithBodyRel,
  type RelationalSetOpWrapper,
  type RelationalSingleQueryPlan,
  type RelationalWithBodyWrapper,
} from "../../shapes/relational-core";
import {
  createSqlRelationalCompileHelpers,
  createSqlRelationalScanBinding,
  requireSqlRelationalProjectMapping,
  type SqlRelationalCompileHelpers,
} from "./planning";
import type {
  SqlRelationalColumnSelection,
  SqlRelationalCompileStrategy,
  SqlRelationalOrderTerm,
  SqlRelationalOutputOrderTerm,
  SqlRelationalQualifiedOrderTerm,
  SqlRelationalQueryTranslationBackend,
  SqlRelationalResolvedEntity,
  SqlRelationalScanBinding,
  SqlRelationalSelection,
  SqlRelationalWindowSelection,
  SqlRelationalWithSelection,
} from "./types";
import { UnsupportedSqlRelationalPlanError } from "./types";

/**
 * SQL-relational query building owns strategy dispatch and backend-neutral traversal of shared
 * single-query, set-op, and WITH shapes. Backends provide only query primitives; this module owns
 * recursive shape handling and the rejection rules for unsupported compositions.
 */
export async function buildSqlRelationalQueryForStrategy<
  TContext,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
>(args: {
  rel: RelNode;
  strategy: SqlRelationalCompileStrategy;
  resolvedEntities: Record<string, TResolvedEntity>;
  backend: SqlRelationalQueryTranslationBackend<
    TContext,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >;
  runtime: TRuntime;
  context: TContext;
  planningHooks: {
    createScanBinding(
      scan: Extract<RelNode, { kind: "scan" }>,
      resolvedEntities: Record<string, TResolvedEntity>,
    ): TBinding;
    buildSingleQueryPlan?(
      rel: RelNode,
      resolvedEntities: Record<string, TResolvedEntity>,
    ): RelationalSingleQueryPlan<TBinding>;
    resolveRelCompileStrategy?(
      node: RelNode,
      resolvedEntities: Record<string, TResolvedEntity>,
      options?: { requireColumnProjectMappings?: boolean },
    ): SqlRelationalCompileStrategy | null;
  };
  options?: {
    requireColumnProjectMappings?: boolean;
  };
}): Promise<TQuery> {
  const { rel, strategy, resolvedEntities, backend, runtime, context, planningHooks, options } =
    args;
  const createPlanningScanBinding = (
    scan: Extract<RelNode, { kind: "scan" }>,
    bindingResolvedEntities: Record<string, TResolvedEntity>,
  ) => planningHooks.createScanBinding(scan, bindingResolvedEntities);
  const compileHelpers = createSqlRelationalCompileHelpers(
    resolvedEntities,
    createPlanningScanBinding,
    {
      ...(planningHooks.buildSingleQueryPlan
        ? {
            buildSingleQueryPlan: (currentRel, compileResolvedEntities) =>
              planningHooks.buildSingleQueryPlan!(currentRel, compileResolvedEntities),
          }
        : {}),
      ...(planningHooks.resolveRelCompileStrategy
        ? {
            resolveRelCompileStrategy: (node, compileResolvedEntities, compileOptions) =>
              planningHooks.resolveRelCompileStrategy!(
                node,
                compileResolvedEntities,
                compileOptions,
              ),
          }
        : {}),
    },
    options,
  );

  return buildSqlRelationalQueryForStrategyWithHelpers(
    rel,
    strategy,
    resolvedEntities,
    backend,
    runtime,
    context,
    options,
    compileHelpers,
  );
}

async function buildSqlRelationalQueryForStrategyWithHelpers<
  TContext,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
>(
  rel: RelNode,
  strategy: SqlRelationalCompileStrategy,
  resolvedEntities: Record<string, TResolvedEntity>,
  backend: SqlRelationalQueryTranslationBackend<
    TContext,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >,
  runtime: TRuntime,
  context: TContext,
  options: { requireColumnProjectMappings?: boolean } | undefined,
  compileHelpers: SqlRelationalCompileHelpers<TResolvedEntity, TBinding>,
): Promise<TQuery> {
  switch (strategy) {
    case "basic":
      return buildBasicSqlRelationalQuery(
        rel,
        resolvedEntities,
        backend,
        runtime,
        context,
        options,
        compileHelpers,
      );
    case "set_op":
      return buildSetOpSqlRelationalQuery(
        rel,
        resolvedEntities,
        backend,
        runtime,
        context,
        options,
        compileHelpers,
      );
    case "with":
      return buildWithSqlRelationalQuery(
        rel,
        resolvedEntities,
        backend,
        runtime,
        context,
        options,
        compileHelpers,
      );
  }
}

async function buildBasicSqlRelationalQuery<
  TContext,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
>(
  rel: RelNode,
  resolvedEntities: Record<string, TResolvedEntity>,
  backend: SqlRelationalQueryTranslationBackend<
    TContext,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >,
  runtime: TRuntime,
  context: TContext,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
  compileHelpers?: SqlRelationalCompileHelpers<TResolvedEntity, TBinding>,
): Promise<TQuery> {
  const defaultScanBinding = (
    scan: Extract<RelNode, { kind: "scan" }>,
    currentResolvedEntities: Record<string, TResolvedEntity>,
  ) => createSqlRelationalScanBinding(scan, currentResolvedEntities) as TBinding;
  const helpers =
    compileHelpers ??
    createSqlRelationalCompileHelpers(resolvedEntities, defaultScanBinding, {}, options);
  const plan = helpers.buildSingleQueryPlan(rel);
  const selection = buildSqlRelationalSelection(plan);
  let query: TQuery = (await backend.createRootQuery({
    runtime,
    root: plan.joinPlan.root,
    context,
    plan,
    selection,
  })) as TQuery;

  for (const join of plan.joinPlan.joins) {
    if (join.joinType === "semi") {
      if (join.right.output.length !== 1) {
        throw new UnsupportedSqlRelationalPlanError(
          "SEMI join subquery must project exactly one output column.",
        );
      }

      const strategy = helpers.resolveStrategy(join.right);
      if (!strategy) {
        throw new UnsupportedSqlRelationalPlanError(
          "SEMI join right-hand rel fragment is not supported for single-query pushdown.",
        );
      }

      const subquery = await buildSqlRelationalQueryForStrategyWithHelpers(
        join.right,
        strategy,
        resolvedEntities,
        backend,
        runtime,
        context,
        options,
        helpers,
      );
      query = (await backend.applySemiJoin({
        query,
        leftKey: join.leftKey,
        subquery,
        aliases: plan.joinPlan.aliases,
        context,
        runtime,
      })) as TQuery;
      continue;
    }

    query = (await backend.applyRegularJoin({
      query,
      join,
      aliases: plan.joinPlan.aliases,
      context,
      runtime,
    })) as TQuery;
  }

  for (const binding of plan.joinPlan.aliases.values()) {
    for (const clause of binding.scan.where ?? []) {
      query = backend.applyWhereClause({
        query,
        clause,
        plan,
        aliases: plan.joinPlan.aliases,
        context,
        runtime,
      });
    }
  }

  for (const filter of plan.pipeline.filters) {
    for (const clause of filter.where ?? []) {
      query = backend.applyWhereClause({
        query,
        clause,
        plan,
        aliases: plan.joinPlan.aliases,
        context,
        runtime,
      });
    }
  }

  query = (await backend.applySelection({
    query,
    plan,
    selection,
    aliases: plan.joinPlan.aliases,
    context,
    runtime,
  })) as TQuery;

  if (plan.pipeline.aggregate && plan.pipeline.aggregate.groupBy.length > 0) {
    query = (await backend.applyGroupBy({
      query,
      groupBy: plan.pipeline.aggregate.groupBy,
      aliases: plan.joinPlan.aliases,
      context,
      runtime,
    })) as TQuery;
  }

  if (plan.pipeline.sort) {
    query = (await backend.applyOrderBy({
      query,
      plan,
      selection,
      orderBy: plan.pipeline.sort.orderBy.map((term) => resolvePlanOrderTerm(plan, term)),
      aliases: plan.joinPlan.aliases,
      context,
      runtime,
    })) as TQuery;
  }

  if (plan.pipeline.limitOffset?.limit != null) {
    query = (await backend.applyLimit({
      query,
      limit: plan.pipeline.limitOffset.limit,
      context,
      runtime,
    })) as TQuery;
  }

  if (plan.pipeline.limitOffset?.offset != null) {
    query = (await backend.applyOffset({
      query,
      offset: plan.pipeline.limitOffset.offset,
      context,
      runtime,
    })) as TQuery;
  }

  return query;
}

async function buildSetOpSqlRelationalQuery<
  TContext,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
>(
  rel: RelNode,
  resolvedEntities: Record<string, TResolvedEntity>,
  backend: SqlRelationalQueryTranslationBackend<
    TContext,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >,
  runtime: TRuntime,
  context: TContext,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
  compileHelpers?: SqlRelationalCompileHelpers<TResolvedEntity, TBinding>,
): Promise<TQuery> {
  const defaultScanBinding = (
    scan: Extract<RelNode, { kind: "scan" }>,
    currentResolvedEntities: Record<string, TResolvedEntity>,
  ) => createSqlRelationalScanBinding(scan, currentResolvedEntities) as TBinding;
  const helpers =
    compileHelpers ??
    createSqlRelationalCompileHelpers(resolvedEntities, defaultScanBinding, {}, options);
  const wrapper = unwrapSetOpRel(rel);
  if (!wrapper) {
    throw new UnsupportedSqlRelationalPlanError("Expected set-op relational shape.");
  }

  const leftStrategy = helpers.resolveStrategy(wrapper.setOp.left);
  const rightStrategy = helpers.resolveStrategy(wrapper.setOp.right);
  if (!leftStrategy || !rightStrategy) {
    throw new UnsupportedSqlRelationalPlanError(
      "Set-op branches are not supported for single-query pushdown.",
    );
  }

  const left = await buildSqlRelationalQueryForStrategyWithHelpers(
    wrapper.setOp.left,
    leftStrategy,
    resolvedEntities,
    backend,
    runtime,
    context,
    options,
    helpers,
  );
  const right = await buildSqlRelationalQueryForStrategyWithHelpers(
    wrapper.setOp.right,
    rightStrategy,
    resolvedEntities,
    backend,
    runtime,
    context,
    options,
    helpers,
  );

  validateSetOpProjection(wrapper);

  let query: TQuery = (await backend.applySetOp({
    left,
    right,
    wrapper,
    context,
    runtime,
  })) as TQuery;

  if (wrapper.sort) {
    query = (await backend.applyOrderBy({
      query,
      plan: wrapper,
      orderBy: wrapper.sort.orderBy.map((term) => {
        if (term.source.alias || term.source.table) {
          throw new UnsupportedSqlRelationalPlanError(
            "Set-op ORDER BY columns must be unqualified output columns.",
          );
        }
        return {
          kind: "output",
          column: term.source.column,
          direction: term.direction,
        } satisfies SqlRelationalOutputOrderTerm;
      }),
      aliases: new Map<string, TBinding>(),
      context,
      runtime,
    })) as TQuery;
  }

  if (wrapper.limitOffset?.limit != null) {
    query = (await backend.applyLimit({
      query,
      limit: wrapper.limitOffset.limit,
      context,
      runtime,
    })) as TQuery;
  }

  if (wrapper.limitOffset?.offset != null) {
    query = (await backend.applyOffset({
      query,
      offset: wrapper.limitOffset.offset,
      context,
      runtime,
    })) as TQuery;
  }

  return query;
}

async function buildWithSqlRelationalQuery<
  TContext,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
>(
  rel: RelNode,
  resolvedEntities: Record<string, TResolvedEntity>,
  backend: SqlRelationalQueryTranslationBackend<
    TContext,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >,
  runtime: TRuntime,
  context: TContext,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
  compileHelpers?: SqlRelationalCompileHelpers<TResolvedEntity, TBinding>,
): Promise<TQuery> {
  const defaultScanBinding = (
    scan: Extract<RelNode, { kind: "scan" }>,
    currentResolvedEntities: Record<string, TResolvedEntity>,
  ) => createSqlRelationalScanBinding(scan, currentResolvedEntities) as TBinding;
  const helpers =
    compileHelpers ??
    createSqlRelationalCompileHelpers(resolvedEntities, defaultScanBinding, {}, options);
  if (rel.kind !== "with") {
    throw new UnsupportedSqlRelationalPlanError(`Expected with node, received "${rel.kind}".`);
  }

  const ctes: Array<{ name: string; query: TQuery }> = [];
  for (const cte of rel.ctes) {
    const strategy = helpers.resolveStrategy(cte.query);
    if (!strategy) {
      throw new UnsupportedSqlRelationalPlanError(
        `CTE "${cte.name}" is not supported for single-query pushdown.`,
      );
    }

    ctes.push({
      name: cte.name,
      query: await buildSqlRelationalQueryForStrategyWithHelpers(
        cte.query,
        strategy,
        resolvedEntities,
        backend,
        runtime,
        context,
        options,
        helpers,
      ),
    });
  }

  const body = unwrapWithBodyRel(rel.body);
  if (!body) {
    throw new UnsupportedSqlRelationalPlanError(
      "Unsupported WITH body shape for single-query pushdown.",
    );
  }

  let query: TQuery = (await backend.buildWithQuery({
    body,
    ctes,
    projection: buildWithSelection(body),
    orderBy: buildWithOrder(body),
    context,
    runtime,
  })) as TQuery;

  if (body.limitOffset?.limit != null) {
    query = (await backend.applyLimit({
      query,
      limit: body.limitOffset.limit,
      context,
      runtime,
    })) as TQuery;
  }

  if (body.limitOffset?.offset != null) {
    query = (await backend.applyOffset({
      query,
      offset: body.limitOffset.offset,
      context,
      runtime,
    })) as TQuery;
  }

  return query;
}

function buildSqlRelationalSelection<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
>(plan: RelationalSingleQueryPlan<TBinding>): SqlRelationalSelection[] {
  if (!plan.pipeline.aggregate) {
    const project = plan.pipeline.project;
    if (!project) {
      return [...plan.joinPlan.aliases.values()].flatMap((binding) =>
        ("outputColumns" in binding &&
        Array.isArray((binding as { outputColumns?: unknown }).outputColumns)
          ? (binding as { outputColumns: string[] }).outputColumns
          : binding.scan.select
        ).map(
          (column) =>
            ({
              kind: "column",
              output: `${binding.alias}.${column}`,
              source: {
                alias: binding.alias,
                column,
              },
            }) satisfies SqlRelationalColumnSelection,
        ),
      );
    }

    return project.columns.map((mapping) => {
      if ("source" in mapping) {
        return {
          kind: "column",
          output: mapping.output,
          source: mapping.source,
        };
      }

      return {
        kind: "expr",
        output: mapping.output,
        expr: mapping.expr,
      };
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
    const mapping = requireSqlRelationalProjectMapping(rawMapping);
    const metric = metricByAs.get(mapping.source.column);
    if (metric) {
      return {
        kind: "metric",
        output: mapping.output,
        metric,
      };
    }

    const groupBy = groupByByColumn.get(mapping.source.column);
    if (!groupBy) {
      throw new UnsupportedSqlRelationalPlanError(
        `Unknown aggregate projection source "${mapping.source.column}".`,
      );
    }

    return {
      kind: "column",
      output: mapping.output,
      source: {
        ...(groupBy.alias ? { alias: groupBy.alias } : {}),
        ...(groupBy.table ? { table: groupBy.table } : {}),
        column: groupBy.column,
      },
    };
  });
}

function resolvePlanOrderTerm<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
>(
  plan: RelationalSingleQueryPlan<TBinding>,
  term: Extract<RelNode, { kind: "sort" }>["orderBy"][number],
): SqlRelationalOrderTerm {
  if (term.source.alias || term.source.table) {
    return {
      kind: "qualified",
      source: term.source,
      direction: term.direction,
    };
  }

  if (plan.pipeline.aggregate) {
    const groupBy = plan.pipeline.aggregate.groupBy.find((entry, index) => {
      const outputName = plan.pipeline.aggregate!.output[index]?.name ?? entry.column;
      return outputName === term.source.column || entry.column === term.source.column;
    });
    if (groupBy) {
      return {
        kind: "qualified",
        source: {
          ...(groupBy.alias ? { alias: groupBy.alias } : {}),
          ...(groupBy.table ? { table: groupBy.table } : {}),
          column: groupBy.column,
        },
        direction: term.direction,
      };
    }
  }

  return {
    kind: "output",
    column: term.source.column,
    direction: term.direction,
  };
}

function validateSetOpProjection(wrapper: RelationalSetOpWrapper): void {
  if (!wrapper.project) {
    return;
  }

  for (const rawMapping of wrapper.project.columns) {
    const mapping = requireSqlRelationalProjectMapping(rawMapping);
    if (
      (mapping.source.alias || mapping.source.table) &&
      mapping.source.column !== mapping.output
    ) {
      throw new UnsupportedSqlRelationalPlanError(
        "Set-op projections with qualified or renamed columns are not supported in single-query pushdown.",
      );
    }
  }
}

function buildWithSelection(body: RelationalWithBodyWrapper): SqlRelationalWithSelection[] {
  const scanAlias = body.cteRef.alias ?? body.cteRef.name;
  const windowByAlias = new Map((body.window?.functions ?? []).map((fn) => [fn.as, fn] as const));
  const projection = body.project?.columns ?? [
    ...body.cteRef.select.map((column: string) => ({
      source: { column },
      output: column,
    })),
    ...[...windowByAlias.values()].map((fn) => ({
      source: { column: fn.as },
      output: fn.as,
    })),
  ];

  return projection.map((rawMapping) => {
    const mapping = requireSqlRelationalProjectMapping(rawMapping);
    if (!mapping.source.alias && !mapping.source.table) {
      const window = windowByAlias.get(mapping.source.column);
      if (window) {
        return {
          kind: "window",
          output: mapping.output,
          window,
        } satisfies SqlRelationalWindowSelection;
      }
    }

    return {
      kind: "column",
      output: mapping.output,
      source: normalizeWithBodySource(mapping.source, scanAlias),
    };
  });
}

function buildWithOrder(body: RelationalWithBodyWrapper): SqlRelationalOrderTerm[] {
  const scanAlias = body.cteRef.alias ?? body.cteRef.name;
  const windowAliases = new Set((body.window?.functions ?? []).map((fn) => fn.as));

  return (body.sort?.orderBy ?? []).map((term) => {
    if (!term.source.alias && !term.source.table && windowAliases.has(term.source.column)) {
      return {
        kind: "output",
        column: term.source.column,
        direction: term.direction,
      } satisfies SqlRelationalOutputOrderTerm;
    }

    return {
      kind: "qualified",
      source: normalizeWithBodySource(term.source, scanAlias),
      direction: term.direction,
    } satisfies SqlRelationalQualifiedOrderTerm;
  });
}

function normalizeWithBodySource(
  source: { alias?: string; table?: string; column: string },
  scanAlias: string,
): { alias: string; column: string } {
  const refAlias = source.alias ?? source.table;
  if (refAlias && refAlias !== scanAlias) {
    throw new UnsupportedSqlRelationalPlanError(
      `WITH body column "${refAlias}.${source.column}" must reference alias "${scanAlias}".`,
    );
  }

  return {
    alias: scanAlias,
    column: source.column,
  };
}
