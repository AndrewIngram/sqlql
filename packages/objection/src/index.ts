import {
  createDataEntityHandle,
  type DataEntityHandle,
  type ProviderAdapter,
  type ProviderCapabilityReport,
  type ProviderCompiledPlan,
  type ProviderFragment,
  type QueryRow,
  type RelNode,
  type ScanFilterClause,
  type TableScanRequest,
} from "sqlql";

export type KnexLikeQueryBuilder = {
  clone?: (...args: any[]) => KnexLikeQueryBuilder;
  as?: (...args: any[]) => KnexLikeQueryBuilder;
  clearSelect?: (...args: any[]) => KnexLikeQueryBuilder;
  select: (...args: any[]) => KnexLikeQueryBuilder;
  from: (...args: any[]) => KnexLikeQueryBuilder;
  innerJoin?: (...args: any[]) => KnexLikeQueryBuilder;
  leftJoin?: (...args: any[]) => KnexLikeQueryBuilder;
  rightJoin?: (...args: any[]) => KnexLikeQueryBuilder;
  fullJoin?: (...args: any[]) => KnexLikeQueryBuilder;
  with?: (...args: any[]) => KnexLikeQueryBuilder;
  union?: (...args: any[]) => KnexLikeQueryBuilder;
  unionAll?: (...args: any[]) => KnexLikeQueryBuilder;
  intersect?: (...args: any[]) => KnexLikeQueryBuilder;
  except?: (...args: any[]) => KnexLikeQueryBuilder;
  where: (...args: any[]) => KnexLikeQueryBuilder;
  whereIn: (...args: any[]) => KnexLikeQueryBuilder;
  whereNull: (...args: any[]) => KnexLikeQueryBuilder;
  whereNotNull: (...args: any[]) => KnexLikeQueryBuilder;
  groupBy: (...args: any[]) => KnexLikeQueryBuilder;
  orderBy: (...args: any[]) => KnexLikeQueryBuilder;
  limit: (...args: any[]) => KnexLikeQueryBuilder;
  offset: (...args: any[]) => KnexLikeQueryBuilder;
  count: (...args: any[]) => KnexLikeQueryBuilder;
  countDistinct: (...args: any[]) => KnexLikeQueryBuilder;
  sum: (...args: any[]) => KnexLikeQueryBuilder;
  avg: (...args: any[]) => KnexLikeQueryBuilder;
  min: (...args: any[]) => KnexLikeQueryBuilder;
  max: (...args: any[]) => KnexLikeQueryBuilder;
  rank?: (...args: any[]) => KnexLikeQueryBuilder;
  denseRank?: (...args: any[]) => KnexLikeQueryBuilder;
  rowNumber?: (...args: any[]) => KnexLikeQueryBuilder;
  execute?: (...args: any[]) => Promise<QueryRow[]>;
};

export type KnexLike = {
  table: (...args: any[]) => KnexLikeQueryBuilder;
  queryBuilder: (...args: any[]) => KnexLikeQueryBuilder;
};

export interface ObjectionProviderEntityConfig<TContext> {
  table?: string;
  /**
   * Builds the mandatory scoped root query for this entity.
   */
  base?: (context: TContext) => KnexLikeQueryBuilder;
}

export interface CreateObjectionProviderOptions<TContext> {
  name?: string;
  knex: KnexLike;
  entities?: Record<string, ObjectionProviderEntityConfig<TContext>>;
}

interface ObjectionRelCompiledPlan {
  strategy: ObjectionRelCompileStrategy;
  rel: RelNode;
}

type ObjectionRelCompileStrategy = "basic" | "set_op" | "with";

interface ResolvedEntityConfig<TContext> {
  entity: string;
  table: string;
  config: ObjectionProviderEntityConfig<TContext>;
}

class UnsupportedSingleQueryPlanError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedSingleQueryPlanError";
  }
}

interface RelPipeline {
  base: RelNode;
  project?: Extract<RelNode, { kind: "project" }>;
  aggregate?: Extract<RelNode, { kind: "aggregate" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
  filters: Extract<RelNode, { kind: "filter" }>[];
}

interface SetOpWrapper {
  setOp: Extract<RelNode, { kind: "set_op" }>;
  project?: Extract<RelNode, { kind: "project" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
}

interface WithBodyWrapper {
  cteScan: Extract<RelNode, { kind: "scan" }>;
  project?: Extract<RelNode, { kind: "project" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
  window?: Extract<RelNode, { kind: "window" }>;
  filters: Extract<RelNode, { kind: "filter" }>[];
}

interface ScanBinding<TContext> {
  alias: string;
  entity: string;
  table: string;
  scan: Extract<RelNode, { kind: "scan" }>;
  config: ObjectionProviderEntityConfig<TContext>;
}

interface RegularJoinStep<TContext> {
  joinType: Exclude<Extract<RelNode, { kind: "join" }>["joinType"], "semi">;
  right: ScanBinding<TContext>;
  leftKey: { alias: string; column: string };
  rightKey: { alias: string; column: string };
}

interface SemiJoinStep {
  joinType: "semi";
  right: RelNode;
  leftKey: { alias: string; column: string };
  rightKey: { alias?: string; column: string };
}

type JoinStep<TContext> = RegularJoinStep<TContext> | SemiJoinStep;

interface JoinPlan<TContext> {
  root: ScanBinding<TContext>;
  joins: JoinStep<TContext>[];
  aliases: Map<string, ScanBinding<TContext>>;
}

interface SingleQueryPlan<TContext> {
  pipeline: RelPipeline;
  joinPlan: JoinPlan<TContext>;
}

function toRef(alias: string | undefined, column: string): { alias?: string; column: string } {
  if (alias) {
    return { alias, column };
  }
  return { column };
}

function isKnexLikeQueryBuilder(value: unknown): value is KnexLikeQueryBuilder {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Partial<KnexLikeQueryBuilder>;
  return typeof candidate.select === "function" && typeof candidate.where === "function";
}

function resolveBaseQueryBuilder<TContext>(
  base: NonNullable<ObjectionProviderEntityConfig<TContext>["base"]>,
  context: TContext,
): KnexLikeQueryBuilder {
  const scoped = base(context);
  if (isKnexLikeQueryBuilder(scoped)) {
    return scoped;
  }
  throw new Error(
    "Objection entity base(context) must return a Knex/Objection query builder synchronously.",
  );
}

async function executeQuery(query: KnexLikeQueryBuilder): Promise<QueryRow[]> {
  if (typeof query.execute === "function") {
    return (await query.execute()) ?? [];
  }
  return (await (query as unknown as Promise<QueryRow[]>)) ?? [];
}

export function createObjectionProvider<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
): ProviderAdapter<TContext> & {
  entities: Record<string, DataEntityHandle>;
} {
  const providerName = options.name ?? "objection";
  const entityConfigs = resolveEntityConfigs(options);

  const handles: Record<string, DataEntityHandle> = Object.fromEntries(
    Object.keys(entityConfigs).map((entityName) => [
      entityName,
      createDataEntityHandle({
        entity: entityName,
        provider: providerName,
      }),
    ]),
  );

  return {
    entities: handles,
    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return !!entityConfigs[fragment.table];
        case "rel": {
          const strategy = resolveObjectionRelCompileStrategy(fragment.rel, entityConfigs);
          return strategy
            ? true
            : {
                supported: false,
                reason: hasSqlNode(fragment.rel)
                  ? "rel fragment must not contain sql nodes."
                  : "Rel fragment is not supported for single-query Objection pushdown.",
              };
        }
        default:
          return false;
      }
    },
    async compile(fragment): Promise<ProviderCompiledPlan> {
      switch (fragment.kind) {
        case "scan":
          if (!entityConfigs[fragment.table]) {
            throw new Error(`Unknown Objection entity config: ${fragment.table}`);
          }
          return {
            provider: providerName,
            kind: "scan",
            payload: fragment,
          };
        case "rel": {
          const strategy = resolveObjectionRelCompileStrategy(fragment.rel, entityConfigs);
          if (!strategy) {
            throw new Error("Unsupported relational fragment for Objection provider.");
          }
          return {
            provider: providerName,
            kind: "rel",
            payload: {
              strategy,
              rel: fragment.rel,
            } satisfies ObjectionRelCompiledPlan,
          };
        }
        default:
          throw new Error(
            `Unsupported Objection fragment kind: ${(fragment as { kind?: unknown }).kind}`,
          );
      }
    },
    async execute(plan, context): Promise<QueryRow[]> {
      switch (plan.kind) {
        case "scan": {
          const fragment = plan.payload as Extract<ProviderFragment, { kind: "scan" }>;
          return executeScan(options.knex, entityConfigs, fragment.request, context);
        }
        case "rel": {
          const compiled = plan.payload as ObjectionRelCompiledPlan;
          return executeRelSingleQuery(
            options.knex,
            entityConfigs,
            compiled.rel,
            compiled.strategy,
            context,
          );
        }
        default:
          throw new Error(`Unsupported Objection compiled plan kind: ${plan.kind}`);
      }
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      const scanRequest: TableScanRequest = {
        table: request.table,
        select: request.select,
        where: [
          ...(request.where ?? []),
          {
            op: "in",
            column: request.key,
            values: request.keys,
          } as ScanFilterClause,
        ],
      };

      return executeScan(options.knex, entityConfigs, scanRequest, context);
    },
  };
}

function resolveEntityConfigs<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
): Record<string, ResolvedEntityConfig<TContext>> {
  const out: Record<string, ResolvedEntityConfig<TContext>> = {};

  for (const [entity, config] of Object.entries(options.entities ?? {})) {
    out[entity] = {
      entity,
      table: config.table ?? entity,
      config,
    };
  }

  return out;
}

async function executeScan<TContext>(
  knex: KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const binding = entityConfigs[request.table];
  if (!binding) {
    throw new Error(`Unknown Objection entity config: ${request.table}`);
  }

  const alias = request.alias ?? binding.table;
  let query = createBaseQuery(knex, binding, context, request.alias);

  const aliases = new Map<string, ScanBinding<TContext>>([
    [
      alias,
      {
        alias,
        entity: binding.entity,
        table: binding.table,
        scan: {
          id: "scan",
          kind: "scan",
          convention: "local",
          table: binding.entity,
          ...(request.alias ? { alias: request.alias } : {}),
          select: request.select,
          ...(request.where ? { where: request.where } : {}),
          output: [],
        },
        config: binding.config,
      },
    ],
  ]);

  for (const clause of request.where ?? []) {
    query = applyWhereClause(query, clause, aliases);
  }

  for (const term of request.orderBy ?? []) {
    query = query.orderBy(`${request.alias ?? binding.table}.${term.column}`, term.direction);
  }

  if (request.limit != null) {
    query = query.limit(request.limit);
  }
  if (request.offset != null) {
    query = query.offset(request.offset);
  }

  query = query.clearSelect?.() ?? query;
  for (const column of request.select) {
    query = query.select({ [column]: `${request.alias ?? binding.table}.${column}` });
  }

  return executeQuery(query);
}

function resolveObjectionRelCompileStrategy<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): ObjectionRelCompileStrategy | null {
  if (canCompileBasicRel(node, entityConfigs)) {
    return "basic";
  }
  if (canCompileSetOpRel(node, entityConfigs)) {
    return "set_op";
  }
  if (canCompileWithRel(node, entityConfigs)) {
    return "with";
  }
  return null;
}

function canCompileBasicRel<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): boolean {
  switch (node.kind) {
    case "scan":
      return !!entityConfigs[node.table];
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return canCompileBasicRel(node.input, entityConfigs);
    case "join":
      return canCompileBasicRel(node.left, entityConfigs) && canCompileBasicRel(node.right, entityConfigs);
    case "window":
    case "set_op":
    case "with":
    case "sql":
      return false;
  }
}

function canCompileSetOpRel<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): boolean {
  const wrapper = unwrapSetOpRel(node);
  if (!wrapper) {
    return false;
  }

  if (!resolveObjectionRelCompileStrategy(wrapper.setOp.left, entityConfigs)) {
    return false;
  }
  if (!resolveObjectionRelCompileStrategy(wrapper.setOp.right, entityConfigs)) {
    return false;
  }

  const topProject = wrapper.project;
  if (topProject) {
    for (const column of topProject.columns) {
      if (column.source.alias || column.source.table) {
        return false;
      }
      if (column.source.column !== column.output) {
        return false;
      }
    }
  }

  for (const term of wrapper.sort?.orderBy ?? []) {
    if (term.source.alias || term.source.table) {
      return false;
    }
  }

  return true;
}

function canCompileWithRel<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): boolean {
  if (node.kind !== "with") {
    return false;
  }

  if (node.ctes.length === 0) {
    return false;
  }

  for (const cte of node.ctes) {
    if (!resolveObjectionRelCompileStrategy(cte.query, entityConfigs)) {
      return false;
    }
  }

  const body = unwrapWithBodyRel(node.body);
  if (!body) {
    return false;
  }
  if (!body.cteScan.table || !node.ctes.some((cte) => cte.name === body.cteScan.table)) {
    return false;
  }

  for (const fn of body.window?.functions ?? []) {
    if (fn.fn !== "dense_rank" && fn.fn !== "rank" && fn.fn !== "row_number") {
      return false;
    }
  }

  return true;
}

function hasSqlNode(node: RelNode): boolean {
  switch (node.kind) {
    case "sql":
      return true;
    case "scan":
      return false;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return hasSqlNode(node.input);
    case "join":
    case "set_op":
      return hasSqlNode(node.left) || hasSqlNode(node.right);
    case "with":
      return node.ctes.some((cte) => hasSqlNode(cte.query)) || hasSqlNode(node.body);
  }
}

async function executeRelSingleQuery<TContext>(
  knex: KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  strategy: ObjectionRelCompileStrategy,
  context: TContext,
): Promise<QueryRow[]> {
  const query = await buildObjectionRelBuilderForStrategy(
    knex,
    entityConfigs,
    rel,
    strategy,
    context,
  );
  return executeQuery(query);
}

async function buildObjectionRelBuilderForStrategy<TContext>(
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

async function buildObjectionBasicRelSingleQueryBuilder<TContext>(
  knex: KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  context: TContext,
): Promise<KnexLikeQueryBuilder> {
  const plan = await buildSingleQueryPlan(rel, entityConfigs);

  const rootSource = createJoinSource(knex, plan.joinPlan.root, context);
  let query = knex.queryBuilder().from(rootSource);

  for (const joinStep of plan.joinPlan.joins) {
    if (joinStep.joinType === "semi") {
      const leftRef = `${joinStep.leftKey.alias}.${joinStep.leftKey.column}`;
      const subquery = await buildObjectionSemiJoinSubquery(
        knex,
        entityConfigs,
        joinStep,
        context,
      );
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

    const rightSource = createJoinSource(knex, joinStep.right, context);
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
    for (const clause of filter.where) {
      query = applyWhereClause(query, clause, plan.joinPlan.aliases);
    }
  }

  query = query.clearSelect?.() ?? query;
  applySelection(query, plan);

  if (plan.pipeline.aggregate && plan.pipeline.aggregate.groupBy.length > 0) {
    query = query.groupBy(
      ...plan.pipeline.aggregate.groupBy.map((ref) =>
        resolveColumnRef(plan.joinPlan.aliases, {
          ...toRef(ref.alias ?? ref.table, ref.column),
        })),
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

async function buildObjectionSetOpRelSingleQueryBuilder<TContext>(
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
    for (const mapping of wrapper.project.columns) {
      if ((mapping.source.alias || mapping.source.table) && mapping.source.column !== mapping.output) {
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

async function buildObjectionWithRelSingleQueryBuilder<TContext>(
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
    throw new UnsupportedSingleQueryPlanError("Unsupported WITH body shape for single-query pushdown.");
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
    for (const clause of filter.where) {
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
  }> = body.project?.columns ?? [
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
    if (!mapping.source.alias && !mapping.source.table && windowAliases.has(mapping.source.column)) {
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

function applyWindowFunction(
  query: KnexLikeQueryBuilder,
  fn: Extract<RelNode, { kind: "window" }>["functions"][number],
  scanAlias: string,
): KnexLikeQueryBuilder {
  const methodName =
    fn.fn === "dense_rank"
      ? "denseRank"
      : fn.fn === "rank"
        ? "rank"
        : "rowNumber";

  const method = (query as unknown as Record<string, unknown>)[methodName];
  if (typeof method !== "function") {
    throw new UnsupportedSingleQueryPlanError(
      `Knex query builder does not support ${methodName} window functions in this dialect.`,
    );
  }

  const orderBy =
    fn.orderBy.length === 1
      ? (() => {
          const firstOrder = fn.orderBy[0];
          if (!firstOrder) {
            throw new UnsupportedSingleQueryPlanError(
              `${methodName} window function requires at least one ORDER BY column.`,
            );
          }
          return {
            column: resolveWithBodyColumnRef(firstOrder.source, scanAlias),
            order: firstOrder.direction,
          };
        })()
      : fn.orderBy.map((term) => ({
          column: resolveWithBodyColumnRef(term.source, scanAlias),
          order: term.direction,
        }));

  const partitionBy =
    fn.partitionBy.length === 0
      ? undefined
      : fn.partitionBy.length === 1
        ? (() => {
            const firstPartition = fn.partitionBy[0];
            if (!firstPartition) {
              return undefined;
            }
            return resolveWithBodyColumnRef(firstPartition, scanAlias);
          })()
        : fn.partitionBy.map((ref) => resolveWithBodyColumnRef(ref, scanAlias));

  return (method as (...args: unknown[]) => KnexLikeQueryBuilder).call(
    query,
    fn.as,
    orderBy,
    partitionBy,
  );
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

function createJoinSource<TContext>(
  knex: KnexLike,
  binding: ScanBinding<TContext>,
  context: TContext,
): unknown {
  if (!binding.config.base) {
    return { [binding.alias]: binding.table };
  }

  const base = resolveBaseQueryBuilder(binding.config.base, context);
  const cloned = base.clone?.() ?? base;
  return (cloned.as?.(binding.alias) ?? cloned) as unknown;
}

function createBaseQuery<TContext>(
  knex: KnexLike,
  binding: ResolvedEntityConfig<TContext>,
  context: TContext,
  alias?: string,
): KnexLikeQueryBuilder {
  if (!binding.config.base) {
    return knex.queryBuilder().from(alias ? { [alias]: binding.table } : binding.table);
  }

  const base = resolveBaseQueryBuilder(binding.config.base, context);
  const query = base.clone?.() ?? base;
  if (alias && query.as) {
    return knex.queryBuilder().from(query.as(alias));
  }
  return query;
}

async function buildSingleQueryPlan<TContext>(
  rel: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): Promise<SingleQueryPlan<TContext>> {
  const pipeline = extractRelPipeline(rel);
  const joinPlan = await buildJoinPlan(pipeline.base, entityConfigs);

  return {
    pipeline,
    joinPlan,
  };
}

function extractRelPipeline(node: RelNode): RelPipeline {
  let current = node;
  const filters: Extract<RelNode, { kind: "filter" }>[] = [];
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let aggregate: Extract<RelNode, { kind: "aggregate" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "filter":
        filters.push(current);
        current = current.input;
        continue;
      case "project":
        if (project) {
          throw new UnsupportedSingleQueryPlanError("Multiple project nodes are not supported.");
        }
        project = current;
        current = current.input;
        continue;
      case "aggregate":
        if (aggregate) {
          throw new UnsupportedSingleQueryPlanError("Multiple aggregate nodes are not supported.");
        }
        aggregate = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          throw new UnsupportedSingleQueryPlanError("Multiple sort nodes are not supported.");
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          throw new UnsupportedSingleQueryPlanError("Multiple limit/offset nodes are not supported.");
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "scan":
      case "join":
        return {
          base: current,
          ...(project ? { project } : {}),
          ...(aggregate ? { aggregate } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
          filters,
        };
      case "set_op":
      case "with":
      case "window":
      case "sql":
        throw new UnsupportedSingleQueryPlanError(
          `Rel node "${current.kind}" is not supported in single-query pushdown.`,
        );
    }
  }
}

function unwrapSetOpRel(node: RelNode): SetOpWrapper | null {
  let current = node;
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "project":
        if (project) {
          return null;
        }
        project = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          return null;
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          return null;
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "set_op":
        return {
          setOp: current,
          ...(project ? { project } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
        };
      default:
        return null;
    }
  }
}

function unwrapWithBodyRel(node: RelNode): WithBodyWrapper | null {
  let current = node;
  const filters: Extract<RelNode, { kind: "filter" }>[] = [];
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;
  let window: Extract<RelNode, { kind: "window" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "filter":
        filters.push(current);
        current = current.input;
        continue;
      case "project":
        if (project) {
          return null;
        }
        project = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          return null;
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          return null;
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "window":
        if (window) {
          return null;
        }
        window = current;
        current = current.input;
        continue;
      case "scan":
        return {
          cteScan: current,
          ...(project ? { project } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
          ...(window ? { window } : {}),
          filters,
        };
      default:
        return null;
    }
  }
}

async function buildJoinPlan<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): Promise<JoinPlan<TContext>> {
  if (node.kind === "scan") {
    const root = createScanBinding(node, entityConfigs);
    return {
      root,
      joins: [],
      aliases: new Map([[root.alias, root]]),
    };
  }

  if (node.kind !== "join") {
    throw new UnsupportedSingleQueryPlanError(`Expected scan/join base node, received "${node.kind}".`);
  }

  const left = await buildJoinPlan(node.left, entityConfigs);

  if (node.joinType === "semi") {
    const leftAlias = node.leftKey.alias ?? node.leftKey.table;
    const rightAlias = node.rightKey.alias ?? node.rightKey.table;
    if (!leftAlias) {
      throw new UnsupportedSingleQueryPlanError("Join keys must be alias-qualified.");
    }

    return {
      root: left.root,
      joins: [
        ...left.joins,
        {
          joinType: "semi",
          right: node.right,
          leftKey: {
            alias: leftAlias,
            column: node.leftKey.column,
          },
          rightKey: {
            ...(rightAlias ? { alias: rightAlias } : {}),
            column: node.rightKey.column,
          },
        },
      ],
      aliases: new Map(left.aliases),
    };
  }

  const right = await buildJoinPlan(node.right, entityConfigs);
  if (right.joins.length > 0) {
    throw new UnsupportedSingleQueryPlanError("Only left-deep join trees are supported.");
  }

  const rightRoot = right.root;
  if (left.aliases.has(rightRoot.alias)) {
    throw new UnsupportedSingleQueryPlanError(`Duplicate alias "${rightRoot.alias}" in join tree.`);
  }

  const leftAlias = node.leftKey.alias ?? node.leftKey.table;
  const rightAlias = node.rightKey.alias ?? node.rightKey.table;
  if (!leftAlias || !rightAlias) {
    throw new UnsupportedSingleQueryPlanError("Join keys must be alias-qualified.");
  }

  const aliases = new Map(left.aliases);
  aliases.set(rightRoot.alias, rightRoot);
  for (const [alias, binding] of right.aliases.entries()) {
    aliases.set(alias, binding);
  }

  return {
    root: left.root,
    joins: [
      ...left.joins,
      {
        joinType: node.joinType,
        right: rightRoot,
        leftKey: {
          alias: leftAlias,
          column: node.leftKey.column,
        },
        rightKey: {
          alias: rightAlias,
          column: node.rightKey.column,
        },
      },
    ],
    aliases,
  };
}

function createScanBinding<TContext>(
  scan: Extract<RelNode, { kind: "scan" }>,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): ScanBinding<TContext> {
  const binding = entityConfigs[scan.table];
  if (!binding) {
    throw new UnsupportedSingleQueryPlanError(`Missing Objection entity config for "${scan.table}".`);
  }

  return {
    alias: scan.alias ?? binding.table,
    entity: binding.entity,
    table: binding.table,
    scan,
    config: binding.config,
  };
}

function applyWhereClause<TContext>(
  query: KnexLikeQueryBuilder,
  clause: ScanFilterClause,
  aliases: Map<string, ScanBinding<TContext>>,
): KnexLikeQueryBuilder {
  const column = resolveColumnFromFilterColumn(aliases, clause.column);

  switch (clause.op) {
    case "eq":
      return query.where(column, "=", clause.value);
    case "neq":
      return query.where(column, "!=", clause.value);
    case "gt":
      return query.where(column, ">", clause.value);
    case "gte":
      return query.where(column, ">=", clause.value);
    case "lt":
      return query.where(column, "<", clause.value);
    case "lte":
      return query.where(column, "<=", clause.value);
    case "in":
      return query.whereIn(column, clause.values);
    case "is_null":
      return query.whereNull(column);
    case "is_not_null":
      return query.whereNotNull(column);
  }
}

function resolveColumnFromFilterColumn<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  column: string,
): string {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    return column;
  }

  return resolveColumnRef(aliases, { column });
}

function resolveColumnRef<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  ref: { alias?: string; column: string },
): string {
  if (ref.alias) {
    const binding = aliases.get(ref.alias);
    if (!binding) {
      throw new UnsupportedSingleQueryPlanError(`Unknown alias "${ref.alias}" in rel fragment.`);
    }
    return `${binding.alias}.${ref.column}`;
  }

  let matched: string | null = null;
  for (const binding of aliases.values()) {
    const available = new Set(binding.scan.select);
    const hasInFilter = (binding.scan.where ?? []).some((entry) => entry.column === ref.column);
    if (!available.has(ref.column) && !hasInFilter) {
      continue;
    }

    const candidate = `${binding.alias}.${ref.column}`;
    if (matched && matched !== candidate) {
      throw new UnsupportedSingleQueryPlanError(
        `Ambiguous unqualified column "${ref.column}" in rel fragment.`,
      );
    }
    matched = candidate;
  }

  if (!matched) {
    throw new UnsupportedSingleQueryPlanError(
      `Unknown unqualified column "${ref.column}" in rel fragment.`,
    );
  }

  return matched;
}

function applySelection<TContext>(
  query: KnexLikeQueryBuilder,
  plan: SingleQueryPlan<TContext>,
): void {
  if (!plan.pipeline.aggregate) {
    const project = plan.pipeline.project;
    if (!project) {
      throw new UnsupportedSingleQueryPlanError("Non-aggregate rel fragment requires a project node.");
    }

    for (const mapping of project.columns) {
      const source = resolveColumnRef(plan.joinPlan.aliases, {
        ...toRef(mapping.source.alias ?? mapping.source.table, mapping.source.column),
      });
      query.select({ [mapping.output]: source });
    }
    return;
  }

  const metricByAs = new Map(plan.pipeline.aggregate.metrics.map((metric) => [metric.as, metric]));
  const groupByByColumn = new Map(
    plan.pipeline.aggregate.groupBy.map((groupBy) => [groupBy.column, groupBy] as const),
  );

  const projection = plan.pipeline.project?.columns ??
    [
      ...plan.pipeline.aggregate.groupBy.map((groupBy) => ({
        source: { column: groupBy.column },
        output: groupBy.column,
      })),
      ...plan.pipeline.aggregate.metrics.map((metric) => ({
        source: { column: metric.as },
        output: metric.as,
      })),
    ];

  for (const mapping of projection) {
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

    const source = resolveColumnRef(plan.joinPlan.aliases, {
      ...toRef(groupBy.alias ?? groupBy.table, groupBy.column),
    });
    query.select({ [mapping.output]: source });
  }
}

function applyMetricSelection<TContext>(
  query: KnexLikeQueryBuilder,
  aliases: Map<string, ScanBinding<TContext>>,
  metric: Extract<RelNode, { kind: "aggregate" }>['metrics'][number],
  output: string,
): void {
  if (metric.fn === "count" && !metric.column) {
    query.count({ [output]: "*" });
    return;
  }

  if (!metric.column) {
    throw new UnsupportedSingleQueryPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const source = resolveColumnRef(aliases, {
    ...toRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  if (metric.fn === "count" && metric.distinct) {
    query.countDistinct({ [output]: source });
    return;
  }

  if (metric.fn === "sum" && metric.distinct) {
    throw new UnsupportedSingleQueryPlanError("Knex sum(distinct ...) is not supported in this adapter yet.");
  }

  if (metric.fn === "avg" && metric.distinct) {
    throw new UnsupportedSingleQueryPlanError("Knex avg(distinct ...) is not supported in this adapter yet.");
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
  term: Extract<RelNode, { kind: "sort" }>['orderBy'][number],
): string {
  if (term.source.alias || term.source.table) {
    return resolveColumnRef(plan.joinPlan.aliases, {
      ...toRef(term.source.alias ?? term.source.table, term.source.column),
    });
  }

  return term.source.column;
}
