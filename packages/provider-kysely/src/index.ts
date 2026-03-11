import type { Selectable } from "kysely";
import {
  AdapterResult,
  bindAdapterEntities,
  collectCapabilityAtomsForFragment,
  createDataEntityHandle,
  inferRouteFamilyForFragment,
  normalizeDataEntityShape,
  type DataEntityShape,
  type DataEntityHandle,
  type DataEntityReadMetadataMap,
  type InferDataEntityShapeMetadata,
  type FragmentProviderAdapter,
  type LookupProviderAdapter,
  type MaybePromise,
  type ProviderCapabilityAtom,
  type ProviderCapabilityReport,
  type ProviderFragment,
  type QueryRow,
  type ProviderRuntimeBinding,
  type ScanFilterClause,
  type TableScanRequest,
} from "@tupl/provider-kit";
import { isRelProjectColumnMapping, stringifyUnknownValue, type RelNode } from "@tupl/foundation";
import {
  UnsupportedRelationalPlanError,
  buildSingleQueryPlan as buildRelationalSingleQueryPlan,
  canCompileBasicRel,
  canCompileSetOpRel,
  canCompileWithRel,
  hasSqlNode,
  isSupportedRelationalPlan,
  resolveColumnFromFilterColumn as resolveRelationalColumnFromFilterColumn,
  resolveColumnRef as resolveRelationalColumnRef,
  resolveRelationalStrategy,
  unwrapSetOpRel,
  unwrapWithBodyRel,
  type RelationalScanBindingBase,
  type RelationalSemiJoinStep,
  type RelationalSingleQueryPlan,
} from "@tupl/provider-kit/shapes";

export type KyselyQueryBuilderLike = {
  select: (...args: any[]) => KyselyQueryBuilderLike;
  innerJoin?: (...args: any[]) => KyselyQueryBuilderLike;
  leftJoin?: (...args: any[]) => KyselyQueryBuilderLike;
  rightJoin?: (...args: any[]) => KyselyQueryBuilderLike;
  fullJoin?: (...args: any[]) => KyselyQueryBuilderLike;
  union?: (...args: any[]) => KyselyQueryBuilderLike;
  unionAll?: (...args: any[]) => KyselyQueryBuilderLike;
  intersect?: (...args: any[]) => KyselyQueryBuilderLike;
  except?: (...args: any[]) => KyselyQueryBuilderLike;
  where: (...args: any[]) => KyselyQueryBuilderLike;
  groupBy?: (...args: any[]) => KyselyQueryBuilderLike;
  orderBy: (...args: any[]) => KyselyQueryBuilderLike;
  limit: (...args: any[]) => KyselyQueryBuilderLike;
  offset: (...args: any[]) => KyselyQueryBuilderLike;
  execute: (...args: any[]) => Promise<QueryRow[]>;
};

export type KyselyDatabaseLike = {
  selectFrom: (...args: any[]) => KyselyQueryBuilderLike;
  with?: (name: string, expression: (db: KyselyDatabaseLike) => unknown) => KyselyDatabaseLike;
};

export interface KyselyProviderEntityConfig<
  TContext,
  TRow extends Record<string, unknown> = Record<string, unknown>,
  TColumns extends string = Extract<keyof TRow, string>,
> {
  table?: string;
  shape?: DataEntityShape<TColumns>;
  /**
   * Applies mandatory scoped constraints to a query rooted at this entity alias.
   */
  base?: (args: {
    db: KyselyDatabaseLike;
    query: KyselyQueryBuilderLike;
    context: TContext;
    entity: string;
    alias: string;
  }) => MaybePromise<KyselyQueryBuilderLike>;
}

export interface CreateKyselyProviderOptions<
  TContext,
  TEntities extends Record<string, KyselyProviderEntityConfig<TContext, any, string>> = Record<
    string,
    KyselyProviderEntityConfig<TContext, any, string>
  >,
> {
  name?: string;
  db: ProviderRuntimeBinding<TContext, unknown>;
  entities?: TEntities;
}

async function resolveKyselyDb<TContext>(
  options: CreateKyselyProviderOptions<TContext>,
  context: TContext,
): Promise<KyselyDatabaseLike> {
  const db = typeof options.db === "function" ? await options.db(context) : options.db;
  const candidate = db as Partial<KyselyDatabaseLike> | null | undefined;
  if (!candidate || typeof candidate.selectFrom !== "function") {
    throw new Error(
      "Kysely provider runtime binding did not resolve to a valid database instance. Check your context and db callback.",
    );
  }
  return candidate as KyselyDatabaseLike;
}

function requireColumnProjectMapping(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
): { source: { alias?: string; table?: string; column: string }; output: string } {
  if (!isRelProjectColumnMapping(mapping)) {
    throw new UnsupportedSingleQueryPlanError(
      "Computed projections are not supported in Kysely single-query pushdown.",
    );
  }
  return mapping;
}

interface KyselyRelCompiledPlan {
  strategy: KyselyRelCompileStrategy;
  rel: RelNode;
}

type KyselyRelCompileStrategy = "basic" | "set_op" | "with";

interface ResolvedEntityConfig<TContext> {
  entity: string;
  table: string;
  config: KyselyProviderEntityConfig<TContext>;
}

class UnsupportedSingleQueryPlanError extends UnsupportedRelationalPlanError {}

interface ScanBinding<TContext> extends RelationalScanBindingBase {
  alias: string;
  entity: string;
  table: string;
  scan: Extract<RelNode, { kind: "scan" }>;
  config: KyselyProviderEntityConfig<TContext>;
}

type SemiJoinStep = RelationalSemiJoinStep;
type SingleQueryPlan<TContext> = RelationalSingleQueryPlan<ScanBinding<TContext>>;

interface BaseBinding<TContext> {
  entity: string;
  config: KyselyProviderEntityConfig<TContext>;
}

type InferKyselyEntityRow<
  TDatabase,
  TEntityName extends string,
  TConfig,
> = TEntityName extends keyof TDatabase
  ? Selectable<Extract<TDatabase[TEntityName], Record<string, unknown>>>
  : TConfig extends KyselyProviderEntityConfig<any, infer TRow, any>
    ? TRow
    : Record<string, unknown>;

type InferKyselyEntityColumns<TDatabase, TEntityName extends string, TConfig> = TConfig extends {
  shape: infer TShape;
}
  ? Extract<keyof Extract<TShape, DataEntityShape<string>>, string>
  : Extract<keyof InferKyselyEntityRow<TDatabase, TEntityName, TConfig>, string>;

type NormalizeKyselyEntityRow<
  TDatabase,
  TEntityName extends string,
  TConfig,
> = InferKyselyEntityRow<TDatabase, TEntityName, TConfig> &
  Partial<Record<InferKyselyEntityColumns<TDatabase, TEntityName, TConfig>, unknown>>;

type InferKyselyEntityColumnMetadata<
  TDatabase,
  TEntityName extends string,
  TConfig,
> = TConfig extends { shape: infer TShape }
  ? InferDataEntityShapeMetadata<
      InferKyselyEntityColumns<TDatabase, TEntityName, TConfig>,
      Extract<TShape, DataEntityShape<InferKyselyEntityColumns<TDatabase, TEntityName, TConfig>>>
    >
  : DataEntityReadMetadataMap<
      InferKyselyEntityColumns<TDatabase, TEntityName, TConfig>,
      NormalizeKyselyEntityRow<TDatabase, TEntityName, TConfig>
    >;

function toRef(alias: string | undefined, column: string): { alias?: string; column: string } {
  if (alias) {
    return { alias, column };
  }
  return { column };
}

export function createKyselyProvider<
  TContext,
  TDatabase extends Record<string, Record<string, unknown>> = Record<
    string,
    Record<string, unknown>
  >,
  TEntities extends Record<string, KyselyProviderEntityConfig<TContext, any, string>> = Record<
    string,
    KyselyProviderEntityConfig<TContext, any, string>
  >,
>(
  options: CreateKyselyProviderOptions<TContext, TEntities>,
): FragmentProviderAdapter<TContext> &
  LookupProviderAdapter<TContext> & {
    entities: {
      [K in keyof TEntities]: DataEntityHandle<
        InferKyselyEntityColumns<TDatabase, Extract<K, string>, TEntities[K]>,
        NormalizeKyselyEntityRow<TDatabase, Extract<K, string>, TEntities[K]>,
        InferKyselyEntityColumnMetadata<TDatabase, Extract<K, string>, TEntities[K]>
      >;
    };
  } {
  const declaredAtoms: readonly ProviderCapabilityAtom[] = [
    "scan.project",
    "scan.filter.basic",
    "scan.filter.set_membership",
    "scan.sort",
    "scan.limit_offset",
    "lookup.bulk",
    "aggregate.group_by",
    "join.inner",
    "join.left",
    "join.right_full",
    "set_op.union_all",
    "set_op.union_distinct",
    "set_op.intersect",
    "set_op.except",
    "cte.non_recursive",
    "window.rank_basic",
  ];

  const providerName = options.name ?? "kysely";
  const entityConfigs = resolveEntityConfigs(options);
  const entityOptions = (options.entities ?? {}) as TEntities;

  const handles = {} as {
    [K in keyof TEntities]: DataEntityHandle<
      InferKyselyEntityColumns<TDatabase, Extract<K, string>, TEntities[K]>,
      NormalizeKyselyEntityRow<TDatabase, Extract<K, string>, TEntities[K]>,
      InferKyselyEntityColumnMetadata<TDatabase, Extract<K, string>, TEntities[K]>
    >;
  };
  const adapter = {
    name: providerName,
    entities: handles,
    routeFamilies: ["scan", "lookup", "aggregate", "rel-core", "rel-advanced"] as const,
    capabilityAtoms: [...declaredAtoms],
    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return !!entityConfigs[fragment.table];
        case "rel": {
          const strategy = resolveKyselyRelCompileStrategy(fragment.rel, entityConfigs);
          const requiredAtoms = collectCapabilityAtomsForFragment(fragment);
          const missingAtoms = requiredAtoms.filter((atom) => !declaredAtoms.includes(atom));
          return strategy
            ? true
            : {
                supported: false,
                routeFamily: inferRouteFamilyForFragment(fragment),
                requiredAtoms,
                missingAtoms,
                reason: hasSqlNode(fragment.rel)
                  ? "rel fragment must not contain sql nodes."
                  : "Rel fragment is not supported for single-query Kysely pushdown.",
              };
        }
        default:
          return false;
      }
    },
    async compile(fragment) {
      switch (fragment.kind) {
        case "scan":
          if (!entityConfigs[fragment.table]) {
            return AdapterResult.err(new Error(`Unknown Kysely entity config: ${fragment.table}`));
          }
          return AdapterResult.ok({
            provider: providerName,
            kind: "scan",
            payload: fragment,
          });
        case "rel": {
          const strategy = resolveKyselyRelCompileStrategy(fragment.rel, entityConfigs);
          if (!strategy) {
            return AdapterResult.err(
              new Error("Unsupported relational fragment for Kysely provider."),
            );
          }
          return AdapterResult.ok({
            provider: providerName,
            kind: "rel",
            payload: {
              strategy,
              rel: fragment.rel,
            } satisfies KyselyRelCompiledPlan,
          });
        }
        default: {
          const fragmentKind = stringifyUnknownValue((fragment as { kind?: unknown }).kind);
          return AdapterResult.err(new Error(`Unsupported Kysely fragment kind: ${fragmentKind}`));
        }
      }
    },
    async execute(plan, context) {
      const db = await resolveKyselyDb(options, context);
      switch (plan.kind) {
        case "scan": {
          const fragment = plan.payload as Extract<ProviderFragment, { kind: "scan" }>;
          return AdapterResult.tryPromise({
            try: () => executeScan(db, entityConfigs, fragment.request, context),
            catch: (error) => (error instanceof Error ? error : new Error(String(error))),
          });
        }
        case "rel": {
          const compiled = plan.payload as KyselyRelCompiledPlan;
          return AdapterResult.tryPromise({
            try: () =>
              executeRelSingleQuery(db, entityConfigs, compiled.rel, compiled.strategy, context),
            catch: (error) => (error instanceof Error ? error : new Error(String(error))),
          });
        }
        default:
          return AdapterResult.err(
            new Error(`Unsupported Kysely compiled plan kind: ${plan.kind}`),
          );
      }
    },
    async lookupMany(request, context) {
      const db = await resolveKyselyDb(options, context);
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

      return AdapterResult.tryPromise({
        try: () => executeScan(db, entityConfigs, scanRequest, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
  } satisfies FragmentProviderAdapter<TContext> &
    LookupProviderAdapter<TContext> & {
      entities: {
        [K in keyof TEntities]: DataEntityHandle<
          InferKyselyEntityColumns<TDatabase, Extract<K, string>, TEntities[K]>,
          NormalizeKyselyEntityRow<TDatabase, Extract<K, string>, TEntities[K]>,
          InferKyselyEntityColumnMetadata<TDatabase, Extract<K, string>, TEntities[K]>
        >;
      };
    };
  for (const entityName of Object.keys(entityConfigs) as Array<Extract<keyof TEntities, string>>) {
    const config = entityOptions[entityName];
    handles[entityName] = createDataEntityHandle({
      entity: entityName,
      provider: providerName,
      adapter,
      ...(config?.shape
        ? {
            columns: normalizeDataEntityShape(
              config.shape as DataEntityShape<
                InferKyselyEntityColumns<TDatabase, typeof entityName, TEntities[typeof entityName]>
              >,
            ),
          }
        : {}),
    }) as never;
  }

  return bindAdapterEntities(adapter);
}

function resolveEntityConfigs<TContext>(
  options: CreateKyselyProviderOptions<TContext>,
): Record<string, ResolvedEntityConfig<TContext>> {
  const raw = options.entities ?? {};
  const out: Record<string, ResolvedEntityConfig<TContext>> = {};

  for (const [entity, config] of Object.entries(raw)) {
    out[entity] = {
      entity,
      table: config.table ?? entity,
      config,
    };
  }

  return out;
}

async function executeScan<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const binding = entityConfigs[request.table];
  if (!binding) {
    throw new Error(`Unknown Kysely entity config: ${request.table}`);
  }

  const alias = request.alias ?? binding.table;
  const from = `${binding.table} as ${alias}`;

  let query = db.selectFrom(from);
  query = await applyBase(query, db, binding, context, alias);

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
    query = query.orderBy(`${alias}.${term.column}`, term.direction);
  }

  if (request.limit != null) {
    query = query.limit(request.limit);
  }
  if (request.offset != null) {
    query = query.offset(request.offset);
  }

  query = query.select((eb: any) =>
    request.select.map((column) => eb.ref(`${alias}.${column}`).as(column)),
  );

  return query.execute();
}

function resolveKyselyRelCompileStrategy<TContext>(
  node: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): KyselyRelCompileStrategy | null {
  return resolveRelationalStrategy(node, {
    basicStrategy: "basic",
    setOpStrategy: "set_op",
    withStrategy: "with",
    canCompileBasic: (current) =>
      canCompileBasicRel(current, (table) => !!entityConfigs[table], {
        requireColumnProjectMappings: true,
      }),
    validateBasic: (current) =>
      isSupportedRelationalPlan(() => {
        buildRelationalSingleQueryPlan(current, (scan) => createScanBinding(scan, entityConfigs));
      }),
    canCompileSetOp: (current) =>
      canCompileSetOpRel(
        current,
        (branch) => resolveKyselyRelCompileStrategy(branch, entityConfigs),
        requireColumnProjectMapping,
      ),
    canCompileWith: (current) =>
      canCompileWithRel(current, (branch) =>
        resolveKyselyRelCompileStrategy(branch, entityConfigs),
      ),
  });
}

async function executeRelSingleQuery<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  rel: RelNode,
  strategy: KyselyRelCompileStrategy,
  context: TContext,
): Promise<QueryRow[]> {
  const query = await buildKyselyRelBuilderForStrategy(db, entityConfigs, rel, strategy, context);
  return query.execute();
}

async function buildKyselyRelBuilderForStrategy<TContext>(
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

async function buildKyselyBasicRelSingleQueryBuilder<TContext>(
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
          resolveColumnRef(plan.joinPlan.aliases, {
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

async function buildKyselySetOpRelSingleQueryBuilder<TContext>(
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

async function buildKyselyWithRelSingleQueryBuilder<TContext>(
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

function buildSingleQueryPlan<TContext>(
  rel: RelNode,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): SingleQueryPlan<TContext> {
  return buildRelationalSingleQueryPlan(rel, (scan) => createScanBinding(scan, entityConfigs));
}

function createScanBinding<TContext>(
  scan: Extract<RelNode, { kind: "scan" }>,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
): ScanBinding<TContext> {
  const binding = entityConfigs[scan.table];
  if (!binding) {
    throw new UnsupportedSingleQueryPlanError(`Missing Kysely entity config for "${scan.table}".`);
  }

  return {
    alias: scan.alias ?? binding.table,
    entity: binding.entity,
    table: binding.table,
    scan,
    config: binding.config,
  };
}

async function applyBase<TContext>(
  query: KyselyQueryBuilderLike,
  db: KyselyDatabaseLike,
  binding: BaseBinding<TContext>,
  context: TContext,
  alias: string,
): Promise<KyselyQueryBuilderLike> {
  if (!binding.config.base) {
    return query;
  }

  return binding.config.base({
    db,
    query,
    context,
    entity: binding.entity,
    alias,
  });
}

function applyWhereClause<TContext>(
  query: KyselyQueryBuilderLike,
  clause: ScanFilterClause,
  aliases: Map<string, ScanBinding<TContext>>,
): KyselyQueryBuilderLike {
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
      return query.where(column, "in", clause.values.length > 0 ? clause.values : [null]);
    case "not_in":
      return query.where(column, "not in", clause.values.length > 0 ? clause.values : [null]);
    case "like":
      return query.where(column, "like", clause.value);
    case "not_like":
      return query.where(column, "not like", clause.value);
    case "is_distinct_from":
      return query.where(column, "is distinct from", clause.value);
    case "is_not_distinct_from":
      return query.where(column, "is not distinct from", clause.value);
    case "is_null":
      return query.where(column, "is", null);
    case "is_not_null":
      return query.where(column, "is not", null);
  }
}

function resolveColumnFromFilterColumn<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  column: string,
): string {
  return resolveRelationalColumnFromFilterColumn(aliases, column);
}

function resolveColumnRef<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  ref: { alias?: string; column: string },
): string {
  return resolveRelationalColumnRef(aliases, ref);
}

type SelectionEntry = {
  output: string;
  toExpression: (eb: any) => unknown;
};

function buildSelection<TContext>(plan: SingleQueryPlan<TContext>): SelectionEntry[] {
  if (!plan.pipeline.aggregate) {
    if (!plan.pipeline.project) {
      throw new UnsupportedSingleQueryPlanError(
        "Non-aggregate rel fragment requires a project node.",
      );
    }

    return plan.pipeline.project.columns.map((rawMapping) => {
      const mapping = requireColumnProjectMapping(rawMapping);
      const source = resolveColumnRef(plan.joinPlan.aliases, {
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

    const source = resolveColumnRef(plan.joinPlan.aliases, {
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

  const ref = resolveColumnRef(aliases, {
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
    return resolveColumnRef(plan.joinPlan.aliases, {
      ...toRef(term.source.alias ?? term.source.table, term.source.column),
    });
  }

  if (plan.pipeline.aggregate) {
    const groupBy = plan.pipeline.aggregate.groupBy.find((entry, index) => {
      const outputName = plan.pipeline.aggregate!.output[index]?.name ?? entry.column;
      return outputName === term.source.column || entry.column === term.source.column;
    });
    if (groupBy) {
      return resolveColumnRef(plan.joinPlan.aliases, {
        ...toRef(groupBy.alias ?? groupBy.table, groupBy.column),
      });
    }
  }

  return term.source.column;
}
