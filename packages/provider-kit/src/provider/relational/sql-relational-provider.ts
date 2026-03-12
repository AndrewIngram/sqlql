import {
  isRelProjectColumnMapping,
  type QueryRow,
  type RelExpr,
  type RelNode,
} from "@tupl/foundation";

import type {
  ProviderCapabilityAtom,
  ProviderCapabilityReport,
  ProviderRouteFamily,
  QueryFallbackPolicy,
} from "../capabilities";
import type { ProviderFragment, ProviderLookupManyRequest, TableScanRequest } from "../contracts";
import type { DataEntityColumnMap } from "../entity-handles";
import {
  AdapterResult,
  type AdapterResult as AdapterResultType,
  type MaybePromise,
} from "../operations";
import {
  buildSingleQueryPlan as buildRelationalSingleQueryPlan,
  canCompileBasicRel,
  canCompileSetOpRel,
  canCompileWithRel,
  isSupportedRelationalPlan,
  resolveRelationalStrategy,
  unwrapSetOpRel,
  unwrapWithBodyRel,
  UnsupportedRelationalPlanError,
  type RelationalRegularJoinStep,
  type RelationalScanBindingBase,
  type RelationalSetOpWrapper,
  type RelationalSingleQueryPlan,
  type RelationalWithBodyWrapper,
} from "../shapes/relational-core";
import {
  type RelationalProviderAdapter,
  type RelationalProviderAdapterWithLookup,
  type RelationalProviderEntityConfig,
} from "./relational-adapter-types";
import { createRelationalProviderAdapter } from "./relational-provider";

export type SqlRelationalCompileStrategy = "basic" | "set_op" | "with";

export interface SqlRelationalCompiledPlan {
  strategy: SqlRelationalCompileStrategy;
  rel: RelNode;
}

export interface SqlRelationalResolvedEntity<TConfig = unknown> {
  entity: string;
  table: string;
  config: TConfig;
}

export interface SqlRelationalScanBinding<
  TResolvedEntity extends SqlRelationalResolvedEntity = SqlRelationalResolvedEntity,
> extends RelationalScanBindingBase {
  alias: string;
  entity: string;
  table: string;
  resolved: TResolvedEntity;
}

export interface SqlRelationalColumnSelection {
  kind: "column";
  output: string;
  source: { alias?: string; table?: string; column: string };
}

export interface SqlRelationalMetricSelection {
  kind: "metric";
  output: string;
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number];
}

export interface SqlRelationalExprSelection {
  kind: "expr";
  output: string;
  expr: RelExpr;
}

export type SqlRelationalSelection =
  | SqlRelationalColumnSelection
  | SqlRelationalMetricSelection
  | SqlRelationalExprSelection;

export interface SqlRelationalWindowSelection {
  kind: "window";
  output: string;
  window: Extract<RelNode, { kind: "window" }>["functions"][number];
}

export type SqlRelationalWithSelection =
  | SqlRelationalColumnSelection
  | SqlRelationalWindowSelection;

export interface SqlRelationalQualifiedOrderTerm {
  kind: "qualified";
  direction: "asc" | "desc";
  source: { alias?: string; table?: string; column: string };
}

export interface SqlRelationalOutputOrderTerm {
  kind: "output";
  direction: "asc" | "desc";
  column: string;
}

export type SqlRelationalOrderTerm = SqlRelationalQualifiedOrderTerm | SqlRelationalOutputOrderTerm;

export interface SqlRelationalBackend<
  TContext,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
> {
  createScanBinding(
    scan: Extract<RelNode, { kind: "scan" }>,
    resolvedEntities: Record<string, TResolvedEntity>,
  ): TBinding;
  createRootQuery(args: {
    runtime: TRuntime;
    root: TBinding;
    context: TContext;
    plan: RelationalSingleQueryPlan<TBinding>;
    selection: SqlRelationalSelection[];
  }): MaybePromise<TQuery>;
  applyRegularJoin(args: {
    query: TQuery;
    join: RelationalRegularJoinStep<TBinding>;
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applySemiJoin(args: {
    query: TQuery;
    leftKey: { alias: string; column: string };
    subquery: TQuery;
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyWhereClause(args: {
    query: TQuery;
    clause: NonNullable<TableScanRequest["where"]>[number];
    plan: RelationalSingleQueryPlan<TBinding> | RelationalWithBodyWrapper;
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): TQuery;
  applySelection(args: {
    query: TQuery;
    plan: RelationalSingleQueryPlan<TBinding>;
    selection: SqlRelationalSelection[];
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyGroupBy(args: {
    query: TQuery;
    groupBy: Extract<RelNode, { kind: "aggregate" }>["groupBy"];
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyOrderBy(args: {
    query: TQuery;
    plan: RelationalSingleQueryPlan<TBinding> | RelationalSetOpWrapper | RelationalWithBodyWrapper;
    selection?: SqlRelationalSelection[] | SqlRelationalWithSelection[];
    orderBy: SqlRelationalOrderTerm[];
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyLimit(args: {
    query: TQuery;
    limit: number;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyOffset(args: {
    query: TQuery;
    offset: number;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applySetOp(args: {
    left: TQuery;
    right: TQuery;
    wrapper: RelationalSetOpWrapper;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  buildWithQuery(args: {
    body: RelationalWithBodyWrapper;
    ctes: Array<{ name: string; query: TQuery }>;
    projection: SqlRelationalWithSelection[];
    orderBy: SqlRelationalOrderTerm[];
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  buildSingleQueryPlan?(
    rel: RelNode,
    resolvedEntities: Record<string, TResolvedEntity>,
  ): RelationalSingleQueryPlan<TBinding>;
  resolveRelCompileStrategy?(
    node: RelNode,
    resolvedEntities: Record<string, TResolvedEntity>,
    options?: { requireColumnProjectMappings?: boolean },
  ): SqlRelationalCompileStrategy | null;
  executeQuery(args: { query: TQuery; context: TContext; runtime: TRuntime }): Promise<QueryRow[]>;
}

export interface SqlRelationalSupportArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TRuntime,
> {
  context: TContext;
  entities: TEntities;
  resolvedEntities: Record<string, TResolvedEntity>;
  fragment: Extract<ProviderFragment, { kind: "rel" }>;
  routeFamily: ProviderRouteFamily;
  requiredAtoms: ProviderCapabilityAtom[];
  missingAtoms: ProviderCapabilityAtom[];
  strategy: SqlRelationalCompileStrategy | null;
  runtime: TRuntime;
}

export interface SqlRelationalScanArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TRuntime,
> {
  context: TContext;
  entities: TEntities;
  resolvedEntities: Record<string, TResolvedEntity>;
  name: string;
  request: TableScanRequest;
  runtime: TRuntime;
}

export interface SqlRelationalLookupArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TRuntime,
> {
  context: TContext;
  entities: TEntities;
  resolvedEntities: Record<string, TResolvedEntity>;
  name: string;
  request: ProviderLookupManyRequest;
  runtime: TRuntime;
}

export interface SqlRelationalEntityArgs<
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TEntityName extends Extract<keyof TEntities, string>,
> {
  config: TEntities[TEntityName];
  entity: TEntityName;
  name: string;
}

interface SqlRelationalProviderOptionsBase<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
> {
  name: string;
  entities: TEntities;
  backend: SqlRelationalBackend<TContext, TResolvedEntity, TBinding, TRuntime, TQuery>;
  resolveRuntime(context: TContext): MaybePromise<TRuntime>;
  resolveEntity<TEntityName extends Extract<keyof TEntities, string>>(
    args: SqlRelationalEntityArgs<TEntities, TEntityName>,
  ): TResolvedEntity;
  compileOptions?: {
    requireColumnProjectMappings?: boolean;
  };
  declaredAtoms?: readonly ProviderCapabilityAtom[];
  fallbackPolicy?: QueryFallbackPolicy;
  routeFamilies?: readonly ProviderRouteFamily[];
  unsupportedRelReasonMessage?: string;
  unsupportedRelCompileMessage?: string;
  resolveEntityColumns?<TEntityName extends Extract<keyof TEntities, string>>(args: {
    config: TEntities[TEntityName];
    entity: TEntityName;
    name: string;
  }): DataEntityColumnMap<string> | undefined;
  isStrategySupported?(
    args: SqlRelationalSupportArgs<TContext, TEntities, TResolvedEntity, TRuntime>,
  ): MaybePromise<true | string | ProviderCapabilityReport>;
  executeScan(
    args: SqlRelationalScanArgs<TContext, TEntities, TResolvedEntity, TRuntime>,
  ): Promise<QueryRow[]>;
}

interface SqlRelationalCompileHelpers<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
> {
  buildSingleQueryPlan(rel: RelNode): RelationalSingleQueryPlan<TBinding>;
  resolveStrategy(node: RelNode): SqlRelationalCompileStrategy | null;
}

export interface CreateSqlRelationalProviderAdapterOptions<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
> extends SqlRelationalProviderOptionsBase<
  TContext,
  TEntities,
  TResolvedEntity,
  TBinding,
  TRuntime,
  TQuery
> {
  lookupMany?: undefined;
}

export interface CreateSqlRelationalProviderAdapterOptionsWithLookup<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
> extends SqlRelationalProviderOptionsBase<
  TContext,
  TEntities,
  TResolvedEntity,
  TBinding,
  TRuntime,
  TQuery
> {
  lookupMany(
    args: SqlRelationalLookupArgs<TContext, TEntities, TResolvedEntity, TRuntime>,
  ): MaybePromise<AdapterResultType<QueryRow[]>>;
}

export class UnsupportedSqlRelationalPlanError extends UnsupportedRelationalPlanError {}

function isPromiseLikeValue<T>(value: MaybePromise<T>): value is PromiseLike<T> {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

function resolveSqlRelationalEntities<
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
>(
  name: string,
  entities: TEntities,
  resolveEntity: <TEntityName extends Extract<keyof TEntities, string>>(
    args: SqlRelationalEntityArgs<TEntities, TEntityName>,
  ) => TResolvedEntity,
): Record<string, TResolvedEntity> {
  const out: Record<string, TResolvedEntity> = {};

  for (const [entity, config] of Object.entries(entities) as Array<
    [Extract<keyof TEntities, string>, TEntities[Extract<keyof TEntities, string>]]
  >) {
    out[entity] = resolveEntity({
      config,
      entity,
      name,
    });
  }

  return out;
}

/**
 * SQL-relational helpers own the ordinary adapter-authoring path for SQL-like backends.
 * They centralize recursive rel compilation so adapters mostly describe backend differences.
 */
export function createSqlRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
>(
  options: CreateSqlRelationalProviderAdapterOptions<
    TContext,
    TEntities,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >,
): RelationalProviderAdapter<TContext, TEntities>;
export function createSqlRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
>(
  options: CreateSqlRelationalProviderAdapterOptionsWithLookup<
    TContext,
    TEntities,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >,
): RelationalProviderAdapterWithLookup<TContext, TEntities>;
export function createSqlRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
>(
  options:
    | CreateSqlRelationalProviderAdapterOptions<
        TContext,
        TEntities,
        TResolvedEntity,
        TBinding,
        TRuntime,
        TQuery
      >
    | CreateSqlRelationalProviderAdapterOptionsWithLookup<
        TContext,
        TEntities,
        TResolvedEntity,
        TBinding,
        TRuntime,
        TQuery
      >,
):
  | RelationalProviderAdapter<TContext, TEntities>
  | RelationalProviderAdapterWithLookup<TContext, TEntities> {
  const resolveEntity = <TEntityName extends Extract<keyof TEntities, string>>(
    args: SqlRelationalEntityArgs<TEntities, TEntityName>,
  ) => options.resolveEntity(args);
  const resolvedEntities = resolveSqlRelationalEntities(
    options.name,
    options.entities,
    resolveEntity,
  );
  const createScanBinding = (
    scan: Extract<RelNode, { kind: "scan" }>,
    resolvedEntities: Record<string, TResolvedEntity>,
  ) => options.backend.createScanBinding(scan, resolvedEntities);
  const compileHelpers = createSqlRelationalCompileHelpers(
    resolvedEntities,
    createScanBinding,
    options.backend,
    options.compileOptions,
  );
  const resolveEntityColumns = options.resolveEntityColumns
    ? <TEntityName extends Extract<keyof TEntities, string>>(args: {
        config: TEntities[TEntityName];
        entity: TEntityName;
        name: string;
      }) => options.resolveEntityColumns!(args)
    : undefined;

  const baseOptions = {
    name: options.name,
    entities: options.entities,
    ...(options.declaredAtoms ? { declaredAtoms: options.declaredAtoms } : {}),
    ...(options.fallbackPolicy ? { fallbackPolicy: options.fallbackPolicy } : {}),
    ...(options.routeFamilies ? { routeFamilies: options.routeFamilies } : {}),
    ...(resolveEntityColumns ? { resolveEntityColumns } : {}),
    ...(options.unsupportedRelReasonMessage
      ? { unsupportedRelReasonMessage: options.unsupportedRelReasonMessage }
      : {}),
    ...(options.unsupportedRelCompileMessage
      ? { unsupportedRelCompileMessage: options.unsupportedRelCompileMessage }
      : {}),
    resolveRelCompileStrategy({
      fragment,
    }: {
      context: TContext;
      entities: TEntities;
      fragment: Extract<ProviderFragment, { kind: "rel" }>;
    }) {
      return compileHelpers.resolveStrategy(fragment.rel);
    },
    ...(options.isStrategySupported
      ? {
          isRelStrategySupported(args: {
            context: TContext;
            entities: TEntities;
            fragment: Extract<ProviderFragment, { kind: "rel" }>;
            routeFamily: ProviderRouteFamily;
            requiredAtoms: ProviderCapabilityAtom[];
            missingAtoms: ProviderCapabilityAtom[];
            strategy: SqlRelationalCompileStrategy | null;
          }) {
            const runtime = options.resolveRuntime(args.context);
            if (isPromiseLikeValue(runtime)) {
              return runtime.then((resolvedRuntime) =>
                options.isStrategySupported!({
                  context: args.context,
                  entities: options.entities,
                  resolvedEntities,
                  fragment: args.fragment,
                  routeFamily: args.routeFamily,
                  requiredAtoms: args.requiredAtoms,
                  missingAtoms: args.missingAtoms,
                  strategy: args.strategy as SqlRelationalCompileStrategy | null,
                  runtime: resolvedRuntime,
                }),
              );
            }

            return options.isStrategySupported!({
              context: args.context,
              entities: options.entities,
              resolvedEntities,
              fragment: args.fragment,
              routeFamily: args.routeFamily,
              requiredAtoms: args.requiredAtoms,
              missingAtoms: args.missingAtoms,
              strategy: args.strategy as SqlRelationalCompileStrategy | null,
              runtime,
            });
          },
        }
      : {}),
    buildRelPlanPayload({
      fragment,
      strategy,
    }: {
      context: TContext;
      entities: TEntities;
      fragment: Extract<ProviderFragment, { kind: "rel" }>;
      name: string;
      strategy: SqlRelationalCompileStrategy;
    }) {
      return {
        strategy,
        rel: fragment.rel,
      } satisfies SqlRelationalCompiledPlan;
    },
    async executeCompiledPlan({
      plan,
      context,
    }: {
      context: TContext;
      entities: TEntities;
      plan: import("../contracts").ProviderCompiledPlan;
      name: string;
    }) {
      return AdapterResult.tryPromise({
        try: async () => {
          const runtime = await options.resolveRuntime(context);

          switch (plan.kind) {
            case "scan":
              return options.executeScan({
                context,
                entities: options.entities,
                resolvedEntities,
                name: options.name,
                request: (plan.payload as Extract<ProviderFragment, { kind: "scan" }>).request,
                runtime,
              });
            case "rel": {
              const compiled = plan.payload as SqlRelationalCompiledPlan;
              const query = await buildSqlRelationalQueryForStrategyWithHelpers(
                compiled.rel,
                compiled.strategy,
                resolvedEntities,
                options.backend,
                runtime,
                context,
                options.compileOptions,
                compileHelpers,
              );
              return options.backend.executeQuery({ query, context, runtime });
            }
            default:
              throw new Error(`Unsupported ${options.name} compiled plan kind: ${plan.kind}`);
          }
        },
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
  };

  if ("lookupMany" in options && options.lookupMany) {
    return createRelationalProviderAdapter<TContext, TEntities, SqlRelationalCompileStrategy>({
      ...baseOptions,
      lookupMany: async ({ request, context }) => {
        const runtime = await options.resolveRuntime(context);
        return options.lookupMany({
          context,
          entities: options.entities,
          resolvedEntities,
          name: options.name,
          request,
          runtime,
        });
      },
    }) as RelationalProviderAdapterWithLookup<TContext, TEntities>;
  }

  return createRelationalProviderAdapter<TContext, TEntities, SqlRelationalCompileStrategy>(
    baseOptions,
  ) as RelationalProviderAdapter<TContext, TEntities>;
}

export function requireSqlRelationalProjectMapping(
  mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
): SqlRelationalColumnSelection {
  if (!isRelProjectColumnMapping(mapping)) {
    throw new UnsupportedSqlRelationalPlanError(
      "Computed projections are not supported in SQL-relational single-query pushdown.",
    );
  }

  return {
    kind: "column",
    output: mapping.output,
    source: mapping.source,
  };
}

export function createSqlRelationalScanBinding<TResolvedEntity extends SqlRelationalResolvedEntity>(
  scan: Extract<RelNode, { kind: "scan" }>,
  resolvedEntities: Record<string, TResolvedEntity>,
): SqlRelationalScanBinding<TResolvedEntity> {
  const resolved = resolvedEntities[scan.table];
  if (!resolved) {
    throw new UnsupportedSqlRelationalPlanError(
      `Missing SQL-relational entity config for "${scan.table}".`,
    );
  }

  return {
    alias: scan.alias ?? resolved.table,
    entity: resolved.entity,
    table: resolved.table,
    scan,
    resolved,
  };
}

export function resolveSqlRelationalCompileStrategy<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
>(
  node: RelNode,
  resolvedEntities: Record<string, TResolvedEntity>,
  createScanBinding: (
    scan: Extract<RelNode, { kind: "scan" }>,
    resolvedEntities: Record<string, TResolvedEntity>,
  ) => TBinding,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
): SqlRelationalCompileStrategy | null {
  return resolveRelationalStrategy(node, {
    basicStrategy: "basic",
    setOpStrategy: "set_op",
    withStrategy: "with",
    canCompileBasic: (current) =>
      canCompileBasicRel(current, (table) => !!resolvedEntities[table], {
        requireColumnProjectMappings: options?.requireColumnProjectMappings ?? true,
      }),
    validateBasic: (current) =>
      isSupportedRelationalPlan(() => {
        buildSqlRelationalSingleQueryPlan(current, resolvedEntities, createScanBinding);
      }),
    canCompileSetOp: (current) =>
      canCompileSetOpRel(
        current,
        (branch) =>
          resolveSqlRelationalCompileStrategy(branch, resolvedEntities, createScanBinding, options),
        requireSqlRelationalProjectMapping,
      ),
    canCompileWith: (current) =>
      canCompileWithRel(current, (branch) =>
        resolveSqlRelationalCompileStrategy(branch, resolvedEntities, createScanBinding, options),
      ),
  });
}

export function buildSqlRelationalSingleQueryPlan<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
>(
  rel: RelNode,
  resolvedEntities: Record<string, TResolvedEntity>,
  createScanBinding: (
    scan: Extract<RelNode, { kind: "scan" }>,
    resolvedEntities: Record<string, TResolvedEntity>,
  ) => TBinding,
): RelationalSingleQueryPlan<TBinding> {
  return buildRelationalSingleQueryPlan(rel, (scan) => createScanBinding(scan, resolvedEntities));
}

function createSqlRelationalCompileHelpers<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
>(
  resolvedEntities: Record<string, TResolvedEntity>,
  createScanBinding: (
    scan: Extract<RelNode, { kind: "scan" }>,
    resolvedEntities: Record<string, TResolvedEntity>,
  ) => TBinding,
  backend: SqlRelationalBackend<any, TResolvedEntity, TBinding, any, any>,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
): SqlRelationalCompileHelpers<TResolvedEntity, TBinding> {
  return {
    buildSingleQueryPlan(rel) {
      return (
        backend.buildSingleQueryPlan?.(rel, resolvedEntities) ??
        buildSqlRelationalSingleQueryPlan(rel, resolvedEntities, createScanBinding)
      );
    },
    resolveStrategy(node) {
      return (
        backend.resolveRelCompileStrategy?.(node, resolvedEntities, options) ??
        resolveSqlRelationalCompileStrategy(node, resolvedEntities, createScanBinding, options)
      );
    },
  };
}

export async function buildSqlRelationalQueryForStrategy<
  TContext,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
>(
  rel: RelNode,
  strategy: SqlRelationalCompileStrategy,
  resolvedEntities: Record<string, TResolvedEntity>,
  backend: SqlRelationalBackend<TContext, TResolvedEntity, TBinding, TRuntime, TQuery>,
  runtime: TRuntime,
  context: TContext,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
): Promise<TQuery> {
  const createScanBinding = (
    scan: Extract<RelNode, { kind: "scan" }>,
    currentResolvedEntities: Record<string, TResolvedEntity>,
  ) => backend.createScanBinding(scan, currentResolvedEntities);
  const compileHelpers = createSqlRelationalCompileHelpers(
    resolvedEntities,
    createScanBinding,
    backend,
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
  backend: SqlRelationalBackend<TContext, TResolvedEntity, TBinding, TRuntime, TQuery>,
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
  backend: SqlRelationalBackend<TContext, TResolvedEntity, TBinding, TRuntime, TQuery>,
  runtime: TRuntime,
  context: TContext,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
  compileHelpers?: SqlRelationalCompileHelpers<TResolvedEntity, TBinding>,
): Promise<TQuery> {
  const helpers =
    compileHelpers ??
    createSqlRelationalCompileHelpers(
      resolvedEntities,
      (scan, currentResolvedEntities) => backend.createScanBinding(scan, currentResolvedEntities),
      backend,
      options,
    );
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
  backend: SqlRelationalBackend<TContext, TResolvedEntity, TBinding, TRuntime, TQuery>,
  runtime: TRuntime,
  context: TContext,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
  compileHelpers?: SqlRelationalCompileHelpers<TResolvedEntity, TBinding>,
): Promise<TQuery> {
  const helpers =
    compileHelpers ??
    createSqlRelationalCompileHelpers(
      resolvedEntities,
      (scan, currentResolvedEntities) => backend.createScanBinding(scan, currentResolvedEntities),
      backend,
      options,
    );
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
  backend: SqlRelationalBackend<TContext, TResolvedEntity, TBinding, TRuntime, TQuery>,
  runtime: TRuntime,
  context: TContext,
  options?: {
    requireColumnProjectMappings?: boolean;
  },
  compileHelpers?: SqlRelationalCompileHelpers<TResolvedEntity, TBinding>,
): Promise<TQuery> {
  const helpers =
    compileHelpers ??
    createSqlRelationalCompileHelpers(
      resolvedEntities,
      (scan, currentResolvedEntities) => backend.createScanBinding(scan, currentResolvedEntities),
      backend,
      options,
    );
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
      if (isRelProjectColumnMapping(mapping)) {
        return {
          kind: "column",
          output: mapping.output,
          source: mapping.source,
        } satisfies SqlRelationalColumnSelection;
      }

      return {
        kind: "expr",
        output: mapping.output,
        expr: mapping.expr,
      } satisfies SqlRelationalExprSelection;
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
      } satisfies SqlRelationalMetricSelection;
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
    } satisfies SqlRelationalColumnSelection;
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
  const scanAlias = body.cteScan.alias ?? body.cteScan.table;
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
    } satisfies SqlRelationalColumnSelection;
  });
}

function buildWithOrder(body: RelationalWithBodyWrapper): SqlRelationalOrderTerm[] {
  const scanAlias = body.cteScan.alias ?? body.cteScan.table;
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
