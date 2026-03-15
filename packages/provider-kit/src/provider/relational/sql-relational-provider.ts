import {
  TuplExecutionError,
  TuplProviderBindingError,
  type QueryRow,
  type RelNode,
} from "@tupl/foundation";

import type { ProviderCapabilityReport, QueryFallbackPolicy } from "../capabilities";
import type { DataEntityColumnMap } from "../entity-handles";
import {
  AdapterResult,
  type AdapterResult as AdapterResultType,
  type MaybePromise,
} from "../operations";
import {
  type LookupCapableRelationalProviderAdapter,
  type RelationalProviderAdapter,
  type RelationalProviderEntityConfig,
  type RelationalProviderHandles,
} from "./relational-adapter-types";
import { createRelationalProviderAdapter } from "./relational-provider";
import { buildSqlRelationalQueryForStrategy } from "./sql-relational/query-building";
import {
  createSqlRelationalCompileHelpers,
  createSqlRelationalScanBinding,
} from "./sql-relational/planning";
import type {
  SqlRelationalCompiledPlan,
  SqlRelationalCompileStrategy,
  SqlRelationalEntityArgs,
  SqlRelationalLookupArgs,
  SqlRelationalQueryTranslationBackend,
  SqlRelationalResolvedEntity,
  SqlRelationalScanBinding,
  SqlRelationalSupportArgs,
} from "./sql-relational/types";
import { UnsupportedSqlRelationalPlanError } from "./sql-relational/types";

/**
 * SQL-relational provider creation owns the ordinary adapter-authoring path for SQL-like backends.
 * It intentionally keeps adapter code focused on runtime resolution and backend primitives while
 * provider-kit hides resolved-entity preparation, compile helper assembly, and plan execution flow.
 */
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
  queryBackend: SqlRelationalQueryTranslationBackend<
    TContext,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >;
  resolveRuntime(context: TContext): MaybePromise<TRuntime>;
  resolveEntity?<TEntityName extends Extract<keyof TEntities, string>>(
    args: SqlRelationalEntityArgs<TEntities, TEntityName>,
  ): TResolvedEntity;
  fallbackPolicy?: QueryFallbackPolicy;
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
}

interface SqlRelationalProviderAdvancedOptions<
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
> {
  createScanBinding?(
    scan: Extract<RelNode, { kind: "scan" }>,
    resolvedEntities: Record<string, TResolvedEntity>,
  ): TBinding;
  buildSingleQueryPlan?(
    rel: RelNode,
    resolvedEntities: Record<string, TResolvedEntity>,
  ): import("../shapes/relational-core").RelationalSingleQueryPlan<TBinding>;
  resolveRelCompileStrategy?(
    node: RelNode,
    resolvedEntities: Record<string, TResolvedEntity>,
    options?: { requireColumnProjectMappings?: boolean },
  ): SqlRelationalCompileStrategy | null;
  compileOptions?: {
    requireColumnProjectMappings?: boolean;
  };
}

/**
 * Canonical authoring surface for ordinary SQL-like adapters.
 * Provider-kit owns resolved-entity fanout, strategy selection, and recursive rel compilation;
 * adapters provide runtime resolution and backend query primitives.
 */
interface SqlRelationalProviderOptions<
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
  advanced?: SqlRelationalProviderAdvancedOptions<TResolvedEntity, TBinding>;
  lookupMany?: (
    args: SqlRelationalLookupArgs<TContext, TEntities, TResolvedEntity, TRuntime>,
  ) => MaybePromise<AdapterResultType<QueryRow[]>>;
}

type SqlRelationalProviderOptionsWithLookup<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
> = SqlRelationalProviderOptions<
  TContext,
  TEntities,
  TResolvedEntity,
  TBinding,
  TRuntime,
  TQuery
> & {
  lookupMany: NonNullable<
    SqlRelationalProviderOptions<
      TContext,
      TEntities,
      TResolvedEntity,
      TBinding,
      TRuntime,
      TQuery
    >["lookupMany"]
  >;
};

function normalizeSqlRelationalExecutionError(error: unknown): Error {
  if (
    error instanceof TuplExecutionError ||
    error instanceof TuplProviderBindingError ||
    error instanceof UnsupportedSqlRelationalPlanError
  ) {
    return error instanceof UnsupportedSqlRelationalPlanError
      ? new TuplExecutionError({
          operation: "execute SQL-relational provider plan",
          message: error.message,
          cause: error,
        })
      : error;
  }

  return error instanceof Error ? error : new Error(String(error));
}

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

function defaultResolveSqlRelationalEntity<
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TEntityName extends Extract<keyof TEntities, string>,
>(
  args: SqlRelationalEntityArgs<TEntities, TEntityName>,
): SqlRelationalResolvedEntity<TEntities[TEntityName]> {
  const tableCandidate = (args.config as { table?: unknown }).table;
  return {
    entity: args.entity,
    table: typeof tableCandidate === "string" ? tableCandidate : args.entity,
    config: args.config,
  };
}

export function createSqlRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
  THandles extends RelationalProviderHandles<TEntities> = RelationalProviderHandles<TEntities>,
>(
  options: SqlRelationalProviderOptions<
    TContext,
    TEntities,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  > & {
    lookupMany?: undefined;
  },
): RelationalProviderAdapter<TContext, TEntities, THandles>;
export function createSqlRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
  THandles extends RelationalProviderHandles<TEntities> = RelationalProviderHandles<TEntities>,
>(
  options: SqlRelationalProviderOptionsWithLookup<
    TContext,
    TEntities,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >,
): LookupCapableRelationalProviderAdapter<TContext, TEntities, THandles>;
export function createSqlRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
  THandles extends RelationalProviderHandles<TEntities> = RelationalProviderHandles<TEntities>,
>(
  options: SqlRelationalProviderOptions<
    TContext,
    TEntities,
    TResolvedEntity,
    TBinding,
    TRuntime,
    TQuery
  >,
):
  | RelationalProviderAdapter<TContext, TEntities, THandles>
  | LookupCapableRelationalProviderAdapter<TContext, TEntities, THandles> {
  const advanced = options.advanced;
  const resolveEntity = <TEntityName extends Extract<keyof TEntities, string>>(
    args: SqlRelationalEntityArgs<TEntities, TEntityName>,
  ) =>
    (options.resolveEntity?.(args) ?? defaultResolveSqlRelationalEntity(args)) as TResolvedEntity;
  const resolvedEntities = resolveSqlRelationalEntities(
    options.name,
    options.entities,
    resolveEntity,
  );
  const createScanBinding = (
    scan: Extract<RelNode, { kind: "scan" }>,
    currentResolvedEntities: Record<string, TResolvedEntity>,
  ) =>
    (advanced?.createScanBinding?.(scan, currentResolvedEntities) ??
      createSqlRelationalScanBinding(scan, currentResolvedEntities)) as TBinding;
  const compileHelpers = createSqlRelationalCompileHelpers(
    resolvedEntities,
    createScanBinding,
    {
      ...(advanced?.buildSingleQueryPlan
        ? {
            buildSingleQueryPlan: (rel, currentResolvedEntities) =>
              advanced.buildSingleQueryPlan!(rel, currentResolvedEntities),
          }
        : {}),
      ...(advanced?.resolveRelCompileStrategy
        ? {
            resolveRelCompileStrategy: (node, currentResolvedEntities, compileOptions) =>
              advanced.resolveRelCompileStrategy!(node, currentResolvedEntities, compileOptions),
          }
        : {}),
    },
    advanced?.compileOptions,
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
    ...(options.fallbackPolicy ? { fallbackPolicy: options.fallbackPolicy } : {}),
    ...(resolveEntityColumns ? { resolveEntityColumns } : {}),
    ...(options.unsupportedRelReasonMessage
      ? { unsupportedRelReasonMessage: options.unsupportedRelReasonMessage }
      : {}),
    ...(options.unsupportedRelCompileMessage
      ? { unsupportedRelCompileMessage: options.unsupportedRelCompileMessage }
      : {}),
    resolveRelCompileStrategy({ rel }: { context: TContext; entities: TEntities; rel: RelNode }) {
      return compileHelpers.resolveStrategy(rel);
    },
    ...(options.isStrategySupported
      ? {
          isRelStrategySupported(args: {
            context: TContext;
            entities: TEntities;
            rel: RelNode;
            strategy: SqlRelationalCompileStrategy | null;
          }) {
            const toUnsupported = (error: unknown) => ({
              supported: false as const,
              reason: error instanceof Error ? error.message : String(error),
            });

            try {
              const runtime = options.resolveRuntime(args.context);
              if (isPromiseLikeValue(runtime)) {
                return Promise.resolve(runtime)
                  .then((resolvedRuntime) =>
                    options.isStrategySupported!({
                      context: args.context,
                      entities: options.entities,
                      resolvedEntities,
                      rel: args.rel,
                      strategy: args.strategy,
                      runtime: resolvedRuntime,
                    }),
                  )
                  .catch(toUnsupported);
              }

              return options.isStrategySupported!({
                context: args.context,
                entities: options.entities,
                resolvedEntities,
                rel: args.rel,
                strategy: args.strategy,
                runtime,
              });
            } catch (error) {
              return toUnsupported(error);
            }
          },
        }
      : {}),
    buildRelPlanPayload({
      rel,
      strategy,
    }: {
      context: TContext;
      entities: TEntities;
      rel: RelNode;
      name: string;
      strategy: SqlRelationalCompileStrategy;
    }) {
      return {
        strategy,
        rel,
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
            case "rel": {
              const compiled = plan.payload as SqlRelationalCompiledPlan;
              const query = await buildSqlRelationalQueryForStrategy({
                rel: compiled.rel,
                strategy: compiled.strategy,
                resolvedEntities,
                backend: options.queryBackend,
                runtime,
                context,
                planningHooks: {
                  createScanBinding,
                  ...(advanced?.buildSingleQueryPlan
                    ? {
                        buildSingleQueryPlan: (
                          rel,
                          currentResolvedEntities: Record<string, TResolvedEntity>,
                        ) => advanced.buildSingleQueryPlan!(rel, currentResolvedEntities),
                      }
                    : {}),
                  ...(advanced?.resolveRelCompileStrategy
                    ? {
                        resolveRelCompileStrategy: (
                          node,
                          currentResolvedEntities: Record<string, TResolvedEntity>,
                          compileOptions,
                        ) =>
                          advanced.resolveRelCompileStrategy!(
                            node,
                            currentResolvedEntities,
                            compileOptions,
                          ),
                      }
                    : {}),
                },
                ...(advanced?.compileOptions ? { options: advanced.compileOptions } : {}),
              });
              return options.queryBackend.executeQuery({ query, context, runtime });
            }
            default:
              throw new TuplExecutionError({
                operation: "execute provider compiled plan",
                message: `Unsupported ${options.name} compiled plan kind: ${plan.kind}`,
              });
          }
        },
        catch: normalizeSqlRelationalExecutionError,
      });
    },
  };

  if ("lookupMany" in options && options.lookupMany) {
    return createRelationalProviderAdapter<
      TContext,
      TEntities,
      SqlRelationalCompileStrategy,
      THandles
    >({
      ...baseOptions,
      lookupMany: async ({ request, context }) => {
        try {
          const runtime = await options.resolveRuntime(context);
          return await Promise.resolve(
            options.lookupMany!({
              context,
              entities: options.entities,
              resolvedEntities,
              name: options.name,
              request,
              runtime,
            }),
          );
        } catch (error) {
          return AdapterResult.err(normalizeSqlRelationalExecutionError(error));
        }
      },
    });
  }

  return createRelationalProviderAdapter<
    TContext,
    TEntities,
    SqlRelationalCompileStrategy,
    THandles
  >(baseOptions);
}
