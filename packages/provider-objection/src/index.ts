import {
  AdapterResult,
  createSqlRelationalProviderAdapter,
  type FragmentProviderAdapter,
  type LookupProviderAdapter,
} from "@tupl/provider-kit";

import { executeLookupMany } from "./execution/lookup-execution";
import { executeScan } from "./execution/scan-execution";
import { objectionSqlRelationalBackend } from "./planning/rel-builder";
import type {
  CreateObjectionProviderOptions,
  KnexLike,
  ObjectionProviderEntities,
  ObjectionProviderEntityConfig,
  ResolvedEntityConfig,
} from "./types";

export type {
  CreateObjectionProviderOptions,
  KnexLike,
  KnexLikeQueryBuilder,
  ObjectionProviderEntityConfig,
  ObjectionProviderShape,
} from "./types";

/**
 * Objection provider entrypoints own runtime binding validation and helper wiring.
 * Query-builder planning and execution details live in the internal planning/execution/backend families.
 */
export function createObjectionProvider<
  TContext,
  TEntities extends Record<string, ObjectionProviderEntityConfig<TContext, any, string>> = Record<
    string,
    ObjectionProviderEntityConfig<TContext, any, string>
  >,
>(
  options: CreateObjectionProviderOptions<TContext, TEntities>,
): FragmentProviderAdapter<TContext> &
  LookupProviderAdapter<TContext> & {
    entities: ObjectionProviderEntities<TEntities>;
  } {
  const providerName = options.name ?? "objection";
  const entityConfigs = resolveEntityConfigs(options);
  const entityOptions = (options.entities ?? {}) as TEntities;

  return createSqlRelationalProviderAdapter({
    name: providerName,
    entities: entityOptions,
    resolvedEntities: entityConfigs,
    backend: objectionSqlRelationalBackend,
    resolveRuntime: (context: TContext) => resolveKnex(options, context),
    unsupportedRelCompileMessage: "Unsupported SQL-relational fragment for Objection provider.",
    unsupportedRelReasonMessage:
      "Rel fragment is not supported for single-query Objection pushdown.",
    async executeScan({ runtime, request, context }) {
      return executeScan(runtime, entityConfigs, request, context);
    },
    async lookupMany({ request, runtime, context }) {
      return AdapterResult.tryPromise({
        try: () => executeLookupMany(runtime, entityConfigs, request, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
  }) as FragmentProviderAdapter<TContext> &
    LookupProviderAdapter<TContext> & {
      entities: ObjectionProviderEntities<TEntities>;
    };
}

export async function resolveKnex<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
  context: TContext,
): Promise<KnexLike> {
  const knex = typeof options.knex === "function" ? await options.knex(context) : options.knex;
  const candidate = knex as Partial<KnexLike> | null | undefined;
  if (
    !candidate ||
    typeof candidate.table !== "function" ||
    typeof candidate.queryBuilder !== "function"
  ) {
    throw new Error(
      "Objection provider runtime binding did not resolve to a valid knex instance. Check your context and knex callback.",
    );
  }
  return candidate as KnexLike;
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
