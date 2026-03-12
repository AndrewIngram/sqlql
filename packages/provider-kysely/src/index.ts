import {
  AdapterResult,
  createSqlRelationalProviderAdapter,
  type FragmentProviderAdapter,
  type LookupProviderAdapter,
} from "@tupl/provider-kit";

import { executeLookupMany } from "./execution/lookup-execution";
import { executeScan } from "./execution/scan-execution";
import { kyselySqlRelationalBackend } from "./planning/rel-builder";
import type {
  CreateKyselyProviderOptions,
  KyselyDatabaseLike,
  KyselyProviderEntities,
  KyselyProviderEntityConfig,
  ResolvedEntityConfig,
} from "./types";

export type {
  CreateKyselyProviderOptions,
  KyselyDatabaseLike,
  KyselyProviderEntityConfig,
} from "./types";

/**
 * Kysely provider entrypoints own runtime binding validation and helper wiring.
 * Relational planning and query-builder details live in the internal planning/execution/backend families.
 */
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
    entities: KyselyProviderEntities<TContext, TDatabase, TEntities>;
  } {
  const providerName = options.name ?? "kysely";
  const entityConfigs = resolveEntityConfigs(options);
  const entityOptions = (options.entities ?? {}) as TEntities;

  return createSqlRelationalProviderAdapter({
    name: providerName,
    entities: entityOptions,
    resolveEntity({ entity, config }) {
      return {
        entity,
        table: config.table ?? entity,
        config,
      };
    },
    backend: kyselySqlRelationalBackend,
    resolveRuntime: (context: TContext) => resolveKyselyDb(options, context),
    unsupportedRelCompileMessage: "Unsupported SQL-relational fragment for Kysely provider.",
    unsupportedRelReasonMessage: "Rel fragment is not supported for single-query Kysely pushdown.",
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
      entities: KyselyProviderEntities<TContext, TDatabase, TEntities>;
    };
}

export async function resolveKyselyDb<TContext>(
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
