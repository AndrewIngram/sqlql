import {
  createSqlRelationalProviderAdapter,
  type FragmentProviderAdapter,
} from "@tupl/provider-kit";
import type { LookupManyCapableProviderAdapter } from "@tupl/provider-kit/shapes";

import { executeLookupManyResult } from "./execution/lookup-execution";
import { kyselyQueryTranslationBackend } from "./planning/rel-builder";
import { resolveKyselyDb } from "./backend/runtime-checks";
import type {
  CreateKyselyProviderOptions,
  KyselyDatabaseLike,
  KyselyQueryBuilderLike,
  KyselyProviderEntities,
  KyselyProviderEntityConfig,
  ResolvedEntityConfig,
  ScanBinding,
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
): FragmentProviderAdapter<TContext> & {
  lookupMany: LookupManyCapableProviderAdapter<TContext>["lookupMany"];
  entities: KyselyProviderEntities<TContext, TDatabase, TEntities>;
} {
  const providerName = options.name ?? "kysely";
  const entityOptions = (options.entities ?? {}) as TEntities;

  return createSqlRelationalProviderAdapter<
    TContext,
    TEntities,
    ResolvedEntityConfig<TContext>,
    ScanBinding<TContext>,
    KyselyDatabaseLike,
    KyselyQueryBuilderLike,
    KyselyProviderEntities<TContext, TDatabase, TEntities>
  >({
    name: providerName,
    entities: entityOptions,
    resolveRuntime(context) {
      return resolveKyselyDb(options, context);
    },
    unsupportedRelCompileMessage: "Unsupported relational fragment for Kysely provider.",
    unsupportedRelReasonMessage: "Rel fragment is not supported for single-query Kysely pushdown.",
    queryBackend: kyselyQueryTranslationBackend,
    async lookupMany({ request, context, resolvedEntities, runtime }) {
      return executeLookupManyResult(runtime, resolvedEntities, request, context);
    },
  });
}
