import {
  createSqlRelationalProviderAdapter,
  type FragmentProviderAdapter,
} from "@tupl/provider-kit";
import type { LookupManyCapableProviderAdapter } from "@tupl/provider-kit/shapes";

import { executeLookupManyResult } from "./execution/lookup-execution";
import {
  resolveDrizzleDbMaybeSync,
  inferDrizzleDialect,
  isStrategyAvailableOnDrizzleDb,
} from "./backend/runtime-checks";
import { deriveEntityColumnsFromTable } from "./backend/table-columns";
import { impossibleCondition, runDrizzleScan } from "./backend/query-helpers";
import { drizzleQueryTranslationBackend } from "./planning/rel-builder";
import {
  type ScanBinding,
  buildSingleQueryPlan,
  resolveDrizzleRelCompileStrategy,
  createScanBinding,
} from "./planning/rel-strategy";
import type {
  CreateDrizzleProviderOptions,
  DrizzleProviderEntities,
  DrizzleProviderTableConfig,
  DrizzleQueryExecutor,
} from "./types";
import type { DrizzleTranslatedQuery } from "./planning/rel-builder";

export type {
  CreateDrizzleProviderOptions,
  DrizzleColumnMap,
  DrizzleProviderTableConfig,
  DrizzleQueryExecutor,
  RunDrizzleScanOptions,
} from "./types";
export { impossibleCondition, runDrizzleScan };

/**
 * Drizzle provider entrypoints own runtime binding validation and helper wiring.
 * Backend planning and query-builder execution live in the internal planning/execution/backend families.
 */
export function createDrizzleProvider<
  TContext,
  TTables extends Record<string, DrizzleProviderTableConfig<TContext>> = Record<
    string,
    DrizzleProviderTableConfig<TContext>
  >,
>(
  options: CreateDrizzleProviderOptions<TContext, TTables>,
): FragmentProviderAdapter<TContext> & {
  lookupMany: LookupManyCapableProviderAdapter<TContext>["lookupMany"];
  entities: DrizzleProviderEntities<TTables>;
} {
  const providerName = options.name ?? "drizzle";
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const dialect = options.dialect ?? inferDrizzleDialect(options.db, tableConfigs);
  void dialect;

  return createSqlRelationalProviderAdapter<
    TContext,
    TTables,
    {
      entity: string;
      table: string;
      config: DrizzleProviderTableConfig<TContext>;
    },
    ScanBinding<TContext>,
    DrizzleQueryExecutor,
    DrizzleTranslatedQuery,
    DrizzleProviderEntities<TTables>
  >({
    name: providerName,
    entities: options.tables,
    resolveRuntime(context) {
      return resolveDrizzleDbMaybeSync(options, context);
    },
    unsupportedRelCompileMessage: "Unsupported relational fragment for drizzle provider.",
    unsupportedRelReasonMessage: "Rel fragment is not supported for single-query drizzle pushdown.",
    queryBackend: drizzleQueryTranslationBackend,
    resolveEntityColumns({ config }) {
      return deriveEntityColumnsFromTable(config.table);
    },
    advanced: {
      createScanBinding,
      buildSingleQueryPlan,
      resolveRelCompileStrategy(rel, resolvedEntities) {
        return resolveDrizzleRelCompileStrategy(rel, resolvedEntities);
      },
    },
    isStrategySupported({ strategy, runtime }) {
      if (strategy == null) {
        return "Rel fragment is not supported for single-query drizzle pushdown.";
      }
      return isStrategyAvailableOnDrizzleDb(strategy, runtime)
        ? true
        : `Drizzle database instance does not support required APIs for "${strategy}" rel pushdown.`;
    },
    async lookupMany({ request, context, runtime }) {
      return executeLookupManyResult(runtime, options, request, context);
    },
  });
}
