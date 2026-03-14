import {
  AdapterResult,
  createRelationalProviderAdapter,
  type FragmentProviderAdapter,
} from "@tupl/provider-kit";
import type { LookupManyCapableProviderAdapter } from "@tupl/provider-kit/shapes";

import { executeCompiledPlan } from "./execution/plan-execution";
import { executeLookupMany } from "./execution/lookup-execution";
import {
  inferDrizzleDialect,
  isPromiseLike,
  isRuntimeBindingResolver,
  isStrategyAvailableOnDrizzleDb,
  resolveDrizzleDbMaybeSync,
} from "./backend/runtime-checks";
import { deriveEntityColumnsFromTable } from "./backend/table-columns";
import { impossibleCondition, runDrizzleScan } from "./backend/query-helpers";
import {
  resolveDrizzleEntityConfigs,
  resolveDrizzleRelCompileStrategy,
  type DrizzleRelCompiledPlan,
  type DrizzleRelCompileStrategy,
} from "./planning/rel-strategy";
import type {
  CreateDrizzleProviderOptions,
  DrizzleProviderEntities,
  DrizzleProviderTableConfig,
  DrizzleQueryExecutor,
} from "./types";

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
  const entityConfigs = resolveDrizzleEntityConfigs(tableConfigs);
  const dialect = options.dialect ?? inferDrizzleDialect(options.db, tableConfigs);
  void dialect;

  return createRelationalProviderAdapter<TContext, TTables, DrizzleRelCompileStrategy>({
    name: providerName,
    entities: options.tables,
    unsupportedRelCompileMessage: "Unsupported relational fragment for drizzle provider.",
    unsupportedRelReasonMessage: "Rel fragment is not supported for single-query drizzle pushdown.",
    resolveEntityColumns({ config }) {
      return deriveEntityColumnsFromTable(config.table);
    },
    resolveRelCompileStrategy({ rel }) {
      return resolveDrizzleRelCompileStrategy(rel, entityConfigs);
    },
    isRelStrategySupported({ context, strategy }) {
      if (strategy == null) {
        return "Rel fragment is not supported for single-query drizzle pushdown.";
      }
      const evaluateWithDb = (db: DrizzleQueryExecutor): true | string =>
        isStrategyAvailableOnDrizzleDb(strategy, db)
          ? true
          : `Drizzle database instance does not support required APIs for "${strategy}" rel pushdown.`;

      if (!isRuntimeBindingResolver(options.db)) {
        return evaluateWithDb(options.db);
      }

      const db = resolveDrizzleDbMaybeSync(options, context);
      return isPromiseLike(db) ? db.then(evaluateWithDb) : evaluateWithDb(db);
    },
    buildRelPlanPayload({ rel, strategy }) {
      return {
        strategy,
        rel,
      } satisfies DrizzleRelCompiledPlan;
    },
    async executeCompiledPlan({ plan, context }) {
      return AdapterResult.tryPromise({
        try: () => executeCompiledPlan(plan, options, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
    async lookupMany({ request, context }) {
      return AdapterResult.tryPromise({
        try: () => executeLookupMany(options, request, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
  }) as FragmentProviderAdapter<TContext> &
    LookupManyCapableProviderAdapter<TContext> & {
      entities: DrizzleProviderEntities<TTables>;
    };
}
