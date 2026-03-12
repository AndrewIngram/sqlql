import {
  AdapterResult,
  createRelationalProviderAdapter,
  type FragmentProviderAdapter,
  type LookupProviderAdapter,
} from "@tupl/provider-kit";
import { hasSqlNode } from "@tupl/provider-kit/shapes";

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
): FragmentProviderAdapter<TContext> &
  LookupProviderAdapter<TContext> & {
    entities: DrizzleProviderEntities<TTables>;
  } {
  const declaredAtoms = [
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
  ] as const;

  const providerName = options.name ?? "drizzle";
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext>>;
  const dialect = options.dialect ?? inferDrizzleDialect(options.db, tableConfigs);
  void dialect;

  return createRelationalProviderAdapter<TContext, TTables, DrizzleRelCompileStrategy>({
    name: providerName,
    declaredAtoms,
    entities: options.tables,
    unsupportedRelCompileMessage: "Unsupported relational fragment for drizzle provider.",
    resolveEntityColumns({ config }) {
      return deriveEntityColumnsFromTable(config.table);
    },
    unsupportedRelReason({ fragment }) {
      return hasSqlNode(fragment.rel)
        ? "rel fragment must not contain sql nodes."
        : "Rel fragment is not supported for single-query drizzle pushdown.";
    },
    resolveRelCompileStrategy({ fragment }) {
      return resolveDrizzleRelCompileStrategy(fragment.rel, tableConfigs);
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
    async compileRelFragment({ fragment, strategy }) {
      return AdapterResult.ok({
        provider: providerName,
        kind: "rel",
        payload: {
          strategy,
          rel: fragment.rel,
        } satisfies DrizzleRelCompiledPlan,
      });
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
    LookupProviderAdapter<TContext> & {
      entities: DrizzleProviderEntities<TTables>;
    };
}
