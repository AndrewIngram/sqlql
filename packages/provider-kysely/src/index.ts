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
  resolveKyselyRelCompileStrategy,
  type KyselyRelCompiledPlan,
  type KyselyRelCompileStrategy,
} from "./planning/rel-strategy";
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

  const providerName = options.name ?? "kysely";
  const entityConfigs = resolveEntityConfigs(options);
  const entityOptions = (options.entities ?? {}) as TEntities;

  return createRelationalProviderAdapter<TContext, TEntities, KyselyRelCompileStrategy>({
    name: providerName,
    declaredAtoms,
    entities: entityOptions,
    unsupportedRelCompileMessage: "Unsupported relational fragment for Kysely provider.",
    unsupportedRelReason({ fragment }) {
      return hasSqlNode(fragment.rel)
        ? "rel fragment must not contain sql nodes."
        : "Rel fragment is not supported for single-query Kysely pushdown.";
    },
    resolveRelCompileStrategy({ fragment }) {
      return resolveKyselyRelCompileStrategy(fragment.rel, entityConfigs);
    },
    async compileRelFragment({ fragment, strategy }) {
      return AdapterResult.ok({
        provider: providerName,
        kind: "rel",
        payload: {
          strategy,
          rel: fragment.rel,
        } satisfies KyselyRelCompiledPlan,
      });
    },
    async executeCompiledPlan({ plan, context }) {
      const db = await resolveKyselyDb(options, context);
      return executeCompiledPlan(db, entityConfigs, plan, context);
    },
    async lookupMany({ request, context }) {
      const db = await resolveKyselyDb(options, context);
      return AdapterResult.tryPromise({
        try: () => executeLookupMany(db, entityConfigs, request, context),
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
