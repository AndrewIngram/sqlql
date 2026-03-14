import {
  AdapterResult,
  createRelationalProviderAdapter,
  type FragmentProviderAdapter,
} from "@tupl/provider-kit";
import type { LookupManyCapableProviderAdapter } from "@tupl/provider-kit/shapes";

import { executeCompiledPlan } from "./execution/plan-execution";
import { executeLookupMany } from "./execution/lookup-execution";
import {
  resolveObjectionRelCompileStrategy,
  type ObjectionRelCompiledPlan,
  type ObjectionRelCompileStrategy,
} from "./planning/rel-strategy";
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
): FragmentProviderAdapter<TContext> & {
  lookupMany: LookupManyCapableProviderAdapter<TContext>["lookupMany"];
  entities: ObjectionProviderEntities<TEntities>;
} {
  const providerName = options.name ?? "objection";
  const entityConfigs = resolveEntityConfigs(options);
  const entityOptions = (options.entities ?? {}) as TEntities;

  return createRelationalProviderAdapter<TContext, TEntities, ObjectionRelCompileStrategy>({
    name: providerName,
    entities: entityOptions,
    unsupportedRelCompileMessage: "Unsupported relational fragment for Objection provider.",
    unsupportedRelReasonMessage:
      "Rel fragment is not supported for single-query Objection pushdown.",
    resolveRelCompileStrategy({ rel }) {
      return resolveObjectionRelCompileStrategy(rel, entityConfigs);
    },
    buildRelPlanPayload({ rel, strategy }) {
      return {
        strategy,
        rel,
      } satisfies ObjectionRelCompiledPlan;
    },
    async executeCompiledPlan({ plan, context }) {
      const knex = await resolveKnex(options, context);
      return AdapterResult.tryPromise({
        try: () => executeCompiledPlan(knex, entityConfigs, plan, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
    async lookupMany({ request, context }) {
      const knex = await resolveKnex(options, context);
      return AdapterResult.tryPromise({
        try: () => executeLookupMany(knex, entityConfigs, request, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    },
  }) as FragmentProviderAdapter<TContext> &
    LookupManyCapableProviderAdapter<TContext> & {
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
