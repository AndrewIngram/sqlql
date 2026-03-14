import type { ProviderCompiledPlan, QueryRow } from "@tupl/provider-kit";

import { executeQuery } from "../backend/query-helpers";
import { buildObjectionRelBuilderForStrategy } from "../planning/rel-builder";
import type { ObjectionRelCompiledPlan } from "../planning/rel-strategy";
import type { KnexLike, ResolvedEntityConfig } from "../types";

export async function executeCompiledPlan<TContext>(
  knex: KnexLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  plan: ProviderCompiledPlan,
  context: TContext,
): Promise<QueryRow[]> {
  if (plan.kind !== "rel") {
    throw new Error(`Unsupported Objection compiled plan kind: ${plan.kind}`);
  }

  const compiled = plan.payload as ObjectionRelCompiledPlan;
  const query = await buildObjectionRelBuilderForStrategy(
    knex,
    entityConfigs,
    compiled.rel,
    compiled.strategy,
    context,
  );

  return executeQuery(query);
}
