import type { ProviderCompiledPlan, QueryRow } from "@tupl/provider-kit";

import { buildKyselyRelBuilderForStrategy } from "../planning/rel-builder";
import type { KyselyDatabaseLike, ResolvedEntityConfig } from "../types";
import type { KyselyRelCompiledPlan } from "../planning/rel-strategy";

export async function executeCompiledPlan<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  plan: ProviderCompiledPlan,
  context: TContext,
): Promise<QueryRow[]> {
  if (plan.kind !== "rel") {
    throw new Error(`Unsupported Kysely compiled plan kind: ${plan.kind}`);
  }

  const compiled = plan.payload as KyselyRelCompiledPlan;
  const builder = await buildKyselyRelBuilderForStrategy(
    db,
    entityConfigs,
    compiled.rel,
    compiled.strategy,
    context,
  );

  return builder.execute();
}
