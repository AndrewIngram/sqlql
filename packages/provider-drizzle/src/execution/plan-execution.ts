import type { ProviderCompiledPlan, QueryRow } from "@tupl/provider-kit";

import { executeDrizzleRelSingleQuery } from "../planning/rel-builder";
import type { DrizzleRelCompiledPlan } from "../planning/rel-strategy";
import type { CreateDrizzleProviderOptions } from "../types";
import { resolveDrizzleDb } from "../backend/runtime-checks";

export async function executeCompiledPlan<TContext>(
  plan: ProviderCompiledPlan,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  if (plan.kind !== "rel") {
    throw new Error(`Unsupported Drizzle compiled plan kind: ${plan.kind}`);
  }

  const compiled = plan.payload as DrizzleRelCompiledPlan;
  const db = await resolveDrizzleDb(options, context);
  return executeDrizzleRelSingleQuery(compiled.rel, compiled.strategy, options, context, db);
}
