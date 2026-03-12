import type { ProviderCompiledPlan, ProviderFragment, QueryRow } from "@tupl/provider-kit";

import { resolveDrizzleDb } from "../backend/runtime-checks";
import type { CreateDrizzleProviderOptions } from "../types";
import type { DrizzleRelCompiledPlan } from "../planning/rel-strategy";
import { executeDrizzleRelSingleQuery } from "../planning/rel-builder";
import { executeScan } from "./scan-execution";

export async function executeCompiledPlan<TContext>(
  plan: ProviderCompiledPlan,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  const db = await resolveDrizzleDb(options, context);

  switch (plan.kind) {
    case "rel": {
      const compiled = plan.payload as DrizzleRelCompiledPlan;
      return executeDrizzleRelSingleQuery(compiled.rel, compiled.strategy, options, context, db);
    }
    case "scan": {
      const fragment = plan.payload as Extract<ProviderFragment, { kind: "scan" }>;
      return executeScan(db, options, fragment.request, context);
    }
    default:
      throw new Error(`Unsupported drizzle compiled plan kind: ${plan.kind}`);
  }
}
