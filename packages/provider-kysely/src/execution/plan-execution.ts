import {
  AdapterResult,
  type ProviderCompiledPlan,
  type ProviderFragment,
} from "@tupl/provider-kit";

import type { KyselyDatabaseLike, ResolvedEntityConfig } from "../types";
import type { KyselyRelCompiledPlan } from "../planning/rel-strategy";
import { buildKyselyRelBuilderForStrategy } from "../planning/rel-builder";
import { executeScan } from "./scan-execution";

export async function executeCompiledPlan<TContext>(
  db: KyselyDatabaseLike,
  entityConfigs: Record<string, ResolvedEntityConfig<TContext>>,
  plan: ProviderCompiledPlan,
  context: TContext,
) {
  switch (plan.kind) {
    case "scan": {
      const fragment = plan.payload as Extract<ProviderFragment, { kind: "scan" }>;
      return AdapterResult.tryPromise({
        try: () => executeScan(db, entityConfigs, fragment.request, context),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    }
    case "rel": {
      const compiled = plan.payload as KyselyRelCompiledPlan;
      return AdapterResult.tryPromise({
        try: async () => {
          const query = await buildKyselyRelBuilderForStrategy(
            db,
            entityConfigs,
            compiled.rel,
            compiled.strategy,
            context,
          );
          return query.execute();
        },
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      });
    }
    default:
      return AdapterResult.err(new Error(`Unsupported Kysely compiled plan kind: ${plan.kind}`));
  }
}
