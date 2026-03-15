import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";
import type { ProvidersMap } from "@tupl/provider-kit";
import type { SchemaDefinition } from "@tupl/schema-model";
import { toPhysicalPlanningError } from "./planner-errors";
import { assignConventions } from "./provider/conventions";
import { analyzeProviderSupportResult } from "./provider/provider-support-analysis";
import { createPhysicalPlanningState } from "./physical/physical-plan-state";
import { planPhysicalNodeResult } from "./physical/local-step-planning";

/**
 * Physical planning owns convention assignment and the step graph built from executable rewritten
 * relational trees. Callers must pass a logical tree that has already completed rewrite stages.
 */
export async function planPhysicalQueryResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
) {
  return Result.gen(async function* () {
    try {
      const plannedRel = assignConventions(rel, schema);
      const providerSupport = yield* Result.await(
        analyzeProviderSupportResult(plannedRel, schema, providers, context),
      );
      const state = createPhysicalPlanningState(providerSupport);
      const rootStepId = yield* Result.await(
        planPhysicalNodeResult(plannedRel, schema, providers, context, state),
      );

      return Result.ok({
        rel: plannedRel,
        rootStepId,
        steps: state.steps,
      });
    } catch (error) {
      return Result.err(toPhysicalPlanningError(error, "plan physical query"));
    }
  });
}
