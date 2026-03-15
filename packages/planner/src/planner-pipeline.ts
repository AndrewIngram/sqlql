import { Result } from "better-result";

import { countRelNodes, type RelNode } from "@tupl/foundation";
import type { PhysicalPlan } from "./physical/physical";
import type { ProvidersMap } from "@tupl/provider-kit";
import type { SchemaDefinition } from "@tupl/schema-model";
import { lowerSqlToRelResult, type RelLoweringResult } from "./sql-lowering";
import { toRelRewriteError } from "./planner-errors";
import { planPhysicalQueryResult } from "./physical-planning";
import { decorrelateRel } from "./subqueries/decorrelation";
import { expandRelViewsResult } from "./view-expansion";

export interface LogicalQueryPlan {
  initialRel: RelNode;
  rewrittenRel: RelNode;
  plannerNodeCount: number;
  tables: string[];
}

export interface PhysicalQueryPlan extends LogicalQueryPlan {
  physicalPlan: PhysicalPlan;
}

/**
 * Planner pipeline owns the canonical staged flow from SQL input to logical and physical plans.
 * Callers depend on this module instead of manually stitching together parser, rewrite, and
 * physical-planning stages.
 */
export function rewriteLogicalRelResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
) {
  return Result.gen(function* () {
    const decorrelated = yield* Result.try({
      try: () => decorrelateRel(rel),
      catch: (error) => toRelRewriteError(error, "decorrelate logical rel"),
    });

    return expandRelViewsResult(decorrelated, schema, context);
  });
}

export function buildLogicalQueryPlanResult<TContext>(
  sql: string,
  schema: SchemaDefinition,
  context?: TContext,
) {
  return Result.gen(function* () {
    const lowered: RelLoweringResult = yield* lowerSqlToRelResult(sql, schema);
    const rewrittenRel = yield* rewriteLogicalRelResult(lowered.rel, schema, context);

    return Result.ok({
      initialRel: lowered.rel,
      rewrittenRel,
      plannerNodeCount: countRelNodes(rewrittenRel),
      tables: lowered.tables,
    });
  });
}

export async function buildPhysicalQueryPlanResult<TContext>(
  sql: string,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
) {
  return Result.gen(async function* () {
    const logicalPlan = yield* buildLogicalQueryPlanResult(sql, schema, context);
    const physicalPlan = yield* Result.await(
      planPhysicalQueryResult(logicalPlan.rewrittenRel, schema, providers, context),
    );

    return Result.ok({
      ...logicalPlan,
      physicalPlan,
    });
  });
}
