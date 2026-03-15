import { Result, type Result as BetterResult } from "better-result";

import { type RelNode, type TuplError } from "@tupl/foundation";
import { buildLogicalQueryPlanResult, buildPhysicalQueryPlanResult } from "@tupl/planner";
import type { QueryRow } from "@tupl/schema-model";

import type { ExplainFragment, ExplainResult, QueryInput } from "./contracts";
import { unwrapQueryResult } from "./diagnostics";
import { executeRelWithProvidersResult } from "./executor";
import {
  describeExplainProviderPlansResult,
  type ExplainProviderDescriptionMode,
} from "./explain/provider-plan-descriptions";
import {
  enforceExecutionRowLimitResult,
  enforcePlannerNodeLimitResult,
  resolveGuardrails,
} from "./policy";
import {
  maybeRejectFallbackResult,
  resolveProviderCapabilityForRel,
  resolveSyncProviderCapabilityForRelResult,
  withTimeoutResult,
} from "./provider/provider-execution";

/**
 * Query runner owns the honest runtime boundary: SQL enters here together with a prepared runtime
 * schema artifact whose schema finalization, linked-enum resolution, and provider validation have
 * already happened. This module is responsible for planning, fallback policy, and execution/explain
 * orchestration, not for mutating schema state on demand.
 */
export async function queryInternalResult<TContext>(
  input: QueryInput<TContext>,
): Promise<BetterResult<QueryRow[], TuplError>> {
  return Result.gen(async function* () {
    const guardrails = resolveGuardrails(input.queryGuardrails);
    const logicalPlan = yield* buildLogicalQueryPlanResult(
      input.sql,
      input.preparedSchema.schema,
      input.context,
    );

    yield* enforcePlannerNodeLimitResult(logicalPlan.plannerNodeCount, guardrails);
    const capabilityResolution = yield* Result.await(
      resolveProviderCapabilityForRel(input, logicalPlan.rewrittenRel),
    );
    yield* maybeRejectFallbackResult(input, capabilityResolution);

    const rows = yield* Result.await(
      withTimeoutResult(
        "execute relational query",
        () =>
          executeRelWithProvidersResult(
            logicalPlan.rewrittenRel,
            input.preparedSchema.schema,
            input.preparedSchema.providers,
            input.context,
            {
              maxExecutionRows: guardrails.maxExecutionRows,
              maxLookupKeysPerBatch: guardrails.maxLookupKeysPerBatch,
              maxLookupBatches: guardrails.maxLookupBatches,
            },
            input.constraintValidation
              ? { constraintValidation: input.constraintValidation }
              : undefined,
          ).then(unwrapQueryResult),
        guardrails.timeoutMs,
      ),
    );

    return enforceExecutionRowLimitResult(rows, guardrails);
  });
}

export async function queryInternal<TContext>(input: QueryInput<TContext>): Promise<QueryRow[]> {
  return unwrapQueryResult(await queryInternalResult(input));
}

function normalizeExplainSql(sql: string): string {
  return sql.replace(/;+$/u, "").replace(/\s+/gu, " ").trim();
}

function getExplainFragmentChildren(node: RelNode): RelNode[] {
  switch (node.kind) {
    case "values":
      return [];
    case "scan":
    case "cte_ref":
      return [];
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return [node.input];
    case "correlate":
      return [node.left, node.right];
    case "join":
    case "set_op":
      return [node.left, node.right];
    case "with":
      return [...node.ctes.map((cte) => cte.query), node.body];
    case "repeat_union":
      return [node.seed, node.iterative];
  }
}

function collectExplainFragments(rel: RelNode): ExplainFragment[] {
  const fragments: ExplainFragment[] = [];
  let nextFragmentId = 1;

  const visit = (node: RelNode, parentConvention?: RelNode["convention"]) => {
    const isBoundary = parentConvention === undefined || parentConvention !== node.convention;
    if (isBoundary) {
      const provider = node.convention.startsWith("provider:")
        ? node.convention.slice("provider:".length)
        : undefined;
      fragments.push({
        id: `fragment_${nextFragmentId}`,
        convention: node.convention,
        ...(provider ? { provider } : {}),
        rel: node,
      });
      nextFragmentId += 1;
    }

    for (const child of getExplainFragmentChildren(node)) {
      visit(child, node.convention);
    }
  };

  visit(rel);
  return fragments;
}

export async function explainInternal<TContext>(
  input: QueryInput<TContext>,
): Promise<ExplainResult> {
  return unwrapQueryResult(await explainInternalResult(input));
}

export async function explainInternalResult<TContext>(
  input: QueryInput<TContext>,
  options: {
    providerDescriptionMode?: ExplainProviderDescriptionMode;
  } = {},
): Promise<BetterResult<ExplainResult, TuplError>> {
  return Result.gen(async function* () {
    const guardrails = resolveGuardrails(input.queryGuardrails);
    const plannedQuery = yield* Result.await(
      buildPhysicalQueryPlanResult(
        input.sql,
        input.preparedSchema.schema,
        input.preparedSchema.providers,
        input.context,
      ),
    );
    yield* enforcePlannerNodeLimitResult(plannedQuery.plannerNodeCount, guardrails);
    const capabilityResolution = yield* resolveSyncProviderCapabilityForRelResult(
      input,
      plannedQuery.rewrittenRel,
    );
    const fragments = collectExplainFragments(plannedQuery.physicalPlan.rel);
    const providerPlans = yield* Result.await(
      describeExplainProviderPlansResult(
        input,
        fragments,
        options.providerDescriptionMode ?? "enriched",
      ),
    );

    return Result.ok({
      sql: normalizeExplainSql(input.sql),
      initialRel: plannedQuery.initialRel,
      rewrittenRel: plannedQuery.rewrittenRel,
      physicalPlan: plannedQuery.physicalPlan,
      fragments,
      providerPlans,
      plannerNodeCount: plannedQuery.plannerNodeCount,
      diagnostics:
        capabilityResolution?.diagnostics.map((diagnostic) => ({
          stage: "physical_planning" as const,
          diagnostic,
        })) ?? [],
    });
  });
}
