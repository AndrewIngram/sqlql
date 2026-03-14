import { Result, type Result as BetterResult } from "better-result";

import { type RelNode, type TuplError } from "@tupl/foundation";
import {
  buildLogicalQueryPlanResult,
  buildPhysicalQueryPlanResult,
  buildProviderFragmentForRelResult,
} from "@tupl/planner";
import { normalizeCapability } from "@tupl/provider-kit";
import {
  resolveSchemaLinkedEnums,
  validateProviderBindings,
  type QueryRow,
} from "@tupl/schema-model";

import type { ExplainFragment, ExplainProviderPlan, ExplainResult, QueryInput } from "./contracts";
import { tryQueryStepAsync, unwrapQueryResult } from "./diagnostics";
import { executeRelWithProvidersResult } from "./executor";
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
 * Query runner owns SQL-to-execution orchestration and explain/query entrypoints for the runtime.
 */
export function normalizeRuntimeSchemaResult<TContext>(
  input: QueryInput<TContext>,
): BetterResult<QueryInput<TContext>, TuplError> {
  return Result.gen(function* () {
    const schema = yield* resolveSchemaLinkedEnums(input.schema);
    const normalizedInput = {
      ...input,
      schema,
    };
    yield* validateProviderBindings(normalizedInput.schema, normalizedInput.providers);
    return Result.ok(normalizedInput);
  });
}

export async function queryInternalResult<TContext>(
  input: QueryInput<TContext>,
): Promise<BetterResult<QueryRow[], TuplError>> {
  return Result.gen(async function* () {
    const resolvedInput = yield* normalizeRuntimeSchemaResult(input);
    const guardrails = resolveGuardrails(input.queryGuardrails);
    const logicalPlan = yield* buildLogicalQueryPlanResult(
      resolvedInput.sql,
      resolvedInput.schema,
      resolvedInput.context,
    );

    yield* enforcePlannerNodeLimitResult(logicalPlan.plannerNodeCount, guardrails);
    const capabilityResolution = yield* Result.await(
      resolveProviderCapabilityForRel(resolvedInput, logicalPlan.rewrittenRel),
    );
    yield* maybeRejectFallbackResult(resolvedInput, capabilityResolution);

    const rows = yield* Result.await(
      withTimeoutResult(
        "execute relational query",
        () =>
          executeRelWithProvidersResult(
            logicalPlan.rewrittenRel,
            resolvedInput.schema,
            resolvedInput.providers,
            resolvedInput.context,
            {
              maxExecutionRows: guardrails.maxExecutionRows,
              maxLookupKeysPerBatch: guardrails.maxLookupKeysPerBatch,
              maxLookupBatches: guardrails.maxLookupBatches,
            },
            resolvedInput.constraintValidation
              ? { constraintValidation: resolvedInput.constraintValidation }
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

async function compileExplainProviderPlansResult<TContext>(
  input: QueryInput<TContext>,
  fragments: ExplainFragment[],
): Promise<BetterResult<ExplainProviderPlan[], TuplError>> {
  return tryQueryStepAsync("compile explain provider plans", async () => {
    const providerPlans: ExplainProviderPlan[] = [];

    for (const fragment of fragments) {
      if (!fragment.provider) {
        continue;
      }

      const adapter = input.providers[fragment.provider];
      if (!adapter) {
        continue;
      }

      const providerFragment = unwrapQueryResult(
        buildProviderFragmentForRelResult(fragment.rel, input.schema, input.context),
      );
      if (!providerFragment) {
        continue;
      }

      const capability = normalizeCapability(
        await Promise.resolve(adapter.canExecute(providerFragment.rel, input.context)),
      );
      if (!capability.supported) {
        providerPlans.push({
          fragmentId: fragment.id,
          provider: fragment.provider,
          kind: "unsupported_fragment",
          rel: fragment.rel,
          descriptionUnavailable: true as const,
        });
        continue;
      }

      const compiledPlan = unwrapQueryResult(
        await adapter.compile(providerFragment.rel, input.context),
      );
      const description =
        typeof adapter.describeCompiledPlan === "function"
          ? await adapter.describeCompiledPlan(compiledPlan, input.context)
          : undefined;

      providerPlans.push({
        fragmentId: fragment.id,
        provider: fragment.provider,
        kind: compiledPlan.kind,
        rel: fragment.rel,
        ...(description ? { description } : { descriptionUnavailable: true as const }),
      });
    }

    return providerPlans;
  });
}

export async function explainInternal<TContext>(
  input: QueryInput<TContext>,
): Promise<ExplainResult> {
  return unwrapQueryResult(await explainInternalResult(input));
}

export async function explainInternalResult<TContext>(
  input: QueryInput<TContext>,
): Promise<BetterResult<ExplainResult, TuplError>> {
  return Result.gen(async function* () {
    const resolvedInput = yield* normalizeRuntimeSchemaResult(input);
    const guardrails = resolveGuardrails(input.queryGuardrails);
    const plannedQuery = yield* Result.await(
      buildPhysicalQueryPlanResult(
        resolvedInput.sql,
        resolvedInput.schema,
        resolvedInput.providers,
        resolvedInput.context,
      ),
    );
    yield* enforcePlannerNodeLimitResult(plannedQuery.plannerNodeCount, guardrails);
    const capabilityResolution = yield* resolveSyncProviderCapabilityForRelResult(
      resolvedInput,
      plannedQuery.rewrittenRel,
    );
    const fragments = collectExplainFragments(plannedQuery.physicalPlan.rel);
    const providerPlans = yield* Result.await(
      compileExplainProviderPlansResult(resolvedInput, fragments),
    );

    return Result.ok({
      sql: normalizeExplainSql(resolvedInput.sql),
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
