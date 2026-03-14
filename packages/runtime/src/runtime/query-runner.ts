import { Result, type Result as BetterResult } from "better-result";

import {
  countRelNodes,
  type TuplError,
  type RelNode,
} from "@tupl/foundation";
import {
  buildProviderFragmentForRelResult,
  expandRelViewsResult,
  lowerSqlToRelResult,
  planPhysicalQueryResult,
} from "@tupl/planner";
import type { FragmentProviderAdapter } from "@tupl/provider-kit";
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
    const lowered = yield* lowerSqlToRelResult(resolvedInput.sql, resolvedInput.schema);
    const plannerNodeCount = countRelNodes(lowered.rel);

    yield* enforcePlannerNodeLimitResult(plannerNodeCount, guardrails);
    const expandedRel = yield* expandRelViewsResult(
      lowered.rel,
      resolvedInput.schema,
      resolvedInput.context,
    );
    const rows = yield* Result.await(
      withTimeoutResult(
        "execute relational query",
        () =>
          executeRelWithProvidersResult(
            expandedRel,
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

function normalizeExplainSql(sql: string): string {
  return sql.replace(/;+$/u, "").replace(/\s+/gu, " ").trim();
}

function getExplainFragmentChildren(node: RelNode): RelNode[] {
  switch (node.kind) {
    case "values":
      return [];
    case "scan":
      return [];
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return [node.input];
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
      if (
        !adapter ||
        typeof (adapter as FragmentProviderAdapter<TContext>).compile !== "function"
      ) {
        continue;
      }

      const providerFragment = unwrapQueryResult(
        buildProviderFragmentForRelResult(fragment.rel, input.schema, input.context),
      );
      if (!providerFragment) {
        continue;
      }

      const compiledPlan = unwrapQueryResult(
        await (adapter as FragmentProviderAdapter<TContext>).compile(
          providerFragment,
          input.context,
        ),
      );
      const description =
        typeof (adapter as FragmentProviderAdapter<TContext>).describeCompiledPlan === "function"
          ? await (adapter as FragmentProviderAdapter<TContext>).describeCompiledPlan!(
              compiledPlan,
              input.context,
            )
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
    const lowered = yield* lowerSqlToRelResult(resolvedInput.sql, resolvedInput.schema);
    const rewrittenRel = yield* expandRelViewsResult(
      lowered.rel,
      resolvedInput.schema,
      resolvedInput.context,
    );
    const plannerNodeCount = countRelNodes(rewrittenRel);

    yield* enforcePlannerNodeLimitResult(plannerNodeCount, guardrails);
    const capabilityResolution = yield* resolveSyncProviderCapabilityForRelResult(
      resolvedInput,
      rewrittenRel,
    );
    const physicalPlan = yield* Result.await(
      planPhysicalQueryResult(
        lowered.rel,
        resolvedInput.schema,
        resolvedInput.providers,
        resolvedInput.context,
        resolvedInput.sql,
      ),
    );
    const fragments = collectExplainFragments(physicalPlan.rel);
    const providerPlans = yield* Result.await(
      compileExplainProviderPlansResult(resolvedInput, fragments),
    );

    return Result.ok({
      sql: normalizeExplainSql(resolvedInput.sql),
      initialRel: lowered.rel,
      rewrittenRel,
      physicalPlan,
      fragments,
      providerPlans,
      plannerNodeCount,
      diagnostics:
        capabilityResolution?.diagnostics.map((diagnostic) => ({
          stage: "physical_planning" as const,
          diagnostic,
        })) ?? [],
    });
  });
}
