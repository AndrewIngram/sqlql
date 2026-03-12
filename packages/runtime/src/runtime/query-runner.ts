import { Result, type Result as BetterResult } from "better-result";

import {
  countRelNodes,
  relContainsSqlNode,
  TuplRuntimeError,
  type RelNode,
  type TuplError,
} from "@tupl/foundation";
import { expandRelViewsResult, lowerSqlToRelResult } from "@tupl/planner";
import {
  resolveSchemaLinkedEnums,
  validateProviderBindingsResult,
  type QueryRow,
} from "@tupl/schema-model";

import type { ExplainResult, QueryInput } from "./contracts";
import { unwrapQueryResult, tryQueryStep } from "./diagnostics";
import { executeRelWithProvidersResult } from "./executor";
import {
  enforceExecutionRowLimitResult,
  enforcePlannerNodeLimitResult,
  resolveGuardrails,
} from "./policy";
import {
  maybeExecuteWholeQueryFragmentResult,
  resolveSyncProviderCapabilityForRelResult,
  withTimeoutResult,
} from "./provider/provider-execution";

/**
 * Query runner owns SQL-to-execution orchestration and explain/query entrypoints for the runtime.
 */
function normalizeRuntimeSchema<TContext>(input: QueryInput<TContext>): QueryInput<TContext> {
  const schema = resolveSchemaLinkedEnums(input.schema);
  return {
    ...input,
    schema,
  };
}

export function normalizeRuntimeSchemaResult<TContext>(
  input: QueryInput<TContext>,
): BetterResult<QueryInput<TContext>, TuplError> {
  return Result.gen(function* () {
    const normalizedInput = yield* tryQueryStep("normalize runtime schema", () =>
      normalizeRuntimeSchema(input),
    );
    yield* validateProviderBindingsResult(normalizedInput.schema, normalizedInput.providers);
    return Result.ok(normalizedInput);
  });
}

export function assertNoSqlNodesWithoutProviderFragmentResult(
  rel: RelNode,
): BetterResult<RelNode, TuplRuntimeError> {
  if (relContainsSqlNode(rel)) {
    return Result.err(
      new TuplRuntimeError({
        operation: "validate provider fragment execution shape",
        message:
          "Query lowered to a SQL-shaped relational node that cannot be executed by the provider runtime without provider rel pushdown.",
      }),
    );
  }

  return Result.ok(rel);
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
    // Prefer a single whole-query provider fragment before local execution. This keeps fully
    // pushdownable queries on the provider path and only falls back once that route is unavailable.
    const remoteRows = yield* Result.await(
      withTimeoutResult(
        "execute whole provider fragment",
        () =>
          maybeExecuteWholeQueryFragmentResult(resolvedInput, expandedRel).then(unwrapQueryResult),
        guardrails.timeoutMs,
      ),
    );

    if (remoteRows) {
      return enforceExecutionRowLimitResult(remoteRows, guardrails);
    }

    // Any remaining SQL-shaped node would require provider execution semantics the local runtime
    // cannot reproduce, so fail fast before entering relational evaluation.
    const executableRel = yield* assertNoSqlNodesWithoutProviderFragmentResult(expandedRel);
    const rows = yield* Result.await(
      withTimeoutResult(
        "execute relational query",
        () =>
          executeRelWithProvidersResult(
            executableRel,
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

export function explainInternal<TContext>(input: QueryInput<TContext>): ExplainResult {
  return unwrapQueryResult(explainInternalResult(input));
}

export function explainInternalResult<TContext>(
  input: QueryInput<TContext>,
): BetterResult<ExplainResult, TuplError> {
  return Result.gen(function* () {
    const resolvedInput = yield* normalizeRuntimeSchemaResult(input);
    const guardrails = resolveGuardrails(input.queryGuardrails);
    const lowered = yield* lowerSqlToRelResult(resolvedInput.sql, resolvedInput.schema);
    const capabilityResolution = yield* resolveSyncProviderCapabilityForRelResult(
      resolvedInput,
      lowered.rel,
    );

    return Result.ok({
      rel: lowered.rel,
      plannerNodeCount: countRelNodes(lowered.rel),
      guardrails,
      ...(capabilityResolution?.diagnostics.length
        ? { diagnostics: capabilityResolution.diagnostics }
        : {}),
    });
  });
}
