import { Result } from "better-result";
import { TuplGuardrailError } from "@tupl/foundation";
import type { QueryRow } from "@tupl/schema-model";

import {
  DEFAULT_QUERY_FALLBACK_POLICY,
  DEFAULT_QUERY_GUARDRAILS,
  type QueryFallbackPolicy,
  type QueryGuardrails,
} from "./contracts";

/**
 * Runtime policy helpers own guardrail normalization and cheap policy decisions.
 */
export function resolveGuardrails(overrides?: Partial<QueryGuardrails>): QueryGuardrails {
  return {
    maxPlannerNodes: overrides?.maxPlannerNodes ?? DEFAULT_QUERY_GUARDRAILS.maxPlannerNodes,
    maxExecutionRows: overrides?.maxExecutionRows ?? DEFAULT_QUERY_GUARDRAILS.maxExecutionRows,
    maxLookupKeysPerBatch:
      overrides?.maxLookupKeysPerBatch ?? DEFAULT_QUERY_GUARDRAILS.maxLookupKeysPerBatch,
    maxLookupBatches: overrides?.maxLookupBatches ?? DEFAULT_QUERY_GUARDRAILS.maxLookupBatches,
    timeoutMs: overrides?.timeoutMs ?? DEFAULT_QUERY_GUARDRAILS.timeoutMs,
  };
}

export function resolveFallbackPolicy(
  queryPolicy?: QueryFallbackPolicy,
  providerPolicy?: QueryFallbackPolicy,
): Required<QueryFallbackPolicy> {
  return {
    ...DEFAULT_QUERY_FALLBACK_POLICY,
    ...providerPolicy,
    ...queryPolicy,
  };
}

export function enforceExecutionRowLimitResult(rows: QueryRow[], guardrails: QueryGuardrails) {
  if (rows.length > guardrails.maxExecutionRows) {
    return Result.err(
      new TuplGuardrailError({
        guardrail: "maxExecutionRows",
        limit: guardrails.maxExecutionRows,
        actual: rows.length,
        message: `Query exceeded maxExecutionRows guardrail (${guardrails.maxExecutionRows}). Received ${rows.length} rows.`,
      }),
    );
  }

  return Result.ok(rows);
}

export function enforcePlannerNodeLimitResult(
  plannerNodeCount: number,
  guardrails: QueryGuardrails,
) {
  if (plannerNodeCount > guardrails.maxPlannerNodes) {
    return Result.err(
      new TuplGuardrailError({
        guardrail: "maxPlannerNodes",
        limit: guardrails.maxPlannerNodes,
        actual: plannerNodeCount,
        message: `Query exceeded maxPlannerNodes guardrail (${guardrails.maxPlannerNodes}). Planned ${plannerNodeCount} nodes.`,
      }),
    );
  }

  return Result.ok(plannerNodeCount);
}

export function isPromiseLike<T>(value: unknown): value is PromiseLike<T> {
  return !!value && typeof value === "object" && "then" in value;
}
