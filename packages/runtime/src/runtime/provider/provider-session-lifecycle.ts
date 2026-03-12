import { Result, type Result as BetterResult } from "better-result";

import { countRelNodes, type RelNode, type TuplError } from "@tupl/foundation";
import type { ProviderFragment } from "@tupl/provider-kit";
import { expandRelViewsResult, lowerSqlToRelResult } from "@tupl/planner";
import { createProviderFragmentSession } from "./provider-fragment-session";

import type { QueryGuardrails, TuplDiagnostic } from "../contracts";
import type {
  QueryExecutionPlan,
  QuerySession,
  QuerySessionInput,
  QueryStepState,
} from "../session/contracts";
import {
  maybeRejectFallbackResult,
  resolveSyncProviderCapabilityForRel,
  resolveSyncProviderCapabilityForRelResult,
} from "./provider-execution";
import { enforcePlannerNodeLimitResult, resolveGuardrails } from "../policy";
import {
  assertNoSqlNodesWithoutProviderFragmentResult,
  normalizeRuntimeSchemaResult,
} from "../query-runner";

/**
 * Provider session lifecycle owns the stable one-step plan and initial step state for provider-fragment sessions.
 */
export interface PreparedSession<TContext> {
  resolvedInput: QuerySessionInput<TContext>;
  guardrails: QueryGuardrails;
  providerSession: QuerySession | null;
  executableRel: RelNode;
  diagnostics: TuplDiagnostic[];
}

export function createProviderFragmentPlan(
  providerName: string,
  fragment: ProviderFragment,
  diagnostics: TuplDiagnostic[],
): QueryExecutionPlan {
  return {
    steps: [
      {
        id: "remote_fragment_1",
        kind: "remote_fragment",
        dependsOn: [],
        summary: `Execute provider fragment (${providerName})`,
        phase: "fetch",
        operation: {
          name: "provider_fragment",
          details: {
            provider: providerName,
          },
        },
        request: {
          fragment: fragment.kind,
        },
        ...(diagnostics.length > 0 ? { diagnostics } : {}),
      },
    ],
    scopes: [
      {
        id: "scope_root",
        kind: "root",
        label: "Root query",
      },
    ],
    ...(diagnostics.length > 0 ? { diagnostics } : {}),
  };
}

export function createInitialProviderFragmentState(
  providerName: string,
  diagnostics: TuplDiagnostic[],
): QueryStepState {
  return {
    id: "remote_fragment_1",
    kind: "remote_fragment",
    status: "ready",
    summary: `Execute provider fragment (${providerName})`,
    dependsOn: [],
    ...(diagnostics.length > 0 ? { diagnostics } : {}),
  };
}

export function resolveSessionPreparationResult<TContext>(
  input: QuerySessionInput<TContext>,
): BetterResult<PreparedSession<TContext>, TuplError> {
  const resolvedInputResult = normalizeRuntimeSchemaResult(input);
  if (Result.isError(resolvedInputResult)) {
    return resolvedInputResult;
  }

  const resolvedInput = resolvedInputResult.value;
  const guardrails = resolveGuardrails(input.queryGuardrails);
  const loweredResult = lowerSqlToRelResult(resolvedInput.sql, resolvedInput.schema);
  if (Result.isError(loweredResult)) {
    return loweredResult;
  }

  const plannerNodeCount = countRelNodes(loweredResult.value.rel);
  const plannerNodeCountResult = enforcePlannerNodeLimitResult(plannerNodeCount, guardrails);
  if (Result.isError(plannerNodeCountResult)) {
    return plannerNodeCountResult;
  }

  const expandedRelResult = expandRelViewsResult(
    loweredResult.value.rel,
    resolvedInput.schema,
    resolvedInput.context,
  );
  if (Result.isError(expandedRelResult)) {
    return expandedRelResult;
  }

  const expandedRel = expandedRelResult.value;
  const providerSessionResult = tryCreateSyncProviderFragmentSession(
    resolvedInput,
    guardrails,
    expandedRel,
  );
  if (Result.isError(providerSessionResult)) {
    return providerSessionResult;
  }

  const capabilityResolutionResult = resolveSyncProviderCapabilityForRelResult(
    resolvedInput,
    expandedRel,
  );
  if (Result.isError(capabilityResolutionResult)) {
    return capabilityResolutionResult;
  }

  const executableRelResult = assertNoSqlNodesWithoutProviderFragmentResult(expandedRel);
  if (Result.isError(executableRelResult)) {
    return executableRelResult;
  }

  return Result.ok<PreparedSession<TContext>>({
    resolvedInput,
    guardrails,
    providerSession: providerSessionResult.value,
    executableRel: executableRelResult.value,
    diagnostics: capabilityResolutionResult.value?.diagnostics ?? [],
  });
}

function tryCreateSyncProviderFragmentSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  rel: RelNode,
): BetterResult<QuerySession | null, TuplError> {
  const resolutionResult = resolveSyncProviderCapabilityForRel(input, rel);
  if (Result.isError(resolutionResult)) {
    return resolutionResult;
  }

  const resolution = resolutionResult.value;
  if (!resolution || !resolution.fragment || !resolution.provider || !resolution.report) {
    return Result.ok<QuerySession | null, TuplError>(null);
  }

  if (!resolution.report.supported) {
    const fallbackResult = maybeRejectFallbackResult(input, resolution);
    if (Result.isError(fallbackResult)) {
      return fallbackResult;
    }
    return Result.ok<QuerySession | null, TuplError>(null);
  }

  return Result.ok<QuerySession | null, TuplError>(
    createProviderFragmentSession(
      input,
      guardrails,
      resolution.provider,
      resolution.fragment.provider,
      resolution.fragment,
      rel,
      resolution.diagnostics,
    ),
  );
}
