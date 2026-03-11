import { Result, type Result as BetterResult } from "better-result";

import type { RelNode, TuplError } from "@tupl/foundation";
import {
  normalizeCapability,
  type ProviderAdapter,
  type ProviderCapabilityReport,
  type ProviderFragment,
} from "@tupl/provider-kit";
import { buildProviderFragmentForRelResult } from "@tupl/planner";

import type { QueryInput, TuplDiagnostic } from "../contracts";
import { buildCapabilityDiagnostics, tryQueryStep, tryQueryStepAsync } from "../diagnostics";
import { isPromiseLike } from "../policy";

/**
 * Provider capability owns fragment construction and provider capability resolution.
 */
export interface QueryCapabilityResolution<TContext> {
  fragment: ProviderFragment | null;
  provider: ProviderAdapter<TContext> | null;
  report: ProviderCapabilityReport | null;
  diagnostics: TuplDiagnostic[];
}

export async function resolveProviderCapabilityForRel<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): Promise<BetterResult<QueryCapabilityResolution<TContext>, TuplError>> {
  const fragmentResult = buildProviderFragmentForRelResult(rel, input.schema, input.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok({
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return Result.ok({
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const capabilityResult = await tryQueryStepAsync("resolve provider capability", () =>
    Promise.resolve(provider.canExecute(fragment, input.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const report = normalizeCapability(capabilityResult.value);
  return Result.ok({
    fragment,
    provider,
    report,
    diagnostics: buildCapabilityDiagnostics(provider, fragment, report, input.fallbackPolicy),
  });
}

export function resolveSyncProviderCapabilityForRel<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): BetterResult<QueryCapabilityResolution<TContext> | null, TuplError> {
  const fragmentResult = buildProviderFragmentForRelResult(rel, input.schema, input.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok({
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return Result.ok({
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const capabilityResult = tryQueryStep("resolve provider capability", () =>
    provider.canExecute(fragment, input.context),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = capabilityResult.value;
  if (isPromiseLike(capability)) {
    return Result.ok(null);
  }

  const report = normalizeCapability(capability);
  return Result.ok({
    fragment,
    provider,
    report,
    diagnostics: buildCapabilityDiagnostics(provider, fragment, report, input.fallbackPolicy),
  });
}
