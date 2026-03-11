import { Result, type Result as BetterResult } from "better-result";

import { TuplDiagnosticError } from "@tupl/foundation";

import type { QueryInput } from "./contracts";
import type { QueryCapabilityResolution } from "./provider-capability";
import { makeDiagnostic, summarizeCapabilityReason } from "./diagnostics";
import { resolveFallbackPolicy } from "./policy";

/**
 * Provider fallback owns policy-based rejection of unsupported provider fragments.
 */
export function maybeRejectFallbackResult<TContext>(
  input: QueryInput<TContext>,
  resolution: QueryCapabilityResolution<TContext>,
): BetterResult<QueryCapabilityResolution<TContext>, TuplDiagnosticError> {
  if (!resolution.provider || !resolution.report || resolution.report.supported) {
    return Result.ok(resolution);
  }

  const policy = resolveFallbackPolicy(input.fallbackPolicy, resolution.provider.fallbackPolicy);
  const exceedsEstimatedCost =
    policy.rejectOnEstimatedCost &&
    resolution.report.estimatedCost != null &&
    Number.isFinite(policy.maxJoinExpansionRisk) &&
    resolution.report.estimatedCost > policy.maxJoinExpansionRisk;

  if (!policy.allowFallback || policy.rejectOnMissingAtom || exceedsEstimatedCost) {
    const diagnostics =
      resolution.diagnostics.length > 0
        ? resolution.diagnostics
        : [
            makeDiagnostic(
              "TUPL_ERR_FALLBACK",
              "error",
              summarizeCapabilityReason(resolution.report),
              {
                provider: resolution.provider.name,
                fragment: resolution.fragment?.kind,
                missingAtoms: resolution.report.missingAtoms,
              },
              "42000",
            ),
          ];

    return Result.err(
      new TuplDiagnosticError({
        message: summarizeCapabilityReason(resolution.report),
        diagnostics,
      }),
    );
  }

  return Result.ok(resolution);
}
