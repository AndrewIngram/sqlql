import { Result, type Result as BetterResult } from "better-result";

import type { RelNode, TuplError } from "@tupl/foundation";

import type { QueryInput } from "./contracts";
import type { QueryCapabilityResolution } from "./provider/provider-capability";
import { maybeRejectFallbackResult } from "./provider/provider-fallback";
import { resolveSyncProviderCapabilityForRel } from "./provider/provider-capability";

/**
 * Provider execution is the curated runtime surface for provider capability, fallback, timeout, and whole-query execution.
 */
export type { QueryCapabilityResolution } from "./provider/provider-capability";
export {
  resolveProviderCapabilityForRel,
  resolveSyncProviderCapabilityForRel,
} from "./provider/provider-capability";
export { maybeRejectFallbackResult } from "./provider/provider-fallback";
export { withTimeoutResult } from "./provider/provider-timeout";
export { maybeExecuteWholeQueryFragmentResult } from "./provider/provider-whole-query";

export function resolveSyncProviderCapabilityForRelResult<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): BetterResult<QueryCapabilityResolution<TContext> | null, TuplError> {
  return Result.gen(function* () {
    const resolution = yield* resolveSyncProviderCapabilityForRel(input, rel);
    if (resolution) {
      yield* maybeRejectFallbackResult(input, resolution);
    }
    return Result.ok(resolution);
  });
}
