import { Result, type Result as BetterResult } from "better-result";

import type { RelNode, TuplError } from "@tupl/foundation";

import type { QueryInput } from "../contracts";
import type { QueryCapabilityResolution } from "./provider-capability";
import { maybeRejectFallbackResult } from "./provider-fallback";
import { resolveSyncProviderCapabilityForRel } from "./provider-capability";

/**
 * Provider execution is the curated runtime surface for provider capability, fallback, and timeout handling.
 */
export type { QueryCapabilityResolution } from "./provider-capability";
export {
  resolveProviderCapabilityForRel,
  resolveSyncProviderCapabilityForRel,
} from "./provider-capability";
export { maybeRejectFallbackResult } from "./provider-fallback";
export { withTimeoutResult } from "./provider-timeout";

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
