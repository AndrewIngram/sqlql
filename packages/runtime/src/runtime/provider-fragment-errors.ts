import type { QueryStepState } from "./contracts";
import { setFailedStepState } from "./session-state";

/**
 * Provider fragment errors own failure-state transitions for remote fragment sessions.
 */
export function failProviderFragmentState(
  state: QueryStepState,
  error: import("@tupl/foundation").TuplError,
  endedAt: number,
): QueryStepState {
  return setFailedStepState(state, error, endedAt);
}
