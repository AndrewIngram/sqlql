import type { QueryStepState } from "../contracts";
import type { TuplError } from "@tupl/foundation";

/**
 * Session state owns shared transition helpers for failed runtime steps.
 */
export function setFailedStepState(
  state: QueryStepState,
  error: TuplError,
  endedAt: number,
): QueryStepState {
  return {
    ...state,
    status: "failed",
    endedAt,
    durationMs: endedAt - (state.startedAt ?? endedAt),
    error: error.message,
  };
}
