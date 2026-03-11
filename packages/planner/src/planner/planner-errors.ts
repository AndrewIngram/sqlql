import { TuplPlanningError } from "@tupl/foundation";

export function toTuplPlanningError(error: unknown, operation: string) {
  return new TuplPlanningError({
    operation,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });
}
