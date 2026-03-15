import {
  PhysicalPlanningError,
  ProviderFragmentBuildError,
  RelLoweringError,
  RelRewriteError,
  UnsupportedQueryShapeError,
} from "@tupl/foundation";

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

export function toUnsupportedQueryShapeError(message: string, cause?: unknown) {
  return new UnsupportedQueryShapeError({
    operation: "validate query shape",
    message,
    ...(cause !== undefined ? { cause } : {}),
  });
}

export function toRelLoweringError(error: unknown, operation: string) {
  if (RelLoweringError.is(error)) {
    return error;
  }

  return new RelLoweringError({
    operation,
    message: errorMessage(error),
    cause: error,
  });
}

export function toRelRewriteError(error: unknown, operation: string) {
  if (RelRewriteError.is(error)) {
    return error;
  }

  return new RelRewriteError({
    operation,
    message: errorMessage(error),
    cause: error,
  });
}

export function toPhysicalPlanningError(error: unknown, operation: string) {
  if (PhysicalPlanningError.is(error)) {
    return error;
  }

  return new PhysicalPlanningError({
    operation,
    message: errorMessage(error),
    cause: error,
  });
}

export function toProviderFragmentBuildError(error: unknown, operation: string) {
  if (ProviderFragmentBuildError.is(error)) {
    return error;
  }

  return new ProviderFragmentBuildError({
    operation,
    message: errorMessage(error),
    cause: error,
  });
}
