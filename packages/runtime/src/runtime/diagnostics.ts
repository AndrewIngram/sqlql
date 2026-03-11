import { Result, type Result as BetterResult } from "better-result";
import {
  TuplDiagnosticError,
  TuplExecutionError,
  TuplGuardrailError,
  TuplRuntimeError,
  TuplTimeoutError,
  type TuplError,
} from "@tupl/foundation";
import type {
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderFragment,
  QueryFallbackPolicy,
  TuplDiagnostic,
} from "@tupl/provider-kit";

import { resolveFallbackPolicy } from "./policy";

/**
 * Runtime diagnostics own error normalization and fallback diagnostics shared by query execution.
 */
export function makeDiagnostic(
  code: string,
  severity: TuplDiagnostic["severity"],
  message: string,
  details?: Record<string, unknown>,
  diagnosticClass: TuplDiagnostic["class"] = "0A000",
): TuplDiagnostic {
  return {
    code,
    class: diagnosticClass,
    severity,
    message,
    ...(details ? { details } : {}),
  };
}

export function toTuplRuntimeError(error: unknown, operation: string) {
  if (
    TuplDiagnosticError.is(error) ||
    TuplExecutionError.is(error) ||
    TuplGuardrailError.is(error) ||
    TuplTimeoutError.is(error) ||
    TuplRuntimeError.is(error)
  ) {
    return error;
  }

  if (error instanceof Error) {
    return new TuplRuntimeError({
      operation,
      message: error.message,
      cause: error,
    });
  }

  return new TuplRuntimeError({
    operation,
    message: String(error),
    cause: error,
  });
}

export function unwrapQueryResult<T, E>(result: BetterResult<T, E>): T {
  if (Result.isOk(result)) {
    return result.value;
  }

  throw result.error;
}

export function tryQueryStep<T>(operation: string, fn: () => T): BetterResult<T, TuplError> {
  return Result.try({
    try: () => fn() as Awaited<T>,
    catch: (error) => toTuplRuntimeError(error, operation),
  }) as BetterResult<T, TuplError>;
}

export async function tryQueryStepAsync<T>(operation: string, fn: () => Promise<T>) {
  return Result.tryPromise({
    try: fn,
    catch: (error) => toTuplRuntimeError(error, operation),
  });
}

export function summarizeCapabilityReason(report: ProviderCapabilityReport | null): string {
  if (!report) {
    return "Provider pushdown is not available for this query shape.";
  }
  if (report.reason && report.reason.length > 0) {
    return report.reason;
  }
  if (report.missingAtoms && report.missingAtoms.length > 0) {
    return `Missing provider capability atoms: ${report.missingAtoms.join(", ")}.`;
  }
  return "Provider pushdown is not available for this query shape.";
}

export function buildCapabilityDiagnostics<TContext>(
  provider: ProviderAdapter<TContext> | null,
  fragment: ProviderFragment | null,
  report: ProviderCapabilityReport | null,
  queryPolicy?: QueryFallbackPolicy,
): TuplDiagnostic[] {
  const diagnostics = [...(report?.diagnostics ?? [])];
  if (!provider || !fragment || !report || report.supported) {
    return diagnostics;
  }

  const policy = resolveFallbackPolicy(queryPolicy, provider.fallbackPolicy);
  const details: Record<string, unknown> = {
    provider: provider.name,
    fragment: fragment.kind,
  };
  if (report.routeFamily) {
    details.routeFamily = report.routeFamily;
  }
  if (report.requiredAtoms?.length) {
    details.requiredAtoms = report.requiredAtoms;
  }
  if (report.missingAtoms?.length) {
    details.missingAtoms = report.missingAtoms;
  }
  if (report.estimatedRows != null) {
    details.estimatedRows = report.estimatedRows;
  }
  if (report.estimatedCost != null) {
    details.estimatedCost = report.estimatedCost;
  }

  diagnostics.push(
    makeDiagnostic(
      policy.allowFallback ? "TUPL_WARN_FALLBACK" : "TUPL_ERR_FALLBACK",
      policy.allowFallback ? "warning" : "error",
      summarizeCapabilityReason(report),
      details,
      policy.allowFallback ? "0A000" : "42000",
    ),
  );

  return diagnostics;
}
