import type { TuplDiagnostic } from "@tupl/foundation";

export type { TuplDiagnostic } from "@tupl/foundation";

export interface QueryFallbackPolicy {
  allowFallback?: boolean;
  warnOnFallback?: boolean;
  rejectOnEstimatedCost?: boolean;
  maxLocalRows?: number;
  maxLookupFanout?: number;
  maxJoinExpansionRisk?: number;
}

export interface ProviderCapabilityReport {
  supported: boolean;
  reason?: string;
  notes?: string[];
  diagnostics?: TuplDiagnostic[];
  estimatedRows?: number;
  estimatedCost?: number;
  fallbackAllowed?: boolean;
}

export interface ProviderEstimate {
  rows: number;
  cost: number;
}

export interface BuildCapabilityReportOptions {
  notes?: string[];
  diagnostics?: TuplDiagnostic[];
  estimatedRows?: number;
  estimatedCost?: number;
  fallbackAllowed?: boolean;
}

export function normalizeCapability(
  capability: boolean | ProviderCapabilityReport,
): ProviderCapabilityReport {
  if (typeof capability === "boolean") {
    return capability ? { supported: true } : { supported: false };
  }

  return capability;
}

export function buildCapabilityReport(
  reason: string,
  options: BuildCapabilityReportOptions = {},
): ProviderCapabilityReport {
  return {
    supported: false,
    reason,
    ...(options.notes ? { notes: options.notes } : {}),
    ...(options.diagnostics ? { diagnostics: options.diagnostics } : {}),
    ...(options.estimatedRows != null ? { estimatedRows: options.estimatedRows } : {}),
    ...(options.estimatedCost != null ? { estimatedCost: options.estimatedCost } : {}),
    ...(options.fallbackAllowed != null ? { fallbackAllowed: options.fallbackAllowed } : {}),
  };
}
