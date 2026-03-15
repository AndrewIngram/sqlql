import type { RelNode, TuplDiagnostic } from "@tupl/foundation";

export type { TuplDiagnostic } from "@tupl/foundation";

export type ProviderRouteFamily = "scan" | "lookup" | "aggregate" | "rel-core" | "rel-advanced";

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
  routeFamily?: ProviderRouteFamily;
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
  routeFamily?: ProviderRouteFamily;
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

export function inferRouteFamilyForRel(rel: RelNode): ProviderRouteFamily {
  if (rel.kind === "scan") {
    return "scan";
  }
  if (rel.kind === "aggregate") {
    return "aggregate";
  }
  return hasAdvancedRelFeatures(rel) ? "rel-advanced" : "rel-core";
}

export function buildCapabilityReport(
  rel: RelNode,
  reason: string,
  options: BuildCapabilityReportOptions = {},
): ProviderCapabilityReport {
  return {
    supported: false,
    reason,
    routeFamily: options.routeFamily ?? inferRouteFamilyForRel(rel),
    ...(options.notes ? { notes: options.notes } : {}),
    ...(options.diagnostics ? { diagnostics: options.diagnostics } : {}),
    ...(options.estimatedRows != null ? { estimatedRows: options.estimatedRows } : {}),
    ...(options.estimatedCost != null ? { estimatedCost: options.estimatedCost } : {}),
    ...(options.fallbackAllowed != null ? { fallbackAllowed: options.fallbackAllowed } : {}),
  };
}

function hasAdvancedRelFeatures(node: RelNode): boolean {
  switch (node.kind) {
    case "window":
    case "correlate":
    case "with":
    case "repeat_union":
    case "set_op":
      return true;
    case "values":
    case "cte_ref":
    case "scan":
      return false;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return hasAdvancedRelFeatures(node.input);
    case "join":
      return hasAdvancedRelFeatures(node.left) || hasAdvancedRelFeatures(node.right);
  }
}
