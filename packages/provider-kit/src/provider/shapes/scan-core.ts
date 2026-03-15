import { buildCapabilityReport, type ProviderCapabilityReport } from "..";
import type { RelNode } from "@tupl/foundation";

export interface ScanEntityBinding {
  entity: string;
}

export function buildScanUnsupportedReport(rel: RelNode, reason: string): ProviderCapabilityReport {
  return buildCapabilityReport(rel, reason, { routeFamily: "scan" });
}
