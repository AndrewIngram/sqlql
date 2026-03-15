import { buildCapabilityReport, type ProviderCapabilityReport } from "..";

export interface ScanEntityBinding {
  entity: string;
}

export function buildScanUnsupportedReport(reason: string): ProviderCapabilityReport {
  return buildCapabilityReport(reason);
}
