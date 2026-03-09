import {
  collectCapabilityAtomsForFragment,
  inferRouteFamilyForFragment,
  type ProviderCapabilityReport,
  type ProviderFragment,
} from "..";

export interface ScanEntityBinding {
  entity: string;
}

export function buildScanUnsupportedReport(
  fragment: ProviderFragment,
  supportedAtoms: readonly string[],
  reason: string,
): ProviderCapabilityReport {
  const requiredAtoms = collectCapabilityAtomsForFragment(fragment);
  return {
    supported: false,
    reason,
    routeFamily: inferRouteFamilyForFragment(fragment),
    requiredAtoms,
    missingAtoms: requiredAtoms.filter((atom) => !supportedAtoms.includes(atom)),
  };
}
