let physicalStepCounter = 0;
let relIdCounter = 0;
let syntheticColumnCounter = 0;

export function nextPhysicalStepId(prefix: string): string {
  physicalStepCounter += 1;
  return `${prefix}_${physicalStepCounter}`;
}

export function nextRelId(prefix: string): string {
  relIdCounter += 1;
  return `${prefix}_${relIdCounter}`;
}

export function nextSyntheticColumnName(prefix: string): string {
  syntheticColumnCounter += 1;
  return `__${prefix}_${syntheticColumnCounter}`;
}
