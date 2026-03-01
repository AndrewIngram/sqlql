import type { QueryCompatibilityMap } from "./types";

export const CUSTOM_QUERY_ID = "__custom_query__";

export function selectionAfterManualSqlEdit(): string {
  return CUSTOM_QUERY_ID;
}

export function selectionAfterSchemaChange(
  currentSelection: string,
  compatibilityById: QueryCompatibilityMap,
): string {
  if (currentSelection === CUSTOM_QUERY_ID) {
    return currentSelection;
  }

  const compatibility = compatibilityById[currentSelection];
  if (!compatibility || !compatibility.compatible) {
    return CUSTOM_QUERY_ID;
  }

  return currentSelection;
}

export function canSelectCatalogQuery(
  queryId: string,
  compatibilityById: QueryCompatibilityMap,
): boolean {
  return compatibilityById[queryId]?.compatible === true;
}

