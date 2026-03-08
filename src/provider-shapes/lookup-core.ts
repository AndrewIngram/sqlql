import { Result } from "better-result";

import {
  collectCapabilityAtomsForFragment,
  inferRouteFamilyForFragment,
  type ProviderFragment,
  type ProviderLookupManyRequest,
  type ProviderOperationResult,
  type ProviderCapabilityReport,
} from "../provider";
import type { QueryRow, ScanFilterClause } from "../schema";

export interface LookupEntityBinding<TColumns extends string = string> {
  lookupKey: TColumns;
  columns: readonly TColumns[];
}

export function buildLookupOnlyUnsupportedReport(
  fragment: ProviderFragment,
  reason: string,
  supportedAtoms: readonly string[] = ["lookup.bulk"],
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

export function validateLookupRequest<TColumns extends string>(
  request: ProviderLookupManyRequest,
  entity: LookupEntityBinding<TColumns>,
): ProviderOperationResult<void> {
  if (request.key !== entity.lookupKey) {
    return Result.err(
      new Error(
        `Unsupported lookup key column for ${request.table}: ${request.key}. Expected ${entity.lookupKey}.`,
      ),
    );
  }

  const allowedColumns = new Set(entity.columns);
  for (const column of request.select) {
    if (!allowedColumns.has(column as TColumns)) {
      return Result.err(
        new Error(`Unsupported lookup select column for ${request.table}: ${column}`),
      );
    }
  }

  return Result.ok(undefined);
}

export function filterLookupRows(
  rows: QueryRow[],
  clauses?: ScanFilterClause[],
): QueryRow[] {
  return (clauses ?? []).reduce(
    (current, clause) => current.filter((row) => matchesLookupClause(row, clause)),
    rows,
  );
}

export function projectLookupRow(row: QueryRow, select: string[]): QueryRow {
  const out: QueryRow = {};
  for (const column of select) {
    out[column] = row[column] ?? null;
  }
  return out;
}

export function matchesLookupClause(row: QueryRow, clause: ScanFilterClause): boolean {
  const value = row[clause.column];

  switch (clause.op) {
    case "eq":
      return value != null && clause.value != null && value === clause.value;
    case "neq":
      return value != null && clause.value != null && value !== clause.value;
    case "gt":
      return typeof value === "number" && value > Number(clause.value);
    case "gte":
      return typeof value === "number" && value >= Number(clause.value);
    case "lt":
      return typeof value === "number" && value < Number(clause.value);
    case "lte":
      return typeof value === "number" && value <= Number(clause.value);
    case "in":
      return value != null && clause.values.filter((entry) => entry != null).includes(value);
    case "not_in":
      return value != null && !clause.values.filter((entry) => entry != null).includes(value);
    case "like":
      return (
        typeof value === "string" &&
        typeof clause.value === "string" &&
        matchesLikePattern(value, clause.value)
      );
    case "not_like":
      return (
        typeof value === "string" &&
        typeof clause.value === "string" &&
        !matchesLikePattern(value, clause.value)
      );
    case "is_distinct_from":
      return value !== clause.value;
    case "is_not_distinct_from":
      return value === clause.value;
    case "is_null":
      return value == null;
    case "is_not_null":
      return value != null;
  }
}

export function matchesLikePattern(value: string, pattern: string): boolean {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    .replace(/%/g, ".*")
    .replace(/_/g, ".");
  return new RegExp(`^${escaped}$`, "su").test(value);
}
